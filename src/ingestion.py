"""Bronze layer: raw XML feed -> Delta table `pdv_raw`.

One row per station per source file, faithful to the XML: nested arrays kept,
every value a string (typing is Silver's job). Four traceability columns added.
Append-only, partitioned by source_type / snapshot_date. A source file already
present in the table is never ingested twice.

Rule learned the hard way: never project a subset of columns on the DataFrame
returned by the XML reader (spark-xml drops nested elements when the optimizer
prunes the schema). Read everything, write everything, analyze from Delta.
"""
import argparse
import logging
from datetime import date
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from src.download import snapshot_date_from_filename
from src.utils import get_spark, load_config, setup_logging

log = logging.getLogger(__name__)


def _s(name: str) -> StructField:
    """A nullable string field. spark-xml maps an XML attribute `x` to a field `_x`."""
    return StructField(name, StringType(), True)


# The XML contract. If the feed changes shape, reading with this schema is where we find out.
PDV_SCHEMA = StructType([
    _s("_id"), _s("_cp"), _s("_latitude"), _s("_longitude"), _s("_pop"),
    _s("adresse"), _s("ville"),
    StructField("horaires", StructType([
        _s("_automate-24-24"),
        StructField("jour", ArrayType(StructType([
            _s("_id"), _s("_nom"), _s("_ferme"),
            StructField("horaire", ArrayType(StructType([_s("_ouverture"), _s("_fermeture")])), True),
        ])), True),
    ]), True),
    StructField("services", StructType([StructField("service", ArrayType(StringType()), True)]), True),
    StructField("prix", ArrayType(StructType([_s("_id"), _s("_nom"), _s("_maj"), _s("_valeur")])), True),
    StructField("rupture", ArrayType(StructType([_s("_id"), _s("_nom"), _s("_debut"), _s("_fin"), _s("_type")])), True),
    StructField("fermeture", ArrayType(StructType([_s("_type"), _s("_debut"), _s("_fin")])), True),
])

# Top-level names only; nested field names stay as in the XML (Silver renames them).
RENAMES = {"_id": "station_id", "_cp": "postal_code", "_latitude": "latitude",
           "_longitude": "longitude", "_pop": "road_type", "adresse": "address", "ville": "city"}


def read_feed(spark: SparkSession, xml_path: str | Path, encoding: str) -> DataFrame:
    """Read one feed file with the explicit schema. FAILFAST: a malformed file stops the run."""
    df = (
        spark.read.format("xml")
        .schema(PDV_SCHEMA)
        .option("rowTag", "pdv")
        .option("charset", encoding)
        .option("mode", "FAILFAST")
        .load(str(xml_path))
    )
    for old, new in RENAMES.items():
        df = df.withColumnRenamed(old, new)
    return df


def add_pipeline_columns(df: DataFrame, source_type: str, source_file: str, snapshot_date: date) -> DataFrame:
    """Traceability: where each row comes from and when we took it. Pure function, reused by the stream."""
    return (
        df.withColumn("source_type", F.lit(source_type))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("ingested_at", F.current_timestamp())
    )


def bronze_path(cfg: dict) -> str:
    return f'{cfg["paths"]["bronze"]}/{cfg["tables"]["bronze_pdv"]}'


def already_ingested(spark: SparkSession, path: str, source_file: str) -> bool:
    if not DeltaTable.isDeltaTable(spark, path):
        return False
    return spark.read.format("delta").load(path).filter(F.col("source_file") == source_file).limit(1).count() > 0


def write_bronze(df: DataFrame, path: str) -> None:
    """Append, partitioned. Every column written: no projection between XML read and Delta write."""
    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("source_type", "snapshot_date")
        .save(path)
    )


def ingest_file(spark: SparkSession, cfg: dict, xml_path: str | Path, source_type: str) -> int:
    """Raw XML file -> Bronze. Returns the number of rows written (0 if skipped)."""
    xml_path = Path(xml_path)
    path = bronze_path(cfg)
    if already_ingested(spark, path, xml_path.name):
        log.info("Bronze already has %s, skipping", xml_path.name)
        return 0

    snapshot = snapshot_date_from_filename(xml_path.name)
    log.info("Ingesting %s (source_type=%s, snapshot_date=%s)", xml_path.name, source_type, snapshot)
    df = read_feed(spark, xml_path, cfg["source"]["encoding"])
    df = add_pipeline_columns(df, source_type, xml_path.name, snapshot)
    write_bronze(df, path)

    written = spark.read.format("delta").load(path).filter(F.col("source_file") == xml_path.name).count()
    log.info("Bronze: %d rows written to %s", written, path)
    return written


def latest_raw_file(cfg: dict, source_type: str) -> Path:
    files = sorted(Path(cfg["paths"]["raw"]).joinpath(source_type).glob("*.xml"))
    if not files:
        raise FileNotFoundError(f"No raw file for source_type={source_type}; run src.download first")
    return files[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest a raw XML feed into Bronze")
    parser.add_argument("--source", choices=["daily", "instant", "yearly"], default="daily")
    parser.add_argument("--file", help="XML path; defaults to the latest file in raw/<source>/")
    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["logging"]["level"])
    spark = get_spark(cfg)
    try:
        xml_path = Path(args.file) if args.file else latest_raw_file(cfg, args.source)
        ingest_file(spark, cfg, xml_path, args.source)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
