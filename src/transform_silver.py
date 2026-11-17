"""Silver layer: Bronze `pdv_raw` -> six clean, typed, deduplicated Delta tables.

Bronze holds one nested row per station per file. Silver splits it by subject:

    price_events      one row per price update            insert-only MERGE
    shortages         one row per shortage                insert + update MERGE
    closures          one row per station closure         insert + update MERGE
    stations          one row per station, current state  insert + update MERGE
    station_services  station x service, current state    replaced, forward in time only
    station_hours     station x weekday x slot            replaced, forward in time only

The module has two halves:
  * pure transformations (DataFrame in, DataFrame out): typing, cleaning, deduplication.
    No I/O, so they are unit-testable with a tiny in-memory DataFrame.
  * writers: the MERGE logic that makes every run idempotent.

A ledger table records which Bronze files have been absorbed, so a daily run
only reads the new file instead of the whole Bronze history.
"""
import argparse
import logging

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.ingestion import bronze_path
from src.utils import get_spark, load_config, setup_logging

log = logging.getLogger(__name__)

TRACE = ["source_type", "source_file", "snapshot_date", "ingested_at"]
PRICE_KEYS = ["station_id", "fuel_id", "updated_at"]
SHORTAGE_KEYS = ["station_id", "fuel_id", "started_at"]
CLOSURE_KEYS = ["station_id", "started_at"]



def newest_first() -> list[Column]:
    """Most recent snapshot first: the ordering used whenever a batch holds several versions of one key.

    A function, not a constant: F.col() needs a running SparkSession, which does not exist at import time.
    """
    return [F.col("snapshot_date").desc(), F.col("ingested_at").desc()]


# --------------------------------------------------------------------------------------
# Pure transformations
# --------------------------------------------------------------------------------------

def latest_per_key(df: DataFrame, keys: list[str], order: list[Column]) -> DataFrame:
    """Keep one row per key: the first one in `order`.

    A window function: rows are grouped by key (partitionBy), sorted (orderBy),
    numbered, and we keep number 1. Unlike dropDuplicates, the winner is deterministic.
    """
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _blank_to_null(col: Column) -> Column:
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def _all_not_null(cols: list[str]) -> Column:
    cond = F.lit(True)
    for c in cols:
        cond = cond & F.col(c).isNotNull()
    return cond


def to_price_events(bronze: DataFrame, price_min: float, price_max: float) -> DataFrame:
    """One row per (station, fuel, update timestamp).

    * explode: one Bronze row with an array of N prices becomes N rows.
    * unit: archives before ~2022 store thousandths of a euro ("1141" = 1.141 EUR),
      recent files store euros ("2.17"). No real price per litre exceeds 50 EUR,
      no milli-euro price is below 50, so the threshold cannot be ambiguous.
    * implausible prices are flagged, not dropped: Silver keeps the fact, Gold decides.
    """
    raw = F.col("p._valeur").cast("double")
    price = F.when(raw > 50, raw / 1000).otherwise(raw).cast("decimal(6,3)")
    events = (
        bronze.select("station_id", F.explode("prix").alias("p"), *TRACE)
        .select(
            "station_id",
            F.col("p._id").cast("int").alias("fuel_id"),
            F.col("p._nom").alias("fuel_name"),
            F.col("p._maj").cast("timestamp").alias("updated_at"),
            price.alias("price"),
            *TRACE,
        )
        .filter(_all_not_null(PRICE_KEYS) & F.col("price").isNotNull())
        .withColumn("is_implausible", ~F.col("price").between(price_min, price_max))
        .withColumn("event_year", F.year("updated_at"))
    )
    return latest_per_key(events, PRICE_KEYS, newest_first())


def to_shortages(bronze: DataFrame) -> DataFrame:
    """One row per (station, fuel, start). `ended_at` is null while the shortage is ongoing.

    The feed shows an ended shortage only on the day it ends: when a batch holds both the
    open and the closed version, the closed one wins (ended_at nulls last).
    """
    s = (
        bronze.select("station_id", F.explode("rupture").alias("r"), *TRACE)
        .select(
            "station_id",
            F.col("r._id").cast("int").alias("fuel_id"),
            F.col("r._nom").alias("fuel_name"),
            F.col("r._debut").cast("timestamp").alias("started_at"),
            F.col("r._fin").cast("timestamp").alias("ended_at"),          # "" casts to null
            _blank_to_null(F.col("r._type")).alias("shortage_type"),      # absent before ~2022
            F.col("snapshot_date").alias("first_seen_date"),
            F.col("snapshot_date").alias("last_seen_date"),
            "source_type", "source_file", "ingested_at",
        )
        .filter(_all_not_null(SHORTAGE_KEYS))
    )
    s = latest_per_key(s, SHORTAGE_KEYS, [F.col("ended_at").desc_nulls_last(), F.col("last_seen_date").desc()])
    return _with_duration(s).withColumn(
        # The source uses 1970-01-01 as "start unknown".
        "has_unknown_start", F.col("started_at") < F.lit("2000-01-01").cast("timestamp")
    )


def to_closures(bronze: DataFrame) -> DataFrame:
    """One row per (station, start). T = temporary, D = definitive."""
    c = (
        bronze.select("station_id", F.explode("fermeture").alias("f"), *TRACE)
        .select(
            "station_id",
            F.col("f._debut").cast("timestamp").alias("started_at"),
            F.col("f._fin").cast("timestamp").alias("ended_at"),
            _blank_to_null(F.col("f._type")).alias("closure_type"),
            F.col("snapshot_date").alias("first_seen_date"),
            F.col("snapshot_date").alias("last_seen_date"),
            "source_type", "source_file", "ingested_at",
        )
        .filter(_all_not_null(CLOSURE_KEYS))
    )
    c = latest_per_key(c, CLOSURE_KEYS, [F.col("ended_at").desc_nulls_last(), F.col("last_seen_date").desc()])
    return _with_duration(c)


def _with_duration(df: DataFrame) -> DataFrame:
    hours = (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")) / 3600
    return df.withColumn("duration_hours", F.round(hours, 2))


def to_stations(bronze: DataFrame) -> DataFrame:
    """One row per station: its attributes in the most recent snapshot of the batch."""
    latest = latest_per_key(bronze, ["station_id"], newest_first())
    postal = F.trim(F.col("postal_code"))
    return latest.select(
        "station_id",
        postal.alias("postal_code"),
        # Overseas departments use three digits (971..976). Corsica stays "20" here;
        # 2A / 2B needs the INSEE commune code, joined from the geo reference in Gold.
        F.when(postal.startswith("97"), F.substring(postal, 1, 3)).otherwise(F.substring(postal, 1, 2)).alias("department_code"),
        (F.col("latitude").cast("double") / 100000).alias("latitude"),     # stored x 100,000 in the feed
        (F.col("longitude").cast("double") / 100000).alias("longitude"),
        F.col("road_type"),
        (F.col("road_type") == "A").alias("is_highway"),
        F.trim(F.col("address")).alias("address"),
        F.trim(F.col("city")).alias("city"),
        (F.col("horaires").getField("_automate-24-24") == "1").alias("is_24_7"),   # null when hours are unknown
        F.col("snapshot_date").alias("first_seen_date"),
        F.col("snapshot_date").alias("last_seen_date"),
        "source_type", "source_file", "ingested_at",
    ).filter(F.col("station_id").isNotNull())


def batch_stations(bronze: DataFrame) -> DataFrame:
    """(station_id, snapshot_date) of each station's most recent row in the batch."""
    return latest_per_key(bronze.select("station_id", *TRACE), ["station_id"], newest_first()).select("station_id", "snapshot_date")


def to_station_services(bronze: DataFrame) -> DataFrame:
    """One row per (station, service), from the most recent snapshot of the batch."""
    latest = latest_per_key(bronze.select("station_id", "services", *TRACE), ["station_id"], newest_first())
    return (
        latest.select("station_id", F.explode("services.service").alias("service"), "snapshot_date")
        .withColumn("service", F.trim("service"))
        .filter(F.col("service") != "")
        .dropDuplicates(["station_id", "service"])
    )


def to_station_hours(bronze: DataFrame) -> DataFrame:
    """One row per (station, weekday, opening slot). A closed day has one row with null times.

    Two levels of nesting to unfold: horaires.jour[] then jour.horaire[].
    posexplode_outer keeps the days without any slot (the plain explode would drop them)
    and gives the slot position.
    """
    latest = latest_per_key(
        bronze.filter(F.col("horaires").isNotNull()).select("station_id", "horaires", *TRACE),
        ["station_id"], newest_first(),
    )
    days = latest.select("station_id", "snapshot_date", F.explode("horaires.jour").alias("d"))
    slots = days.select(
        "station_id",
        F.col("d._id").cast("int").alias("weekday"),          # 1 = Monday
        F.col("d._nom").alias("day_name"),
        (F.col("d._ferme") == "1").alias("is_closed"),
        F.posexplode_outer("d.horaire").alias("slot_index", "h"),
        "snapshot_date",
    )
    return slots.select(
        "station_id", "weekday", "day_name", "is_closed",
        F.coalesce("slot_index", F.lit(0)).alias("slot_index"),
        F.regexp_replace(F.col("h._ouverture"), r"\.", ":").alias("opens"),     # "07.00" -> "07:00"
        F.regexp_replace(F.col("h._fermeture"), r"\.", ":").alias("closes"),
        "snapshot_date",
    ).filter(F.col("weekday").isNotNull())


# --------------------------------------------------------------------------------------
# Writers
# --------------------------------------------------------------------------------------

def silver_path(cfg: dict, table: str) -> str:
    return f'{cfg["paths"]["silver"]}/{cfg["tables"]["silver"][table]}'


def _ensure_table(spark: SparkSession, path: str, df: DataFrame, partition_by: list[str] | None = None) -> DeltaTable:
    """Create the empty Delta table on first run, so that first load and daily load share one code path: MERGE."""
    if not DeltaTable.isDeltaTable(spark, path):
        builder = DeltaTable.create(spark).location(path).addColumns(df.schema)
        if partition_by:
            builder = builder.partitionedBy(*partition_by)
        builder.execute()
    return DeltaTable.forPath(spark, path)


def _on(keys: list[str]) -> str:
    return " AND ".join(f"t.{k} = s.{k}" for k in keys)


def _version(table: DeltaTable) -> int:
    return table.history(1).select("version").first()[0]


def _metrics_since(table: DeltaTable, version_before: int) -> dict:
    """What the MERGE did, read from the Delta transaction log.

    A MERGE that changes nothing writes no commit at all: the version stays the same.
    That is the physical proof of idempotence, so we report zeros in that case.
    """
    last = table.history(1).select("version", "operationMetrics").first()
    if last["version"] == version_before:
        return {"inserted": 0, "updated": 0}
    m = last["operationMetrics"] or {}
    return {"inserted": int(m.get("numTargetRowsInserted", 0)), "updated": int(m.get("numTargetRowsUpdated", 0))}


def merge_price_events(spark: SparkSession, path: str, events: DataFrame) -> dict:
    """Insert-only MERGE: a price update that already exists is left untouched.

    `t.event_year = s.event_year` is redundant logically but lets Delta skip every
    partition (year folder) the batch does not touch.
    """
    target = _ensure_table(spark, path, events, partition_by=["event_year"])
    before = _version(target)
    (
        target.alias("t")
        .merge(events.alias("s"), _on(PRICE_KEYS) + " AND t.event_year = s.event_year")
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _metrics_since(target, before)


def merge_lifecycle(spark: SparkSession, path: str, df: DataFrame, keys: list[str], type_col: str) -> dict:
    """MERGE for facts that are born open and get closed later (shortages, closures).

    Matched rows are updated only if the batch brings something new: a more recent
    sighting, an earlier one (backfill), or the end date we did not have. Re-running
    the same file therefore updates nothing.
    """
    target = _ensure_table(spark, path, df)
    before = _version(target)
    (
        target.alias("t")
        .merge(df.alias("s"), _on(keys))
        .whenMatchedUpdate(
            condition=(
                "s.last_seen_date > t.last_seen_date OR s.first_seen_date < t.first_seen_date "
                "OR (t.ended_at IS NULL AND s.ended_at IS NOT NULL)"
            ),
            set={
                "ended_at": "coalesce(s.ended_at, t.ended_at)",
                "duration_hours": "coalesce(s.duration_hours, t.duration_hours)",
                type_col: f"coalesce(s.{type_col}, t.{type_col})",
                "first_seen_date": "least(t.first_seen_date, s.first_seen_date)",
                "last_seen_date": "greatest(t.last_seen_date, s.last_seen_date)",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _metrics_since(target, before)


def merge_stations(spark: SparkSession, path: str, stations: DataFrame) -> dict:
    """Upsert the current state of each station.

    Two matched clauses, evaluated in order:
      1. the batch is more recent than what we hold -> refresh every attribute;
      2. the batch is older (backfill) -> only push first_seen_date back.
    """
    target = _ensure_table(spark, path, stations)
    before = _version(target)
    attributes = [c for c in stations.columns if c not in ("station_id", "first_seen_date")]
    (
        target.alias("t")
        .merge(stations.alias("s"), "t.station_id = s.station_id")
        .whenMatchedUpdate(
            condition="s.last_seen_date > t.last_seen_date",
            set={**{c: f"s.{c}" for c in attributes}, "first_seen_date": "least(t.first_seen_date, s.first_seen_date)"},
        )
        .whenMatchedUpdate(
            condition="s.first_seen_date < t.first_seen_date",
            set={"first_seen_date": "s.first_seen_date"},
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    return _metrics_since(target, before)


def replace_station_state(spark: SparkSession, path: str, new_rows: DataFrame, stations_in_batch: DataFrame, snapshot_date) -> dict:
    """Refresh a 'current state' table (services, hours) from a snapshot.

    State only moves forward in time:
      * snapshot newer than the table -> for every station of the snapshot, old rows out, new rows in;
      * snapshot not newer (re-run, or backfill of an old archive) -> only stations the table
        does not know yet are added; nothing existing is overwritten.
    """
    if not DeltaTable.isDeltaTable(spark, path):
        new_rows.write.format("delta").save(path)
        return {"inserted": new_rows.count(), "updated": 0}

    current = spark.read.format("delta").load(path)
    table_date = current.agg(F.max("snapshot_date")).first()[0]

    if table_date is None or snapshot_date > table_date:
        kept = current.join(stations_in_batch.select("station_id"), "station_id", "left_anti")
        added = new_rows
    else:
        kept = current
        added = new_rows.join(current.select("station_id").distinct(), "station_id", "left_anti")

    n_added = added.count()
    if n_added == 0:
        return {"inserted": 0, "updated": 0}
    # Delta reads a consistent snapshot of the table, so overwriting the table we are reading is safe.
    kept.unionByName(added).write.format("delta").mode("overwrite").save(path)
    return {"inserted": n_added, "updated": 0}


# --------------------------------------------------------------------------------------
# Orchestration of the layer
# --------------------------------------------------------------------------------------

def pending_files(spark: SparkSession, cfg: dict, force: bool = False) -> list:
    """Bronze files Silver has not absorbed yet, oldest snapshot first."""
    files = spark.read.format("delta").load(bronze_path(cfg)).select("source_file", "source_type", "snapshot_date").distinct()
    ledger = silver_path(cfg, "processed_files")
    if not force and DeltaTable.isDeltaTable(spark, ledger):
        files = files.join(spark.read.format("delta").load(ledger).select("source_file"), "source_file", "left_anti")
    return files.orderBy("snapshot_date", "source_file").collect()


def process_file(spark: SparkSession, cfg: dict, f) -> dict:
    """Absorb one Bronze file into the six Silver tables. Returns per-table metrics."""
    bronze = spark.read.format("delta").load(bronze_path(cfg)).filter(
        # The first two predicates are partition columns: Delta opens one folder only.
        (F.col("source_type") == f.source_type) & (F.col("snapshot_date") == f.snapshot_date) & (F.col("source_file") == f.source_file)
    )
    # The same Bronze rows feed six tables. Without persist, Spark would re-read them six times.
    bronze.persist(StorageLevel.MEMORY_AND_DISK)
    q = cfg["quality"]
    try:
        in_batch = batch_stations(bronze)
        metrics = {
            "price_events": merge_price_events(spark, silver_path(cfg, "price_events"), to_price_events(bronze, q["price_min"], q["price_max"])),
            "shortages": merge_lifecycle(spark, silver_path(cfg, "shortages"), to_shortages(bronze), SHORTAGE_KEYS, "shortage_type"),
            "closures": merge_lifecycle(spark, silver_path(cfg, "closures"), to_closures(bronze), CLOSURE_KEYS, "closure_type"),
            "stations": merge_stations(spark, silver_path(cfg, "stations"), to_stations(bronze)),
            "station_services": replace_station_state(spark, silver_path(cfg, "station_services"), to_station_services(bronze), in_batch, f.snapshot_date),
            "station_hours": replace_station_state(spark, silver_path(cfg, "station_hours"), to_station_hours(bronze), in_batch, f.snapshot_date),
        }
    finally:
        bronze.unpersist()

    # Record the file only once every table has been written.
    ledger_row = spark.createDataFrame([(f.source_file, f.source_type, f.snapshot_date)], "source_file string, source_type string, snapshot_date date")
    ledger_row.withColumn("processed_at", F.current_timestamp()).write.format("delta").mode("append").save(silver_path(cfg, "processed_files"))
    return metrics


def run_silver(spark: SparkSession, cfg: dict, force: bool = False) -> dict:
    files = pending_files(spark, cfg, force)
    if not files:
        log.info("Silver is up to date: no new Bronze file")
        return {}
    results = {}
    for f in files:
        log.info("Silver <- %s (source_type=%s, snapshot_date=%s)", f.source_file, f.source_type, f.snapshot_date)
        results[f.source_file] = process_file(spark, cfg, f)
        for table, m in results[f.source_file].items():
            log.info("  %-17s inserted=%-9d updated=%d", table, m["inserted"], m["updated"])
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Silver tables from Bronze")
    parser.add_argument("--force", action="store_true", help="reprocess every Bronze file, ignoring the ledger")
    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["logging"]["level"])
    spark = get_spark(cfg)
    try:
        run_silver(spark, cfg, force=args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
