"""Shared helpers: configuration, logging and the Delta-enabled SparkSession.

Every layer imports from here. Nothing in this module knows about fuel prices:
it only knows how to read the config and how to start Spark.
"""
import logging
import os
from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# The repo root, computed from this file's location, so paths in the config
# resolve the same way whatever the current working directory is.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "config.yaml"

log = logging.getLogger(__name__)


def load_config(path: str | Path | None = None) -> dict:
    """Read the YAML config. Precedence: explicit argument, FUEL_CONFIG env var, default file.

    The env var is how Airflow or a cloud job will point the same code at another config.
    """
    config_path = Path(path or os.environ.get("FUEL_CONFIG", DEFAULT_CONFIG))
    with open(config_path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Turn relative paths into absolute ones anchored at the repo root.
    # A URI (s3://...) is left untouched: Path would mangle it.
    for layer, p in cfg["paths"].items():
        if "://" not in p:
            cfg["paths"][layer] = str(PROJECT_ROOT / p)
    return cfg


def setup_logging(level: str = "INFO") -> None:
    """One logging format for the whole pipeline. Idempotent: safe to call twice."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )


def get_spark(cfg: dict | None = None) -> SparkSession:
    """Build the SparkSession from the config, with Delta Lake enabled.

    The builder is only a description; the JVM starts at getOrCreate().
    """
    cfg = cfg or load_config()
    s = cfg["spark"]

    # Spark is a JVM: it needs Java, and the java on macOS PATH is an Apple stub.
    if "JAVA_HOME" not in os.environ and s.get("java_home"):
        os.environ["JAVA_HOME"] = s["java_home"]

    builder = (
        SparkSession.builder
        .appName(s["app_name"])
        .master(s["master"])
        # Delta: SQL syntax (MERGE, time travel) and catalog support.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Sizing for the machine we run on.
        .config("spark.driver.memory", s["driver_memory"])
        .config("spark.sql.shuffle.partitions", s["shuffle_partitions"])
    )
    # Adds the Delta JAR (and any extra Maven package) to the JVM classpath.
    spark = configure_spark_with_delta_pip(builder, extra_packages=s.get("extra_packages") or []).getOrCreate()
    # Spark's own logs are very chatty at INFO; our pipeline logs stay at the configured level.
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s started (master=%s)", spark.version, s["master"])
    return spark
