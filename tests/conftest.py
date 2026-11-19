"""Shared test fixtures: one small local SparkSession and a builder of Bronze-shaped rows."""
from datetime import date, datetime

import pytest
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from src.ingestion import PDV_SCHEMA, RENAMES
from src.utils import get_spark, load_config


@pytest.fixture(scope="session")
def spark():
    """A SparkSession for the whole test run: 2 cores, 2 shuffle partitions, Delta enabled."""
    cfg = load_config()
    cfg["spark"].update(master="local[2]", shuffle_partitions=2, app_name="fuel-lakehouse-tests", driver_memory="2g")
    session = get_spark(cfg)
    yield session
    session.stop()


# The Bronze schema: the XML contract with top-level columns renamed, plus the four traceability columns.
BRONZE_SCHEMA = StructType(
    [StructField(RENAMES.get(f.name, f.name), f.dataType, True) for f in PDV_SCHEMA.fields]
    + [
        StructField("source_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("snapshot_date", DateType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)


def pdv(station_id="1", snapshot="2026-11-13", prix=None, rupture=None, fermeture=None,
        services=None, horaires=None, **attrs) -> dict:
    """One Bronze row as a dict. Only what a test cares about needs to be passed."""
    snap = date.fromisoformat(snapshot)
    row = {
        "station_id": station_id, "postal_code": "01000", "latitude": "4620100", "longitude": "520000",
        "road_type": "R", "address": "1 RUE DU TEST", "city": "TESTVILLE",
        "horaires": horaires,
        "services": {"service": services} if services is not None else None,
        "prix": prix, "rupture": rupture, "fermeture": fermeture,
        "source_type": "daily", "source_file": f"file_{snapshot}.xml",
        "snapshot_date": snap, "ingested_at": datetime(snap.year, snap.month, snap.day, 23, 0, 0),
    }
    row.update(attrs)
    return row


def prix(fuel_id, name, maj, valeur) -> dict:
    return {"_id": fuel_id, "_nom": name, "_maj": maj, "_valeur": valeur}


def rupture(fuel_id, name, debut, fin="", type_="temporaire") -> dict:
    return {"_id": fuel_id, "_nom": name, "_debut": debut, "_fin": fin, "_type": type_}


def fermeture(type_, debut, fin="") -> dict:
    return {"_type": type_, "_debut": debut, "_fin": fin}


@pytest.fixture
def bronze(spark):
    """Factory: bronze(pdv(...), pdv(...)) -> a Bronze-shaped DataFrame."""
    def _make(*rows):
        return spark.createDataFrame(list(rows), BRONZE_SCHEMA)
    return _make
