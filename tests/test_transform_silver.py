"""Tests of the Silver transformations and MERGE writers.

Each test builds a handful of Bronze rows in memory, runs one function and checks the result.
Run one function's tests with:  pytest tests/test_transform_silver.py -k <function_name>
"""
from decimal import Decimal

from pyspark.sql import functions as F

from src import transform_silver as ts
from tests.conftest import fermeture, pdv, prix, rupture


def rows(df, *cols, order=None):
    """Collect a small DataFrame as a list of tuples, sorted for stable comparison."""
    out = df.select(*cols) if cols else df
    return [tuple(r) for r in out.orderBy(*(order or out.columns)).collect()]


# ---------------------------------------------------------------------------- latest_per_key

def test_latest_per_key_keeps_the_first_row_of_each_key(spark):
    df = spark.createDataFrame([("a", 1), ("a", 3), ("a", 2), ("b", 5)], "k string, v int")
    result = ts.latest_per_key(df, ["k"], [F.col("v").desc()])
    assert rows(result) == [("a", 3), ("b", 5)]
    assert result.columns == ["k", "v"]          # the helper column must not leak


# ---------------------------------------------------------------------------- to_price_events

def test_to_price_events_explodes_and_types(bronze):
    df = bronze(pdv("1", prix=[prix("1", "Gazole", "2026-11-13T08:00:00", "2.17"),
                               prix("6", "SP98", "2026-11-13T09:30:00", "2.265")]))
    result = ts.to_price_events(df, 0.2, 5.0)
    assert result.count() == 2
    types = dict(result.dtypes)
    assert types["fuel_id"] == "int"
    assert types["updated_at"] == "timestamp"
    assert types["price"] == "decimal(6,3)"
    assert types["is_implausible"] == "boolean"
    assert rows(result, "station_id", "fuel_id", "fuel_name", "price", "event_year") == [
        ("1", 1, "Gazole", Decimal("2.170"), 2026),
        ("1", 6, "SP98", Decimal("2.265"), 2026),
    ]
    for col in ("source_type", "source_file", "snapshot_date", "ingested_at"):
        assert col in result.columns


def test_to_price_events_converts_milli_euros_and_old_timestamps(bronze):
    df = bronze(pdv("1", prix=[prix("1", "Gazole", "2007-01-04 18:43:38.841521", "999"),
                               prix("1", "Gazole", "2015-01-02T11:01:45", "1141")]))
    result = ts.to_price_events(df, 0.2, 5.0)
    assert rows(result, "price", "event_year") == [(Decimal("0.999"), 2007), (Decimal("1.141"), 2015)]


def test_to_price_events_deduplicates_on_the_key_keeping_the_newest_snapshot(bronze):
    same = prix("1", "Gazole", "2026-11-13T08:00:00", "2.17")
    df = bronze(pdv("1", snapshot="2026-11-13", prix=[same, same]),      # duplicated inside one file
                pdv("1", snapshot="2026-11-16", prix=[same]))            # and repeated in a later file
    result = ts.to_price_events(df, 0.2, 5.0)
    assert result.count() == 1
    assert rows(result, "source_file") == [("file_2026-11-16.xml",)]


def test_to_price_events_flags_implausible_prices_without_dropping_them(bronze):
    df = bronze(pdv("1", prix=[prix("1", "Gazole", "2026-11-13T08:00:00", "2.0"),
                               prix("2", "SP95", "2026-11-13T08:00:00", "0.001"),
                               prix("3", "E85", "2026-11-13T08:00:00", "9.999")]))
    result = ts.to_price_events(df, 0.2, 5.0)
    assert rows(result, "fuel_id", "is_implausible") == [(1, False), (2, True), (3, True)]


def test_to_price_events_drops_rows_without_a_usable_key(bronze):
    df = bronze(pdv("1", prix=[prix("1", "Gazole", "", "2.0"),                       # no timestamp
                               prix("", "SP95", "2026-11-13T08:00:00", "2.0"),       # no fuel id
                               prix("3", "E85", "2026-11-13T08:00:00", "")]),        # no price
                pdv("2", prix=None))                                                 # station without prices
    assert ts.to_price_events(df, 0.2, 5.0).count() == 0


# ---------------------------------------------------------------------------- to_shortages

def test_to_shortages_open_and_closed(bronze):
    df = bronze(pdv("1", rupture=[rupture("4", "GPLc", "2019-05-22T14:48:16", "", "definitive"),
                                  rupture("6", "SP98", "2026-11-13T13:12:58", "2026-11-13T16:34:07")]))
    result = ts.to_shortages(df)
    got = {r["fuel_id"]: r for r in result.collect()}
    assert got[4]["ended_at"] is None and got[4]["duration_hours"] is None
    assert got[4]["shortage_type"] == "definitive"
    assert got[6]["ended_at"] is not None and got[6]["duration_hours"] == 3.35
    assert got[6]["first_seen_date"] == got[6]["last_seen_date"]
    assert dict(result.dtypes)["started_at"] == "timestamp"


def test_to_shortages_closed_version_wins_inside_a_batch(bronze):
    df = bronze(pdv("1", snapshot="2026-11-16", rupture=[rupture("6", "SP98", "2026-11-13T13:12:58", "")]),
                pdv("1", snapshot="2026-11-13", rupture=[rupture("6", "SP98", "2026-11-13T13:12:58", "2026-11-13T16:34:07")]))
    result = ts.to_shortages(df)
    assert result.count() == 1
    assert result.first()["ended_at"] is not None


def test_to_shortages_blank_type_and_unknown_start(bronze):
    df = bronze(pdv("1", rupture=[rupture("3", "E85", "1970-01-01T00:59:59", "", ""),
                                  rupture("1", "Gazole", "2026-04-05T13:17:11", "", "temporaire"),
                                  rupture("2", "SP95", "", "", "temporaire")]))      # no start: dropped
    got = {r["fuel_id"]: r for r in ts.to_shortages(df).collect()}
    assert set(got) == {1, 3}
    assert got[3]["shortage_type"] is None and got[3]["has_unknown_start"] is True
    assert got[1]["has_unknown_start"] is False


# ---------------------------------------------------------------------------- to_closures

def test_to_closures(bronze):
    same = fermeture("T", "2025-10-08T02:00:00", "2026-12-31T01:00:00")
    df = bronze(pdv("1", fermeture=[same, same]),                                    # the feed repeats entries
                pdv("2", fermeture=[fermeture("D", "2026-01-01T00:00:00", "")]))
    result = ts.to_closures(df)
    got = {r["station_id"]: r for r in result.collect()}
    assert result.count() == 2
    assert got["1"]["closure_type"] == "T" and got["1"]["duration_hours"] > 0
    assert got["2"]["closure_type"] == "D" and got["2"]["ended_at"] is None


# ---------------------------------------------------------------------------- to_stations

def test_to_stations_types_and_derived_columns(bronze):
    df = bronze(pdv("1", postal_code="01000", latitude="4620100", longitude="520000", road_type="R",
                    address="  1 RUE DU TEST ", city=" TESTVILLE ", horaires={"_automate-24-24": "1", "jour": []}),
                pdv("2", postal_code="97400", latitude="", longitude="", road_type="A", horaires=None))
    got = {r["station_id"]: r for r in ts.to_stations(df).collect()}
    a, b = got["1"], got["2"]
    assert (a["postal_code"], a["department_code"]) == ("01000", "01")       # leading zero kept
    assert (round(a["latitude"], 5), round(a["longitude"], 5)) == (46.201, 5.2)
    assert (a["address"], a["city"]) == ("1 RUE DU TEST", "TESTVILLE")       # trimmed
    assert a["is_highway"] is False and a["is_24_7"] is True
    assert b["department_code"] == "974"                                     # overseas: three digits
    assert b["latitude"] is None and b["is_highway"] is True and b["is_24_7"] is None
    assert str(a["first_seen_date"]) == str(a["last_seen_date"]) == "2026-11-13"


def test_to_stations_keeps_the_newest_snapshot_of_the_batch(bronze):
    df = bronze(pdv("1", snapshot="2026-11-13", city="OLD NAME"), pdv("1", snapshot="2026-11-16", city="NEW NAME"))
    result = ts.to_stations(df)
    assert rows(result, "station_id", "city") == [("1", "NEW NAME")]
    assert str(result.first()["last_seen_date"]) == "2026-11-16"


# ---------------------------------------------------------------------------- to_station_services

def test_to_station_services(bronze):
    df = bronze(pdv("1", snapshot="2026-11-13", services=["Lavage manuel", "Wifi"]),
                pdv("1", snapshot="2026-11-16", services=[" Wifi ", "Wifi", "Bar"]),     # newest wins, trimmed, deduplicated
                pdv("2", snapshot="2026-11-16", services=None))
    result = ts.to_station_services(df)
    assert rows(result, "station_id", "service") == [("1", "Bar"), ("1", "Wifi")]
    assert "snapshot_date" in result.columns


# ---------------------------------------------------------------------------- to_station_hours

def test_to_station_hours(bronze):
    hours = {"_automate-24-24": "", "jour": [
        {"_id": "1", "_nom": "Lundi", "_ferme": "", "horaire": [{"_ouverture": "07.00", "_fermeture": "12.00"},
                                                                {"_ouverture": "14.00", "_fermeture": "20.00"}]},
        {"_id": "7", "_nom": "Dimanche", "_ferme": "1", "horaire": None},
    ]}
    df = bronze(pdv("1", horaires=hours), pdv("2", horaires=None))
    result = ts.to_station_hours(df)
    assert rows(result, "station_id", "weekday", "day_name", "is_closed", "slot_index", "opens", "closes") == [
        ("1", 1, "Lundi", False, 0, "07:00", "12:00"),
        ("1", 1, "Lundi", False, 1, "14:00", "20:00"),
        ("1", 7, "Dimanche", True, 0, None, None),
    ]


# ---------------------------------------------------------------------------- MERGE writers

def test_merge_price_events_is_insert_only_and_idempotent(spark, bronze, tmp_path):
    path = str(tmp_path / "price_events")
    day1 = ts.to_price_events(bronze(pdv("1", prix=[prix("1", "Gazole", "2026-11-13T08:00:00", "2.17"),
                                                    prix("6", "SP98", "2026-11-13T09:00:00", "2.20")])), 0.2, 5.0)
    assert ts.merge_price_events(spark, path, day1) == {"inserted": 2, "updated": 0}
    assert ts.merge_price_events(spark, path, day1) == {"inserted": 0, "updated": 0}       # same batch again

    day2 = ts.to_price_events(bronze(pdv("1", snapshot="2026-11-16", prix=[
        prix("1", "Gazole", "2026-11-13T08:00:00", "9.99"),       # known key: must be ignored, even with another price
        prix("1", "Gazole", "2026-11-16T07:00:00", "2.19")])), 0.2, 5.0)
    assert ts.merge_price_events(spark, path, day2) == {"inserted": 1, "updated": 0}

    table = spark.read.format("delta").load(path)
    assert table.count() == 3
    assert rows(table.filter("fuel_id = 1"), "price") == [(Decimal("2.170"),), (Decimal("2.190"),)]


def test_merge_lifecycle_closes_an_open_shortage(spark, bronze, tmp_path):
    path = str(tmp_path / "shortages")
    opened = ts.to_shortages(bronze(pdv("1", snapshot="2026-11-13", rupture=[rupture("6", "SP98", "2026-11-13T13:12:58", "")])))
    closed = ts.to_shortages(bronze(pdv("1", snapshot="2026-11-16", rupture=[rupture("6", "SP98", "2026-11-13T13:12:58", "2026-11-13T16:34:07")])))

    assert ts.merge_lifecycle(spark, path, opened, ts.SHORTAGE_KEYS, "shortage_type") == {"inserted": 1, "updated": 0}
    assert ts.merge_lifecycle(spark, path, opened, ts.SHORTAGE_KEYS, "shortage_type") == {"inserted": 0, "updated": 0}
    assert ts.merge_lifecycle(spark, path, closed, ts.SHORTAGE_KEYS, "shortage_type") == {"inserted": 0, "updated": 1}
    assert ts.merge_lifecycle(spark, path, closed, ts.SHORTAGE_KEYS, "shortage_type") == {"inserted": 0, "updated": 0}

    row = spark.read.format("delta").load(path).first()
    assert row["ended_at"] is not None and row["duration_hours"] == 3.35
    assert (str(row["first_seen_date"]), str(row["last_seen_date"])) == ("2026-11-13", "2026-11-16")

    # An older file that still shows the shortage open must not erase the end date.
    assert ts.merge_lifecycle(spark, path, opened, ts.SHORTAGE_KEYS, "shortage_type") == {"inserted": 0, "updated": 0}
    assert spark.read.format("delta").load(path).first()["ended_at"] is not None


def test_merge_stations_moves_forward_only(spark, bronze, tmp_path):
    path = str(tmp_path / "stations")
    v1 = ts.to_stations(bronze(pdv("1", snapshot="2026-11-13", city="NAME V1")))
    v2 = ts.to_stations(bronze(pdv("1", snapshot="2026-11-16", city="NAME V2"), pdv("2", snapshot="2026-11-16")))
    old = ts.to_stations(bronze(pdv("1", snapshot="2025-12-31", city="NAME 2025")))

    assert ts.merge_stations(spark, path, v1) == {"inserted": 1, "updated": 0}
    assert ts.merge_stations(spark, path, v1) == {"inserted": 0, "updated": 0}
    assert ts.merge_stations(spark, path, v2) == {"inserted": 1, "updated": 1}       # station 2 new, station 1 refreshed
    assert ts.merge_stations(spark, path, old) == {"inserted": 0, "updated": 1}      # backfill: only first_seen moves

    row = spark.read.format("delta").load(path).filter("station_id = '1'").first()
    assert row["city"] == "NAME V2"
    assert (str(row["first_seen_date"]), str(row["last_seen_date"])) == ("2025-12-31", "2026-11-16")


def test_replace_station_state_moves_forward_only(spark, bronze, tmp_path):
    from datetime import date
    path = str(tmp_path / "station_services")

    def apply(*pdvs):
        b = bronze(*pdvs)
        snapshot = b.agg(F.max("snapshot_date")).first()[0]
        return ts.replace_station_state(spark, path, ts.to_station_services(b), ts.batch_stations(b), snapshot)

    def state():
        return rows(spark.read.format("delta").load(path), "station_id", "service")

    apply(pdv("A", snapshot="2026-11-13", services=["Wifi", "Bar"]), pdv("B", snapshot="2026-11-13", services=["Laverie"]))
    assert state() == [("A", "Bar"), ("A", "Wifi"), ("B", "Laverie")]

    # Newer snapshot holding only A: A is replaced, B (absent from the snapshot) is kept.
    assert apply(pdv("A", snapshot="2026-11-16", services=["Wifi"]))["inserted"] == 1
    assert state() == [("A", "Wifi"), ("B", "Laverie")]

    # Older snapshot: A must not be overwritten, the unknown station C is added.
    assert apply(pdv("A", snapshot="2025-12-31", services=["Douches"]), pdv("C", snapshot="2025-12-31", services=["GNV"]))["inserted"] == 1
    assert state() == [("A", "Wifi"), ("B", "Laverie"), ("C", "GNV")]

    # Same older snapshot again: nothing to do.
    assert apply(pdv("A", snapshot="2025-12-31", services=["Douches"]), pdv("C", snapshot="2025-12-31", services=["GNV"])) == {"inserted": 0, "updated": 0}
