# PROJECT.md — project memory

Living document. Context, decisions and open questions, so the project can be picked up at any time without re-reading the whole conversation.

## Context

- Capstone of a 7-day PySpark training. Two goals: consolidate Spark on real volume, and build a production-grade portfolio project to present in interviews.
- Learning project: build one layer at a time, test it, understand it, then the next one. No generated-all-at-once code.
- Target: deployed in production (Airflow + cloud) by the end of Phase 4.

## Dataset decision log

| Date | Candidate | Decision | Reason |
|------|-----------|----------|--------|
| 2026-10-27 | DVF (property sales) | Dropped | Published only twice a year, no daily refresh |
| 2026-10-27 | Meta Ad Library API | Dropped | Political ads only outside EU, keyword-based queries, bucketed metrics, ID verification |
| 2026-10-27 | Inside Airbnb | Dropped | Quarterly snapshots, only 12 months free |
| 2026-10-27 | France Travail job offers API | Dropped | API withdrawn from public access in October 2026, restricted on request |
| 2026-10-27 | Adzuna, SIRENE, TED | Considered | Valid alternatives, kept as possible second connectors |
| 2026-10-27 | **Fuel prices (prix-carburants.gouv.fr)** | **Chosen** | Daily and 10-minute feeds, no auth, archives since 2007, clear business KPIs |

## Source facts (verified 2026-10-27)

- Feeds: instant (10 min), daily (J-1), yearly archives 2007 → today. Zip of one XML, ISO-8859-1.
- Daily file: 9,947 stations, 30,275 current prices, 14 MB. Yearly 2025: 14,420 stations, 5.3 M price updates, 386 MB.
- The current-year archive (`annee/2026`) is refreshed daily (verified 2026-11-14: latest update 2026-11-13T23:59, 4.76 M events). No gap between the yearly backfill and the daily feed.
- Daily file is a snapshot of current prices with `maj` = last update timestamp. The yearly archive contains every update. Hence: backfill from archives, then daily MERGE on (station_id, fuel_id, maj).
- Coordinates are stored as integers multiplied by 100,000.
- No brand in the feed. Brand must be inferred or joined from an external source (decision pending).
- `pop`: R = road station, A = highway station.

## Technical decisions

- Architecture: Medallion (Bronze / Silver / Gold) on Delta Lake, PySpark, Python 3.11.
- Spark version (decided 2026-11-11): pyspark 3.5.9 + delta-spark 3.3.3 + Java 17 (Homebrew openjdk@17). The pair most companies run; XML will be read through the `spark-xml` package. Spark 4.0 reads XML natively but has fewer tutorials and less production adoption.
- Python environment: managed with `uv` (standalone CPython 3.11). Reason: the Homebrew Python 3.11 and 3.14 builds on this Mac ship a `pyexpat` linked against a newer system libexpat than macOS 26.2 provides, which breaks `plistlib`, `platform.mac_ver()` and therefore `pip` inside a classic venv. uv's standalone Python embeds its own libexpat.
- `JAVA_HOME=/opt/homebrew/opt/openjdk@17` must be set: the `java` on PATH is an Apple stub.
- Config externalized in `config/config.yaml`. Logging via `logging`, no print.
- Bronze is append-only and never deleted: it is the system of record for the history we build.

- Bronze schema (decided 2026-11-15): every field a string, nested structure kept, no `_VALUE`. Reason: `_cp` inferred as long loses the leading zero (01000 -> 1000); strings cannot fail conversion silently. Typing happens in Silver.
- spark-xml + nested schema pruning: when the optimizer prunes the XML schema, spark-xml drops nested elements silently (half the stations lost their prices in a multi-column count). Mitigation: Bronze reads and writes every column with no projection in between, `spark.sql.optimizer.nestedSchemaPruning.enabled=false` in the session, and a reconciliation check (exploded `prix` count == `<prix` tags in the raw file).

- Price unit (found 2026-11-17): archives before ~2022 store thousandths of a euro (`valeur="1141"`), recent files store euros (`"2.17"`). Silver divides by 1000 when the raw value exceeds 50. Old archives also use `maj="2007-01-04 18:43:38.841521"` (space, microseconds) and have no `type` on `rupture`.
- Ancient dates: the feed contains typos (a shortage starting in year 0216) and a 1970-01-01 sentinel for "start unknown". Kept as-is, flagged with `has_unknown_start`; Parquet rebase mode set to CORRECTED so they can be written.
- Ended shortages appear in the daily feed only on the day they end. A missed day leaves them open forever unless the running-year archive is merged: the weekly reconciliation is required, not optional.
- State tables (stations attributes, services, hours) only move forward in time: an older snapshot never overwrites a newer state, it only adds stations not known yet.
- Running-year archive is stamped `_asof_<yesterday>`: like the daily feed it holds data up to J-1.
- Silver ledger `_processed_files`: a daily run reads only the new Bronze file. `--force` reprocesses everything and must change nothing.
- Session timezone pinned to Europe/Paris: feed timestamps are local time without offset.

## Open questions

- `price_events` has near-empty year partitions (stale prices from 2009-2021 seen in the daily feed). They fill up with the backfill; revisit file sizes at step 6 (OPTIMIZE).

- Brand attribution: regex on address/name, or join with OpenStreetMap `amenity=fuel` nodes (which carry `brand`)?
- Crude oil reference series for pass-through analysis: which free source (e.g. EIA Brent daily)?
- Cloud provider: Phase 3.

## Step log

- 2026-11-11, step 0 done: SparkSession with Delta starts, writes and reads a Delta table (`scripts/smoke_test.py`).
- 2026-11-11, step 1 done: config.yaml, src/utils.py (load_config, setup_logging, get_spark), requirements.txt. Session starts from the config.
- 2026-11-15, step 2 done: `src/download.py` (feeds -> raw, idempotent) and `src/ingestion.py` (XML -> Bronze `pdv_raw`, explicit all-string schema, nested arrays kept, partition source_type/snapshot_date, skip if source_file already ingested). Reconciled with the raw file: 9,953 stations, 31,691 prices, 26,420 shortages, 63,039 services.

- 2026-11-17, step 3 done: `src/transform_silver.py`. Six Silver tables (price_events, shortages, closures, stations, station_services, station_hours), pure transformations + MERGE writers, ledger of processed Bronze files. Verified: forced reprocess of a file = 0 inserted / 0 updated (no Delta commit written); second daily = 15,128 new price events only; 2026 archive (372 MB) = 4.76 M events merged in about 15 s; the daily of Nov 16 processed after the archive inserted 0 rows (every daily price exists in the archive: the sources converge). 4,790,090 price events, 0 duplicate key in any table.

## Roadmap

Phase 1 (local), Phase 2 (Airflow), Phase 3 (cloud), Phase 4 (prod). Details in README.md.
