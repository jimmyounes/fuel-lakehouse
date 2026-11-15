"""Fetch the fuel price feeds into the raw layer.

Pure Python, no Spark: downloading is I/O, not data processing.
Each feed is a zip holding one XML. We keep the XML, named by source and date,
under data/raw/<source_type>/. Downloading the same file twice is a no-op.
"""
import argparse
import logging
import re
import zipfile
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from urllib.request import Request, urlopen

from src.utils import load_config, setup_logging

log = logging.getLogger(__name__)

# How the server names its files, e.g. PrixCarburants_quotidien_20261113.zip
_DATE_IN_NAME = re.compile(r"_(\d{8})\.")
_YEAR_IN_NAME = re.compile(r"_annuel_(\d{4})\.")


def _fetch(url: str) -> bytes:
    """Download a URL and return its bytes (a zip for every feed)."""
    req = Request(url, headers={"User-Agent": "fuel-lakehouse/0.1"})
    with urlopen(req, timeout=300) as resp:
        return resp.read()


def snapshot_date_from_filename(name: str) -> date:
    """The date a file describes, read from its name.

    Daily files carry YYYYMMDD, yearly files carry the year (we use Dec 31),
    instant files carry nothing (we use today).
    """
    if m := _DATE_IN_NAME.search(name):
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    if m := _YEAR_IN_NAME.search(name):
        return date(int(m.group(1)), 12, 31)
    return date.today()


def download_feed(cfg: dict, source_type: str, year: int | None = None) -> Path:
    """Download one feed into raw/<source_type>/ and return the XML path.

    source_type: 'daily', 'instant' or 'yearly' (yearly needs a year).
    """
    src = cfg["source"]
    if source_type == "yearly":
        if year is None:
            raise ValueError("source_type='yearly' needs a year")
        url = src["yearly_url"].format(year=year)
    else:
        url = src[f"{source_type}_url"]

    target_dir = Path(cfg["paths"]["raw"]) / source_type
    target_dir.mkdir(parents=True, exist_ok=True)

    # Yearly archives have a predictable name: skip the 30+ MB download if we have it.
    if source_type == "yearly":
        expected = target_dir / f"PrixCarburants_annuel_{year}.xml"
        if expected.exists():
            log.info("Already present, skipping: %s", expected.name)
            return expected

    log.info("Downloading %s feed from %s", source_type, url)
    payload = _fetch(url)

    with zipfile.ZipFile(BytesIO(payload)) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".xml")]
        if len(members) != 1:
            raise RuntimeError(f"Expected one XML in {url}, found {members}")
        member = members[0]

        # The XML inside the zip carries the real name (PrixCarburants_quotidien_YYYYMMDD.xml).
        # The instant feed is always named the same: stamp it ourselves.
        xml_name = Path(member).name
        if source_type == "instant":
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            xml_name = f"PrixCarburants_instantane_{stamp}.xml"

        xml_path = target_dir / xml_name
        if xml_path.exists():
            log.info("Already present, skipping: %s", xml_path.name)
            return xml_path
        xml_path.write_bytes(zf.read(member))


    log.info("Saved %s (%.1f MB)", xml_path, xml_path.stat().st_size / 1e6)
    return xml_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download a fuel price feed into the raw layer")
    parser.add_argument("--source", choices=["daily", "instant", "yearly"], default="daily")
    parser.add_argument("--year", type=int, help="required with --source yearly")
    args = parser.parse_args()

    cfg = load_config()
    setup_logging(cfg["logging"]["level"])
    path = download_feed(cfg, args.source, args.year)
    log.info("Snapshot date: %s", snapshot_date_from_filename(path.name))


if __name__ == "__main__":
    main()
