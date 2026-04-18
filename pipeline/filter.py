"""
filter.py — Extract AIS records for major Danish ports.

Strategy
--------
Each daily CSV contains ~15 M rows covering all Danish waters.  Reading the full
file into memory is wasteful; instead we stream it in chunks, apply a bounding-box
filter immediately, and accumulate only the matching rows.  Output is written as
Parquet — roughly 5-10× smaller than CSV and much faster for downstream reads.

Port bounding boxes are defined in config.py (Esbjerg, Aarhus, Copenhagen,
Aalborg, Fredericia).  Boxes are intentionally generous: they cover the full
port basin, anchorages, and the approach channel so that arriving/departing
vessels are included.

Usage
-----
    python -m pipeline.filter                        # process all days
    python -m pipeline.filter --date 2026-02-01      # single day
    python -m pipeline.filter --workers 4            # parallel (default: 1)
"""

from __future__ import annotations

import argparse
import csv
import logging
import multiprocessing
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PORTS, UNZIPPED_DIR, FILTERED_DIR, CHUNK_SIZE

STATUS_CSV = Path(__file__).resolve().parent.parent / "data" / "pipeline_status.csv"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RAW_DIR = UNZIPPED_DIR
OUT_DIR = FILTERED_DIR

# Only load columns we actually need; skip the noisy/empty DMA tail columns.
USECOLS = [
    "# Timestamp",
    "Type of mobile",
    "MMSI",
    "Latitude",
    "Longitude",
    "Navigational status",
    "SOG",
    "COG",
    "Heading",
    "Ship type",
    "Name",
    "Destination",
]

DTYPE_MAP = {
    "MMSI":      "int64",
    "Latitude":  "float32",
    "Longitude": "float32",
    "SOG":       "float32",
    "COG":       "float32",
    "Heading":   "float32",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline status helpers
# ---------------------------------------------------------------------------

def _read_status_row(date_str: str) -> dict:
    """Return the pipeline_status row for date_str, or {} if absent."""
    if not STATUS_CSV.exists():
        return {}
    with open(STATUS_CSV, newline="") as f:
        for row in csv.DictReader(f):
            if row["date"] == date_str:
                return row
    return {}


def _update_status(date_str: str, **fields) -> None:
    """Set column values for one date row in pipeline_status.csv."""
    if not STATUS_CSV.exists():
        return
    with open(STATUS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    for row in rows:
        if row["date"] == date_str:
            row.update({k: str(v) for k, v in fields.items()})
            break
    with open(STATUS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _bbox_mask(df: pd.DataFrame) -> pd.Series:
    """Return a boolean mask selecting rows that fall inside any port bbox."""
    lat = df["Latitude"]
    lon = df["Longitude"]
    mask = pd.Series(False, index=df.index)
    for lat_min, lat_max, lon_min, lon_max in PORTS.values():
        mask |= (
            lat.between(lat_min, lat_max)
            & lon.between(lon_min, lon_max)
        )
    return mask


def _port_label(df: pd.DataFrame) -> pd.Series:
    """Add a 'port' column indicating which port each row belongs to."""
    # Use plain str dtype during assignment; convert to Categorical at the end
    # so pandas doesn't reject new category values mid-loop.
    label = pd.Series("", index=df.index, dtype=str)
    lat = df["Latitude"]
    lon = df["Longitude"]
    for name, (lat_min, lat_max, lon_min, lon_max) in PORTS.items():
        in_box = (
            lat.between(lat_min, lat_max)
            & lon.between(lon_min, lon_max)
        )
        label[in_box] = name
    return label.astype("category")


def _find_csvs(folder: Path) -> list[Path]:
    """
    Return all CSV files in a folder, sorted by name.

    Handles both naming conventions:
      - Daily  : aisdk-YYYY-MM-DD.csv   (one file per folder)
      - Monthly: aisdk_YYYYMMDD.csv     (one file per day inside a month folder)
    """
    return sorted(folder.glob("*.csv"))


def _write_filtered_status(date_str: str, out_path: Path) -> None:
    """Update pipeline_status with filtered=True, size, and compression."""
    fsize = out_path.stat().st_size
    status_row = _read_status_row(date_str)
    try:
        unzipped_size = float(status_row.get("unzipped_size") or 0)
        compression = round((1 - fsize / unzipped_size) * 100, 1) if unzipped_size else None
    except (ZeroDivisionError, TypeError, ValueError):
        compression = None
    _update_status(date_str, filtered=True, filtered_size=fsize, filtered_compression=compression)


def filter_day(date_str: str) -> int:
    """
    Filter one period's CSV(s) and write the result to Parquet.

    Parameters
    ----------
    date_str : str
        ``"YYYY-MM-DD"`` for a daily folder or ``"YYYY-MM"`` for a monthly folder.

    Returns
    -------
    int
        Number of rows written.
    """
    folder = RAW_DIR / f"aisdk-{date_str}"
    if not folder.exists():
        log.warning("Missing folder: %s — skipped", folder)
        return 0

    csv_files = _find_csvs(folder)
    if not csv_files:
        log.warning("No CSV files in %s — skipped", folder)
        return 0

    out_path = OUT_DIR / f"aisdk-{date_str}.parquet"
    if _read_status_row(date_str).get("filtered") == "True":
        log.info("Already filtered (status): %s — skipped", date_str)
        return 0

    log.info("Filtering %s (%d file(s)) …", date_str, len(csv_files))

    chunks: list[pd.DataFrame] = []

    for csv_path in csv_files:
        reader = pd.read_csv(
            csv_path,
            usecols=USECOLS,
            dtype=DTYPE_MAP,
            chunksize=CHUNK_SIZE,
            low_memory=False,
            on_bad_lines="skip",
        )
        for chunk in reader:
            filtered = chunk[_bbox_mask(chunk)]
            if not filtered.empty:
                chunks.append(filtered)

    if not chunks:
        log.info("  No matching rows for %s", date_str)
        # Write an empty parquet so we don't reprocess this day.
        pd.DataFrame(columns=USECOLS).to_parquet(out_path, index=False)
        _write_filtered_status(date_str, out_path)
        return 0

    combined = pd.concat(chunks, ignore_index=True)
    combined["port"] = _port_label(combined)
    combined.rename(columns={"# Timestamp": "Timestamp"}, inplace=True)
    combined["Timestamp"] = pd.to_datetime(
        combined["Timestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False, compression="snappy")

    log.info("  %s → %d rows → %s", date_str, len(combined), out_path.name)
    _write_filtered_status(date_str, out_path)
    return len(combined)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", help="Process a single date (YYYY-MM-DD)")
    p.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel worker processes (default: 1). Use ≤ CPU count.",
    )
    return p.parse_args()


def _discover_dates() -> list[str]:
    """Return sorted list of date strings found in RAW_DIR."""
    dirs = sorted(d.name for d in RAW_DIR.iterdir() if d.is_dir() and d.name.startswith("aisdk-"))
    return [d.replace("aisdk-", "") for d in dirs]


def main() -> None:
    args = _parse_args()
    dates = [args.date] if args.date else _discover_dates()

    if not dates:
        log.error("No date folders found under %s", RAW_DIR)
        return

    log.info("Processing %d day(s) with %d worker(s)", len(dates), args.workers)

    if args.workers > 1:
        with multiprocessing.Pool(processes=args.workers) as pool:
            totals = pool.map(filter_day, dates)
    else:
        totals = [filter_day(d) for d in dates]

    log.info("Done. Total rows written: %d", sum(totals))


if __name__ == "__main__":
    main()
