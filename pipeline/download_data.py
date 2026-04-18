"""
Download AIS zip files from aisdata.ais.dk and update pipeline_status.csv.

URL structure:
  - Monthly (2006-03 to 2024-02): http://aisdata.ais.dk/{YYYY}/aisdk-{YYYY-MM}.zip
  - Daily with year subdir (2024-03-01 to 2025-02-26):
                             http://aisdata.ais.dk/{YYYY}/aisdk-{YYYY-MM-DD}.zip
  - Daily flat root (2025-02-27 onwards):
                             http://aisdata.ais.dk/aisdk-{YYYY-MM-DD}.zip

Usage:
    python -m pipeline.download_data 2026-02-01             # single day
    python -m pipeline.download_data 2026-02-01 2026-02-28  # daily range
    python -m pipeline.download_data 2024-02                # single month
    python -m pipeline.download_data 2024-02 2024-06        # monthly range
"""

import csv
import os
import sys
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

_ROOT      = Path(__file__).resolve().parent.parent
RAW_DIR    = _ROOT / "data" / "raw"
STATUS_CSV = _ROOT / "data" / "pipeline_status.csv"

BASE_URL        = "http://aisdata.ais.dk"
FLAT_ROOT_FROM  = date(2025, 2, 27)   # flat root URL from this date onwards
MAX_WORKERS     = 4


# ── URL construction ──────────────────────────────────────────────────────────

def _url(date_str: str) -> str:
    if len(date_str) == 7:          # monthly: YYYY-MM
        year = date_str[:4]
        return f"{BASE_URL}/{year}/aisdk-{date_str}.zip"
    d = date.fromisoformat(date_str)
    if d >= FLAT_ROOT_FROM:
        return f"{BASE_URL}/aisdk-{date_str}.zip"
    return f"{BASE_URL}/{d.year}/aisdk-{date_str}.zip"


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _read_status() -> dict[str, dict]:
    """Return pipeline_status rows keyed by date, or {} if file absent."""
    if not STATUS_CSV.exists():
        return {}
    with open(STATUS_CSV, newline="") as f:
        return {row["date"]: row for row in csv.DictReader(f)}


def _update_status(date_str: str, **fields):
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


# ── download ──────────────────────────────────────────────────────────────────

def download_file(date_str: str, done: set[str]):
    """Download one zip unless pipeline_status already marks it raw=True."""
    if date_str in done:
        print(f"Skipping (raw=True in status): {date_str}")
        return

    filename = f"aisdk-{date_str}.zip"
    output_path = RAW_DIR / filename

    url = _url(date_str)
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            if r.status_code != 200:
                print(f"Not found ({r.status_code}): {url}")
                return
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        print(f"Downloaded: {filename}")
        _update_status(date_str, raw=True)
    except Exception as e:
        print(f"Error downloading {filename}: {e}")


# ── date range helpers ────────────────────────────────────────────────────────

def _month_range(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield date(y, m, 1).strftime("%Y-%m")
        m += 1
        if m > 12:
            m, y = 1, y + 1


def _day_range(start: date, end: date):
    current = start
    while current <= end:
        yield str(current)
        current += timedelta(days=1)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    if not args:
        print("Usage: python -m pipeline.download_data <date> [end_date]")
        print("  date: YYYY-MM (monthly) or YYYY-MM-DD (daily)")
        sys.exit(1)

    start_str = args[0]
    end_str   = args[1] if len(args) > 1 else start_str
    monthly   = len(start_str) == 7

    if monthly:
        start     = date.fromisoformat(start_str + "-01")
        end       = date.fromisoformat(end_str   + "-01")
        date_strs = list(_month_range(start, end))
    else:
        start     = date.fromisoformat(start_str)
        end       = date.fromisoformat(end_str)
        date_strs = list(_day_range(start, end))

    RAW_DIR.mkdir(exist_ok=True)
    status = _read_status()
    done   = {d for d, row in status.items() if row.get("raw") == "True"}
    print(f"Downloading {len(date_strs)} file(s) to {RAW_DIR} ({len(done)} already done)")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(lambda d: download_file(d, done), date_strs)
