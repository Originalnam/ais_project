"""
Generate / refresh data/pipeline_status.csv.

One row per period available on the source (aisdata.ais.dk S3 bucket):
  - Monthly zips (2006-03 to 2024-02): date column is YYYY-MM
  - Daily zips  (2024-03-01 onwards) : date column is YYYY-MM-DD

Run this script to rebuild or update the status file.
"""

import csv
import os
from datetime import date, timedelta

# ── source coverage ───────────────────────────────────────────────────────────
# Monthly zips: 2006-03 through 2024-02  (aisdk-YYYY-MM.zip)
MONTHLY_START = date(2006, 3, 1)
MONTHLY_END   = date(2024, 2, 1)

# Daily zips: 2024-03-01 through latest known (aisdk-YYYY-MM-DD.zip)
DAILY_START   = date(2024, 3, 1)
DAILY_END     = date(2026, 4, 13)

# ── paths ─────────────────────────────────────────────────────────────────────
ROOT         = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR             = os.path.join(ROOT, "data", "raw")
UNZIP_DIR           = os.path.join(ROOT, "data", "raw", "unzipped")
FILTERED_DIR        = os.path.join(ROOT, "data", "processed", "filtered")
AARHUS_FILTERED_DIR = os.path.join(ROOT, "data", "processed", "filtered", "aarhus")
OUTPUT              = os.path.join(ROOT, "data", "pipeline_status.csv")

# ── helpers ───────────────────────────────────────────────────────────────────

def month_range(start: date, end: date):
    """Yield the first day of each month from start to end (inclusive)."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield date(year, month, 1)
        month += 1
        if month > 12:
            month, year = 1, year + 1


def day_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _is_raw(key: str) -> bool:
    """Return True if aisdk-{key}.zip is present in raw/."""
    return os.path.isfile(os.path.join(RAW_DIR, f"aisdk-{key}.zip"))


def _is_unzipped(key: str) -> bool:
    """Return True if the unzipped folder aisdk-{key}/ exists."""
    return os.path.isdir(os.path.join(UNZIP_DIR, f"aisdk-{key}"))


def _unzipped_size(key: str):
    """Return total byte size of the unzipped folder, or None if absent."""
    folder = os.path.join(UNZIP_DIR, f"aisdk-{key}")
    if not os.path.isdir(folder):
        return None
    total = 0
    for dirpath, _, filenames in os.walk(folder):
        for fname in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, fname))
            except OSError:
                pass
    return total


def _is_filtered(key: str) -> bool:
    """Return True if the filtered Parquet file exists."""
    return os.path.isfile(os.path.join(FILTERED_DIR, f"aisdk-{key}.parquet"))


def _filtered_size(key: str):
    """Return byte size of the filtered Parquet file, or None if absent."""
    path = os.path.join(FILTERED_DIR, f"aisdk-{key}.parquet")
    if not os.path.isfile(path):
        return None
    return os.path.getsize(path)


def _filtered_compression(key: str, unzipped_size) -> float | None:
    """Return % reduction in size from unzipped CSV to filtered Parquet."""
    if not unzipped_size:
        return None
    fsize = _filtered_size(key)
    if fsize is None:
        return None
    try:
        return round((1 - fsize / float(unzipped_size)) * 100, 1)
    except (ZeroDivisionError, TypeError, ValueError):
        return None


def _filtered_aarhus_size(key: str):
    """Return byte size of the per-period Aarhus filtered Parquet, or None if absent."""
    path = os.path.join(AARHUS_FILTERED_DIR, f"aisdk-{key}.parquet")
    if not os.path.isfile(path):
        return None
    return os.path.getsize(path)


def _filtered_aarhus_compression(key: str, filtered_size) -> float | None:
    """Return % reduction from full filtered Parquet to Aarhus-only Parquet."""
    if not filtered_size:
        return None
    asize = _filtered_aarhus_size(key)
    if asize is None:
        return None
    try:
        return round((1 - asize / float(filtered_size)) * 100, 1)
    except (ZeroDivisionError, TypeError, ValueError):
        return None


# ── load existing CSV (preserve sticky True values) ───────────────────────────
# raw=True and unzipped=True are write-once: once set, they never revert even
# if the source file has been deleted from disk.

_existing: dict[str, dict] = {}
if os.path.isfile(OUTPUT):
    with open(OUTPUT, newline="") as _f:
        for _row in csv.DictReader(_f):
            _existing[_row["date"]] = _row


# ── build rows ────────────────────────────────────────────────────────────────

rows = []

# Monthly rows — format YYYY-MM
for d in month_range(MONTHLY_START, MONTHLY_END):
    key        = d.strftime("%Y-%m")
    _ex        = _existing.get(key, {})
    _unzipped  = _is_unzipped(key) or _ex.get("unzipped") == "True"
    _raw       = _is_raw(key) or _unzipped or _ex.get("raw") == "True"
    _size      = _unzipped_size(key) or (_ex.get("unzipped_size") or None)
    _filtered  = _is_filtered(key) or _ex.get("filtered") == "True"
    _fsize     = _filtered_size(key) or (_ex.get("filtered_size") or None)
    _fcomp     = _filtered_compression(key, _size) or (_ex.get("filtered_compression") or None)
    _asize     = _filtered_aarhus_size(key) or (_ex.get("filtered_aarhus_size") or None)
    _acomp     = _filtered_aarhus_compression(key, _fsize) or (_ex.get("filtered_aarhus_compression") or None)
    rows.append({
        "date":                       key,
        "raw":                        _raw,
        "unzipped":                   _unzipped,
        "unzipped_size":              _size,
        "filtered":                   _filtered,
        "filtered_size":              _fsize,
        "filtered_compression":       _fcomp,
        "filtered_aarhus_size":       _asize,
        "filtered_aarhus_compression": _acomp,
    })

# Daily rows — format YYYY-MM-DD
for d in day_range(DAILY_START, DAILY_END):
    key        = str(d)
    _ex        = _existing.get(key, {})
    _unzipped  = _is_unzipped(key) or _ex.get("unzipped") == "True"
    _raw       = _is_raw(key) or _unzipped or _ex.get("raw") == "True"
    _size      = _unzipped_size(key) or (_ex.get("unzipped_size") or None)
    _filtered  = _is_filtered(key) or _ex.get("filtered") == "True"
    _fsize     = _filtered_size(key) or (_ex.get("filtered_size") or None)
    _fcomp     = _filtered_compression(key, _size) or (_ex.get("filtered_compression") or None)
    _asize     = _filtered_aarhus_size(key) or (_ex.get("filtered_aarhus_size") or None)
    _acomp     = _filtered_aarhus_compression(key, _fsize) or (_ex.get("filtered_aarhus_compression") or None)
    rows.append({
        "date":                       key,
        "raw":                        _raw,
        "unzipped":                   _unzipped,
        "unzipped_size":              _size,
        "filtered":                   _filtered,
        "filtered_size":              _fsize,
        "filtered_compression":       _fcomp,
        "filtered_aarhus_size":       _asize,
        "filtered_aarhus_compression": _acomp,
    })

# ── write CSV ─────────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

with open(OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=[
        "date", "raw", "unzipped", "unzipped_size",
        "filtered", "filtered_size", "filtered_compression",
        "filtered_aarhus_size", "filtered_aarhus_compression",
    ])
    writer.writeheader()
    writer.writerows(rows)

monthly_count  = sum(1 for r in rows if len(r["date"]) == 7)
daily_count    = len(rows) - monthly_count
raw_count      = sum(1 for r in rows if r["raw"])
unzipped_count = sum(1 for r in rows if r["unzipped"])
filtered_count = sum(1 for r in rows if r["filtered"])

print(f"Written {len(rows)} rows -> {os.path.relpath(OUTPUT)}")
print(f"  monthly rows    : {monthly_count}  (2006-03 to 2024-02)")
print(f"  daily rows      : {daily_count}  (2024-03-01 to {DAILY_END})")
print(f"  raw=True        : {raw_count}")
print(f"  unzipped=True   : {unzipped_count}")
print(f"  filtered=True   : {filtered_count}")
