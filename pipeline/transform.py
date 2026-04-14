"""
transform.py — Compute per-port analytics from filtered Parquet files.

Reads all Parquet files produced by filter.py (one per day) and computes:

  1. port_hourly_stats  — unique vessel count and congestion index per port/date/hour
  2. vessel_visits      — one row per vessel per port per day with dwell time
  3. type_distribution  — vessel type counts per port/date

Output is written as Parquet to data/processed/ and is consumed by load.py.

Usage
-----
    python -m pipeline.transform
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import FILTERED_DIR, PROCESSED_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Map raw DMA ship-type strings to 8 normalised categories.
_TYPE_MAP: dict[str, str] = {
    "cargo":          "Cargo",
    "tanker":         "Tanker",
    "passenger":      "Passenger",
    "fishing":        "Fishing",
    "tug":            "Tug",
    "pleasure craft": "Pleasure Craft",
    "hsc":            "HSC",
    "sar":            "SAR",
}


def _normalise_type(series: pd.Series) -> pd.Series:
    lowered = series.str.lower().str.strip()
    result = pd.Series("Other", index=series.index)
    for key, label in _TYPE_MAP.items():
        result[lowered.str.contains(key, na=False)] = label
    return result


def load_filtered() -> pd.DataFrame:
    """Load and concatenate all filtered Parquet files."""
    files = sorted(FILTERED_DIR.glob("aisdk-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No filtered Parquet files found in {FILTERED_DIR}")

    log.info("Loading %d filtered Parquet file(s) …", len(files))
    frames = []
    for f in files:
        df = pd.read_parquet(f)
        if df.empty:
            continue
        # date is encoded in the filename: aisdk-YYYY-MM-DD.parquet
        df["date"] = pd.to_datetime(f.stem.replace("aisdk-", "")).date()
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    log.info("  Total rows: %d", len(combined))
    return combined


def compute_hourly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vessel count and congestion index per (port, date, hour).

    vessel_count    — unique MMSIs active in that hour
    congestion_index — same value; reserved for future weighting by vessel size
    """
    df = df.copy()
    df["hour"] = pd.to_datetime(df["Timestamp"]).dt.hour

    stats = (
        df.groupby(["port", "date", "hour"])["MMSI"]
        .nunique()
        .reset_index()
        .rename(columns={"MMSI": "vessel_count"})
    )
    stats["congestion_index"] = stats["vessel_count"]
    return stats


def compute_vessel_visits(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (MMSI, port, date) with first/last ping and dwell time.

    Vessels with only a single ping are kept (dwell_minutes = 0).
    """
    df = df.copy()
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))
    ts = pd.to_datetime(df["Timestamp"])

    visits = df.groupby(["MMSI", "port", "date"]).agg(
        first_seen=("Timestamp", "min"),
        last_seen=("Timestamp", "max"),
        vessel_type=("vessel_type", lambda s: s.mode().iloc[0] if not s.mode().empty else "Other"),
        ping_count=("Timestamp", "count"),
    ).reset_index()

    visits["dwell_minutes"] = (
        (pd.to_datetime(visits["last_seen"]) - pd.to_datetime(visits["first_seen"]))
        .dt.total_seconds()
        / 60
    ).round(1)

    return visits


def compute_type_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Vessel type counts per (port, date)."""
    df = df.copy()
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))

    dist = (
        df.drop_duplicates(subset=["MMSI", "port", "date"])
        .groupby(["port", "date", "vessel_type"])
        .size()
        .reset_index(name="count")
    )
    return dist


def main() -> None:
    df = load_filtered()

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    hourly = compute_hourly_stats(df)
    out = PROCESSED_DIR / "hourly_stats.parquet"
    hourly.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(hourly), out.name)

    visits = compute_vessel_visits(df)
    out = PROCESSED_DIR / "vessel_visits.parquet"
    visits.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(visits), out.name)

    dist = compute_type_distribution(df)
    out = PROCESSED_DIR / "type_distribution.parquet"
    dist.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(dist), out.name)

    log.info("Transform complete.")


if __name__ == "__main__":
    main()
