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
from datetime import timedelta
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
    Vessel count per (port, date, hour, vessel_type).

    vessel_count — unique MMSIs of that type active in that hour.
    Summing vessel_count across vessel_type gives total unique MMSIs for the
    hour (each MMSI maps to exactly one type, so there is no double-counting).
    """
    df = df.copy()
    df["hour"] = pd.to_datetime(df["Timestamp"]).dt.hour
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))

    stats = (
        df.groupby(["port", "date", "hour", "vessel_type"])["MMSI"]
        .nunique()
        .reset_index()
        .rename(columns={"MMSI": "vessel_count"})
    )
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

    visits["entry_hour"] = pd.to_datetime(visits["first_seen"]).dt.hour
    visits["exit_hour"]  = pd.to_datetime(visits["last_seen"]).dt.hour

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


_SOG_SENTINEL = 102.3  # AIS "speed not available" sentinel


def compute_port_navstatus_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ping count by navigational status per (port, date).

    Port-level analogue of aarhus_navstatus_stats.  Enables % moored,
    % underway, % at anchor comparisons across all five ports.
    """
    nav_col = "Navigational status"
    return (
        df.groupby(["port", "date", nav_col])
        .size()
        .reset_index(name="ping_count")
        .rename(columns={nav_col: "nav_status"})
    )


def compute_port_speed_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    SOG (Speed Over Ground) distribution per (port, date, hour).

    Port-level analogue of aarhus_zone_speed_stats.
    pct_stationary = % of pings with SOG < 0.5 knots (proxy for stopped/waiting).
    Excludes the AIS "not available" sentinel (SOG >= 102.3).
    """
    valid = df[df["SOG"] < _SOG_SENTINEL].copy()
    valid["hour"] = pd.to_datetime(valid["Timestamp"]).dt.hour
    valid["stationary"] = (valid["SOG"] < 0.5).astype(int)

    stats = (
        valid.groupby(["port", "date", "hour"])
        .agg(
            sog_mean=("SOG", "mean"),
            sog_median=("SOG", "median"),
            sog_p95=("SOG", lambda x: x.quantile(0.95)),
            pct_stationary=("stationary", lambda x: round(100 * x.mean(), 1)),
        )
        .round({"sog_mean": 2, "sog_median": 2, "sog_p95": 2})
        .reset_index()
    )
    return stats


def compute_port_daily_flow(visits: pd.DataFrame) -> pd.DataFrame:
    """
    True entry/exit counts per (port, date, vessel_type).

    Entry = MMSI seen in this port today but NOT in the same port yesterday.
    Exit  = MMSI seen in this port today but NOT in the same port tomorrow.

    Parameters
    ----------
    visits : pd.DataFrame
        Output of compute_vessel_visits() — one row per (MMSI, port, date).
    """
    visits = visits.copy()
    visits["date"] = pd.to_datetime(visits["date"]).dt.date

    # Build (port, date) → set of MMSIs lookup
    mmsi_by_port_date: dict = {}
    for (port, d), grp in visits.groupby(["port", "date"]):
        mmsi_by_port_date[(port, d)] = set(grp["MMSI"])

    rows = []
    for (port, d), today_mmsis in mmsi_by_port_date.items():
        prev_mmsis = mmsi_by_port_date.get((port, d - timedelta(days=1)), set())
        next_mmsis = mmsi_by_port_date.get((port, d + timedelta(days=1)), set())
        entries = today_mmsis - prev_mmsis   # new arrivals (not seen yesterday)
        exits   = today_mmsis - next_mmsis   # departing  (not seen tomorrow)

        day_visits = visits[(visits["port"] == port) & (visits["date"] == d)]
        for vtype, grp in day_visits.groupby("vessel_type"):
            type_mmsis = set(grp["MMSI"])
            positive_dwell = grp[grp["dwell_minutes"] > 0]["dwell_minutes"]
            avg_dwell = round(positive_dwell.mean(), 1) if not positive_dwell.empty else None
            rows.append({
                "port":              port,
                "date":              str(d),
                "vessel_type":       vtype,
                "unique_vessels":    len(type_mmsis),
                "entries":           len(type_mmsis & entries),
                "exits":             len(type_mmsis & exits),
                "avg_dwell_minutes": avg_dwell,
            })

    return pd.DataFrame(rows)


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

    navstatus = compute_port_navstatus_stats(df)
    out = PROCESSED_DIR / "port_navstatus_stats.parquet"
    navstatus.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(navstatus), out.name)

    speed = compute_port_speed_stats(df)
    out = PROCESSED_DIR / "port_speed_stats.parquet"
    speed.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(speed), out.name)

    flow = compute_port_daily_flow(visits)
    out = PROCESSED_DIR / "port_daily_flow.parquet"
    flow.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(flow), out.name)

    log.info("Transform complete.")


if __name__ == "__main__":
    main()
