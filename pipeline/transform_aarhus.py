"""
transform_aarhus.py — Compute Aarhus sub-zone analytics from filtered Parquet.

Reads the filtered Parquet files produced by filter.py, isolates Aarhus pings,
labels each ping with one of four operational sub-zones, then computes:

  1. aarhus_zone_hourly_stats  — unique vessel count per (zone, date, hour)
  2. aarhus_zone_visits        — one row per (MMSI, zone, date) with dwell time
  3. aarhus_navstatus_stats    — ping count by navigational status per (zone, date)
  4. aarhus_zone_speed_stats   — SOG distribution (mean/median/p95) per (zone, date, hour)

Zone definitions come from AARHUS_ZONES in config.py — the single source of truth.
Sub-zone boundaries are applied here (not in filter.py) so they can be tuned
without re-streaming the raw CSVs.

Output is written as Parquet to data/processed/ with aarhus_ prefix and is
consumed by load.py.

Usage
-----
    python -m pipeline.transform_aarhus
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import csv

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    AARHUS_FILTERED_DIR, AARHUS_ZONES, ANCHORAGE_DIAGONAL,
    FILTERED_DIR, PIPELINE_STATUS, PROCESSED_DIR,
)
from pipeline.transform import _normalise_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# AIS sentinel value for "speed not available"
_SOG_SENTINEL = 102.3


def _update_pipeline_status(aarhus_sizes: dict[str, tuple[int, float]]) -> None:
    """
    Write filtered_aarhus_size and filtered_aarhus_compression into pipeline_status.csv.

    Adds columns if missing; preserves all existing values.  Called after
    per-period Aarhus parquets are written so sizes come from disk.
    """
    if not PIPELINE_STATUS.exists():
        log.warning("pipeline_status.csv not found — skipping status update")
        return

    with open(PIPELINE_STATUS, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    for new_col in ("filtered_aarhus_size", "filtered_aarhus_compression"):
        if new_col not in fieldnames:
            fieldnames.append(new_col)

    for row in rows:
        key = row["date"]
        if key in aarhus_sizes:
            size, comp = aarhus_sizes[key]
            row["filtered_aarhus_size"]        = size
            row["filtered_aarhus_compression"] = comp
        else:
            row.setdefault("filtered_aarhus_size",        "")
            row.setdefault("filtered_aarhus_compression", "")

    with open(PIPELINE_STATUS, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Updated pipeline_status.csv for %d Aarhus period(s)", len(aarhus_sizes))


def load_aarhus_filtered() -> pd.DataFrame:
    """
    Load filtered Parquet files and return only Aarhus-port rows.

    For each period, writes a per-period Aarhus parquet to
    data/processed/filtered/aarhus/aisdk-{key}.parquet and records
    filtered_aarhus_size and filtered_aarhus_compression in pipeline_status.csv.
    """
    files = sorted(FILTERED_DIR.glob("aisdk-*.parquet"))
    if not files:
        raise FileNotFoundError(f"No filtered Parquet files found in {FILTERED_DIR}")

    AARHUS_FILTERED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading %d filtered Parquet file(s) for Aarhus …", len(files))
    frames = []
    aarhus_sizes: dict[str, tuple[int, float]] = {}

    for f in files:
        df = pd.read_parquet(f)
        if df.empty:
            continue
        df_aarhus = df[df["port"] == "aarhus"].copy()
        if df_aarhus.empty:
            continue

        # Write per-period Aarhus parquet (no date column — derived from filename)
        key = f.stem.replace("aisdk-", "")
        aarhus_path = AARHUS_FILTERED_DIR / f"aisdk-{key}.parquet"
        df_aarhus.to_parquet(aarhus_path, index=False)

        # Record sizes: compression relative to the full filtered parquet
        filtered_size = f.stat().st_size
        aarhus_size   = aarhus_path.stat().st_size
        compression   = round((1 - aarhus_size / filtered_size) * 100, 1)
        aarhus_sizes[key] = (aarhus_size, compression)

        df_aarhus["date"] = pd.to_datetime(f.stem.replace("aisdk-", "")).date()
        frames.append(df_aarhus)

    if not frames:
        raise ValueError("No Aarhus rows found in filtered Parquet files.")

    _update_pipeline_status(aarhus_sizes)

    combined = pd.concat(frames, ignore_index=True)
    log.info("  Aarhus rows: %d", len(combined))
    return combined


def assign_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Label each ping with its Aarhus sub-zone.

    outer_approach and north_terminal use simple bounding-box tests.
    anchorage and south_terminal share the same lat band (56.14-56.17) and are
    separated by a diagonal line defined in ANCHORAGE_DIAGONAL:
      west of diagonal → anchorage, east of diagonal → south_terminal.

    Pings that fall in no zone (the NW corner open-water gap) are dropped.
    Returns a copy of df with a new string column 'zone'.
    """
    df = df.copy()
    df["zone"] = pd.NA
    lat = df["Latitude"]
    lon = df["Longitude"]

    # outer_approach and north_terminal: plain bbox
    for name in ("outer_approach", "north_terminal"):
        lat_min, lat_max, lon_min, lon_max = AARHUS_ZONES[name]
        mask = lat.between(lat_min, lat_max) & lon.between(lon_min, lon_max)
        df.loc[mask & df["zone"].isna(), "zone"] = name

    # anchorage / south_terminal: shared lat band, split by diagonal
    lat_min = AARHUS_ZONES["anchorage"][0]
    lat_max = AARHUS_ZONES["anchorage"][1]
    lon_min = AARHUS_ZONES["anchorage"][2]
    lon_max = AARHUS_ZONES["south_terminal"][3]
    band = lat.between(lat_min, lat_max) & lon.between(lon_min, lon_max)

    (lat_a, lon_a), (lat_b, lon_b) = ANCHORAGE_DIAGONAL
    lon_boundary = lon_a + (lat - lat_a) * (lon_b - lon_a) / (lat_b - lat_a)

    df.loc[band & (lon <  lon_boundary) & df["zone"].isna(), "zone"] = "anchorage"
    df.loc[band & (lon >= lon_boundary) & df["zone"].isna(), "zone"] = "south_terminal"

    n_unzoned = df["zone"].isna().sum()
    if n_unzoned:
        log.debug("  Dropped %d unzoned pings (NW channel gap)", n_unzoned)

    return df[df["zone"].notna()].copy()


def compute_zone_hourly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unique vessel count per (zone, date, hour, vessel_type).

    Direct per-zone analogue of port_hourly_stats — same grain, same query
    pattern: SUM(vessel_count) GROUP BY zone, date, hour before AVG/MAX.
    Adding vessel_type enables type-stratified zone congestion analysis
    (e.g. "how many cargo vessels are queuing in the anchorage at 14:00?").
    """
    df = df.copy()
    df["hour"] = pd.to_datetime(df["Timestamp"]).dt.hour
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))

    stats = (
        df.groupby(["zone", "date", "hour", "vessel_type"])["MMSI"]
        .nunique()
        .reset_index()
        .rename(columns={"MMSI": "vessel_count"})
    )
    return stats


def compute_zone_visits(df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (MMSI, zone, date) with first/last ping and dwell time.

    Vessels that cross multiple zones on the same day generate one row per
    zone — this captures zone-specific dwell, which is the key logistics KPI
    (e.g., time at anchorage = waiting time; time at north_terminal = berth
    occupancy).

    Single-ping visits are kept (dwell_minutes = 0).
    """
    df = df.copy()
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))

    visits = df.groupby(["MMSI", "zone", "date"]).agg(
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


def compute_navstatus_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ping count by navigational status per (zone, date, vessel_type).

    The at-anchor percentage in the anchorage zone is a direct congestion proxy
    (high at-anchor % = vessels queuing for berths).  The moored percentage in
    terminal zones reflects berth occupancy.  Vessel_type grain lets the
    dashboard filter navstatus proportions by vessel category.
    """
    df = df.copy()
    df["vessel_type"] = _normalise_type(df["Ship type"].fillna(""))
    nav_col = "Navigational status"
    stats = (
        df.groupby(["zone", "date", "vessel_type", nav_col])
        .size()
        .reset_index(name="ping_count")
        .rename(columns={nav_col: "nav_status"})
    )
    return stats


def compute_zone_speed_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    SOG (Speed Over Ground) distribution per (zone, date, hour, vessel_type).

    Excludes the AIS "not available" sentinel (SOG >= 102.3).  The p95 SOG in
    the outer_approach zone flags unusually fast arrivals.  Near-zero mean SOG
    in the approach combined with low vessel count indicates drifting/waiting.
    p025 and p975 bound a 95% interval used as an uncertainty ribbon in the
    speed chart.
    """
    valid = df[df["SOG"] < _SOG_SENTINEL].copy()
    valid["hour"] = pd.to_datetime(valid["Timestamp"]).dt.hour
    valid["vessel_type"] = _normalise_type(valid["Ship type"].fillna(""))

    stats = (
        valid.groupby(["zone", "date", "hour", "vessel_type"])["SOG"]
        .agg(
            sog_mean="mean",
            sog_median="median",
            sog_p025=lambda x: x.quantile(0.025),
            sog_p95=lambda x: x.quantile(0.95),
            sog_p975=lambda x: x.quantile(0.975),
        )
        .round(2)
        .reset_index()
    )
    return stats


def main() -> None:
    df_raw = load_aarhus_filtered()
    df = assign_zones(df_raw)
    log.info("Pings assigned to zones: %d (of %d Aarhus pings)", len(df), len(df_raw))

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    hourly = compute_zone_hourly_stats(df)
    out = PROCESSED_DIR / "aarhus_zone_hourly_stats.parquet"
    hourly.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(hourly), out.name)

    visits = compute_zone_visits(df)
    out = PROCESSED_DIR / "aarhus_zone_visits.parquet"
    visits.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(visits), out.name)

    navstatus = compute_navstatus_stats(df)
    out = PROCESSED_DIR / "aarhus_navstatus_stats.parquet"
    navstatus.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(navstatus), out.name)

    speed = compute_zone_speed_stats(df)
    out = PROCESSED_DIR / "aarhus_zone_speed_stats.parquet"
    speed.to_parquet(out, index=False)
    log.info("Wrote %d rows → %s", len(speed), out.name)

    log.info("Aarhus zone transform complete.")


if __name__ == "__main__":
    main()
