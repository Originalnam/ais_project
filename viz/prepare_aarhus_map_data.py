"""
prepare_aarhus_map_data.py — Export full-month Aarhus vessel tracks for the interactive map.

Two data sources are combined:
  1. data/processed/filtered/*.parquet — core Aarhus bbox (lon 10.15-10.30), fast read.
  2. data/raw/unzipped/*/aisdk-*.csv   — eastern approach corridor (lon 10.30-10.55),
     filtered to vessels already seen in the core bbox.  This eliminates the "jump into
     frame" artefact for vessels approaching from the Kattegat.  Only ~137 known MMSIs
     pass the allowlist filter, so almost nothing survives from each 15M-row daily CSV.

Downsamples to 1-min buckets and writes viz/data/aarhus_vessels_feb2026.js
(sets window.AARHUS_VESSEL_DATA so the page works via file:// without a server).

Usage
-----
    python viz/prepare_aarhus_map_data.py
    python viz/prepare_aarhus_map_data.py --out viz/data/aarhus_vessels_feb2026.js
    python viz/prepare_aarhus_map_data.py --skip-approach  # skip raw CSV step (faster)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from config import FILTERED_DIR, UNZIPPED_DIR, AARHUS_ZONES, ANCHORAGE_DIAGONAL

DEFAULT_OUT = PROJECT_ROOT / "viz" / "data" / "aarhus_vessels_feb2026.js"

# ── Constants ─────────────────────────────────────────────────────────────────
EPOCH         = pd.Timestamp("2026-02-01 00:00:00")
TOTAL_MINUTES = 28 * 1440                        # 40 320
BUCKET_MINUTES = 1                               # one representative point per 1-min window
MIN_POINTS    = 3                                # minimum downsampled points to include vessel
MOBILE_TYPES  = {"Class A", "Class B"}
SOG_SENTINEL  = 102.3                            # AIS "speed not available" sentinel

# Core analytics bbox (must have >=1 ping here to be included in the map)
CORE_LON_MIN, CORE_LON_MAX = 10.15, 10.30
CORE_LAT_MIN, CORE_LAT_MAX = 56.10, 56.22

# Extended eastern approach corridor — captures vessels before they enter the core bbox.
# Spans from the core east edge out ~25 km into the Kattegat.
APPROACH_LAT_MIN, APPROACH_LAT_MAX = 56.05, 56.25
APPROACH_LON_MIN, APPROACH_LON_MAX = 10.30, 10.55

TOP_TYPES = ["Cargo", "Tanker", "Passenger", "Fishing", "Tug", "Pleasure Craft", "HSC", "SAR"]

ZONE_ORDER = ["outer_approach", "anchorage", "south_terminal", "north_terminal"]
ZONE_INDEX = {z: i for i, z in enumerate(ZONE_ORDER)}

# Raw CSV column names (before rename in filter.py)
RAW_COLS = ["# Timestamp", "Type of mobile", "MMSI",
            "Latitude", "Longitude", "SOG", "Ship type", "Name"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise_type(raw: str) -> str:
    if not isinstance(raw, str):
        return "Other"
    rl = raw.lower()
    for t in TOP_TYPES:
        if t.lower() in rl:
            return t
    return "Other"


def assign_zone(lat: float, lon: float) -> int | None:
    """Return zone index (0-3) for a position inside an Aarhus sub-zone, else None."""
    # outer_approach and north_terminal: plain bbox
    for name in ("outer_approach", "north_terminal"):
        lat_min, lat_max, lon_min, lon_max = AARHUS_ZONES[name]
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return ZONE_INDEX[name]

    # anchorage / south_terminal: shared lat band, split by diagonal
    lat_min = AARHUS_ZONES["anchorage"][0]
    lat_max = AARHUS_ZONES["anchorage"][1]
    lon_min = AARHUS_ZONES["anchorage"][2]
    lon_max = AARHUS_ZONES["south_terminal"][3]
    if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
        (lat_a, lon_a), (lat_b, lon_b) = ANCHORAGE_DIAGONAL
        lon_boundary = lon_a + (lat - lat_a) * (lon_b - lon_a) / (lat_b - lat_a)
        return ZONE_INDEX["anchorage"] if lon < lon_boundary else ZONE_INDEX["south_terminal"]

    return None


def load_core(parquet_files: list[Path]) -> pd.DataFrame:
    """Load Aarhus core data from filtered Parquets."""
    LOAD_COLS = ["Timestamp", "MMSI", "Latitude", "Longitude",
                 "SOG", "Ship type", "Name", "Type of mobile", "port"]
    chunks = []
    for pf in parquet_files:
        df = pd.read_parquet(pf, columns=LOAD_COLS)
        df = df[df["port"] == "aarhus"]
        if not df.empty:
            chunks.append(df)
    if not chunks:
        return pd.DataFrame()
    df = pd.concat(chunks, ignore_index=True)
    df = df[df["Type of mobile"].isin(MOBILE_TYPES)].dropna(subset=["Timestamp"])
    return df


def load_approach(dates: list[str], allowed_mmsis: set[int]) -> pd.DataFrame:
    """
    Read raw CSVs for the eastern approach corridor, restricted to known Aarhus MMSIs.

    Applying the MMSI allowlist before accumulating means only a handful of rows
    survive from each 15M-row daily CSV, keeping memory use low.
    """
    chunks = []
    for i, date in enumerate(dates):
        csv_path = UNZIPPED_DIR / f"aisdk-{date}" / f"aisdk-{date}.csv"
        if not csv_path.exists():
            continue
        print(f"  Approach corridor: day {i+1}/{len(dates)} ({date})…", end="\r")
        for chunk in pd.read_csv(
            csv_path,
            usecols=RAW_COLS,
            chunksize=200_000,
            low_memory=False,
            on_bad_lines="skip",
        ):
            mask = (
                chunk["MMSI"].isin(allowed_mmsis)
                & chunk["Latitude"].between(APPROACH_LAT_MIN, APPROACH_LAT_MAX)
                & chunk["Longitude"].between(APPROACH_LON_MIN, APPROACH_LON_MAX)
                & chunk["Type of mobile"].isin(MOBILE_TYPES)
            )
            filtered = chunk[mask]
            if not filtered.empty:
                chunks.append(filtered)

    print()  # newline after \r progress
    if not chunks:
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    df = df.rename(columns={"# Timestamp": "Timestamp"})
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%d/%m/%Y %H:%M:%S",
                                     errors="coerce")
    return df.dropna(subset=["Timestamp"])


# ── Main ──────────────────────────────────────────────────────────────────────

def prepare(out_path: Path, skip_approach: bool = False) -> None:
    parquet_files = sorted(FILTERED_DIR.glob("aisdk-2026-02-*.parquet"))
    if not parquet_files:
        sys.exit(f"ERROR: no Parquet files found in {FILTERED_DIR}")

    dates = [pf.stem.replace("aisdk-", "") for pf in parquet_files]

    # ── Step 1: Core Aarhus data from filtered Parquets ──────────────────────
    print(f"[1/3] Loading core data from {len(parquet_files)} filtered Parquets…")
    core_df = load_core(parquet_files)
    if core_df.empty:
        sys.exit("ERROR: no Aarhus rows found in filtered Parquets.")
    core_mmsis = set(core_df["MMSI"].unique())
    print(f"      {len(core_df):,} rows  |  {len(core_mmsis):,} vessels")

    # ── Step 2: Approach corridor from raw CSVs ───────────────────────────────
    if skip_approach:
        approach_df = pd.DataFrame()
        print("[2/3] Skipping approach corridor (--skip-approach).")
    else:
        print(f"[2/3] Loading approach corridor (lon {APPROACH_LON_MIN}-{APPROACH_LON_MAX})"
              f" for {len(core_mmsis)} known MMSIs…")
        approach_df = load_approach(dates, core_mmsis)
        if approach_df.empty:
            print("      No approach data found (raw CSVs missing?). Skipping.")
        else:
            # Add columns needed for combining
            approach_df["type"] = approach_df["Ship type"].apply(normalise_type)
            print(f"      {len(approach_df):,} approach rows from "
                  f"{approach_df['MMSI'].nunique():,} vessels")

    # ── Step 3: Combine, downsample, build tracks ─────────────────────────────
    print("[3/3] Downsampling and building tracks…")

    core_df["type"] = core_df["Ship type"].apply(normalise_type)

    # Standardise columns for concat
    keep_cols = ["Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "Name", "type"]
    combined = pd.concat(
        [c[keep_cols] for c in [core_df, approach_df] if not c.empty and all(col in c.columns for col in keep_cols)],
        ignore_index=True,
    )

    combined["t"] = ((combined["Timestamp"] - EPOCH).dt.total_seconds() / 60).astype("int32")
    combined = combined[(combined["t"] >= 0) & (combined["t"] < TOTAL_MINUTES)]

    # Deduplicate in case a ping is at the bbox edge and appears in both sources
    combined = combined.drop_duplicates(subset=["MMSI", "t"])

    # Downsample: one representative point per (MMSI, BUCKET_MINUTES window)
    combined["bucket"] = (combined["t"] // BUCKET_MINUTES).astype("int32")
    df_down = (
        combined.sort_values("Timestamp")
                .groupby(["MMSI", "bucket"], sort=False)
                .first()
                .reset_index()
    )

    # Vessel metadata (dominant name/type)
    meta = (
        df_down.groupby("MMSI")
               .agg(
                   name=("Name",  lambda s: s.dropna().mode().iloc[0] if len(s.dropna()) > 0 else ""),
                   type=("type",  lambda s: s.mode().iloc[0]),
               )
               .reset_index()
    )

    # Build per-vessel track arrays
    vessels: list[dict] = []
    for mmsi, grp in df_down.groupby("MMSI"):
        grp = grp.sort_values("t")
        if len(grp) < MIN_POINTS:
            continue
        row_meta = meta.loc[meta["MMSI"] == mmsi]
        if row_meta.empty:
            continue
        row_meta = row_meta.iloc[0]

        track = []
        for _, r in grp.iterrows():
            lat = round(float(r["Latitude"]), 5)
            lon = round(float(r["Longitude"]), 5)
            pt: dict = {"t": int(r["t"]), "lat": lat, "lon": lon}

            sog = r["SOG"]
            if pd.notna(sog) and float(sog) < SOG_SENTINEL:
                pt["sog"] = round(float(sog), 1)

            z = assign_zone(lat, lon)
            if z is not None:
                pt["z"] = z

            track.append(pt)

        vessels.append({
            "mmsi": int(mmsi),
            "name": str(row_meta["name"]).strip(),
            "type": str(row_meta["type"]),
            "track": track,
        })

    print(f"      {len(vessels):,} vessels  (>= {MIN_POINTS} downsampled points)")

    payload = {
        "start_date":     "2026-02-01",
        "end_date":       "2026-02-28",
        "bucket_minutes": BUCKET_MINUTES,
        "total_minutes":  TOTAL_MINUTES,
        "zone_order":     ZONE_ORDER,
        "ship_types":     TOP_TYPES + ["Other"],
        "vessels":        vessels,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("window.AARHUS_VESSEL_DATA=")
        json.dump(payload, f, separators=(",", ":"))
        f.write(";")

    size_mb = out_path.stat().st_size / 1_048_576
    print(f"Done.  Written -> {out_path}  ({size_mb:.1f} MB)")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="Output .js path (default: viz/data/aarhus_vessels_feb2026.js)")
    parser.add_argument("--skip-approach", action="store_true",
                        help="Skip eastern approach corridor (no raw CSV reads, faster)")
    args = parser.parse_args()
    prepare(args.out, skip_approach=args.skip_approach)
