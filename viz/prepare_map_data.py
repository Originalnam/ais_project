"""
prepare_map_data.py
-------------------
Reads one day of raw AIS data, downsamples it to a browser-friendly JSON file
for the interactive map visualisation.

Usage:
    python viz/prepare_map_data.py
    python viz/prepare_map_data.py --date 2026-02-01 --out viz/data/vessels_2026-02-01.json
"""

import argparse
import json
import os
import sys

import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────
BBOX = dict(lat_min=54.0, lat_max=62.0, lon_min=7.0, lon_max=17.0)

COLS = [
    '# Timestamp', 'Type of mobile', 'MMSI',
    'Latitude', 'Longitude', 'Navigational status',
    'SOG', 'Ship type', 'Name',
]

# Keep only real vessels (exclude base stations, AtoN beacons, etc.)
MOBILE_TYPES = {'Class A', 'Class B'}

# Downsample bucket size in minutes — one representative point per bucket
BUCKET_MINUTES = 10

# Minimum number of downsampled points to include a vessel (ensures visible routes)
MIN_POINTS = 10

# Ship types to label explicitly; everything else → "Other"
TOP_TYPES = [
    'Cargo', 'Tanker', 'Passenger', 'Fishing',
    'Tug', 'Pleasure Craft', 'HSC', 'SAR',
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def normalise_type(raw: str) -> str:
    """Map raw ship type string to one of TOP_TYPES or 'Other'."""
    if not isinstance(raw, str):
        return 'Other'
    for t in TOP_TYPES:
        if t.lower() in raw.lower():
            return t
    return 'Other'


def minutes_from_midnight(ts: pd.Series) -> pd.Series:
    return ts.dt.hour * 60 + ts.dt.minute


# ── Main ──────────────────────────────────────────────────────────────────────

def prepare(date: str, data_dir: str, out_path: str) -> None:
    csv_path = os.path.join(data_dir, f'aisdk-{date}', f'aisdk-{date}.csv')
    if not os.path.exists(csv_path):
        sys.exit(f'ERROR: data file not found: {csv_path}')

    print(f'Reading {csv_path} …')
    chunks = []
    total_rows = 0
    for chunk in pd.read_csv(csv_path, usecols=COLS, chunksize=200_000, low_memory=False):
        total_rows += len(chunk)
        mask = (
            chunk['Type of mobile'].isin(MOBILE_TYPES) &
            chunk['Latitude'].between(BBOX['lat_min'], BBOX['lat_max']) &
            chunk['Longitude'].between(BBOX['lon_min'], BBOX['lon_max'])
        )
        chunks.append(chunk[mask])

    df = pd.concat(chunks, ignore_index=True)
    print(f'  Total rows read   : {total_rows:,}')
    print(f'  After bbox filter : {len(df):,}  ({df["MMSI"].nunique():,} vessels)')

    # Parse timestamp → minutes from midnight
    df['timestamp'] = pd.to_datetime(df['# Timestamp'], dayfirst=True)
    df['t'] = minutes_from_midnight(df['timestamp'])

    # Normalise ship type
    df['type'] = df['Ship type'].map(normalise_type)

    # Downsample: one point per BUCKET_MINUTES per vessel
    df['bucket'] = (df['t'] // BUCKET_MINUTES).astype(int)
    df_down = (
        df.sort_values('timestamp')
          .groupby(['MMSI', 'bucket'], sort=False)
          .first()
          .reset_index()
    )

    # Vessel-level metadata (most common name / type)
    meta = (
        df.groupby('MMSI')
          .agg(
              name=('Name', lambda s: s.dropna().mode().iloc[0] if s.dropna().any() else ''),
              type=('type', lambda s: s.mode().iloc[0]),
          )
          .reset_index()
    )

    # Build per-vessel track lists and filter by MIN_POINTS
    vessels = []
    for mmsi, grp in df_down.groupby('MMSI'):
        grp = grp.sort_values('t')
        if len(grp) < MIN_POINTS:
            continue

        row_meta = meta[meta['MMSI'] == mmsi].iloc[0]
        track = [
            {
                't': int(r['t']),
                'lat': round(float(r['Latitude']), 5),
                'lon': round(float(r['Longitude']), 5),
                'sog': round(float(r['SOG']), 1) if pd.notna(r['SOG']) else None,
            }
            for _, r in grp.iterrows()
        ]
        vessels.append({
            'mmsi': int(mmsi),
            'name': str(row_meta['name']).strip(),
            'type': str(row_meta['type']),
            'track': track,
        })

    print(f'  Vessels in output : {len(vessels):,}  (>= {MIN_POINTS} downsampled points)')

    payload = {
        'date': date,
        'bbox': BBOX,
        'bucket_minutes': BUCKET_MINUTES,
        'ship_types': TOP_TYPES + ['Other'],
        'vessels': vessels,
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write('window.VESSEL_DATA=')
        json.dump(payload, f, separators=(',', ':'))
        f.write(';')

    size_mb = os.path.getsize(out_path) / 1_048_576
    print(f'  Written -> {out_path}  ({size_mb:.1f} MB)')


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prepare AIS map data JSON')
    parser.add_argument('--date', default='2026-02-01', help='YYYY-MM-DD')
    parser.add_argument(
        '--data-dir',
        default=os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'unzipped'),
        help='Root folder containing aisdk-YYYY-MM-DD subfolders',
    )
    parser.add_argument(
        '--out',
        default=None,
        help='Output JSON path (default: viz/data/vessels-<date>.json)',
    )
    args = parser.parse_args()

    out = args.out or os.path.join(
        os.path.dirname(__file__), 'data', f'vessels_{args.date}.js'
    )
    prepare(args.date, args.data_dir, out)
