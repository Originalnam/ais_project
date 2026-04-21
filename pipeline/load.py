"""
load.py — Write transform outputs to SQLite (portfolio) or Azure SQL (production).

Reads the three Parquet files produced by transform.py and loads them into a
SQLite database at data/port_analytics.db.  The connection string is the only
thing that needs to change to point at Azure SQL instead.

Usage
-----
    python -m pipeline.load
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PROCESSED_DIR, DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Map Parquet filename → target table name
_TABLES = {
    "hourly_stats.parquet":                "port_hourly_stats",
    "vessel_visits.parquet":               "vessel_visits",
    "type_distribution.parquet":           "type_distribution",
    # New port-level tables (produced by pipeline/transform.py)
    "port_navstatus_stats.parquet":        "port_navstatus_stats",
    "port_speed_stats.parquet":            "port_speed_stats",
    "port_daily_flow.parquet":             "port_daily_flow",
    # Aarhus sub-zone tables (produced by pipeline/transform_aarhus.py)
    "aarhus_zone_hourly_stats.parquet":    "aarhus_zone_hourly_stats",
    "aarhus_zone_visits.parquet":          "aarhus_zone_visits",
    "aarhus_navstatus_stats.parquet":      "aarhus_navstatus_stats",
    "aarhus_zone_speed_stats.parquet":     "aarhus_zone_speed_stats",
}


def load_to_sqlite(db_path: Path = DB_PATH) -> None:
    """Load all transform Parquet outputs into SQLite."""
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        for filename, table in _TABLES.items():
            parquet_path = PROCESSED_DIR / filename
            if not parquet_path.exists():
                log.warning("Missing: %s — skipped (run transform.py first)", parquet_path.name)
                continue

            df = pd.read_parquet(parquet_path)
            df.to_sql(table, conn, if_exists="replace", index=False)
            log.info("Loaded %d rows → %s", len(df), table)

        conn.commit()
        log.info("Database written to %s", db_path)
    finally:
        conn.close()


def main() -> None:
    load_to_sqlite()


if __name__ == "__main__":
    main()
