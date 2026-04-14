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
    "hourly_stats.parquet":      "port_hourly_stats",
    "vessel_visits.parquet":     "vessel_visits",
    "type_distribution.parquet": "type_distribution",
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
