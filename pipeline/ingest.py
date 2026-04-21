"""
ingest.py — Load a single day of raw AIS data into a pandas DataFrame.

This is a thin wrapper around pandas.read_csv.  Heavy processing (filtering,
chunking) happens in filter.py; ingest.py is only responsible for locating the
raw CSV and returning it as a DataFrame for ad-hoc / notebook use.

Usage
-----
    from pipeline.ingest import load_day
    df = load_day("2026-02-01")
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import UNZIPPED_DIR

log = logging.getLogger(__name__)

# Columns present in every DMA CSV that are worth loading.
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


def load_day(date_str: str, chunksize: int | None = None) -> pd.DataFrame:
    """
    Load raw AIS data from the unzipped directory.

    Parameters
    ----------
    date_str : str
        ``"YYYY-MM-DD"`` for a daily folder or ``"YYYY-MM"`` for a monthly
        folder.  Monthly folders contain multiple CSV files (one per day in
        the month); all are concatenated into a single DataFrame.
    chunksize : int, optional
        If provided, reads each file in chunks before concatenating.  Useful
        for controlling peak RAM on large monthly files.

    Returns
    -------
    pd.DataFrame
        Raw AIS records.  Timestamp column is parsed to datetime and renamed
        from ``# Timestamp`` to ``Timestamp``.
    """
    folder = UNZIPPED_DIR / f"aisdk-{date_str}"
    if not folder.exists():
        raise FileNotFoundError(f"Unzipped folder not found: {folder}")

    # Daily folders: aisdk-YYYY-MM-DD.csv
    # Monthly folders: aisdk_YYYYMMDD.csv  (one file per day in the month)
    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {folder}")

    log.info("Loading %s (%d file(s)) …", date_str, len(csv_files))

    read_kwargs: dict = dict(
        usecols=USECOLS,
        dtype=DTYPE_MAP,
        low_memory=False,
        on_bad_lines="skip",
    )

    parts: list[pd.DataFrame] = []
    for csv_path in csv_files:
        if chunksize:
            read_kwargs["chunksize"] = chunksize
            df_part = pd.concat(
                list(pd.read_csv(csv_path, **read_kwargs)), ignore_index=True
            )
        else:
            df_part = pd.read_csv(csv_path, **read_kwargs)
        parts.append(df_part)

    df = pd.concat(parts, ignore_index=True) if len(parts) > 1 else parts[0]

    df.rename(columns={"# Timestamp": "Timestamp"}, inplace=True)
    df["Timestamp"] = pd.to_datetime(
        df["Timestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    log.info("  Loaded %d rows from %s", len(df), date_str)
    return df
