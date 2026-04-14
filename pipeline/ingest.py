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
    Load one day of raw AIS data from the unzipped directory.

    Parameters
    ----------
    date_str : str
        ISO date, e.g. ``"2026-02-01"``.
    chunksize : int, optional
        If provided, reads in chunks and concatenates.  Defaults to reading
        the full file in one pass (fine for exploration; use chunked mode for
        large files to control peak RAM).

    Returns
    -------
    pd.DataFrame
        Raw AIS records for the given day.  Timestamp column is parsed to
        datetime and renamed from ``# Timestamp`` to ``Timestamp``.
    """
    csv_path = UNZIPPED_DIR / f"aisdk-{date_str}" / f"aisdk-{date_str}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Raw CSV not found: {csv_path}")

    log.info("Loading %s …", csv_path.name)

    read_kwargs: dict = dict(
        usecols=USECOLS,
        dtype=DTYPE_MAP,
        low_memory=False,
        on_bad_lines="skip",
    )
    if chunksize:
        read_kwargs["chunksize"] = chunksize
        df = pd.concat(
            list(pd.read_csv(csv_path, **read_kwargs)), ignore_index=True
        )
    else:
        df = pd.read_csv(csv_path, **read_kwargs)

    df.rename(columns={"# Timestamp": "Timestamp"}, inplace=True)
    df["Timestamp"] = pd.to_datetime(
        df["Timestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )

    log.info("  Loaded %d rows from %s", len(df), date_str)
    return df
