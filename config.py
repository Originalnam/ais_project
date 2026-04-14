"""
config.py — Central configuration for the AIS pipeline.

All pipeline modules import paths, port definitions, and processing
parameters from here so there is a single source of truth.
"""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RAW_ZIPS_DIR = PROJECT_ROOT / "raw"
UNZIPPED_DIR = PROJECT_ROOT / "data" / "raw" / "unzipped"
FILTERED_DIR = PROJECT_ROOT / "data" / "processed" / "filtered"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = PROJECT_ROOT / "data" / "port_analytics.db"

# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------
CHUNK_SIZE = 200_000  # rows per chunk — fits comfortably in ~100 MB RAM

# Date range of available unzipped data
DATA_START = "2026-02-01"
DATA_END = "2026-02-28"

# ---------------------------------------------------------------------------
# Port bounding boxes  {name: (lat_min, lat_max, lon_min, lon_max)}
#
# Boxes are intentionally generous: they cover the port basin, anchorages,
# and approach channels so that arriving/departing vessels are captured.
#
#   Esbjerg    : 55.40–55.55 N, 8.35–8.55 E  (North Sea oil/cargo port)
#   Aarhus     : 56.10–56.22 N, 10.15–10.30 E (container & ferry hub)
#   Copenhagen : 55.60–55.75 N, 12.50–12.70 E (passenger, cruise, ro-ro)
#   Aalborg    : 57.00–57.10 N, 9.85–9.98 E  (industrial & cement port)
#   Fredericia : 55.53–55.62 N, 9.68–9.82 E  (oil/chemical terminal)
# ---------------------------------------------------------------------------
PORTS: dict[str, tuple[float, float, float, float]] = {
    "esbjerg":    (55.40, 55.55, 8.35,  8.55),
    "aarhus":     (56.10, 56.22, 10.15, 10.30),
    "copenhagen": (55.60, 55.75, 12.50, 12.70),
    "aalborg":    (57.00, 57.10, 9.85,  9.98),
    "fredericia": (55.53, 55.62, 9.68,  9.82),
}
