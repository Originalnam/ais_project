"""
tests/test_filter.py — Unit tests for pipeline/filter.py.

Run with:
    python -m pytest tests/test_filter.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.filter import _bbox_mask, _port_label, PORTS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal AIS-like DataFrame with Latitude and Longitude columns."""
    return pd.DataFrame(rows, columns=["Latitude", "Longitude"]).astype(
        {"Latitude": "float32", "Longitude": "float32"}
    )


# Pick a known port for parametric tests: esbjerg (55.40–55.55, 8.35–8.55)
_PORT = "esbjerg"
_LAT_MID = (PORTS[_PORT][0] + PORTS[_PORT][1]) / 2
_LON_MID = (PORTS[_PORT][2] + PORTS[_PORT][3]) / 2
_LAT_OUT = 60.0   # clearly outside all port boxes
_LON_OUT = 15.0


# ---------------------------------------------------------------------------
# _bbox_mask
# ---------------------------------------------------------------------------

class TestBboxMask:
    def test_inside_returns_true(self):
        df = _make_df([{"Latitude": _LAT_MID, "Longitude": _LON_MID}])
        assert bool(_bbox_mask(df).iloc[0])

    def test_outside_returns_false(self):
        df = _make_df([{"Latitude": _LAT_OUT, "Longitude": _LON_OUT}])
        assert not _bbox_mask(df).iloc[0]

    def test_mixed_rows(self):
        df = _make_df([
            {"Latitude": _LAT_MID, "Longitude": _LON_MID},  # inside
            {"Latitude": _LAT_OUT, "Longitude": _LON_OUT},  # outside
        ])
        mask = _bbox_mask(df)
        assert mask.iloc[0]
        assert not mask.iloc[1]

    def test_boundary_inclusive(self):
        """Rows exactly on the bounding-box edge should be included."""
        lat_min, lat_max, lon_min, lon_max = PORTS[_PORT]
        df = _make_df([
            {"Latitude": lat_min, "Longitude": lon_min},
            {"Latitude": lat_max, "Longitude": lon_max},
        ])
        assert _bbox_mask(df).all()

    def test_empty_dataframe(self):
        df = _make_df([])
        assert _bbox_mask(df).empty

    def test_all_ports_covered(self):
        """At least one row per port should be flagged as inside."""
        rows = [
            {"Latitude": (b[0] + b[1]) / 2, "Longitude": (b[2] + b[3]) / 2}
            for b in PORTS.values()
        ]
        df = _make_df(rows)
        assert _bbox_mask(df).all()


# ---------------------------------------------------------------------------
# _port_label
# ---------------------------------------------------------------------------

class TestPortLabel:
    def test_correct_label_assigned(self):
        df = _make_df([{"Latitude": _LAT_MID, "Longitude": _LON_MID}])
        labels = _port_label(df)
        assert labels.iloc[0] == _PORT

    def test_outside_gets_empty_string(self):
        df = _make_df([{"Latitude": _LAT_OUT, "Longitude": _LON_OUT}])
        labels = _port_label(df)
        assert labels.iloc[0] == ""

    def test_each_port_labelled_correctly(self):
        for name, (lat_min, lat_max, lon_min, lon_max) in PORTS.items():
            mid_lat = (lat_min + lat_max) / 2
            mid_lon = (lon_min + lon_max) / 2
            df = _make_df([{"Latitude": mid_lat, "Longitude": mid_lon}])
            assert _port_label(df).iloc[0] == name, f"Wrong label for {name}"

    def test_multiple_rows_multiple_ports(self):
        rows = [
            {"Latitude": (b[0] + b[1]) / 2, "Longitude": (b[2] + b[3]) / 2}
            for b in PORTS.values()
        ]
        df = _make_df(rows)
        labels = _port_label(df)
        assert list(labels) == list(PORTS.keys())
