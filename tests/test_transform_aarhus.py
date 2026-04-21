"""
tests/test_transform_aarhus.py — Unit tests for pipeline/transform_aarhus.py.

Run with:
    python -m pytest tests/test_transform_aarhus.py
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import AARHUS_ZONES
from pipeline.transform_aarhus import (
    assign_zones,
    compute_navstatus_stats,
    compute_zone_hourly_stats,
    compute_zone_speed_stats,
    compute_zone_visits,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _zone_mid(zone: str) -> tuple[float, float]:
    """Return the centroid lat/lon of a named Aarhus zone."""
    lat_min, lat_max, lon_min, lon_max = AARHUS_ZONES[zone]
    return (lat_min + lat_max) / 2, (lon_min + lon_max) / 2


def _make_ping(
    mmsi: int,
    zone: str,
    timestamp: str,
    ship_type: str = "Cargo",
    sog: float = 5.0,
    nav_status: str = "Under way using engine",
    date_val: date | None = None,
) -> dict:
    """Build a minimal AIS-like ping dict centred in the given zone."""
    lat, lon = _zone_mid(zone)
    d = date_val or datetime.fromisoformat(timestamp).date()
    return {
        "MMSI": mmsi,
        "Latitude": lat,
        "Longitude": lon,
        "Timestamp": timestamp,
        "Ship type": ship_type,
        "SOG": sog,
        "Navigational status": nav_status,
        "port": "aarhus",
        "date": d,
    }


_COLUMNS = [
    "MMSI", "Latitude", "Longitude", "Timestamp", "Ship type",
    "SOG", "Navigational status", "port", "date",
]

def _make_df(pings: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(pings, columns=_COLUMNS) if not pings else pd.DataFrame(pings)
    for col, dtype in [("Latitude", "float32"), ("Longitude", "float32"), ("SOG", "float32")]:
        if col in df.columns:
            df[col] = df[col].astype(dtype)
    return df


# ---------------------------------------------------------------------------
# assign_zones
# ---------------------------------------------------------------------------

class TestAssignZones:
    def test_each_zone_centroid_labelled_correctly(self):
        """A ping at the centroid of each zone should receive that zone's label."""
        pings = [
            _make_ping(100 + i, zone, "2026-02-01 08:00:00")
            for i, zone in enumerate(AARHUS_ZONES)
        ]
        df = _make_df(pings)
        result = assign_zones(df)

        for zone in AARHUS_ZONES:
            rows = result[result["zone"] == zone]
            assert len(rows) == 1, f"Expected 1 row for zone '{zone}'"

    def test_boundary_inclusive(self):
        """Pings exactly on zone boundaries should be assigned to a zone."""
        pings = []
        for zone, (lat_min, lat_max, lon_min, lon_max) in AARHUS_ZONES.items():
            pings.append({
                "MMSI": 1, "Latitude": float(lat_min), "Longitude": float(lon_min),
                "Timestamp": "2026-02-01 08:00:00", "Ship type": "Cargo",
                "SOG": 5.0, "Navigational status": "Under way using engine",
                "port": "aarhus", "date": date(2026, 2, 1),
            })
        df = _make_df(pings)
        result = assign_zones(df)
        assert len(result) == len(AARHUS_ZONES)

    def test_unzoned_ping_dropped(self):
        """A ping in the NW corner gap (no sub-zone) should be dropped."""
        # The gap is 56.17–56.22 N, 10.15–10.18 E — clearly outside all zones.
        ping = {
            "MMSI": 999, "Latitude": 56.19, "Longitude": 10.16,
            "Timestamp": "2026-02-01 08:00:00", "Ship type": "Other",
            "SOG": 0.0, "Navigational status": "At anchor",
            "port": "aarhus", "date": date(2026, 2, 1),
        }
        df = _make_df([ping])
        result = assign_zones(df)
        assert result.empty

    def test_empty_dataframe(self):
        df = _make_df([])
        result = assign_zones(df)
        assert result.empty

    def test_zone_column_added(self):
        ping = _make_ping(1, "north_terminal", "2026-02-01 12:00:00")
        df = _make_df([ping])
        result = assign_zones(df)
        assert "zone" in result.columns
        assert result["zone"].iloc[0] == "north_terminal"

    def test_mixed_zones(self):
        """Pings from different zones are all retained and correctly labelled."""
        zones = list(AARHUS_ZONES.keys())
        pings = [_make_ping(i, z, "2026-02-01 10:00:00") for i, z in enumerate(zones)]
        df = _make_df(pings)
        result = assign_zones(df)
        assert set(result["zone"].unique()) == set(zones)


# ---------------------------------------------------------------------------
# compute_zone_hourly_stats
# ---------------------------------------------------------------------------

class TestComputeZoneHourlyStats:
    def _base_df(self) -> pd.DataFrame:
        pings = [
            _make_ping(1, "north_terminal", "2026-02-01 08:00:00"),
            _make_ping(2, "north_terminal", "2026-02-01 08:30:00"),
            _make_ping(1, "north_terminal", "2026-02-01 09:00:00"),  # same MMSI, next hour
            _make_ping(3, "anchorage",      "2026-02-01 08:00:00"),
        ]
        df = _make_df(pings)
        return assign_zones(df)

    def test_vessel_count_is_unique_mmsi(self):
        """vessel_count should count distinct MMSIs per (zone, date, hour)."""
        result = compute_zone_hourly_stats(self._base_df())
        row = result[(result["zone"] == "north_terminal") &
                     (result["hour"] == 8)].iloc[0]
        assert row["vessel_count"] == 2  # MMSIs 1 and 2

    def test_different_hours_split(self):
        """Same MMSI in two different hours yields two rows."""
        result = compute_zone_hourly_stats(self._base_df())
        north = result[result["zone"] == "north_terminal"]
        assert set(north["hour"].tolist()) == {8, 9}

    def test_output_columns(self):
        result = compute_zone_hourly_stats(self._base_df())
        assert set(result.columns) >= {"zone", "date", "hour", "vessel_count"}


# ---------------------------------------------------------------------------
# compute_zone_visits
# ---------------------------------------------------------------------------

class TestComputeZoneVisits:
    def _base_df(self) -> pd.DataFrame:
        pings = [
            _make_ping(10, "north_terminal", "2026-02-01 08:00:00", ship_type="Cargo ship"),
            _make_ping(10, "north_terminal", "2026-02-01 10:00:00", ship_type="Cargo ship"),
            _make_ping(10, "anchorage",      "2026-02-01 06:00:00", ship_type="Cargo ship"),
        ]
        return assign_zones(_make_df(pings))

    def test_one_row_per_mmsi_zone_date(self):
        result = compute_zone_visits(self._base_df())
        assert len(result) == 2  # one row for north_terminal, one for anchorage

    def test_dwell_minutes_correct(self):
        result = compute_zone_visits(self._base_df())
        row = result[result["zone"] == "north_terminal"].iloc[0]
        assert row["dwell_minutes"] == pytest.approx(120.0)

    def test_single_ping_dwell_is_zero(self):
        pings = [_make_ping(20, "anchorage", "2026-02-01 07:00:00")]
        df = assign_zones(_make_df(pings))
        result = compute_zone_visits(df)
        assert result.iloc[0]["dwell_minutes"] == 0.0

    def test_output_columns(self):
        result = compute_zone_visits(self._base_df())
        expected = {"MMSI", "zone", "date", "first_seen", "last_seen",
                    "vessel_type", "ping_count", "dwell_minutes"}
        assert expected.issubset(set(result.columns))


# ---------------------------------------------------------------------------
# compute_navstatus_stats
# ---------------------------------------------------------------------------

class TestComputeNavstatusStats:
    def _base_df(self) -> pd.DataFrame:
        pings = [
            _make_ping(1, "anchorage", "2026-02-01 06:00:00", nav_status="At anchor"),
            _make_ping(2, "anchorage", "2026-02-01 07:00:00", nav_status="At anchor"),
            _make_ping(3, "anchorage", "2026-02-01 08:00:00", nav_status="Under way using engine"),
        ]
        return assign_zones(_make_df(pings))

    def test_ping_counts_correct(self):
        result = compute_navstatus_stats(self._base_df())
        at_anchor = result[
            (result["zone"] == "anchorage") & (result["nav_status"] == "At anchor")
        ]
        assert at_anchor["ping_count"].sum() == 2

    def test_output_columns(self):
        result = compute_navstatus_stats(self._base_df())
        assert set(result.columns) >= {"zone", "date", "vessel_type", "nav_status", "ping_count"}

    def test_vessel_type_splits_rows(self):
        """Pings with different ship types in the same (zone,date,nav_status) split into separate rows."""
        pings = [
            _make_ping(1, "anchorage", "2026-02-01 06:00:00", ship_type="Cargo",  nav_status="At anchor"),
            _make_ping(2, "anchorage", "2026-02-01 07:00:00", ship_type="Tanker", nav_status="At anchor"),
        ]
        result = compute_navstatus_stats(assign_zones(_make_df(pings)))
        at_anchor = result[
            (result["zone"] == "anchorage") & (result["nav_status"] == "At anchor")
        ]
        assert set(at_anchor["vessel_type"]) == {"Cargo", "Tanker"}
        assert at_anchor["ping_count"].sum() == 2


# ---------------------------------------------------------------------------
# compute_zone_speed_stats
# ---------------------------------------------------------------------------

class TestComputeZoneSpeedStats:
    def _base_df(self) -> pd.DataFrame:
        pings = [
            _make_ping(1, "outer_approach", "2026-02-01 08:00:00", sog=6.0),
            _make_ping(2, "outer_approach", "2026-02-01 08:15:00", sog=8.0),
            _make_ping(3, "outer_approach", "2026-02-01 08:30:00", sog=102.3),  # sentinel
        ]
        return assign_zones(_make_df(pings))

    def test_sog_sentinel_excluded(self):
        """SOG >= 102.3 (AIS sentinel) must not affect stats."""
        result = compute_zone_speed_stats(self._base_df())
        row = result[(result["zone"] == "outer_approach") & (result["hour"] == 8)].iloc[0]
        # mean of [6.0, 8.0] = 7.0; if sentinel included → mean > 7
        assert row["sog_mean"] == pytest.approx(7.0, abs=0.1)

    def test_output_columns(self):
        result = compute_zone_speed_stats(self._base_df())
        assert set(result.columns) >= {
            "zone", "date", "hour", "vessel_type",
            "sog_mean", "sog_median", "sog_p025", "sog_p95", "sog_p975",
        }

    def test_percentile_order(self):
        """Quantile columns must respect p025 <= median <= p975."""
        pings = [
            _make_ping(i, "outer_approach", "2026-02-01 08:00:00", sog=float(i))
            for i in range(1, 21)
        ]
        result = compute_zone_speed_stats(assign_zones(_make_df(pings)))
        row = result[(result["zone"] == "outer_approach") & (result["hour"] == 8)].iloc[0]
        assert row["sog_p025"] <= row["sog_median"] <= row["sog_p975"]
        assert row["sog_p025"] <= row["sog_p95"] <= row["sog_p975"]

    def test_vessel_type_splits_rows(self):
        """Different ship types at the same (zone,date,hour) produce separate rows."""
        pings = [
            _make_ping(1, "outer_approach", "2026-02-01 08:00:00", sog=6.0, ship_type="Cargo"),
            _make_ping(2, "outer_approach", "2026-02-01 08:15:00", sog=8.0, ship_type="Tanker"),
        ]
        result = compute_zone_speed_stats(assign_zones(_make_df(pings)))
        hour8 = result[(result["zone"] == "outer_approach") & (result["hour"] == 8)]
        assert set(hour8["vessel_type"]) == {"Cargo", "Tanker"}

    def test_all_sentinel_hour_dropped(self):
        """An hour with only sentinel SOG values should produce no row."""
        pings = [_make_ping(1, "outer_approach", "2026-02-01 09:00:00", sog=102.3)]
        df = assign_zones(_make_df(pings))
        result = compute_zone_speed_stats(df)
        assert result.empty
