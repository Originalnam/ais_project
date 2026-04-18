"""
prepare_aarhus_data.py — Export Aarhus sub-zone analytics to JSON for the D3 dashboard.

Reads data/port_analytics.db (aarhus_* tables) and writes
viz/data/aarhus_analytics.json with seven datasets consumed by
viz/aarhus_dashboard.html:

  zone_daily_traffic       — vessel count per zone per day (stacked area chart)
  zone_type_mix            — visit count per zone/vessel-type (small-multiple donuts)
  dwell_by_zone_type       — avg dwell hours per zone/vessel-type (grouped bar)
  navstatus_by_zone        — ping count + pct per zone/nav-status (stacked % bar)
  zone_speed_all           — hourly SOG mean/median/p95 for all four zones (multi-line)
  zone_congestion_heatmap  — avg vessel count by dow × hour per zone (heatmap)
  summary                  — scalar KPI cards

Usage
-----
    python viz/prepare_aarhus_data.py
    python viz/prepare_aarhus_data.py --db data/port_analytics.db --out viz/data/aarhus_analytics.json
    python viz/prepare_aarhus_data.py --date-from 2026-02-01 --date-to 2026-02-28
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB  = PROJECT_ROOT / "data" / "port_analytics.db"
DEFAULT_OUT = PROJECT_ROOT / "viz" / "data" / "aarhus_analytics.json"

sys.path.insert(0, str(PROJECT_ROOT))
from config import AARHUS_ZONES, ANCHORAGE_DIAGONAL  # noqa: E402


def _zone_polygons() -> dict[str, list[list[float]]]:
    """Rebuild the four sub-zone polygons (anchorage / south_terminal share a
    diagonal) from config.AARHUS_ZONES + ANCHORAGE_DIAGONAL. [lat, lon] vertices
    ordered CCW so Leaflet renders them correctly.
    """
    (s_lat, s_lon), (n_lat, n_lon) = ANCHORAGE_DIAGONAL

    zones: dict[str, list[list[float]]] = {}
    for name, (lat_min, lat_max, lon_min, lon_max) in AARHUS_ZONES.items():
        if name == "anchorage":
            # western quadrilateral — diagonal on east side
            zones[name] = [
                [lat_min, lon_min],
                [lat_min, s_lon],
                [lat_max, n_lon],
                [lat_max, lon_min],
            ]
        elif name == "south_terminal":
            # eastern quadrilateral — diagonal on west side
            zones[name] = [
                [lat_min, s_lon],
                [lat_min, lon_max],
                [lat_max, lon_max],
                [lat_max, n_lon],
            ]
        else:
            zones[name] = [
                [lat_min, lon_min],
                [lat_min, lon_max],
                [lat_max, lon_max],
                [lat_max, lon_min],
            ]
    return zones

ZONE_LABELS = {
    "outer_approach": "Outer Approach",
    "anchorage":      "Anchorage",
    "south_terminal": "South Terminal (Ferry/RoRo)",
    "north_terminal": "North Terminal (Container)",
}

ZONE_ORDER = ["outer_approach", "anchorage", "south_terminal", "north_terminal"]


def fetch(conn: sqlite3.Connection, sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_one(conn: sqlite3.Connection, sql: str) -> dict | None:
    rows = fetch(conn, sql)
    return rows[0] if rows else None


def build_aarhus(db_path: Path, out_path: Path,
                 date_from: str = "2026-02-01",
                 date_to: str   = "2026-02-28") -> None:
    conn = sqlite3.connect(db_path)
    # Applied to every query that has a bare `date` column.
    df = f"date BETWEEN '{date_from}' AND '{date_to}'"

    # -------------------------------------------------------------------------
    # 1. Zone daily traffic — unique vessel count per zone per day.
    #    Drives the stacked area chart (time series per zone).
    #    Keeps vessel_type in the grain so the dashboard can filter by type.
    # -------------------------------------------------------------------------
    zone_daily_traffic = fetch(conn, f"""
        SELECT zone, date, vessel_type, SUM(vessel_count) AS vessel_count
        FROM aarhus_zone_hourly_stats
        WHERE {df}
        GROUP BY zone, date, vessel_type
        ORDER BY date, zone, vessel_type
    """)

    # -------------------------------------------------------------------------
    # 2. Vessel type mix per zone — visit count per (zone, vessel_type).
    #    Powers the four small-multiple donuts.
    # -------------------------------------------------------------------------
    zone_type_mix = fetch(conn, f"""
        SELECT zone, vessel_type, COUNT(*) AS visit_count
        FROM aarhus_zone_visits
        WHERE {df}
        GROUP BY zone, vessel_type
        ORDER BY zone, visit_count DESC
    """)

    # -------------------------------------------------------------------------
    # 3. Dwell time by zone and vessel type.
    #    Grouped bar: avg hours per vessel type, one group per zone.
    #    Single-ping visits (dwell = 0) excluded — they represent transits.
    # -------------------------------------------------------------------------
    dwell_by_zone_type = fetch(conn, f"""
        SELECT
            zone,
            vessel_type,
            COUNT(*)                            AS visit_count,
            ROUND(AVG(dwell_minutes), 1)        AS avg_dwell_minutes,
            ROUND(AVG(dwell_minutes) / 60.0, 2) AS avg_dwell_hours
        FROM aarhus_zone_visits
        WHERE dwell_minutes > 0 AND {df}
        GROUP BY zone, vessel_type
        ORDER BY zone, avg_dwell_minutes DESC
    """)

    # -------------------------------------------------------------------------
    # 4. Navigational status by zone — raw ping counts per (zone, vessel_type,
    #    nav_status). Exported at the fullest grain so the dashboard can filter
    #    by vessel_type and recompute the per-zone percentages client-side.
    # -------------------------------------------------------------------------
    navstatus_by_zone = fetch(conn, f"""
        SELECT zone, vessel_type, nav_status, SUM(ping_count) AS ping_count
        FROM aarhus_navstatus_stats
        WHERE {df}
        GROUP BY zone, vessel_type, nav_status
        ORDER BY zone, vessel_type, ping_count DESC
    """)

    # -------------------------------------------------------------------------
    # 5. Speed profile for all four zones — hourly SOG stats averaged over month.
    #    Multi-line chart: one line per zone shows each zone's speed fingerprint.
    #    outer_approach: 5-15 kn (transiting); anchorage: ~0 kn (waiting);
    #    terminals: 0-2 kn (manoeuvring/moored).
    #    Keeps vessel_type in the grain; dashboard averages over it when the
    #    type filter is "all". p025/p975 drive the 95% confidence band.
    # -------------------------------------------------------------------------
    zone_speed_all = fetch(conn, f"""
        SELECT
            zone,
            hour,
            vessel_type,
            ROUND(AVG(sog_mean),   2) AS sog_mean,
            ROUND(AVG(sog_median), 2) AS sog_median,
            ROUND(AVG(sog_p025),   2) AS sog_p025,
            ROUND(AVG(sog_p95),    2) AS sog_p95,
            ROUND(AVG(sog_p975),   2) AS sog_p975
        FROM aarhus_zone_speed_stats
        WHERE {df}
        GROUP BY zone, hour, vessel_type
        ORDER BY zone, hour, vessel_type
    """)

    # -------------------------------------------------------------------------
    # 6. Zone congestion heatmap — avg vessel count by day-of-week and hour.
    #    SQLite strftime('%w') returns 0=Sunday; remapped to 0=Monday.
    #    Keeps vessel_type in the grain so the dashboard can filter by type
    #    (it sums across vessel_type per (zone,date,hour) before averaging).
    # -------------------------------------------------------------------------
    zone_congestion_heatmap = fetch(conn, f"""
        SELECT
            zone,
            vessel_type,
            (CAST(strftime('%w', date) AS INTEGER) + 6) % 7 AS dow,
            hour,
            ROUND(AVG(vessel_count), 2)                       AS avg_vessel_count
        FROM aarhus_zone_hourly_stats
        WHERE {df}
        GROUP BY zone, vessel_type, dow, hour
        ORDER BY zone, vessel_type, dow, hour
    """)

    # -------------------------------------------------------------------------
    # 7. Summary KPIs — one scalar per card.
    # -------------------------------------------------------------------------
    # Total vessel-hours and peak hourly count across all zones.
    agg = fetch_one(conn, f"""
        WITH hourly_totals AS (
            SELECT zone, date, hour, SUM(vessel_count) AS vessel_count
            FROM aarhus_zone_hourly_stats
            WHERE {df}
            GROUP BY zone, date, hour
        )
        SELECT
            SUM(vessel_count) AS total_vessel_hours,
            MAX(vessel_count) AS peak_hourly_count
        FROM hourly_totals
    """)

    # Busiest zone by total vessel-hours.
    busiest_zone = fetch_one(conn, f"""
        WITH zone_totals AS (
            SELECT zone, SUM(vessel_count) AS total
            FROM aarhus_zone_hourly_stats
            WHERE {df}
            GROUP BY zone
        )
        SELECT zone, total AS total_vessel_hours
        FROM zone_totals
        ORDER BY total DESC
        LIMIT 1
    """)

    # Avg anchorage dwell for Cargo and Tanker — the logistics waiting-time KPI.
    anchorage_dwell = fetch_one(conn, f"""
        SELECT ROUND(AVG(dwell_minutes) / 60.0, 1) AS avg_anchorage_dwell_cargo_hours
        FROM aarhus_zone_visits
        WHERE zone = 'anchorage'
          AND vessel_type IN ('Cargo', 'Tanker')
          AND dwell_minutes > 0
          AND {df}
    """)

    # % moored in anchorage — proxy for vessels occupying waiting berths.
    # "At anchor" is rarely transmitted in Danish coastal AIS; "Moored" is
    # the dominant stationary status in this dataset.
    at_anchor = fetch_one(conn, f"""
        WITH total AS (
            SELECT SUM(ping_count) AS n
            FROM aarhus_navstatus_stats
            WHERE zone = 'anchorage'
              AND date BETWEEN '{date_from}' AND '{date_to}'
        )
        SELECT ROUND(100.0 * SUM(n.ping_count) / t.n, 1) AS pct_moored_in_anchorage
        FROM aarhus_navstatus_stats n, total t
        WHERE n.zone = 'anchorage'
          AND n.nav_status = 'Moored'
          AND n.date BETWEEN '{date_from}' AND '{date_to}'
    """)

    conn.close()

    summary = {
        "total_vessel_hours":              (agg or {}).get("total_vessel_hours"),
        "peak_hourly_count":               (agg or {}).get("peak_hourly_count"),
        "busiest_zone":                    (busiest_zone or {}).get("zone"),
        "busiest_zone_vessel_hours":       (busiest_zone or {}).get("total_vessel_hours"),
        "avg_anchorage_dwell_cargo_hours": (anchorage_dwell or {}).get("avg_anchorage_dwell_cargo_hours"),
        "pct_moored_in_anchorage":         (at_anchor or {}).get("pct_moored_in_anchorage"),
    }

    payload = {
        "generated":              str(Path(db_path).stat().st_mtime),
        "date_from":              date_from,
        "date_to":                date_to,
        "zones":                  ZONE_ORDER,
        "zone_labels":            ZONE_LABELS,
        "zone_polygons":          _zone_polygons(),
        "summary":                summary,
        "zone_daily_traffic":     zone_daily_traffic,
        "zone_type_mix":          zone_type_mix,
        "dwell_by_zone_type":     dwell_by_zone_type,
        "navstatus_by_zone":      navstatus_by_zone,
        "zone_speed_all":         zone_speed_all,
        "zone_congestion_heatmap": zone_congestion_heatmap,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")
    print(f"  zone_daily_traffic      : {len(zone_daily_traffic)} rows (grain: zone×date×vessel_type)")
    print(f"  zone_type_mix           : {len(zone_type_mix)} rows")
    print(f"  dwell_by_zone_type      : {len(dwell_by_zone_type)} rows")
    print(f"  navstatus_by_zone       : {len(navstatus_by_zone)} rows")
    print(f"  zone_speed_all          : {len(zone_speed_all)} rows")
    print(f"  zone_congestion_heatmap : {len(zone_congestion_heatmap)} rows")
    print(f"  summary.busiest_zone    : {summary['busiest_zone']}")
    print(f"  summary.pct_moored      : {summary['pct_moored_in_anchorage']}%")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",        type=Path, default=DEFAULT_DB)
    p.add_argument("--out",       type=Path, default=DEFAULT_OUT)
    p.add_argument("--date-from", type=str,  default="2026-02-01",
                   help="Start of date range (YYYY-MM-DD, inclusive)")
    p.add_argument("--date-to",   type=str,  default="2026-02-28",
                   help="End of date range (YYYY-MM-DD, inclusive)")
    args = p.parse_args()
    build_aarhus(args.db, args.out, args.date_from, args.date_to)


if __name__ == "__main__":
    main()
