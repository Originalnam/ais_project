"""
prepare_dashboard_data.py — Export SQLite analytics to JSON for the D3 dashboard.

Reads data/port_analytics.db and writes viz/data/dashboard.json with seven
datasets consumed by viz/port_dashboard.html:

  daily_traffic       — unique vessel count per port per day
  type_distribution   — vessel type breakdown per port (aggregated over all days)
  congestion_heatmap  — avg vessel count by dow × hour per port
  stationary_heatmap  — avg pct_stationary by dow × hour per port
  daily_flow          — entries and exits per port per day
  movement_behaviour  — avg pct_stationary by port and hour (line chart)
  summary             — single KPI row per port (total vessels, arrivals, turnover, etc.)

Usage
-----
    python viz/prepare_dashboard_data.py
    python viz/prepare_dashboard_data.py --db data/port_analytics.db --out viz/data/dashboard.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB  = PROJECT_ROOT / "data" / "port_analytics.db"
DEFAULT_OUT = PROJECT_ROOT / "viz" / "data" / "dashboard.json"


def fetch(conn: sqlite3.Connection, sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_dashboard(db_path: Path, out_path: Path,
                    date_from: str = "2026-02-01",
                    date_to: str   = "2026-02-28") -> None:
    conn = sqlite3.connect(db_path)
    # Applied to every query that has a date column.
    df = f"date BETWEEN '{date_from}' AND '{date_to}'"
    # daily_flow consumes 3 days of padding on each side of the output window.
    # Pipeline ingests Jan 29-31 + Mar 1-3 2026 so the first/last exported day
    # has true "yesterday"/"tomorrow" context; without padding, day 1 looks
    # like all arrivals and the last day looks like all exits.
    flow_from = str(date.fromisoformat(date_from) - timedelta(days=3))
    flow_to   = str(date.fromisoformat(date_to)   + timedelta(days=3))
    dff = f"date BETWEEN '{flow_from}' AND '{flow_to}'"

    # 1. Daily traffic — unique vessel count per port per day
    daily_traffic = fetch(conn, f"""
        SELECT port, date, SUM(vessel_count) AS vessel_count
        FROM port_hourly_stats
        WHERE {df}
        GROUP BY port, date
        ORDER BY port, date
    """)

    # 2. Type distribution — aggregated over all days, per port
    type_distribution = fetch(conn, f"""
        SELECT port, vessel_type, SUM(count) AS count
        FROM type_distribution
        WHERE {df}
        GROUP BY port, vessel_type
        ORDER BY port, count DESC
    """)

    # 3. Congestion heatmap — avg vessel count by day-of-week and hour
    #    SQLite strftime('%w') returns 0=Sunday; we remap to 0=Monday for the viz.
    #    Sum across vessel_type first so AVG reflects total vessels per hour.
    congestion_heatmap = fetch(conn, f"""
        WITH hourly_totals AS (
            SELECT port, date, hour, SUM(vessel_count) AS vessel_count
            FROM port_hourly_stats
            WHERE {df}
            GROUP BY port, date, hour
        )
        SELECT
            port,
            (CAST(strftime('%w', date) AS INTEGER) + 6) % 7  AS dow,
            hour,
            ROUND(AVG(vessel_count), 2)                       AS avg_vessel_count
        FROM hourly_totals
        GROUP BY port, dow, hour
        ORDER BY port, dow, hour
    """)

    # 4. Stationary heatmap — avg pct_stationary by day-of-week and hour
    #    Mirrors congestion_heatmap but shows movement behaviour rather than volume.
    stationary_heatmap = fetch(conn, f"""
        SELECT
            port,
            (CAST(strftime('%w', date) AS INTEGER) + 6) % 7 AS dow,
            hour,
            ROUND(AVG(pct_stationary), 1)                    AS avg_pct_stationary
        FROM port_speed_stats
        WHERE {df}
        GROUP BY port, dow, hour
        ORDER BY port, dow, hour
    """)

    # 5. Daily flow — entries and exits per port per day (summed across vessel types).
    #    Query the padded range so the entry/exit calculation has true "yesterday"
    #    and "tomorrow" for the first and last exported day, then filter to the
    #    output window [date_from, date_to] here.
    daily_flow_padded = fetch(conn, f"""
        SELECT port, date,
               SUM(entries)        AS entries,
               SUM(exits)          AS exits,
               SUM(unique_vessels) AS unique_vessels
        FROM port_daily_flow
        WHERE {dff}
        GROUP BY port, date
        ORDER BY port, date
    """)
    daily_flow = [r for r in daily_flow_padded if date_from <= r["date"] <= date_to]

    # 6. Movement behaviour — avg pct_stationary by hour of day, per port
    #    24 data points per port; used for a line chart separate from the heatmap.
    movement_behaviour = fetch(conn, f"""
        SELECT port, hour,
               ROUND(AVG(pct_stationary), 1) AS avg_pct_stationary
        FROM port_speed_stats
        WHERE {df}
        GROUP BY port, hour
        ORDER BY port, hour
    """)

    # 7. Summary KPIs per port — one row per port
    #    Uses clean CTEs to avoid cross-join inflation in the original query.
    summary = fetch(conn, f"""
        WITH hourly_totals AS (
            SELECT port, date, hour, SUM(vessel_count) AS vessel_count
            FROM port_hourly_stats
            WHERE {df}
            GROUP BY port, date, hour
        ),
        hourly_agg AS (
            SELECT port,
                   SUM(vessel_count) AS total_vessel_hours,
                   MAX(vessel_count) AS peak_hourly_count
            FROM hourly_totals
            GROUP BY port
        ),
        dwell_agg AS (
            SELECT port,
                   ROUND(AVG(dwell_minutes), 1) AS avg_dwell_minutes
            FROM vessel_visits
            WHERE {df}
            GROUP BY port
        ),
        flow_agg AS (
            SELECT port,
                   SUM(entries) AS total_entries,
                   ROUND(
                       CAST(SUM(entries) AS REAL) /
                       NULLIF(COUNT(DISTINCT date), 0),
                   1) AS avg_daily_arrivals
            FROM port_daily_flow
            WHERE {df}
            GROUP BY port
        )
        SELECT
            h.port,
            h.total_vessel_hours,
            h.peak_hourly_count,
            d.avg_dwell_minutes,
            f.total_entries,
            f.avg_daily_arrivals
        FROM hourly_agg h
        LEFT JOIN dwell_agg d ON d.port = h.port
        LEFT JOIN flow_agg  f ON f.port = h.port
        ORDER BY total_vessel_hours DESC
    """)

    # 8. Hourly totals — per (port, date, hour) summed across vessel_type.
    #    summary.peak_hourly_count is per-port only (MAX grouped by port), so
    #    the client cannot recover the true combined peak from it. Exporting
    #    this compact dataset (~5 ports × 28 days × 24 hours ≈ 3360 rows) lets
    #    the dashboard recompute the correct multi-port peak by summing across
    #    the active ports at each (date, hour) and then taking the max.
    hourly_totals = fetch(conn, f"""
        SELECT port, date, hour, SUM(vessel_count) AS vessel_count
        FROM port_hourly_stats
        WHERE {df}
        GROUP BY port, date, hour
        ORDER BY port, date, hour
    """)

    conn.close()

    payload = {
        "generated":   str(Path(db_path).stat().st_mtime),
        "date_from":   date_from,
        "date_to":     date_to,
        "ports": sorted({r["port"] for r in daily_traffic}),
        "daily_traffic": daily_traffic,
        "type_distribution": type_distribution,
        "congestion_heatmap": congestion_heatmap,
        "stationary_heatmap": stationary_heatmap,
        "daily_flow": daily_flow,
        "movement_behaviour": movement_behaviour,
        "summary": summary,
        "hourly_totals": hourly_totals,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")
    print(f"  daily_traffic      : {len(daily_traffic)} rows")
    print(f"  type_distribution  : {len(type_distribution)} rows")
    print(f"  congestion_heatmap : {len(congestion_heatmap)} rows")
    print(f"  stationary_heatmap : {len(stationary_heatmap)} rows")
    print(f"  daily_flow         : {len(daily_flow)} rows")
    print(f"  movement_behaviour : {len(movement_behaviour)} rows")
    print(f"  summary            : {len(summary)} ports")
    print(f"  hourly_totals      : {len(hourly_totals)} rows")


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
    build_dashboard(args.db, args.out, args.date_from, args.date_to)


if __name__ == "__main__":
    main()
