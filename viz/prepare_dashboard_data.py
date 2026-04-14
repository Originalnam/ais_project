"""
prepare_dashboard_data.py — Export SQLite analytics to JSON for the D3 dashboard.

Reads data/port_analytics.db and writes viz/data/dashboard.json with four
datasets consumed by viz/port_dashboard.html:

  daily_traffic     — unique vessel count per port per day
  type_distribution — vessel type breakdown per port (aggregated over all days)
  congestion_heatmap — avg vessel count by dow × hour per port
  summary           — single KPI row per port (total vessels, peak day, etc.)

Usage
-----
    python viz/prepare_dashboard_data.py
    python viz/prepare_dashboard_data.py --db data/port_analytics.db --out viz/data/dashboard.json
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB  = PROJECT_ROOT / "data" / "port_analytics.db"
DEFAULT_OUT = PROJECT_ROOT / "viz" / "data" / "dashboard.json"


def fetch(conn: sqlite3.Connection, sql: str) -> list[dict]:
    cur = conn.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def build_dashboard(db_path: Path, out_path: Path) -> None:
    conn = sqlite3.connect(db_path)

    # 1. Daily traffic — unique vessel count per port per day
    daily_traffic = fetch(conn, """
        SELECT port, date, SUM(vessel_count) AS vessel_count
        FROM port_hourly_stats
        GROUP BY port, date
        ORDER BY port, date
    """)

    # 2. Type distribution — aggregated over all days, per port
    type_distribution = fetch(conn, """
        SELECT port, vessel_type, SUM(count) AS count
        FROM type_distribution
        GROUP BY port, vessel_type
        ORDER BY port, count DESC
    """)

    # 3. Congestion heatmap — avg vessel count by day-of-week and hour
    #    SQLite strftime('%w') returns 0=Sunday; we remap to 0=Monday for the viz.
    congestion_heatmap = fetch(conn, """
        SELECT
            port,
            (CAST(strftime('%w', date) AS INTEGER) + 6) % 7  AS dow,
            hour,
            ROUND(AVG(vessel_count), 2)                       AS avg_vessel_count
        FROM port_hourly_stats
        GROUP BY port, dow, hour
        ORDER BY port, dow, hour
    """)

    # 4. Summary KPIs per port
    summary = fetch(conn, """
        SELECT
            h.port,
            SUM(h.vessel_count)                                       AS total_vessel_hours,
            MAX(h.vessel_count)                                       AS peak_hourly_count,
            ROUND(AVG(v.dwell_minutes), 1)                            AS avg_dwell_minutes
        FROM port_hourly_stats h
        LEFT JOIN vessel_visits v ON v.port = h.port
        GROUP BY h.port
        ORDER BY total_vessel_hours DESC
    """)

    conn.close()

    payload = {
        "generated": str(Path(db_path).stat().st_mtime),
        "ports": sorted({r["port"] for r in daily_traffic}),
        "daily_traffic": daily_traffic,
        "type_distribution": type_distribution,
        "congestion_heatmap": congestion_heatmap,
        "summary": summary,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))

    print(f"Wrote {out_path}  ({out_path.stat().st_size // 1024} KB)")
    print(f"  daily_traffic     : {len(daily_traffic)} rows")
    print(f"  type_distribution : {len(type_distribution)} rows")
    print(f"  congestion_heatmap: {len(congestion_heatmap)} rows")
    print(f"  summary           : {len(summary)} ports")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",  type=Path, default=DEFAULT_DB)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = p.parse_args()
    build_dashboard(args.db, args.out)


if __name__ == "__main__":
    main()
