-- kpis.sql
-- Analytical KPI queries for the Port Activity Analytics pipeline.
-- All queries run against the SQLite database at data/port_analytics.db.
-- Replace the port name literals to switch focus port.

-- ---------------------------------------------------------------------------
-- 1. Daily traffic trend
--    Total unique vessels per port per day over the data window.
-- ---------------------------------------------------------------------------
SELECT
    port,
    date,
    SUM(vessel_count)  AS daily_vessel_count
FROM port_hourly_stats
GROUP BY port, date
ORDER BY port, date;


-- ---------------------------------------------------------------------------
-- 2. Peak hour per port per day
--    Which hour of the day sees the highest total vessel count?
--    vessel_count is now per vessel_type, so we sum across types first.
-- ---------------------------------------------------------------------------
WITH hourly_totals AS (
    SELECT port, date, hour, SUM(vessel_count) AS total_vessel_count
    FROM port_hourly_stats
    GROUP BY port, date, hour
)
SELECT h.port, h.date, h.hour, h.total_vessel_count
FROM hourly_totals h
WHERE (h.port, h.date, h.total_vessel_count) IN (
    SELECT port, date, MAX(total_vessel_count)
    FROM hourly_totals
    GROUP BY port, date
)
ORDER BY h.port, h.date;


-- ---------------------------------------------------------------------------
-- 3. Vessel type distribution (aggregated over all days)
-- ---------------------------------------------------------------------------
SELECT
    port,
    vessel_type,
    SUM(count)                        AS total_vessels,
    ROUND(
        100.0 * SUM(count) / SUM(SUM(count)) OVER (PARTITION BY port),
        1
    )                                 AS pct
FROM type_distribution
GROUP BY port, vessel_type
ORDER BY port, total_vessels DESC;


-- ---------------------------------------------------------------------------
-- 4. Average dwell time by vessel type
-- ---------------------------------------------------------------------------
SELECT
    port,
    vessel_type,
    COUNT(*)                          AS visit_count,
    ROUND(AVG(dwell_minutes), 1)      AS avg_dwell_minutes,
    ROUND(AVG(dwell_minutes) / 60, 2) AS avg_dwell_hours
FROM vessel_visits
WHERE dwell_minutes > 0
GROUP BY port, vessel_type
ORDER BY port, avg_dwell_minutes DESC;


-- ---------------------------------------------------------------------------
-- 5. Congestion heatmap — average vessel count by hour-of-day and day-of-week
--    (day-of-week: 0=Sunday … 6=Saturday in SQLite strftime)
--    Sum across vessel_type first so AVG reflects total vessels per hour.
-- ---------------------------------------------------------------------------
WITH hourly_totals AS (
    SELECT port, date, hour, SUM(vessel_count) AS total_vessel_count
    FROM port_hourly_stats
    GROUP BY port, date, hour
)
SELECT
    port,
    CAST(strftime('%w', date) AS INTEGER)  AS day_of_week,
    hour,
    ROUND(AVG(total_vessel_count), 1)      AS avg_vessel_count
FROM hourly_totals
GROUP BY port, day_of_week, hour
ORDER BY port, day_of_week, hour;


-- ---------------------------------------------------------------------------
-- 6. Busiest port overall (total vessel-hours across the data window)
-- ---------------------------------------------------------------------------
SELECT
    port,
    SUM(vessel_count) AS total_vessel_hours
FROM port_hourly_stats
GROUP BY port
ORDER BY total_vessel_hours DESC;


-- ---------------------------------------------------------------------------
-- 7. Vessel type breakdown per hour (type-stratified congestion)
--    Which vessel types drive peak hours at each port?
-- ---------------------------------------------------------------------------
SELECT
    port,
    vessel_type,
    CAST(strftime('%w', date) AS INTEGER)  AS day_of_week,
    hour,
    ROUND(AVG(vessel_count), 1)            AS avg_vessel_count
FROM port_hourly_stats
GROUP BY port, vessel_type, day_of_week, hour
ORDER BY port, day_of_week, hour, avg_vessel_count DESC;


-- ---------------------------------------------------------------------------
-- 8. Arrival patterns by vessel type — what hour do vessels typically arrive?
--    entry_hour = hour of first ping in port on that day.
-- ---------------------------------------------------------------------------
SELECT
    port,
    vessel_type,
    entry_hour,
    COUNT(*) AS arrival_count
FROM vessel_visits
GROUP BY port, vessel_type, entry_hour
ORDER BY port, vessel_type, entry_hour;


-- ---------------------------------------------------------------------------
-- 9. Navigational status breakdown per port
--    % moored, % underway, % at anchor over the data window.
-- ---------------------------------------------------------------------------
WITH totals AS (
    SELECT port, SUM(ping_count) AS port_total
    FROM port_navstatus_stats
    GROUP BY port
)
SELECT
    n.port,
    n.nav_status,
    SUM(n.ping_count)                                    AS ping_count,
    ROUND(100.0 * SUM(n.ping_count) / t.port_total, 1)  AS pct
FROM port_navstatus_stats n
JOIN totals t ON n.port = t.port
GROUP BY n.port, n.nav_status
ORDER BY n.port, ping_count DESC;


-- ---------------------------------------------------------------------------
-- 10. Movement behaviour — average speed and % stationary by port
--     Identifies ports where vessels spend more time waiting (high pct_stationary).
-- ---------------------------------------------------------------------------
SELECT
    port,
    ROUND(AVG(sog_mean), 2)        AS avg_sog_mean,
    ROUND(AVG(sog_p95), 2)         AS avg_sog_p95,
    ROUND(AVG(pct_stationary), 1)  AS avg_pct_stationary
FROM port_speed_stats
GROUP BY port
ORDER BY avg_pct_stationary DESC;


-- ---------------------------------------------------------------------------
-- 11. Daily flow — arrival and departure rates by port and vessel type
--     Entries = new arrivals (not present yesterday).
--     Exits   = departures (not present tomorrow).
-- ---------------------------------------------------------------------------
SELECT
    port,
    vessel_type,
    ROUND(AVG(entries), 1)            AS avg_daily_entries,
    ROUND(AVG(exits), 1)              AS avg_daily_exits,
    ROUND(AVG(unique_vessels), 1)     AS avg_daily_vessels,
    ROUND(AVG(avg_dwell_minutes), 1)  AS avg_dwell_minutes
FROM port_daily_flow
GROUP BY port, vessel_type
ORDER BY port, avg_daily_vessels DESC;
