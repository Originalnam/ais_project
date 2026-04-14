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
--    Which hour of the day sees the highest vessel count?
-- ---------------------------------------------------------------------------
SELECT
    port,
    date,
    hour,
    vessel_count
FROM port_hourly_stats
WHERE (port, date, vessel_count) IN (
    SELECT port, date, MAX(vessel_count)
    FROM port_hourly_stats
    GROUP BY port, date
)
ORDER BY port, date;


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
-- ---------------------------------------------------------------------------
SELECT
    port,
    CAST(strftime('%w', date) AS INTEGER)  AS day_of_week,
    hour,
    ROUND(AVG(vessel_count), 1)            AS avg_vessel_count
FROM port_hourly_stats
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
