-- kpis_aarhus.sql
-- Analytical KPI queries for Aarhus port sub-zone analysis.
-- Targets the four aarhus_* tables produced by pipeline/transform_aarhus.py.
--
-- Run against data/port_analytics.db after pipeline/load.py has populated
-- the aarhus_* tables.  Results are pre-computed and exported to
-- viz/data/aarhus_analytics.json by viz/prepare_aarhus_data.py.

-- ---------------------------------------------------------------------------
-- 1. Zone daily traffic
-- Vessel count per zone per day — the time-series backbone.
-- Drives the stacked area chart showing how traffic evolved across the month.
-- SUM across vessel_type first: aarhus_zone_hourly_stats grain is now
-- (zone, date, hour, vessel_type), so AVG/MAX must use a CTE.
-- ---------------------------------------------------------------------------
SELECT
    zone,
    date,
    SUM(vessel_count) AS vessel_count
FROM aarhus_zone_hourly_stats
GROUP BY zone, date
ORDER BY date, zone;


-- ---------------------------------------------------------------------------
-- 1b. Type-stratified zone congestion
-- Which vessel types drive congestion in each zone?
-- Answers: "Is the anchorage queue cargo vessels waiting for berths?"
-- ---------------------------------------------------------------------------
WITH hourly_totals AS (
    SELECT zone, date, hour, vessel_type, SUM(vessel_count) AS vessel_count
    FROM aarhus_zone_hourly_stats
    GROUP BY zone, date, hour, vessel_type
)
SELECT
    zone,
    vessel_type,
    (CAST(strftime('%w', date) AS INTEGER) + 6) % 7 AS dow,
    hour,
    ROUND(AVG(vessel_count), 2) AS avg_vessel_count
FROM hourly_totals
GROUP BY zone, vessel_type, dow, hour
ORDER BY zone, vessel_type, dow, hour;

-- ---------------------------------------------------------------------------
-- 2. Average dwell time by zone and vessel type
-- Core logistics efficiency KPI: how long does each vessel type spend in each
-- operational zone?  High anchorage dwell for Cargo = congestion / waiting.
-- ---------------------------------------------------------------------------
SELECT
    zone,
    vessel_type,
    COUNT(*)                                    AS visit_count,
    ROUND(AVG(dwell_minutes), 1)                AS avg_dwell_minutes,
    ROUND(AVG(dwell_minutes) / 60.0, 2)         AS avg_dwell_hours
FROM aarhus_zone_visits
WHERE dwell_minutes > 0           -- exclude single-ping transits
GROUP BY zone, vessel_type
ORDER BY zone, avg_dwell_minutes DESC;

-- ---------------------------------------------------------------------------
-- 3. Navigational status distribution by zone
-- Percentage breakdown of navigational states per zone, aggregated across the
-- full month.  The moored % in terminal zones reflects berth occupancy.
-- Note: "At anchor" is rarely transmitted in Danish coastal AIS; "Moored" is
-- the dominant stationary status in this dataset.
-- ---------------------------------------------------------------------------
WITH totals AS (
    SELECT zone, SUM(ping_count) AS zone_total
    FROM aarhus_navstatus_stats
    GROUP BY zone
)
SELECT
    n.zone,
    n.nav_status,
    SUM(n.ping_count)                                   AS ping_count,
    ROUND(100.0 * SUM(n.ping_count) / t.zone_total, 1) AS pct
FROM aarhus_navstatus_stats n
JOIN totals t ON n.zone = t.zone
GROUP BY n.zone, n.nav_status
ORDER BY n.zone, ping_count DESC;

-- ---------------------------------------------------------------------------
-- 4. Speed profile in outer_approach by hour of day
-- Mean and p95 SOG averaged across all days for each hour (0–23).
-- Shows whether vessels slow down in busy hours (congestion) or maintain
-- consistent approach speeds.  p95 flags occasional fast movers.
-- ---------------------------------------------------------------------------
SELECT
    hour,
    ROUND(AVG(sog_mean), 2)   AS sog_mean,
    ROUND(AVG(sog_median), 2) AS sog_median,
    ROUND(AVG(sog_p95), 2)    AS sog_p95
FROM aarhus_zone_speed_stats
WHERE zone = 'outer_approach'
GROUP BY hour
ORDER BY hour;

-- ---------------------------------------------------------------------------
-- 5. Vessel type mix per zone (visit count)
-- Validates that sub-zones capture real operational differences.
-- Expected: north_terminal → Cargo/Tanker; south_terminal → Passenger.
-- ---------------------------------------------------------------------------
SELECT
    zone,
    vessel_type,
    COUNT(*) AS visit_count
FROM aarhus_zone_visits
GROUP BY zone, vessel_type
ORDER BY zone, visit_count DESC;
