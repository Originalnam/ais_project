-- schema.sql
-- Table definitions for the Port Activity Analytics pipeline.
-- Written for SQLite; all types and constraints are also valid Azure SQL.
--
-- Tables are populated by pipeline/load.py from the Parquet outputs of
-- pipeline/transform.py.

-- ---------------------------------------------------------------------------
-- port_hourly_stats
-- One row per (port, date, hour, vessel_type).
-- vessel_count = unique MMSIs of that type active in this hour.
-- Summing vessel_count across vessel_type gives total unique MMSIs for the hour
-- (each MMSI maps to exactly one type).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_hourly_stats (
    port          TEXT    NOT NULL,
    date          TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    hour          INTEGER NOT NULL,   -- 0–23
    vessel_type   TEXT    NOT NULL,   -- normalised category (Cargo, Tanker, …, Other)
    vessel_count  INTEGER NOT NULL,   -- unique MMSIs of this type active in this hour
    PRIMARY KEY (port, date, hour, vessel_type)
);

-- ---------------------------------------------------------------------------
-- vessel_visits
-- One row per (MMSI, port, date).  Entry/exit times and dwell duration.
-- entry_hour / exit_hour are the hour-of-day of the first and last ping.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vessel_visits (
    mmsi           INTEGER NOT NULL,
    port           TEXT    NOT NULL,
    date           TEXT    NOT NULL,
    first_seen     TEXT    NOT NULL,   -- ISO-8601 datetime
    last_seen      TEXT    NOT NULL,
    vessel_type    TEXT    NOT NULL,
    ping_count     INTEGER NOT NULL,
    dwell_minutes  REAL    NOT NULL,
    entry_hour     INTEGER NOT NULL,   -- hour of first ping (0–23)
    exit_hour      INTEGER NOT NULL,   -- hour of last ping (0–23)
    PRIMARY KEY (mmsi, port, date)
);

-- ---------------------------------------------------------------------------
-- type_distribution
-- Vessel type counts per (port, date).  Used for donut / bar charts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS type_distribution (
    port         TEXT    NOT NULL,
    date         TEXT    NOT NULL,
    vessel_type  TEXT    NOT NULL,
    count        INTEGER NOT NULL,
    PRIMARY KEY (port, date, vessel_type)
);

-- ---------------------------------------------------------------------------
-- port_navstatus_stats
-- Ping count by navigational status per (port, date).
-- Port-level analogue of aarhus_navstatus_stats.
-- Enables % moored, % underway, % at anchor per port per day.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_navstatus_stats (
    port        TEXT    NOT NULL,
    date        TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    nav_status  TEXT    NOT NULL,
    ping_count  INTEGER NOT NULL,
    PRIMARY KEY (port, date, nav_status)
);

-- ---------------------------------------------------------------------------
-- port_speed_stats
-- SOG (Speed Over Ground) distribution per (port, date, hour).
-- Port-level analogue of aarhus_zone_speed_stats.
-- pct_stationary = % of pings with SOG < 0.5 knots (proxy for stopped/waiting).
-- SOG >= 102.3 (AIS "not available" sentinel) excluded before aggregation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_speed_stats (
    port            TEXT    NOT NULL,
    date            TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    hour            INTEGER NOT NULL,   -- 0–23
    sog_mean        REAL    NOT NULL,
    sog_median      REAL    NOT NULL,
    sog_p95         REAL    NOT NULL,
    pct_stationary  REAL    NOT NULL,   -- % pings with SOG < 0.5 knots
    PRIMARY KEY (port, date, hour)
);

-- ---------------------------------------------------------------------------
-- port_daily_flow
-- Entries, exits, and unique vessel counts per (port, date, vessel_type).
-- Entry  = MMSI seen in this port today but NOT in the same port yesterday.
-- Exit   = MMSI seen in this port today but NOT in the same port tomorrow.
-- Enables arrival rate, departure rate, and flow & turnover KPIs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_daily_flow (
    port               TEXT    NOT NULL,
    date               TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    vessel_type        TEXT    NOT NULL,
    unique_vessels     INTEGER NOT NULL,   -- MMSIs active in port this day
    entries            INTEGER NOT NULL,   -- MMSIs not present yesterday
    exits              INTEGER NOT NULL,   -- MMSIs not present tomorrow
    avg_dwell_minutes  REAL,              -- NULL if all visits are single-ping
    PRIMARY KEY (port, date, vessel_type)
);

-- ===========================================================================
-- Aarhus sub-zone tables
-- Populated by pipeline/transform_aarhus.py + pipeline/load.py.
-- Zones: outer_approach | anchorage | south_terminal | north_terminal
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- aarhus_zone_hourly_stats
-- One row per (zone, date, hour).  Per-zone traffic volume for trend charts
-- and congestion heatmaps.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aarhus_zone_hourly_stats (
    zone          TEXT    NOT NULL,   -- outer_approach | anchorage | south_terminal | north_terminal
    date          TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    hour          INTEGER NOT NULL,   -- 0–23
    vessel_type   TEXT    NOT NULL,   -- normalised category (Cargo, Tanker, …, Other)
    vessel_count  INTEGER NOT NULL,   -- unique MMSIs of this type active in this zone-hour
    PRIMARY KEY (zone, date, hour, vessel_type)
);

-- ---------------------------------------------------------------------------
-- aarhus_zone_visits
-- One row per (MMSI, zone, date).  Per-zone dwell times, the primary input
-- for logistics efficiency KPIs (turnaround, waiting time, throughput).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aarhus_zone_visits (
    mmsi           INTEGER NOT NULL,
    zone           TEXT    NOT NULL,
    date           TEXT    NOT NULL,
    first_seen     TEXT    NOT NULL,   -- ISO-8601 datetime
    last_seen      TEXT    NOT NULL,
    vessel_type    TEXT    NOT NULL,
    ping_count     INTEGER NOT NULL,
    dwell_minutes  REAL    NOT NULL,
    PRIMARY KEY (mmsi, zone, date)
);

-- ---------------------------------------------------------------------------
-- aarhus_navstatus_stats
-- Ping count by navigational status per (zone, date, vessel_type).
-- The at-anchor % in the anchorage zone is a direct congestion / waiting-queue
-- proxy. Vessel_type grain enables filtering the dashboard by vessel category.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aarhus_navstatus_stats (
    zone          TEXT    NOT NULL,
    date          TEXT    NOT NULL,
    vessel_type   TEXT    NOT NULL,
    nav_status    TEXT    NOT NULL,
    ping_count    INTEGER NOT NULL,
    PRIMARY KEY (zone, date, vessel_type, nav_status)
);

-- ---------------------------------------------------------------------------
-- aarhus_zone_speed_stats
-- SOG (Speed Over Ground) distribution per (zone, date, hour, vessel_type).
-- Covers outer_approach speed profiling and in-terminal maneuvering detection.
-- SOG >= 102 (AIS "not available" sentinel) excluded before aggregation.
-- sog_p025 / sog_p975 bound a 95% interval for uncertainty-band rendering.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aarhus_zone_speed_stats (
    zone         TEXT    NOT NULL,
    date         TEXT    NOT NULL,
    hour         INTEGER NOT NULL,
    vessel_type  TEXT    NOT NULL,
    sog_mean     REAL    NOT NULL,
    sog_median   REAL    NOT NULL,
    sog_p025     REAL    NOT NULL,
    sog_p95      REAL    NOT NULL,
    sog_p975     REAL    NOT NULL,
    PRIMARY KEY (zone, date, hour, vessel_type)
);
