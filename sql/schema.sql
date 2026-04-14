-- schema.sql
-- Table definitions for the Port Activity Analytics pipeline.
-- Written for SQLite; all types and constraints are also valid Azure SQL.
--
-- Tables are populated by pipeline/load.py from the Parquet outputs of
-- pipeline/transform.py.

-- ---------------------------------------------------------------------------
-- port_hourly_stats
-- One row per (port, date, hour).  Captures traffic volume and congestion.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS port_hourly_stats (
    port              TEXT    NOT NULL,
    date              TEXT    NOT NULL,   -- ISO-8601: YYYY-MM-DD
    hour              INTEGER NOT NULL,   -- 0–23
    vessel_count      INTEGER NOT NULL,   -- unique MMSIs active in this hour
    congestion_index  INTEGER NOT NULL,   -- currently == vessel_count; reserved for weighting
    PRIMARY KEY (port, date, hour)
);

-- ---------------------------------------------------------------------------
-- vessel_visits
-- One row per (MMSI, port, date).  Entry/exit times and dwell duration.
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
