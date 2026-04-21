# Port Activity Analytics Pipeline

An end-to-end data engineering portfolio project processing AIS (Automatic Identification System) vessel tracking data from the [Danish Maritime Authority](https://www.dma.dk/). The central business question: **how does vessel traffic and port activity evolve over time, and what patterns emerge around congestion or vessel type distribution?**

The pipeline is framed as a general operational analytics system — the patterns (chunked ingestion, bbox filtering, OLTP → OLAP export, static frontend delivery) transfer directly to logistics, energy, or manufacturing domains.

---

## What it does

Raw AIS data for Danish waters runs to ~15 million GPS pings per day. This pipeline reduces that to port-relevant, analysis-ready aggregates through four stages:

```
data/raw/*.zip
    → unzip      (raw CSVs, ~1.5 GB/day)
    → filter     (bounding-box filter → ~2–5 MB Parquet per day, >99% reduction)
    → transform  (aggregated metrics per port/zone → ten Parquet outputs)
    → load       (SQLite / Azure SQL)
    → viz        (static JSON → D3 dashboards + Leaflet maps)
```

**Five target ports:** Esbjerg · Aarhus · Copenhagen · Aalborg · Fredericia

**Aarhus sub-zone analysis** (depth showcase): the port is divided into four operational zones — `outer_approach`, `anchorage`, `south_terminal`, `north_terminal` — enabling dwell-time, congestion, and vessel-type breakdowns at zone level.

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion & transform | Python · pandas · PyArrow |
| Analytical store | SQLite (local) · Azure SQL (production target, one-line swap) |
| Query layer | Plain SQL (`sql/schema.sql`, `sql/kpis.sql`, `sql/kpis_aarhus.sql`) |
| Visualisation | D3.js v7 · Leaflet 1.9.4 |
| Portfolio delivery | Self-contained IIFE modules (`viz/portfolio/`) |

---

## Pipeline outputs

### Transactional layer (SQLite — ten tables)

| Table | Grain |
|---|---|
| `port_hourly_stats` | port · date · hour · vessel\_type |
| `vessel_visits` | MMSI · port · date |
| `type_distribution` | port · date |
| `port_navstatus_stats` | port · date |
| `port_speed_stats` | port · date · hour |
| `port_daily_flow` | port · date · vessel\_type |
| `aarhus_zone_hourly_stats` | zone · date · hour · vessel\_type |
| `aarhus_zone_visits` | MMSI · zone · date |
| `aarhus_navstatus_stats` | zone · date · vessel\_type |
| `aarhus_zone_speed_stats` | zone · date · hour · vessel\_type |

### Analytical layer (static JSON / JS globals)

Pre-aggregated KPI exports consumed directly by the frontend — zero query cost at display time:

- `viz/portfolio/data/dashboard.js` — port KPIs, daily traffic, type mix, heatmaps, daily flow, movement behaviour
- `viz/portfolio/data/aarhus_analytics.js` — zone KPIs, speed bands (p2.5–p97.5), dwell time, nav-status, type mix, zone polygons
- `viz/portfolio/data/vessels_2026-02-01.js` — one day of AIS tracks across all five port bboxes (map playback)
- `viz/portfolio/data/aarhus_vessels_feb2026.js` — full-month 30-min downsampled Aarhus tracks with zone index

---

## Visualisations

Four interactive dashboards served over HTTP:

| URL | Description |
|---|---|
| `/port_dashboard.html` | Five-port analytics — KPI strip, stacked bar trend, vessel-type donut, arrivals/departures, % stationary by hour, congestion heatmap (toggle vessel count ↔ % stationary) |
| `/aarhus_dashboard.html` | Aarhus zone analytics — vessel-type pill filter on all six charts, Leaflet mini-map inset, speed chart with 95% confidence band |
| `/map.html` | Danish waters AIS playback — port bbox overlays, vessel-type filter |
| `/aarhus_map.html` | Aarhus full-month playback — 30-min downsampled, zone overlay, All/None type + zone filters |

```bash
cd viz && python -m http.server 8000
```

### Portfolio modules (`viz/portfolio/`)

Self-contained IIFE modules for embedding in `yoerivda.be`. Each module mounts into a host `<div>` and reads from a `window.*` data global — no `fetch()`, no runtime requests required.

```bash
cd viz/portfolio && python -m http.server 8001
# open http://localhost:8001/test.html
```

---

## Running the pipeline

```bash
# Activate environment
source .venv/Scripts/activate

# Sync pipeline status from disk
python pipeline/build_status.py

# Download → unzip → filter
python -m pipeline.download_data 2026-02-01 2026-02-28
python pipeline/unzip.py
python -m pipeline.filter          # add --workers 4 for parallel

# Transform and load
python -m pipeline.transform
python -m pipeline.transform_aarhus
python -m pipeline.load

# Verify
sqlite3 data/port_analytics.db "SELECT port, SUM(vessel_count) FROM port_hourly_stats GROUP BY port;"
sqlite3 data/port_analytics.db "SELECT zone, SUM(vessel_count) FROM aarhus_zone_hourly_stats GROUP BY zone;"

# Rebuild viz data files
python viz/prepare_dashboard_data.py
python viz/prepare_aarhus_data.py
python viz/prepare_aarhus_map_data.py
```

Each pipeline script reads `data/pipeline_status.csv` to skip already-completed stages — re-runs are safe and idempotent.

---

## Architecture decisions

**Why SQLite + static JSON instead of a live cloud database?**

The portfolio target is a personal website with zero ongoing infrastructure cost. SQLite holds the full analytical store; pre-aggregated JSON exports serve the frontend with no query latency. The pipeline code is structured as if targeting Azure SQL (normalised schema, indexed tables, SQL KPI layer) — switching is a one-line connection string change in `pipeline/load.py`.

**Why bounding-box filtering so early?**

Raw AIS data for Danish waters is ~15M rows/day. The bounding-box filter in `pipeline/filter.py` runs in 200k-row chunks and reduces each day to port-relevant rows only (typically >99% reduction by row count, >95% by file size). All downstream transforms and the SQLite DB operate on this small slice — no large files need to be committed or kept in memory.

**Why sub-zone boundaries in `transform_aarhus.py`, not `filter.py`?**

Separating the concerns means zone boundaries can be tuned (in `config.py`) without re-streaming raw CSVs. The Aarhus Parquet cache is reused; only the transform reruns.

---

## Testing

```bash
python -m pytest tests/
python -m pytest tests/test_filter.py        # filter + bbox logic (10 tests)
python -m pytest tests/test_transform_aarhus.py  # zone labeling + aggregation (18 tests)
```

---

## Data

AIS data: [Danish Maritime Authority open data](https://www.dma.dk/safety-at-sea/navigational-information/ais-data). Primary analytical dataset covers February 2026 (full month, daily granularity). Raw zips and extracted CSVs are not committed — the pipeline reproduces them from source.
