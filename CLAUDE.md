# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Port Activity Analytics Pipeline — a portfolio project processing AIS (Automatic Identification System) vessel tracking data from the Danish Maritime Authority (DMA). The business question: how does vessel traffic and port activity evolve over time, and what patterns emerge around congestion or vessel type distribution? The pipeline is deliberately framed as a general operational analytics pipeline (transferable to logistics, energy, manufacturing), not a maritime niche project.

**Agreed stack:** Python/pandas · SQLite (local) · Azure SQL (production target) · D3.js  

**Data:** DMA AIS data covering Danish waters. Primary analytical dataset is Feb 2026 (full month, daily granularity). Additional historical months (2006-03, 2006-04, …) are downloaded as the pipeline expands. Two zip formats exist on the source:
- **Monthly** (2006-03 to 2024-02): one zip per month, contains ~30 daily CSVs named `aisdk_YYYYMMDD.csv`, 22 columns
- **Daily** (2024-03-01 onwards): one zip per day, contains one CSV named `aisdk-YYYY-MM-DD.csv`, 26 columns (extra A/B/C/D ignored by pipeline)

**Target ports:** Five major Danish ports — Esbjerg, Aarhus, Copenhagen, Aalborg, Fredericia. Bounding boxes defined in `config.py` (single source of truth).

**Aarhus sub-zones:** Aarhus is additionally divided into four operational sub-zones (`AARHUS_ZONES` in `config.py`): `outer_approach`, `anchorage`, `south_terminal`, `north_terminal`. Sub-zone labeling is applied in `pipeline/transform_aarhus.py` (downstream of the bbox filter) so boundaries can be tuned without re-streaming raw CSVs.

## Portfolio goals

The project is displayed on GitHub and a personal website. It must demonstrate:

- **Pipeline design** — end-to-end ETL from raw zip archives to analytical outputs, with clear separation of concerns across pipeline stages
- **Transformations** — chunked streaming ingestion, bounding-box spatial filtering, aggregation into business-meaningful metrics
- **Insights** — KPI cards, trend analysis, vessel type distribution, traffic heatmaps
- **Architecture decisions** — explicit rationale for storage choices, cost trade-offs, and design constraints

### Azure architecture showcase

The project demonstrates understanding of Azure data architecture without incurring long-term fixed costs:

- **Development/testing phase** — Azure SQL can be used temporarily to validate the production connection path (connection string swap in `pipeline/load.py`). Tear down after validation.
- **Portfolio/long-term** — all data is stored as static files (SQLite, Parquet, JSON) committed to the repo or served statically. No running cloud services required once built.
- **Architecture is still evident** — the code is structured as if targeting Azure SQL: schema design, indexed tables, separation of transactional and analytical layers. The swap is one line.

### Transactional vs analytical storage

- **Transactional layer** — ten SQLite tables mirror a normalized OLTP store (Azure SQL in production): `port_hourly_stats`, `vessel_visits`, `type_distribution`, `port_navstatus_stats`, `port_speed_stats`, `port_daily_flow`, plus four `aarhus_*` zone tables.
- **Analytical layer** — pre-aggregated KPIs exported to `viz/data/dashboard.json`. Flat, denormalized, optimized for read performance in the frontend.

### Cost-aware data strategy

| Layer | Granularity | Storage | Rationale |
|---|---|---|---|
| KPI dashboard | Pre-aggregated (hourly/daily) | Static JSON | Full month, zero query cost |
| Port analytics DB | Aggregated stats | SQLite (committed) | Full month, no cloud dependency |
| Aarhus zone analytics | Zone-level aggregates | Static JSON + SQLite (committed) | ~15 KB JSON; ~5 MB added to DB |
| Vessel map playback (Danish waters) | Full AIS rows | Static JSON, limited to 1–3 days | Storage cost scales with raw rows; scope-limited for portfolio |
| Aarhus vessel map playback | 30-min downsampled AIS rows | Static JSON, full month | Aarhus bbox only keeps file to ~2 MB; zone index per point enables client-side colouring |
| Raw / filtered Parquet | Full granularity | Not committed (`.gitignore`) | Reproducible from source zips; not needed at rest |

## Environment

```bash
source .venv/Scripts/activate   # activate venv (bash on Windows)
jupyter lab                      # notebook work
python -m pytest tests/          # run all tests
python -m pytest tests/test_filter.py  # single test file
```

## Pipeline control: pipeline_status.csv

`data/pipeline_status.csv` is the single source of truth for pipeline progress. One row per available date period. All stage flags are **write-once** — once `True`, they never revert even if source files are deleted.

| Column | Type | Meaning |
|---|---|---|
| `date` | `YYYY-MM` or `YYYY-MM-DD` | Period key |
| `raw` | bool | Zip downloaded to `data/raw/` |
| `unzipped` | bool | Zip extracted to `data/raw/unzipped/` |
| `unzipped_size` | int (bytes) | Total size of extracted CSVs |
| `filtered` | bool | Filtered Parquet written to `data/processed/filtered/` |
| `filtered_size` | int (bytes) | Parquet file size |
| `filtered_compression` | float (%) | Size reduction vs unzipped (`(1 - filtered/unzipped) * 100`) |
| `filtered_aarhus_size` | int (bytes) | Aarhus-only Parquet written to `data/processed/filtered/aarhus/` |
| `filtered_aarhus_compression` | float (%) | Size reduction vs filtered (`(1 - aarhus/filtered) * 100`) |

Each pipeline script reads this file to decide what to skip, and writes to it after completing a stage:
- `pipeline/build_status.py` — regenerates the full CSV from disk state; preserves existing `True` values
- `pipeline/download_data.py` — skips if `raw=True`; sets `raw=True` after download
- `pipeline/unzip.py` — skips if `unzipped=True`; sets `unzipped=True` + `unzipped_size` after extraction
- `pipeline/filter.py` — skips if `filtered=True`; sets `filtered=True` + size + compression after writing Parquet
- `pipeline/transform_aarhus.py` — writes per-period Aarhus parquets; sets `filtered_aarhus_size` + `filtered_aarhus_compression`

## Data flow

```
data/raw/*.zip  →  data/raw/unzipped/  →  filter  →  transform  →  load (SQLite)  →  viz
                                                           ↓
                                                  transform_aarhus  →  load  →  aarhus viz
```

1. **Download** — `pipeline/download_data.py` fetches zips from aisdata.ais.dk to `data/raw/`. Three URL patterns: monthly (`/{YYYY}/aisdk-{YYYY-MM}.zip`), daily-with-year-dir (up to 2025-02-26), daily-flat-root (from 2025-02-27).
2. **Unzip** — `pipeline/unzip.py` extracts `data/raw/aisdk-*.zip` → `data/raw/unzipped/aisdk-{key}/`.
3. **Filter** — `pipeline/filter.py` streams each CSV in 200k-row chunks, applies port bounding boxes, writes Parquet to `data/processed/filtered/`. Critical reduction step (~15M rows/day → port-relevant only). Handles both monthly (30 files/folder) and daily (1 file/folder) naming conventions transparently via `glob("*.csv")`.
4. **Ingest** — `pipeline/ingest.py` exposes `load_day(date_str)` for ad-hoc/notebook use only; the pipeline uses `filter.py` directly.
5. **Transform** — `pipeline/transform.py` reads filtered Parquet → six outputs (all five ports, written as Parquet to `data/processed/`):
   - `hourly_stats` — unique vessel count per (port, date, hour, **vessel_type**)
   - `vessel_visits` — one row per vessel per port per day; includes `entry_hour` / `exit_hour`
   - `type_distribution` — vessel type counts per (port, date)
   - `port_navstatus_stats` — ping count by navigational status per (port, date)
   - `port_speed_stats` — SOG mean/median/p95 + `pct_stationary` per (port, date, hour)
   - `port_daily_flow` — true entries/exits/unique_vessels per (port, date, vessel_type)
6. **Transform Aarhus** — `pipeline/transform_aarhus.py` isolates Aarhus rows, writes per-period Aarhus parquets to `data/processed/filtered/aarhus/`, applies sub-zone labels → four zone-level outputs:
   - `aarhus_zone_hourly_stats` — unique vessel count per (zone, date, hour, **vessel_type**); same grain pattern as `port_hourly_stats`
   - `aarhus_zone_visits` — one row per (MMSI, zone, date) with dwell time and vessel_type
   - `aarhus_navstatus_stats` — ping count by navigational status per (zone, date)
   - `aarhus_zone_speed_stats` — SOG mean/median/p95 per (zone, date, hour)
   Also updates `pipeline_status.csv` with `filtered_aarhus_size` and `filtered_aarhus_compression`.
7. **Load** — `pipeline/load.py` writes all ten Parquet outputs into SQLite (`data/port_analytics.db`). Schema in `sql/schema.sql`; KPI queries in `sql/kpis.sql` (11 queries) and `sql/kpis_aarhus.sql` (6 Aarhus queries).
8. **Viz** — static JSON exports consumed by D3 dashboards (see Viz section below).

## Key directories and files

- `config.py` — single source of truth: port bounding boxes, Aarhus sub-zones, all paths (`FILTERED_DIR`, `AARHUS_FILTERED_DIR`, `PIPELINE_STATUS`, …), chunk size, date range
- `data/pipeline_status.csv` — pipeline control file (see above)
- `pipeline/`
  - `build_status.py` — regenerate `pipeline_status.csv` from disk state
  - `download_data.py` — download zips from aisdata.ais.dk → `data/raw/`
  - `unzip.py` — extract zips → `data/raw/unzipped/`
  - `ingest.py` — `load_day(date_str)` for notebook/ad-hoc use
  - `filter.py` — chunked bounding-box filter → `data/processed/filtered/`
  - `transform.py` — six five-port outputs: hourly stats (by vessel_type), vessel visits (entry/exit hour), type distribution, navstatus stats, speed stats, daily flow
  - `transform_aarhus.py` — writes per-period Aarhus parquets, applies sub-zone labels, produces four zone-level outputs, updates pipeline_status.csv
  - `load.py` — load all ten Parquet outputs into SQLite
- `data/raw/` — zip archives
- `data/raw/unzipped/` — extracted CSVs, one folder per period (`aisdk-{key}/`)
- `data/processed/filtered/` — per-period filtered Parquet files (all five ports)
- `data/processed/filtered/aarhus/` — per-period Aarhus-only filtered Parquet files
- `data/processed/` — transform outputs (ten Parquet files)
- `data/port_analytics.db` — SQLite database (ten tables)
- `sql/` — `schema.sql`, `kpis.sql` (11 five-port queries), `kpis_aarhus.sql` (6 Aarhus queries)
- `tests/` — `test_filter.py` (10 tests), `test_transform_aarhus.py` (18 tests)
- `notebooks/exploration.ipynb`, `notebooks/database.ipynb`
- `viz/` — D3 dashboards and data preparation scripts (see below)
- `viz/portfolio/` — self-contained IIFE modules for `yoerivda.be` integration (see below)

## Running the pipeline end-to-end

```bash
# Rebuild/update status file (run anytime to sync disk state)
python pipeline/build_status.py

# Download → unzip → filter (each script skips already-done periods)
python -m pipeline.download_data 2026-02-01 2026-02-28
python pipeline/unzip.py
python -m pipeline.filter                    # add --workers 4 for parallel

# Transform and load
python -m pipeline.transform
python -m pipeline.transform_aarhus
python -m pipeline.load

# Verify
sqlite3 data/port_analytics.db "SELECT port, SUM(vessel_count) FROM port_hourly_stats GROUP BY port;"
sqlite3 data/port_analytics.db "SELECT zone, SUM(vessel_count) FROM aarhus_zone_hourly_stats GROUP BY zone;"
```

## Viewing the visualisations

Viz files use `fetch()` / `d3.json()` — must be served over HTTP, not opened as `file://`.

```bash
cd viz && python -m http.server 8000
```

- `http://localhost:8000/port_dashboard.html` — five-port analytics dashboard
- `http://localhost:8000/aarhus_dashboard.html` — Aarhus zone analytics dashboard
- `http://localhost:8000/map.html` — Danish waters vessel playback
- `http://localhost:8000/aarhus_map.html` — Aarhus full-month vessel map with zone overlays

Rebuild data files after re-running the pipeline:

```bash
python viz/prepare_dashboard_data.py          # defaults to 2026-02-01 → 2026-02-28
python viz/prepare_dashboard_data.py --date-from 2026-01-01 --date-to 2026-01-31  # different period
python viz/prepare_aarhus_data.py
python viz/prepare_aarhus_map_data.py   # reads filtered Parquets directly (not SQLite)
```

### Portfolio modules (`viz/portfolio/`)

Self-contained IIFE modules adapted for embedding in `yoerivda.be`. Each module mounts into a host `<div>` and reads from a `window.*` data global — no `fetch()`, no runtime requests. Dependencies (D3 v7, Leaflet 1.9.4) are loaded by the host page.

| File | Mount div | Data global | Notes |
|---|---|---|---|
| `port-dashboard.js` | `#chart-port` | `window.DASHBOARD_DATA` | Dark theme; vessel-type donut + stacked bars + line charts + heatmap |
| `aarhus-dashboard.js` | `#chart-aarhus` | `window.AARHUS_DATA` | Requires Leaflet (zones inset map); vessel-type pill filter on all 6 charts |
| `map.js` | `#chart-map` | `window.VESSEL_DATA` | Host must set height; 48 px side margins prevent scroll-trap |
| `aarhus-map.js` | `#chart-aarhus-map` | `window.AARHUS_VESSEL_DATA` | Host must set height; 48 px side margins prevent scroll-trap |
| `viz.css` | — | — | Shared dark-navy CSS; `pa-` prefix; must be loaded by host |
| `layout-brief.md` | — | — | Wiring guide and dataset descriptions for the host `index.html` |

Data files in `viz/portfolio/data/` (JS files that assign window globals):

| File | Global | Content |
|---|---|---|
| `dashboard.js` | `window.DASHBOARD_DATA` | Port KPIs, daily traffic, type mix, heatmaps, daily flow, movement |
| `aarhus_analytics.js` | `window.AARHUS_DATA` | Zone KPIs, speed, dwell, nav-status, type mix, congestion, zone polygons |
| `vessels_2026-02-01.js` | `window.VESSEL_DATA` | One day of AIS tracks for all five port bboxes |
| `aarhus_vessels_feb2026.js` | `window.AARHUS_VESSEL_DATA` | Full-month 30-min downsampled Aarhus tracks with zone index |

**To test portfolio modules locally:**

```bash
cd viz/portfolio && python -m http.server 8001
# open http://localhost:8001/test.html
```

`test.html` loads all four modules on one dark page using the static data files — no pipeline re-run needed. Hard-refresh (Ctrl+Shift+R) if CSS changes don't appear.

### Port dashboard datasets (`viz/data/dashboard.json`)

`prepare_dashboard_data.py` exports seven datasets filtered to the specified date range:

| Key | Description |
|---|---|
| `daily_traffic` | Unique vessel count per port per day — drives the stacked bar trend chart |
| `type_distribution` | Vessel type counts per port — drives the donut chart |
| `congestion_heatmap` | Avg vessel count by day-of-week × hour per port — heatmap "vessel count" mode |
| `stationary_heatmap` | Avg pct\_stationary by day-of-week × hour per port — heatmap "% stationary" mode |
| `daily_flow` | Entries and exits per port per day — trimmed 3 days each end (see note below) |
| `movement_behaviour` | Avg pct\_stationary by hour per port (24 points) — hourly line chart |
| `summary` | One KPI row per port: total\_vessel\_hours, peak\_hourly\_count, avg\_dwell\_minutes, total\_entries, avg\_daily\_arrivals |

The dashboard layout (3-column CSS grid):
- **Row 1:** Daily vessel count (stacked bar, cols 1–2) | Vessel type mix donut (col 3, spans rows 1–2)
- **Row 2:** Daily arrivals & departures (col 1) | % stationary by hour (col 2)
- **Row 3:** Congestion heatmap full-width — toggle between "Vessel count" and "% Stationary" views
- **KPI strip:** Total vessel-hours · Peak hourly count · Avg dwell time · Monthly arrivals · Avg daily arrivals

## Scope constraints

- Five Danish ports; Aarhus sub-zone analysis is the depth showcase
- Bounding box filter happens early to limit memory pressure (~15M rows/day → port-relevant only)
- Sub-zone boundaries applied in `transform_aarhus.py`, not `filter.py` — keeps filter fast; zone tuning works from cached Parquet
- No dbt or orchestration tooling — plain Python scripts are intentional
- SQLite is the local/portfolio target; Azure SQL is the production target (connection string swap only)

## AIS data notes

- `Navigational status`: `"Moored"` is the dominant stationary status in Danish coastal AIS; `"At anchor"` is rare. `"Unknown value"` (code 15) is common for Class B transponders.
- `SOG`: values ≥ 102.3 are the AIS "not available" sentinel — exclude before speed statistics. `pct_stationary` in `port_speed_stats` counts pings with SOG < 0.5 knots.
- Vessel type normalization: raw DMA strings collapsed to 8 categories via substring matching in `transform.py::_normalise_type`, reused by `transform_aarhus.py`.
- `port_hourly_stats` and `aarhus_zone_hourly_stats` grain: includes `vessel_type` — `vessel_count` is unique MMSIs of that type. Summing across vessel_type gives total unique MMSIs per hour (no double-counting; each MMSI has one type). Queries that need totals must `SUM(vessel_count) GROUP BY …, hour` before further aggregation (AVG/MAX on raw rows operates on per-type counts).
- `port_daily_flow` entry/exit definition: entry = MMSI seen in port today but not yesterday; exit = seen today but not tomorrow. First and last days of the data window have one-sided context only — the `daily_flow` dataset in `dashboard.json` trims 3 days from each end of the date range to suppress this artefact in the chart.
- Multi-day visits: `vessel_visits` and `aarhus_zone_visits` group by calendar date, so a vessel berthed for 3 days produces 3 rows. Dwell time is within-day span (first to last ping), not true port tenure. `entry_hour` / `exit_hour` are the hour of first and last ping on that calendar day.
