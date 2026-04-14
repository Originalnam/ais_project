# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Port Activity Analytics Pipeline — a portfolio project processing AIS (Automatic Identification System) vessel tracking data from the Danish Maritime Authority (DMA). The business question: how does vessel traffic and port activity evolve over time, and what patterns emerge around congestion or vessel type distribution? The pipeline is deliberately framed as a general operational analytics pipeline (transferable to logistics, energy, manufacturing), not a maritime niche project.

**Agreed stack:** Python/pandas · SQLite (local) · Azure SQL (production target) · D3.js  
**Deliberately excludes:** dbt, Databricks, Power BI

**Data:** Full month of DMA AIS data (2026-02-01 to 2026-02-28) covering Danish waters. The load layer targets SQLite for local development; swapping to Azure SQL only requires changing the connection string in `pipeline/load.py`.

**Target ports:** Five major Danish ports — Esbjerg, Aarhus, Copenhagen, Aalborg, Fredericia. Bounding boxes are defined in `config.py` (single source of truth).

## Environment

The project uses a local `.venv`. Activate it before running anything:

```bash
source .venv/Scripts/activate   # bash on Windows
```

Launch JupyterLab for notebook work:

```bash
jupyter lab
```

Run tests:

```bash
python -m pytest tests/
```

Run a single test file:

```bash
python -m pytest tests/test_filter.py
```

## Data flow

```
raw/*.zip  →  data/raw/unzipped/  →  filter  →  transform  →  load (SQLite)  →  viz
```

1. **Raw data** — daily zip files in `raw/` named `aisdk-YYYY-MM-DD.zip`, each containing one day of AIS CSV data from the DMA.
2. **Unzip** — `pipeline/unzip.py` extracts to `data/raw/unzipped/aisdk-YYYY-MM-DD/`.
3. **Ingest** — `pipeline/ingest.py` exposes `load_day(date_str)` for ad-hoc / notebook use. The pipeline uses `filter.py` directly (chunked streaming); `ingest.py` is for exploration.
4. **Filter** — `pipeline/filter.py` streams each CSV in 200k-row chunks, applies port bounding boxes, and writes Parquet to `data/processed/filtered/`. This is the critical reduction step (~15M rows → port-relevant records only).
5. **Transform** — `pipeline/transform.py` reads filtered Parquet and computes three outputs: `hourly_stats`, `vessel_visits`, `type_distribution` (all written as Parquet to `data/processed/`).
6. **Load** — `pipeline/load.py` writes the three Parquet outputs into SQLite (`data/port_analytics.db`). Schema defined in `sql/schema.sql`; KPI queries in `sql/kpis.sql`.
7. **Viz** — `viz/prepare_dashboard_data.py` exports `viz/data/dashboard.json` from SQLite; `viz/port_dashboard.html` renders the D3 analytics dashboard. `viz/map.html` shows full Danish-waters vessel playback (separate from the port dashboard).

## Key directories and files

- `config.py` — single source of truth: port bounding boxes, all paths, chunk size, date range
- `pipeline/` — ETL modules; each step is its own file
  - `unzip.py` — extract zips from `raw/` → `data/raw/unzipped/`
  - `ingest.py` — `load_day(date_str)` for notebook/ad-hoc use
  - `filter.py` — chunked bounding-box filter → Parquet in `data/processed/filtered/`
  - `transform.py` — computes hourly stats, vessel visits, type distribution
  - `load.py` — loads Parquet outputs into SQLite
- `raw/` — source zip archives (not processed data)
- `data/raw/unzipped/` — extracted CSVs, one folder per day
- `data/processed/filtered/` — per-day filtered Parquet files
- `data/processed/` — transform outputs (`hourly_stats.parquet`, `vessel_visits.parquet`, `type_distribution.parquet`)
- `data/port_analytics.db` — SQLite database (produced by `load.py`)
- `notebooks/` — exploratory analysis (`exploration.ipynb`)
- `sql/` — `schema.sql` (table definitions) and `kpis.sql` (6 analytical KPI queries)
- `tests/` — `test_filter.py` (10 unit tests for `_bbox_mask` and `_port_label`)
- `viz/` — visualisations for the portfolio website
  - `map.html` + `prepare_map_data.py` — interactive 24h vessel playback (full Danish waters)
  - `port_dashboard.html` — D3 port analytics dashboard (KPI cards, trend line, donut, heatmap)
  - `prepare_dashboard_data.py` — exports SQLite → `viz/data/dashboard.json`
  - `data/dashboard.json` — pre-built data file consumed by `port_dashboard.html`

## Running the pipeline end-to-end

```bash
python pipeline/unzip.py                          # extract all zips
python -m pipeline.filter                         # filter all days (add --workers 4 for parallel)
python -m pipeline.transform                      # compute analytics
python -m pipeline.load                           # write to SQLite
sqlite3 data/port_analytics.db "SELECT port, SUM(vessel_count) FROM port_hourly_stats GROUP BY port;"
```

## Viewing the visualisations

The viz files use `fetch()` / `d3.json()` and must be served over HTTP — they will not work opened directly as `file://` URLs.

```bash
cd viz
python -m http.server 8000
```

Then open:
- `http://localhost:8000/port_dashboard.html` — port analytics dashboard
- `http://localhost:8000/map.html` — Danish waters vessel playback

To rebuild the dashboard data after re-running the pipeline:

```bash
python viz/prepare_dashboard_data.py
```

## Scope constraints

- Five Danish ports, full month of data (Feb 2026) — keep scope tight
- Bounding box filter happens early in the pipeline to limit memory pressure from the full DMA dataset (~15M rows/day)
- No dbt or orchestration tooling — plain Python scripts are intentional
- SQLite is the local/portfolio target; Azure SQL is the production target (connection string swap only)
