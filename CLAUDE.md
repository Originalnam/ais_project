# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Port Activity Analytics Pipeline — a portfolio project processing AIS (Automatic Identification System) vessel tracking data from the Danish Maritime Authority (DMA). The business question: how does vessel traffic and port activity evolve over time, and what patterns emerge around congestion or vessel type distribution? The pipeline is deliberately framed as a general operational analytics pipeline (transferable to logistics, energy, manufacturing), not a maritime niche project.

**Agreed stack:** Python/pandas · Azure Blob Storage · Azure SQL · D3.js  
**Deliberately excludes:** dbt, Databricks, Power BI

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
raw/*.zip  →  data/raw/unzipped/  →  ingest  →  filter  →  transform  →  load (Azure SQL)
```

1. **Raw data** — daily zip files in `raw/` named `aisdk-YYYY-MM-DD.zip`, each containing one day of AIS CSV data from the DMA.
2. **Unzip** — `pipeline/unzip.py` (or similar) extracts to `data/raw/unzipped/aisdk-YYYY-MM-DD/`.
3. **Ingest** — `pipeline/ingest.py` reads CSVs into pandas DataFrames.
4. **Filter** — `pipeline/filter.py` applies a bounding box to isolate a single port (Antwerp or Zeebrugge). This is the first and most critical reduction step — it cuts the full Danish dataset down to port-relevant records.
5. **Transform** — `pipeline/transform.py` computes analytics (vessel type distribution, congestion patterns, dwell times, etc.).
6. **Load** — `pipeline/load.py` writes results to Azure SQL using the schema in `sql/schema.sql`. KPI queries live in `sql/kpis.sql`.

## Key directories

- `pipeline/` — the ETL modules; each step is its own file
- `raw/` — source zip archives (not processed data)
- `data/raw/unzipped/` — extracted CSVs, one folder per day
- `data/processed/` — cleaned/filtered outputs
- `notebooks/` — exploratory analysis (`exploration.ipynb`)
- `sql/` — `schema.sql` (table definitions) and `kpis.sql` (analytical queries)
- `viz/` — D3.js visualisations for the portfolio website

## Scope constraints

- One port (Antwerp or Zeebrugge), one month of data — keep scope tight
- Bounding box filter happens early in the pipeline to limit memory pressure from the full DMA dataset
- No dbt or orchestration tooling — plain Python scripts are intentional
