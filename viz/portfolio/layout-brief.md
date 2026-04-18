# Portfolio Layout Brief — Port Activity Analytics

Four self-contained JS modules for `yoerivda.be`. Each is an IIFE; load its data script first, then the module script. All share `viz.css` and the dark-navy `#0f172a` theme.

---

## 1. Port Dashboard · `port-dashboard.js`

**Mount:** `<div id="chart-port"></div>`

**Data:** `<script src="data/dashboard.js"></script>` → sets `window.DASHBOARD_DATA`

**Dependencies:** D3 v7, `viz.css`

**Layout the module injects:**
- Port-pill selector row (multi-select, colour-coded)
- KPI strip — 5 cards: Total vessel-hours · Peak hourly count · Avg dwell · Monthly arrivals · Avg daily arrivals
- 3-column CSS grid:
  - Row 1 cols 1–2: Stacked-bar daily vessel count (with optional legend)
  - Row 1–2 col 3: Vessel type donut (320 px wide, spans 2 rows)
  - Row 2 col 1: Daily arrivals & departures line chart
  - Row 2 col 2: % stationary vessels by hour (multi-line)
  - Row 3 full-width: Heatmap with toggle (Vessel count / % Stationary)
- Tooltip (fixed within the IIFE scope)

**Interactions:** port multi-select, heatmap mode toggle, hover tooltips on all charts.

---

## 2. Aarhus Zone Dashboard · `aarhus-dashboard.js`

**Mount:** `<div id="chart-aarhus"></div>`

**Data:** `<script src="data/aarhus_analytics.js"></script>` → sets `window.AARHUS_DATA`

**Dependencies:** D3 v7, Leaflet 1.9.4 (CSS + JS), `viz.css`

**Layout the module injects:**
- Zone-pill selector row (multi-select, zone-coloured: outer_approach / anchorage / south_terminal / north_terminal)
- Vessel-type pill row (All · None · per-type; affects all 6 charts)
- KPI strip — 5 cards: Vessel-hours · Avg peak hourly count · Busiest zone · Anchorage wait · % Stationary in anchorage
- 2-column CSS grid:
  - Row 1 full-width: Stacked area — daily vessel count by zone + Leaflet mini-map inset (220×220 px, top-right, zone polygons on dark tiles)
  - Row 2 col 1: Grouped-bar dwell time by vessel type & zone
  - Row 2 col 2: Multi-line speed by hour with 95% shaded band (p2.5–p97.5)
  - Row 3 col 1: Stacked % nav-status bars per zone
  - Row 3 col 2: Small-multiple donuts — vessel type mix per zone
  - Row 4 full-width: Zone congestion heatmap (dow × hour) with per-zone selector
- Tooltip

**Interactions:** zone multi-select, type multi-select, heatmap zone selector, hover tooltips.

---

## 3. Danish Waters Map · `map.js`

**Mount:** `<div id="chart-map" style="height:600px;"></div>` *(host must set height)*

**Data:** `<script src="data/vessels_2026-02-01.js"></script>` → sets `window.VESSEL_DATA`

**Dependencies:** Leaflet 1.9.4 (CSS + JS), `viz.css`

**Layout the module injects (position:absolute overlays inside mount):**
- Full Leaflet map (dark CartoDB tiles)
- Canvas vessel overlay (trails + dots, colour by vessel type)
- Header badge — top-left, with bbox caption line
- Vessel count badge — top-right
- Controls panel — bottom-centre:
  - Play/Pause + time slider + UTC display
  - Trail selector pills (30 min / 1 hr / 3 hr / All day)
  - Type filter pills (All · None · per-type coloured dots)
- Port bbox rectangles — amber dashed (`#ffd166`), sourced from payload `data.ports`; persistent NW-corner labels
- Port anchor markers at centroid (hover tooltip)

**Interactions:** play/pause, scrub slider, trail selector, type filter.

---

## 4. Aarhus Zone Map · `aarhus-map.js`

**Mount:** `<div id="chart-aarhus-map" style="height:600px;"></div>` *(host must set height)*

**Data:** `<script src="data/aarhus_vessels_feb2026.js"></script>` → sets `window.AARHUS_VESSEL_DATA`

**Dependencies:** Leaflet 1.9.4 (CSS + JS), `viz.css`

**Layout the module injects (position:absolute overlays inside mount):**
- Full Leaflet map zoomed to Aarhus (dark CartoDB tiles)
- Canvas vessel overlay (trails + dots, colour by type or zone)
- Zone polygon overlays — dashed outline, 6% fill, persistent centroid labels
- Header badge — top-left
- Vessel count badge — top-right
- Controls panel — bottom-centre:
  - Pause · Slow/Normal/Fast speed buttons · time display (Feb DD · HH:MM) · slider
  - Trail selector pills (1 hr / 6 hr / 1 day / All)
  - Zone filter pills (All · None · per-zone coloured dots · Unzoned)
  - Type filter pills (All · None · per-type coloured dots)
  - Color mode toggle (By type / By zone)

**Interactions:** pause/speed, scrub, trail selector, zone filter, type filter, color toggle.

---

## Wiring example

```html
<!-- Shared -->
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://d3js.org/d3.v7.min.js"></script>
<link rel="stylesheet" href="viz.css">

<!-- Dashboard sections -->
<div id="chart-port"></div>
<script src="data/dashboard.js"></script>
<script src="port-dashboard.js"></script>

<div id="chart-aarhus"></div>
<script src="data/aarhus_analytics.js"></script>
<script src="aarhus-dashboard.js"></script>

<!-- Map sections (host sets height) -->
<div id="chart-map" style="height:600px;"></div>
<script src="data/vessels_2026-02-01.js"></script>
<script src="map.js"></script>

<div id="chart-aarhus-map" style="height:600px;"></div>
<script src="data/aarhus_vessels_feb2026.js"></script>
<script src="aarhus-map.js"></script>
```
