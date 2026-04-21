/**
 * aarhus-dashboard.js — Port Activity Analytics · portfolio build
 *
 * Mounts the Aarhus zone analytics dashboard into document.getElementById('chart-aarhus').
 * Dependencies (load before this script):
 *   <script src="https://d3js.org/d3.v7.min.js"></script>
 *   <link  rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
 *   <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
 *   <script src="data/aarhus_analytics.js"></script>   <!-- sets window.AARHUS_DATA -->
 *   <link  rel="stylesheet" href="viz.css">
 */
(function () {
  const mount = document.getElementById('chart-aarhus');
  if (!mount) return;
  const data = window.AARHUS_DATA;
  if (!data) { mount.textContent = 'Aarhus data not loaded (missing data/aarhus_analytics.js).'; return; }

  // ── Inject DOM skeleton ──────────────────────────────────────────────────
  mount.classList.add('pa-dashboard');
  mount.innerHTML = `
    <div class="pa-filter-bar" id="ca-zone-bar">
      <span class="bar-label">Zones</span>
    </div>
    <div class="pa-filter-bar" id="ca-type-bar">
      <span class="bar-label">Vessel type</span>
    </div>
    <div class="pa-kpi-strip" id="ca-kpi-strip"></div>
    <div class="pa-grid" id="ca-grid" style="grid-template-columns:1fr 1fr;grid-template-rows:auto auto auto auto;">
      <div class="pa-card" id="ca-card-trend" style="grid-column:1/-1;grid-row:1;">
        <h2>Daily vessel count by zone — Feb 2026</h2>
        <div style="display:flex;gap:16px;align-items:flex-start;">
          <div id="ca-chart-trend" style="flex:1;min-width:0;"></div>
          <div class="pa-zones-inset" id="ca-zones-inset"></div>
        </div>
      </div>
      <div class="pa-card" id="ca-card-dwell" style="grid-column:1;grid-row:2;">
        <h2>Avg dwell time by zone &amp; vessel type (hours)</h2>
        <div id="ca-chart-dwell"></div>
      </div>
      <div class="pa-card" id="ca-card-speed" style="grid-column:2;grid-row:2;">
        <h2>Avg speed by zone &amp; hour of day (knots)</h2>
        <div id="ca-chart-speed"></div>
      </div>
      <div class="pa-card" id="ca-card-navstatus" style="grid-column:1;grid-row:3;">
        <h2>Navigational status mix by zone</h2>
        <div id="ca-chart-navstatus"></div>
      </div>
      <div class="pa-card" id="ca-card-donuts" style="grid-column:2;grid-row:3;">
        <h2>Vessel type mix by zone</h2>
        <div id="ca-donuts-grid"></div>
      </div>
      <div class="pa-card" id="ca-card-heatmap" style="grid-column:1/-1;grid-row:4;">
        <div class="pa-card-header">
          <h2>Zone congestion — avg vessels by hour &amp; day of week</h2>
          <div class="hz-btns" id="ca-heatmap-zone-btns"></div>
        </div>
        <div id="ca-chart-heatmap"></div>
      </div>
    </div>
    <div class="pa-tooltip" id="ca-tooltip"></div>
  `;

  // ── Constants ────────────────────────────────────────────────────────────
  const ZONE_COLORS = {
    'outer_approach': '#4a6cf7',
    'anchorage':      '#ff7f0e',
    'south_terminal': '#2ca02c',
    'north_terminal': '#9467bd',
  };
  const TYPE_COLORS = {
    'Cargo':          '#1f77b4',
    'Tanker':         '#ff7f0e',
    'Passenger':      '#2ca02c',
    'Fishing':        '#d62728',
    'Tug':            '#9467bd',
    'Pleasure Craft': '#8c564b',
    'HSC':            '#e377c2',
    'SAR':            '#7f7f7f',
    'Other':          '#bcbd22',
  };
  const NAV_COLORS = d3.scaleOrdinal(d3.schemeTableau10);
  const DOW_LABELS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];

  // Dark-theme SVG text fills
  const T_DIM  = 'rgba(255,255,255,0.45)';
  const T_MID  = 'rgba(255,255,255,0.60)';
  const T_MAIN = '#e2e8f0';

  const tooltip = document.getElementById('ca-tooltip');
  function showTip(html, event) {
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';
    tooltip.style.left = (event.clientX + 14) + 'px';
    tooltip.style.top  = (event.clientY - 10) + 'px';
  }
  function moveTip(event) {
    tooltip.style.left = (event.clientX + 14) + 'px';
    tooltip.style.top  = (event.clientY - 10) + 'px';
  }
  function hideTip() { tooltip.style.display = 'none'; }

  // ── State ────────────────────────────────────────────────────────────────
  const zones    = data.zones;
  const allTypes = [...new Set(data.zone_daily_traffic.map(d => d.vessel_type))].sort();
  let activeZones = [...zones];
  let activeTypes = [...allTypes];
  let heatmapZone = zones[0];

  // ── Zone selector ────────────────────────────────────────────────────────
  const zoneBar = document.getElementById('ca-zone-bar');
  zones.forEach(z => {
    const btn = document.createElement('button');
    btn.className = 'pa-zone-btn active';
    btn.textContent = data.zone_labels[z];
    btn.dataset.zone = z;
    btn.style.setProperty('--c', ZONE_COLORS[z]);
    btn.addEventListener('click', () => {
      const idx = activeZones.indexOf(z);
      if (idx === -1) { activeZones.push(z); btn.classList.add('active'); }
      else { if (activeZones.length === 1) return; activeZones.splice(idx, 1); btn.classList.remove('active'); }
      update(activeZones);
    });
    zoneBar.appendChild(btn);
  });

  // ── Type selector ────────────────────────────────────────────────────────
  const typeBar = document.getElementById('ca-type-bar');

  function syncTypeBtnUI() {
    typeBar.querySelectorAll('.pa-type-btn[data-type]').forEach(b =>
      b.classList.toggle('active', activeTypes.includes(b.dataset.type))
    );
  }

  const typeAllBtn = document.createElement('button');
  typeAllBtn.className = 'pa-type-btn';
  typeAllBtn.textContent = 'All';
  typeAllBtn.addEventListener('click', () => {
    activeTypes = [...allTypes]; syncTypeBtnUI(); update(activeZones);
  });
  typeBar.appendChild(typeAllBtn);

  const typeNoneBtn = document.createElement('button');
  typeNoneBtn.className = 'pa-type-btn';
  typeNoneBtn.textContent = 'None';
  typeNoneBtn.addEventListener('click', () => {
    activeTypes = []; syncTypeBtnUI(); update(activeZones);
  });
  typeBar.appendChild(typeNoneBtn);

  allTypes.forEach(t => {
    const btn = document.createElement('button');
    btn.className = 'pa-type-btn active';
    btn.textContent = t;
    btn.dataset.type = t;
    btn.addEventListener('click', () => {
      const idx = activeTypes.indexOf(t);
      if (idx === -1) { activeTypes.push(t); btn.classList.add('active'); }
      else { activeTypes.splice(idx, 1); btn.classList.remove('active'); }
      update(activeZones);
    });
    typeBar.appendChild(btn);
  });

  // ── Heatmap zone selector ────────────────────────────────────────────────
  const hzBtns = document.getElementById('ca-heatmap-zone-btns');
  zones.forEach(z => {
    const btn = document.createElement('button');
    btn.className = 'pa-hz-btn' + (z === heatmapZone ? ' active' : '');
    btn.textContent = data.zone_labels[z];
    btn.style.setProperty('--c', ZONE_COLORS[z]);
    btn.addEventListener('click', () => {
      heatmapZone = z;
      hzBtns.querySelectorAll('.pa-hz-btn').forEach(b => b.classList.toggle('active', b === btn));
      renderHeatmap(heatmapZone, activeTypes);
    });
    hzBtns.appendChild(btn);
  });

  // ── KPIs ─────────────────────────────────────────────────────────────────
  function renderKPIs(activeZones) {
    const strip = document.getElementById('ca-kpi-strip');
    strip.innerHTML = '';

    const totalVH = d3.sum(
      data.zone_daily_traffic.filter(d =>
        activeZones.includes(d.zone) && activeTypes.includes(d.vessel_type)
      ), d => d.vessel_count
    );

    const zoneVH = {};
    data.zone_daily_traffic
      .filter(d => activeZones.includes(d.zone) && activeTypes.includes(d.vessel_type))
      .forEach(d => { zoneVH[d.zone] = (zoneVH[d.zone] || 0) + d.vessel_count; });
    const busiestEntry = Object.entries(zoneVH).sort((a, b) => b[1] - a[1])[0];
    const busiestZone  = busiestEntry ? busiestEntry[0] : null;
    const busiestVH   = busiestEntry ? busiestEntry[1] : null;

    const heatRows = data.zone_congestion_heatmap.filter(d =>
      activeZones.includes(d.zone) && activeTypes.includes(d.vessel_type)
    );
    let peakAvg = null;
    if (heatRows.length) {
      const byCell = d3.rollup(heatRows,
        v => d3.sum(v, d => d.avg_vessel_count),
        d => `${d.zone}|${d.dow}|${d.hour}`
      );
      peakAvg = d3.max([...byCell.values()]);
    }

    const anchorageActive = activeZones.includes('anchorage');
    const dwellRows = data.dwell_by_zone_type.filter(d =>
      d.zone === 'anchorage' && ['Cargo','Tanker'].includes(d.vessel_type) &&
      activeTypes.includes(d.vessel_type)
    );
    const totalVisits = d3.sum(dwellRows, d => d.visit_count);
    const weightedDwell = totalVisits > 0
      ? d3.sum(dwellRows, d => d.avg_dwell_hours * d.visit_count) / totalVisits
      : null;

    const mooredRows  = data.navstatus_by_zone.filter(
      d => d.zone === 'anchorage' && activeTypes.includes(d.vessel_type)
    );
    const totalPings  = d3.sum(mooredRows, d => d.ping_count);
    const mooredPings = d3.sum(mooredRows.filter(d => d.nav_status === 'Moored'), d => d.ping_count);
    const pctMoored   = totalPings > 0 ? (mooredPings / totalPings * 100).toFixed(1) : null;

    [
      { label: 'Vessel-hours (Feb)',            value: d3.format(',')(totalVH),
        sub: 'across selected zones' },
      { label: 'Avg peak hourly count',         value: peakAvg != null ? peakAvg.toFixed(1) : '—',
        sub: 'highest avg vessels in any zone-hour' },
      { label: 'Busiest zone',                  value: busiestZone ? data.zone_labels[busiestZone] : '—',
        sub: busiestVH != null ? d3.format(',')(busiestVH) + ' vessel-hours' : '' },
      { label: 'Anchorage wait (cargo & tanker)',
        value: anchorageActive && weightedDwell != null ? weightedDwell.toFixed(1) + ' h' : '—',
        sub: anchorageActive ? 'avg dwell, excl. transits' : 'anchorage not selected' },
      { label: 'Stationary in anchorage',
        value: anchorageActive && pctMoored != null ? pctMoored + '%' : '—',
        sub: anchorageActive ? 'of pings showing moored status' : 'anchorage not selected' },
    ].forEach(k => {
      const card = document.createElement('div');
      card.className = 'pa-kpi-card';
      card.innerHTML = `<div class="label">${k.label}</div><div class="value">${k.value}</div><div class="sub">${k.sub}</div>`;
      strip.appendChild(card);
    });
  }

  // ── Trend — stacked area by zone ─────────────────────────────────────────
  function renderTrend(activeZones, activeTypes) {
    const el = document.getElementById('ca-chart-trend');
    el.innerHTML = '';

    const dates = [...new Set(data.zone_daily_traffic.map(d => d.date))].sort();
    const byDate = dates.map(dateStr => {
      const row = { dateStr, date: new Date(dateStr + 'T12:00:00') };
      zones.forEach(z => {
        const total = d3.sum(
          data.zone_daily_traffic.filter(d =>
            d.zone === z && d.date === dateStr && activeTypes.includes(d.vessel_type)
          ), d => d.vessel_count
        );
        row[z] = activeZones.includes(z) ? total : 0;
      });
      return row;
    });

    const activeStack = activeZones.slice().sort((a, b) => zones.indexOf(a) - zones.indexOf(b));
    const series = d3.stack().keys(activeStack)(byDate);
    const W = el.getBoundingClientRect().width || 900;
    const H = 200;
    const margin = { top: 12, right: activeStack.length > 1 ? 130 : 16, bottom: 30, left: 44 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;

    const x = d3.scaleTime().domain(d3.extent(byDate, d => d.date)).range([0, iW]);
    const maxY = d3.max(byDate, row => activeStack.reduce((s, z) => s + row[z], 0));
    const y = d3.scaleLinear().domain([0, maxY * 1.1]).range([iH, 0]);

    const area = z => d3.area()
      .x(d => x(d.data.date))
      .y0(d => y(d[0]))
      .y1(d => y(d[1]))
      .curve(d3.curveMonotoneX);

    const svg = d3.select(el).append('svg').attr('width', W).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    g.append('g').attr('class','pa-svg-grid')
      .call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(5))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();

    series.forEach(s => {
      const z = s.key;
      g.append('path').datum(s)
        .attr('d', area(z))
        .attr('fill', ZONE_COLORS[z])
        .attr('opacity', 0.75)
        .on('mouseover', event => showTip(`<b>${data.zone_labels[z]}</b>`, event))
        .on('mousemove', moveTip)
        .on('mouseout', hideTip);
    });

    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x).ticks(d3.timeDay.every(7)).tickFormat(d3.timeFormat('%b %d')));
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.selectAll('.pa-axis text').attr('fill', T_DIM);
    g.selectAll('.pa-axis line').attr('stroke','rgba(255,255,255,0.12)');

    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(5));
    g.selectAll('.pa-axis-y text').attr('fill', T_DIM);
    g.select('.pa-axis-y .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.selectAll('.pa-axis-y line').attr('stroke','rgba(255,255,255,0.12)');

    if (activeStack.length > 1) {
      const lg = svg.append('g').attr('transform', `translate(${W - margin.right + 10},${margin.top})`);
      [...activeStack].reverse().forEach((z, i) => {
        const row = lg.append('g').attr('transform', `translate(0,${i * 16})`);
        row.append('rect').attr('width',10).attr('height',10).attr('rx',2)
          .attr('fill', ZONE_COLORS[z]).attr('opacity', 0.75);
        row.append('text').attr('x',14).attr('y',9).attr('font-size','10px').attr('fill', T_MID)
          .text(data.zone_labels[z]);
      });
    }
  }

  // ── Dwell — grouped bars by vessel type, one group per zone ──────────────
  function renderDwell(activeZones, activeTypes) {
    const el = document.getElementById('ca-chart-dwell');
    el.innerHTML = '';

    const rows = data.dwell_by_zone_type.filter(d =>
      activeZones.includes(d.zone) && d.avg_dwell_hours > 0 && activeTypes.includes(d.vessel_type)
    );
    if (!rows.length) { el.textContent = 'No dwell data for selection.'; return; }

    const types = [...new Set(rows.map(d => d.vessel_type))].sort();
    const W = el.getBoundingClientRect().width || 420;
    const H = 220;
    const margin = { top: 12, right: 16, bottom: 60, left: 44 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;

    const x0 = d3.scaleBand().domain(types).range([0, iW]).padding(0.2);
    const x1 = d3.scaleBand().domain(activeZones).range([0, x0.bandwidth()]).padding(0.05);
    const y  = d3.scaleLinear()
      .domain([0, d3.max(rows, d => d.avg_dwell_hours) * 1.1]).range([iH, 0]);

    const svg = d3.select(el).append('svg').attr('width', W).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    g.append('g').attr('class','pa-svg-grid')
      .call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(5))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();

    types.forEach(type => {
      activeZones.forEach(z => {
        const d = rows.find(r => r.vessel_type === type && r.zone === z);
        if (!d) return;
        g.append('rect')
          .attr('x', x0(type) + x1(z))
          .attr('y', y(d.avg_dwell_hours))
          .attr('width', x1.bandwidth())
          .attr('height', iH - y(d.avg_dwell_hours))
          .attr('fill', ZONE_COLORS[z])
          .attr('rx', 2)
          .on('mouseover', event => showTip(
            `<b>${data.zone_labels[z]}</b><br>${type}: <b>${d.avg_dwell_hours.toFixed(1)} h</b><br>(${d.visit_count} visits)`, event
          ))
          .on('mousemove', moveTip)
          .on('mouseout', hideTip);
      });
    });

    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x0).tickSize(0))
      .selectAll('text').attr('fill', T_DIM).attr('transform','rotate(-30)').attr('text-anchor','end').attr('dy','0.5em');
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');

    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(5).tickFormat(d => d + 'h'));
    g.selectAll('.pa-axis-y text').attr('fill', T_DIM);
    g.select('.pa-axis-y .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.selectAll('.pa-axis-y line').attr('stroke','rgba(255,255,255,0.12)');

    const lg = svg.append('g').attr('transform', `translate(${margin.left},${H - 14})`);
    activeZones.forEach((z, i) => {
      const row = lg.append('g').attr('transform', `translate(${i * 120},0)`);
      row.append('rect').attr('width',9).attr('height',9).attr('rx',2).attr('fill', ZONE_COLORS[z]);
      row.append('text').attr('x',13).attr('y',8).attr('font-size','9px').attr('fill', T_MID)
        .text(data.zone_labels[z]);
    });
  }

  // ── Speed — multi-zone lines with 95% band ────────────────────────────────
  function renderSpeed(activeZones, activeTypes) {
    const el = document.getElementById('ca-chart-speed');
    el.innerHTML = '';

    function aggregateByHour(z) {
      return d3.range(0, 24).map(h => {
        const matching = data.zone_speed_all.filter(d =>
          d.zone === z && d.hour === h && activeTypes.includes(d.vessel_type)
        );
        if (!matching.length) return null;
        return {
          hour:       h,
          sog_mean:   d3.mean(matching, d => d.sog_mean),
          sog_median: d3.mean(matching, d => d.sog_median),
          sog_p025:   d3.mean(matching, d => d.sog_p025),
          sog_p95:    d3.mean(matching, d => d.sog_p95),
          sog_p975:   d3.mean(matching, d => d.sog_p975),
        };
      }).filter(Boolean);
    }

    const byZone = {};
    activeZones.forEach(z => { byZone[z] = aggregateByHour(z); });
    const allRows = Object.values(byZone).flat();
    if (!allRows.length) { el.textContent = 'No speed data for selection.'; return; }

    const W = el.getBoundingClientRect().width || 420;
    const H = 240;
    const margin = { top: 16, right: 16, bottom: 30, left: 40 };
    const iW = W - margin.left - margin.right;
    const iH = H - margin.top - margin.bottom;

    const x = d3.scaleLinear().domain([0, 23]).range([0, iW]);
    const maxSOG = d3.max(allRows, d => d.sog_p975 ?? d.sog_mean) * 1.15 || 1;
    const y = d3.scaleLinear().domain([0, maxSOG]).range([iH, 0]);

    const line = d3.line()
      .x(d => x(d.hour)).y(d => y(d.sog_mean))
      .curve(d3.curveCatmullRom.alpha(0.5));

    const svg = d3.select(el).append('svg').attr('width', W).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    g.append('g').attr('class','pa-svg-grid')
      .call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(5))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();

    activeZones.forEach(z => {
      const rows = byZone[z];
      if (!rows.length) return;
      const color = ZONE_COLORS[z];

      const bandArea = d3.area()
        .x(d => x(d.hour)).y0(d => y(d.sog_p025)).y1(d => y(d.sog_p975))
        .curve(d3.curveCatmullRom.alpha(0.5))(rows);

      g.append('path').attr('d', bandArea).attr('fill', color).attr('opacity', 0.12).attr('pointer-events','none');
      g.append('path').datum(rows).attr('d', line)
        .attr('fill','none').attr('stroke', color).attr('stroke-width', 2).attr('opacity', 0.85);

      g.selectAll(`.ca-dot-${z}`).data(rows).join('circle')
        .attr('class', `ca-dot-${z}`)
        .attr('cx', d => x(d.hour)).attr('cy', d => y(d.sog_mean)).attr('r', 7)
        .attr('fill','transparent')
        .on('mouseover', (event, d) => showTip(
          `<b>${data.zone_labels[z]}</b> · ${String(d.hour).padStart(2,'0')}:00<br>`+
          `Mean: <b>${d.sog_mean.toFixed(2)} kn</b> · Median: ${d.sog_median.toFixed(2)} kn<br>`+
          `P95: ${d.sog_p95.toFixed(2)} kn · 95% band: [${d.sog_p025.toFixed(2)}, ${d.sog_p975.toFixed(2)}]`, event
        ))
        .on('mousemove', moveTip).on('mouseout', hideTip);

      g.selectAll(`.ca-vdot-${z}`).data(rows).join('circle')
        .attr('class', `ca-vdot-${z}`)
        .attr('cx', d => x(d.hour)).attr('cy', d => y(d.sog_mean)).attr('r', 2.5)
        .attr('fill', color).attr('pointer-events','none');
    });

    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0,24,4))
        .tickFormat(h => String(h).padStart(2,'0')+':00'));
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.selectAll('.pa-axis text').attr('fill', T_DIM);
    g.selectAll('.pa-axis line').attr('stroke','rgba(255,255,255,0.12)');

    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(5).tickFormat(d => d + ' kn'));
    g.selectAll('.pa-axis-y text').attr('fill', T_DIM);
    g.select('.pa-axis-y .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.selectAll('.pa-axis-y line').attr('stroke','rgba(255,255,255,0.12)');

    const lg = svg.append('g').attr('transform', `translate(${margin.left},${H - 2})`);
    let lx = 0;
    activeZones.forEach(z => {
      const item = lg.append('g').attr('transform', `translate(${lx},0)`);
      item.append('line').attr('x1',0).attr('x2',14).attr('y1',-4).attr('y2',-4)
        .attr('stroke', ZONE_COLORS[z]).attr('stroke-width', 2);
      const label = data.zone_labels[z].replace(' (Ferry/RoRo)','').replace(' (Container)','');
      item.append('text').attr('x',18).attr('y',0).attr('font-size','9px').attr('fill', T_MID).text(label);
      lx += label.length * 5.2 + 28;
    });
  }

  // ── Nav status — stacked % bars ──────────────────────────────────────────
  function renderNavStatus(activeZones, activeTypes) {
    const el = document.getElementById('ca-chart-navstatus');
    el.innerHTML = '';

    const rows = data.navstatus_by_zone.filter(d =>
      activeZones.includes(d.zone) && activeTypes.includes(d.vessel_type)
    );
    if (!rows.length) { el.textContent = 'No data for selection.'; return; }

    const statusTotals = d3.rollup(rows, v => d3.sum(v, d => d.ping_count), d => d.nav_status);
    const topStatuses  = [...statusTotals.entries()].sort((a,b) => b[1]-a[1]).slice(0,6).map(([s]) => s);
    const zoneTotals   = d3.rollup(rows, v => d3.sum(v, d => d.ping_count), d => d.zone);
    NAV_COLORS.domain(topStatuses);

    const W = el.getBoundingClientRect().width || 420;
    const barH = 32;
    const legendH = Math.ceil(topStatuses.length / 2) * 14 + 8;
    const margin  = { top: 12, right: 16, bottom: legendH, left: 120 };
    const H = activeZones.length * (barH + 6) + margin.top + margin.bottom;
    const iW = W - margin.left - margin.right;

    const svg = d3.select(el).append('svg').attr('width', W).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    activeZones.forEach((z, zi) => {
      const zoneRows = rows.filter(r => r.zone === z);
      const total = zoneTotals.get(z) || 1;
      let x0 = 0;

      topStatuses.forEach(status => {
        const row = zoneRows.filter(r => r.nav_status === status);
        const pct = row.length ? d3.sum(row, r => r.ping_count) / total : 0;
        const bW  = pct * iW;
        if (bW < 0.5) return;
        g.append('rect')
          .attr('x', x0).attr('y', zi * (barH + 6))
          .attr('width', bW).attr('height', barH)
          .attr('fill', NAV_COLORS(status))
          .on('mouseover', event => showTip(
            `<b>${data.zone_labels[z]}</b><br>${status}<br>${(pct * 100).toFixed(1)}% of pings`, event
          ))
          .on('mousemove', moveTip).on('mouseout', hideTip);
        if (bW > 30) {
          g.append('text')
            .attr('x', x0 + bW / 2).attr('y', zi * (barH + 6) + barH / 2 + 4)
            .attr('text-anchor','middle').attr('font-size','9px').attr('fill','#fff').attr('pointer-events','none')
            .text((pct * 100).toFixed(0) + '%');
        }
        x0 += bW;
      });

      g.append('text')
        .attr('x', -8).attr('y', zi * (barH + 6) + barH / 2 + 4)
        .attr('text-anchor','end').attr('font-size','10px').attr('fill', T_MID)
        .text(data.zone_labels[z].replace(' (Ferry/RoRo)','').replace(' (Container)',''));
    });

    const lg = svg.append('g').attr('transform', `translate(${margin.left},${H - legendH + 4})`);
    topStatuses.forEach((s, i) => {
      const col = i % 2, row = Math.floor(i / 2);
      const item = lg.append('g').attr('transform', `translate(${col * (iW / 2)},${row * 14})`);
      item.append('rect').attr('width',9).attr('height',9).attr('rx',2).attr('fill', NAV_COLORS(s));
      item.append('text').attr('x',13).attr('y',8).attr('font-size','9px').attr('fill', T_MID).text(s);
    });
  }

  // ── Donuts — vessel type mix per zone ────────────────────────────────────
  function renderDonuts(activeZones, activeTypes) {
    const grid = document.getElementById('ca-donuts-grid');
    grid.innerHTML = '';
    grid.style.display = 'grid';
    grid.style.gridTemplateColumns = '1fr 1fr';
    grid.style.gap = '8px';

    activeZones.forEach(z => {
      const cell = document.createElement('div');
      cell.style.textAlign = 'center';

      const label = document.createElement('div');
      label.style.cssText = 'font-size:0.72rem;font-weight:600;color:rgba(255,255,255,0.6);margin-bottom:4px;padding-bottom:3px;';
      label.style.borderBottom = `2px solid ${ZONE_COLORS[z]}`;
      label.textContent = data.zone_labels[z].replace(' (Ferry/RoRo)','').replace(' (Container)','');
      cell.appendChild(label);

      const svgEl = document.createElement('div');
      cell.appendChild(svgEl);
      grid.appendChild(cell);

      const types = data.zone_type_mix.filter(d =>
        d.zone === z && activeTypes.includes(d.vessel_type)
      );
      if (!types.length) return;

      const total = d3.sum(types, d => d.visit_count);
      const R = 56, W = 160, H = R * 2 + 20;

      const svg = d3.select(svgEl).append('svg').attr('width', W).attr('height', H);
      const gd  = svg.append('g').attr('transform', `translate(${W/2},${R+8})`);

      const pie  = d3.pie().value(d => d.visit_count).sort(null);
      const arc  = d3.arc().innerRadius(R*0.5).outerRadius(R);
      const arcH = d3.arc().innerRadius(R*0.5).outerRadius(R+5);

      gd.selectAll('path').data(pie(types)).join('path')
        .attr('d', arc)
        .attr('fill', d => TYPE_COLORS[d.data.vessel_type] || '#666')
        .attr('stroke','#1e293b').attr('stroke-width',1.5)
        .on('mouseover', function(event, d) {
          d3.select(this).attr('d', arcH);
          const pct = (d.data.visit_count / total * 100).toFixed(1);
          showTip(`<b>${data.zone_labels[z]}</b><br>${d.data.vessel_type}<br>${d.data.visit_count} visits (${pct}%)`, event);
        })
        .on('mousemove', moveTip)
        .on('mouseout', function() { d3.select(this).attr('d', arc); hideTip(); });

      gd.append('text').attr('text-anchor','middle').attr('dy','-0.1em')
        .attr('font-size','0.95rem').attr('font-weight','700').attr('fill', T_MAIN).text(total);
      gd.append('text').attr('text-anchor','middle').attr('dy','1em')
        .attr('font-size','0.6rem').attr('fill', T_DIM).text('visits');
    });

    const donutTypes = [...new Set(data.zone_type_mix.map(d => d.vessel_type))]
      .sort((a, b) => {
        const ta = d3.sum(data.zone_type_mix.filter(d => d.vessel_type === a), d => d.visit_count);
        const tb = d3.sum(data.zone_type_mix.filter(d => d.vessel_type === b), d => d.visit_count);
        return tb - ta;
      });
    const legendDiv = document.createElement('div');
    legendDiv.style.cssText = 'grid-column:1/-1;display:flex;flex-wrap:wrap;gap:8px 16px;margin-top:8px;font-size:10px;color:rgba(255,255,255,0.6);';
    donutTypes.forEach(t => {
      const item = document.createElement('span');
      item.style.cssText = 'display:flex;align-items:center;gap:4px;';
      item.innerHTML = `<svg width="10" height="10"><rect width="10" height="10" rx="2" fill="${TYPE_COLORS[t] || '#666'}"/></svg>${t}`;
      legendDiv.appendChild(item);
    });
    grid.appendChild(legendDiv);
  }

  // ── Heatmap — avg vessel count by dow × hour for selected zone ───────────
  function renderHeatmap(zone, activeTypes) {
    const el = document.getElementById('ca-chart-heatmap');
    el.innerHTML = '';

    const rows = data.zone_congestion_heatmap.filter(d =>
      d.zone === zone && activeTypes.includes(d.vessel_type)
    );
    if (!rows.length) { el.textContent = 'No heatmap data for this zone.'; return; }

    const mat = Array.from({length: 7}, () => Array(24).fill(0));
    rows.forEach(d => { mat[d.dow][d.hour] += d.avg_vessel_count; });
    const matMax = d3.max(mat.flat()) || 1;
    const colorScale = d3.scaleSequential(d3.interpolateBlues).domain([0, matMax]);

    const W = el.getBoundingClientRect().width || 900;
    const cellH = 22;
    const cellW = Math.floor((W - 52) / 24);
    const margin = { top: 8, right: 8, bottom: 28, left: 44 };
    const iW = cellW * 24, iH = cellH * 7;
    const H = iH + margin.top + margin.bottom;

    const svg = d3.select(el).append('svg')
      .attr('width', iW + margin.left + margin.right).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    for (let dow = 0; dow < 7; dow++) {
      for (let hour = 0; hour < 24; hour++) {
        const val = mat[dow][hour];
        g.append('rect')
          .attr('x', hour * cellW).attr('y', dow * cellH)
          .attr('width', cellW - 1).attr('height', cellH - 1).attr('rx', 2)
          .attr('fill', val > 0 ? colorScale(val) : 'rgba(255,255,255,0.04)')
          .on('mouseover', event => showTip(
            `<b>${DOW_LABELS[dow]} ${String(hour).padStart(2,'0')}:00</b><br>Avg vessels: <b>${val.toFixed(1)}</b>`, event
          ))
          .on('mousemove', moveTip).on('mouseout', hideTip);
      }
    }

    DOW_LABELS.forEach((d, i) => {
      g.append('text')
        .attr('x', -6).attr('y', i * cellH + cellH / 2 + 4)
        .attr('text-anchor','end').attr('font-size','10px').attr('fill', T_DIM).text(d);
    });
    d3.range(0, 24, 4).forEach(h => {
      g.append('text')
        .attr('x', h * cellW + cellW / 2).attr('y', iH + 16)
        .attr('text-anchor','middle').attr('font-size','10px').attr('fill', T_DIM)
        .text(String(h).padStart(2,'0') + ':00');
    });

    const lgW = Math.min(180, iW / 3);
    const lgG = svg.append('g')
      .attr('transform', `translate(${iW + margin.left - lgW},${H - margin.bottom + 6})`);
    const defs = svg.append('defs');
    const grad = defs.append('linearGradient').attr('id','ca-hm-grad');
    [0, 0.25, 0.5, 0.75, 1].forEach(t =>
      grad.append('stop').attr('offset', t).attr('stop-color', colorScale(t * matMax))
    );
    lgG.append('rect').attr('width', lgW).attr('height', 8).attr('rx', 2).attr('fill','url(#ca-hm-grad)');
    lgG.append('text').attr('x',0).attr('y',18).attr('font-size','9px').attr('fill', T_DIM).text('0');
    lgG.append('text').attr('x',lgW).attr('y',18).attr('font-size','9px').attr('fill', T_DIM)
      .attr('text-anchor','end').text(matMax.toFixed(1) + ' avg vessels');
  }

  // ── Zones inset (Leaflet mini-map) ───────────────────────────────────────
  function addZonePolygons(leafletMap, permanent) {
    const allLatLngs = [];
    zones.forEach(z => {
      const verts = data.zone_polygons[z];
      if (!verts || !verts.length) return;
      L.polygon(verts, {
        color: ZONE_COLORS[z], weight: 1.5, dashArray: '4 3',
        fillColor: ZONE_COLORS[z], fillOpacity: 0.18,
      })
        .bindTooltip(data.zone_labels[z], {
          permanent, direction: 'center', className: 'pa-inset-zone-label',
        })
        .addTo(leafletMap);
      verts.forEach(v => allLatLngs.push(v));
    });
    return allLatLngs;
  }

  function renderZonesInset() {
    if (typeof L === 'undefined') return;
    const container = document.getElementById('ca-zones-inset');
    if (!container || !data.zone_polygons) return;

    container.style.position = 'relative';

    const insetMap = L.map(container, {
      attributionControl: false, zoomControl: false, dragging: false,
      scrollWheelZoom: false, doubleClickZoom: false, boxZoom: false,
      keyboard: false, touchZoom: false,
    });

    L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      { subdomains: 'abcd', maxZoom: 14 }
    ).addTo(insetMap);

    const allLatLngs = addZonePolygons(insetMap, false);
    if (allLatLngs.length) insetMap.fitBounds(allLatLngs, { padding: [6, 6] });

    // Expand button
    const expandBtn = document.createElement('button');
    expandBtn.title = 'Expand map';
    expandBtn.textContent = '⛶';
    expandBtn.style.cssText = 'position:absolute;top:6px;right:6px;z-index:9999;width:26px;height:26px;border-radius:6px;border:1px solid rgba(255,255,255,.2);background:rgba(15,23,42,.85);color:#e2e8f0;font-size:14px;line-height:1;cursor:pointer;display:flex;align-items:center;justify-content:center;';
    container.appendChild(expandBtn);

    expandBtn.addEventListener('click', () => {
      const overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.72);z-index:200000;display:flex;align-items:center;justify-content:center;';

      const panel = document.createElement('div');
      panel.style.cssText = 'position:relative;width:900px;height:640px;background:#0f172a;border-radius:14px;overflow:hidden;box-shadow:0 24px 80px rgba(0,0,0,.6);';

      const closeBtn = document.createElement('button');
      closeBtn.textContent = '✕  Close';
      closeBtn.style.cssText = 'position:absolute;top:12px;right:12px;z-index:10001;padding:7px 16px;border-radius:8px;border:1px solid rgba(255,255,255,.35);background:rgba(15,23,42,.92);color:#e2e8f0;font-size:13px;font-weight:600;cursor:pointer;letter-spacing:.02em;box-shadow:0 2px 12px rgba(0,0,0,.5);';

      const mapDiv = document.createElement('div');
      mapDiv.style.cssText = 'width:100%;height:100%;';

      panel.appendChild(mapDiv);
      panel.appendChild(closeBtn);
      overlay.appendChild(panel);
      document.body.appendChild(overlay);

      const modalMap = L.map(mapDiv, {
        attributionControl: false, zoomControl: true,
      });
      L.tileLayer(
        'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        { subdomains: 'abcd', maxZoom: 18 }
      ).addTo(modalMap);
      const modalLatLngs = addZonePolygons(modalMap, true);
      if (modalLatLngs.length) modalMap.fitBounds(modalLatLngs, { padding: [24, 24] });

      function closeModal() { document.body.removeChild(overlay); }
      closeBtn.addEventListener('click', closeModal);
      overlay.addEventListener('click', e => { if (e.target === overlay) closeModal(); });
    });
  }

  // ── Master update ────────────────────────────────────────────────────────
  function update(activeZones) {
    renderKPIs(activeZones);
    renderTrend(activeZones, activeTypes);
    renderDwell(activeZones, activeTypes);
    renderSpeed(activeZones, activeTypes);
    renderNavStatus(activeZones, activeTypes);
    renderDonuts(activeZones, activeTypes);
    renderHeatmap(heatmapZone, activeTypes);
  }

  renderZonesInset();
  update(activeZones);
})();
