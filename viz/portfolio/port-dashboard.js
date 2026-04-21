/**
 * port-dashboard.js — Port Activity Analytics · portfolio build
 *
 * Mounts the five-port dashboard into document.getElementById('chart-port').
 * Dependencies (load before this script):
 *   <script src="https://d3js.org/d3.v7.min.js"></script>
 *   <script src="data/dashboard.js"></script>   <!-- sets window.DASHBOARD_DATA -->
 *   <link  rel="stylesheet" href="viz.css">
 */
(function () {
  const mount = document.getElementById('chart-port');
  if (!mount) return;
  const data = window.DASHBOARD_DATA;
  if (!data) { mount.textContent = 'Dashboard data not loaded (missing data/dashboard.js).'; return; }

  // ── Inject DOM skeleton ──────────────────────────────────────────────────
  mount.classList.add('pa-dashboard');
  mount.innerHTML = `
    <div class="pa-filter-bar" id="cp-port-bar">
      <span class="bar-label">Ports</span>
    </div>
    <div class="pa-kpi-strip" id="cp-kpi-strip"></div>
    <div class="pa-grid" id="cp-grid" style="grid-template-columns:1fr 1fr 320px;grid-template-rows:auto auto auto;">
      <div class="pa-card" id="cp-card-trend"   style="grid-column:1/3;grid-row:1;">
        <h2>Daily vessel count</h2>
        <div id="cp-chart-trend"></div>
      </div>
      <div class="pa-card" id="cp-card-donut"   style="grid-column:3;grid-row:1/3;text-align:center;">
        <h2>Vessel type mix</h2>
        <div id="cp-chart-donut"></div>
      </div>
      <div class="pa-card" id="cp-card-flow"    style="grid-column:1;grid-row:2;">
        <h2>Daily arrivals &amp; departures</h2>
        <div id="cp-chart-flow"></div>
      </div>
      <div class="pa-card" id="cp-card-movement" style="grid-column:2;grid-row:2;">
        <h2>% stationary vessels by hour</h2>
        <div id="cp-chart-movement"></div>
      </div>
      <div class="pa-card" id="cp-card-heatmap" style="grid-column:1/4;grid-row:3;">
        <div class="pa-card-header">
          <h2>Heatmap — avg by hour &amp; day of week</h2>
          <div class="pa-toggle-group" id="cp-heatmap-toggle">
            <button class="pa-tgl active" data-mode="count">Vessel count</button>
            <button class="pa-tgl"        data-mode="stationary">% Stationary</button>
          </div>
        </div>
        <div id="cp-chart-heatmap"></div>
      </div>
    </div>
    <div class="pa-tooltip" id="cp-tooltip"></div>
  `;

  // ── Constants ────────────────────────────────────────────────────────────
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
  const PORT_COLORS  = d3.schemeTableau10;
  const DOW          = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];

  // Dark-theme SVG text fills
  const T_DIM  = 'rgba(255,255,255,0.45)';
  const T_MID  = 'rgba(255,255,255,0.60)';
  const T_MAIN = '#e2e8f0';

  const tooltip = document.getElementById('cp-tooltip');
  function showTip(html, event) {
    tooltip.innerHTML = html;
    tooltip.style.display = 'block';
    tooltip.style.left = (event.clientX + 14) + 'px';
    tooltip.style.top  = (event.clientY - 10) + 'px';
  }
  function hideTip() { tooltip.style.display = 'none'; }

  // ── State ────────────────────────────────────────────────────────────────
  const ports = data.ports;
  let activePorts  = [ports[0]];
  let heatmapMode  = 'count';

  // ── Heatmap toggle ───────────────────────────────────────────────────────
  document.getElementById('cp-heatmap-toggle').addEventListener('click', e => {
    const btn = e.target.closest('.pa-tgl');
    if (!btn) return;
    heatmapMode = btn.dataset.mode;
    document.querySelectorAll('#cp-heatmap-toggle .pa-tgl')
      .forEach(b => b.classList.toggle('active', b === btn));
    renderHeatmap(activePorts);
  });

  // ── Port selector ────────────────────────────────────────────────────────
  const portBar = document.getElementById('cp-port-bar');
  ports.forEach((p, i) => {
    const btn = document.createElement('button');
    btn.className = 'pa-port-btn' + (i === 0 ? ' active' : '');
    btn.textContent = p.charAt(0).toUpperCase() + p.slice(1);
    btn.dataset.port = p;
    btn.style.setProperty('--c', PORT_COLORS[i % PORT_COLORS.length]);
    btn.addEventListener('click', () => {
      const idx = activePorts.indexOf(p);
      if (idx === -1) { activePorts.push(p); btn.classList.add('active'); }
      else { if (activePorts.length === 1) return; activePorts.splice(idx, 1); btn.classList.remove('active'); }
      update(activePorts);
    });
    portBar.appendChild(btn);
  });

  // ── KPI cards ────────────────────────────────────────────────────────────
  function renderKPIs(activePorts) {
    const strip = document.getElementById('cp-kpi-strip');
    strip.innerHTML = '';
    const sums = activePorts.map(p => data.summary.find(d => d.port === p) || {});
    const totalVesselHours = d3.sum(sums, s => s.total_vessel_hours ?? 0);
    const activeSet = new Set(activePorts);
    const rollup = d3.rollup(
      (data.hourly_totals || []).filter(d => activeSet.has(d.port)),
      v => d3.sum(v, d => d.vessel_count), d => d.date, d => d.hour
    );
    let peakHourly = 0;
    for (const byHour of rollup.values())
      for (const v of byHour.values()) if (v > peakHourly) peakHourly = v;
    const dwellSamples   = sums.filter(s => s.avg_dwell_minutes != null);
    const avgDwell       = dwellSamples.length ? d3.mean(dwellSamples, s => s.avg_dwell_minutes) : null;
    const totalEntries   = d3.sum(sums.filter(s => s.total_entries != null), s => s.total_entries);
    const arrSamples     = sums.filter(s => s.avg_daily_arrivals != null);
    const avgDaily       = arrSamples.length ? d3.mean(arrSamples, s => s.avg_daily_arrivals) : null;
    [
      { label: 'Total vessel-hours', value: d3.format(',')(totalVesselHours), sub: 'Feb 2026' },
      { label: 'Peak hourly count',  value: d3.format(',')(peakHourly),       sub: 'vessels in one hour' },
      { label: 'Avg dwell time',
        value: avgDwell != null ? (avgDwell >= 60 ? (avgDwell/60).toFixed(1)+' h' : avgDwell.toFixed(0)+' min') : '—',
        sub: 'per vessel visit' },
      { label: 'Monthly arrivals',   value: d3.format(',')(totalEntries),     sub: 'vessel entries this month' },
      { label: 'Avg daily arrivals', value: avgDaily != null ? avgDaily.toFixed(1) : '—', sub: 'new vessels per day' },
    ].forEach(k => {
      const card = document.createElement('div');
      card.className = 'pa-kpi-card';
      card.innerHTML = `<div class="label">${k.label}</div><div class="value">${k.value}</div><div class="sub">${k.sub}</div>`;
      strip.appendChild(card);
    });
  }

  // ── Trend — stacked bar ──────────────────────────────────────────────────
  function renderTrend(activePorts) {
    const el = document.getElementById('cp-chart-trend');
    el.innerHTML = '';
    const dateSet = new Set();
    activePorts.forEach(p => data.daily_traffic.filter(d => d.port === p).forEach(d => dateSet.add(d.date)));
    const dates = [...dateSet].sort();
    const byDate = dates.map(dateStr => {
      const row = { dateStr, date: new Date(dateStr + 'T12:00:00') };
      activePorts.forEach(p => {
        const found = data.daily_traffic.find(d => d.port === p && d.date === dateStr);
        row[p] = found ? found.vessel_count : 0;
      });
      return row;
    });
    const W = el.getBoundingClientRect().width || 640;
    const H = 210;
    const showLegend = activePorts.length > 1;
    const margin = { top: 16, right: showLegend ? 110 : 20, bottom: 30, left: 44 };
    const iW = W - margin.left - margin.right, iH = H - margin.top - margin.bottom;
    const series = d3.stack().keys(activePorts)(byDate);
    const x = d3.scaleBand().domain(dates).range([0, iW]).padding(0.15);
    const maxY = d3.max(byDate, row => activePorts.reduce((s, p) => s + row[p], 0));
    const y = d3.scaleLinear().domain([0, maxY * 1.1]).range([iH, 0]);
    const svg = d3.select(el).append('svg').attr('width', W).attr('height', H);
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    g.append('g').attr('class','pa-svg-grid')
      .call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(5))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();
    series.forEach((s, si) => {
      const color = PORT_COLORS[ports.indexOf(activePorts[si]) % PORT_COLORS.length];
      g.selectAll(null).data(s).join('rect')
        .attr('x', d => x(d.data.dateStr)).attr('y', d => y(d[1]))
        .attr('height', d => Math.max(0, y(d[0]) - y(d[1]))).attr('width', x.bandwidth())
        .attr('fill', color)
        .on('mouseover', (event, d) => showTip(
          `<b>${d3.timeFormat('%b %d')(d.data.date)}</b><br>${activePorts[si]}: ${d3.format(',')(d[1]-d[0])} vessels`, event))
        .on('mousemove', (event) => { tooltip.style.left=(event.clientX+14)+'px'; tooltip.style.top=(event.clientY-10)+'px'; })
        .on('mouseout', hideTip);
    });
    const tickDates = dates.filter((_, i) => i % 7 === 0);
    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x).tickValues(tickDates).tickFormat(d => d3.timeFormat('%b %d')(new Date(d+'T12:00:00'))))
      .selectAll('text').attr('fill', T_DIM);
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.select('.pa-axis').selectAll('line').attr('stroke','rgba(255,255,255,0.12)');
    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(5))
      .selectAll('text').attr('fill', T_DIM);
    g.select('.pa-axis-y .domain').attr('stroke','rgba(255,255,255,0.12)');
    if (showLegend) {
      const lg = svg.append('g').attr('transform',`translate(${W-margin.right+10},${margin.top})`);
      activePorts.forEach((p, i) => {
        const color = PORT_COLORS[ports.indexOf(p) % PORT_COLORS.length];
        const row = lg.append('g').attr('transform',`translate(0,${i*16})`);
        row.append('rect').attr('width',10).attr('height',10).attr('rx',2).attr('fill',color);
        row.append('text').attr('x',14).attr('y',9).attr('font-size','10px').attr('fill',T_MID)
          .text(p.charAt(0).toUpperCase()+p.slice(1));
      });
    }
  }

  // ── Donut chart ──────────────────────────────────────────────────────────
  function renderDonut(activePorts) {
    const el = document.getElementById('cp-chart-donut');
    el.innerHTML = '';
    const typeMap = {};
    activePorts.forEach(p => data.type_distribution.filter(d => d.port === p)
      .forEach(d => { typeMap[d.vessel_type] = (typeMap[d.vessel_type] || 0) + d.count; }));
    const types = Object.entries(typeMap).map(([vessel_type,count]) => ({vessel_type,count}))
      .sort((a,b) => b.count-a.count);
    const W = 280, R = 90, donutCY = R + 24;
    const cols = 2, colGap = 16, legendMargin = 25, blockW = 2*R, colW = (blockW-colGap)/2;
    const legendItemH = 15, legendTopPad = 16, legendRows = Math.ceil(types.length/cols);
    const H = donutCY + R + legendTopPad + legendRows*legendItemH + 12;
    const svg = d3.select(el).append('svg').attr('width',W).attr('height',H);
    const g = svg.append('g').attr('transform',`translate(${W/2},${donutCY})`);
    const pie  = d3.pie().value(d => d.count).sort(null);
    const arc  = d3.arc().innerRadius(R*0.55).outerRadius(R);
    const arcH = d3.arc().innerRadius(R*0.55).outerRadius(R+6);
    const total = d3.sum(types, d => d.count);
    g.selectAll('path').data(pie(types)).join('path')
      .attr('d', arc).attr('fill', d => TYPE_COLORS[d.data.vessel_type] || '#666')
      .attr('stroke','#1e293b').attr('stroke-width',1.5)
      .on('mouseover', function(event,d) {
        d3.select(this).attr('d',arcH);
        showTip(`<b>${d.data.vessel_type}</b><br>${d3.format(',')(d.data.count)} vessels (${(d.data.count/total*100).toFixed(1)}%)`, event);
      })
      .on('mousemove', (event) => { tooltip.style.left=(event.clientX+14)+'px'; tooltip.style.top=(event.clientY-10)+'px'; })
      .on('mouseout', function() { d3.select(this).attr('d',arc); hideTip(); });
    g.append('text').attr('text-anchor','middle').attr('dy','-0.2em')
      .attr('font-size','1.4rem').attr('font-weight','700').attr('fill',T_MAIN).text(d3.format(',')(total));
    g.append('text').attr('text-anchor','middle').attr('dy','1.1em')
      .attr('font-size','0.68rem').attr('fill',T_DIM).text('vessel-days');
    const legendY = donutCY+R+legendTopPad, legendX = W/2-R+legendMargin;
    const legend = svg.append('g').attr('transform',`translate(${legendX},${legendY})`);
    types.forEach((d,i) => {
      const row=Math.floor(i/cols), col=i%cols;
      const itemsInRow=Math.min(cols,types.length-row*cols);
      const xOffset = itemsInRow<cols ? (blockW-colW)/2 : col*(colW+colGap);
      const lg = legend.append('g').attr('transform',`translate(${xOffset},${row*legendItemH})`);
      lg.append('rect').attr('width',9).attr('height',9).attr('rx',2).attr('fill',TYPE_COLORS[d.vessel_type]||'#666');
      lg.append('text').attr('x',13).attr('y',8).attr('font-size','10px').attr('fill',T_MID).text(d.vessel_type);
    });
  }

  // ── Flow chart — daily arrivals vs departures ────────────────────────────
  function renderFlow(activePorts) {
    const el = document.getElementById('cp-chart-flow');
    el.innerHTML = '';
    const portFlows = activePorts.map(p =>
      data.daily_flow.filter(d => d.port===p).sort((a,b)=>a.date.localeCompare(b.date)));
    const allDates = [...new Set(portFlows.flatMap(pf => pf.map(d=>d.date)))].sort();
    if (!allDates.length) return;
    const W = el.getBoundingClientRect().width || 400, H = 195;
    const margin = {top:16,right:90,bottom:30,left:40};
    const iW=W-margin.left-margin.right, iH=H-margin.top-margin.bottom;
    const x = d3.scalePoint().domain(allDates).range([0,iW]).padding(0.05);
    const maxY = d3.max(data.daily_flow.filter(d=>activePorts.includes(d.port)), d=>Math.max(d.entries,d.exits))||10;
    const y = d3.scaleLinear().domain([0,maxY*1.15]).range([iH,0]).nice();
    const lineGen = key => d3.line().x(d=>x(d.date)).y(d=>y(d[key])).defined(d=>d[key]!=null);
    const svg = d3.select(el).append('svg').attr('width',W).attr('height',H);
    const g = svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);
    g.append('g').attr('class','pa-svg-grid').call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(4))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();
    activePorts.forEach(p => {
      const color = PORT_COLORS[ports.indexOf(p)%PORT_COLORS.length];
      const pf = data.daily_flow.filter(d=>d.port===p).sort((a,b)=>a.date.localeCompare(b.date));
      const lineEntries = d3.line().x(d=>x(d.date)).y(d=>y(d.entries)).defined(d=>d.entries!=null);
      const lineExits   = d3.line().x(d=>x(d.date)).y(d=>y(d.exits)).defined(d=>d.exits!=null);
      g.append('path').datum(pf).attr('fill','none').attr('d',lineEntries(pf))
        .style('stroke',color).style('stroke-width','2px');
      g.append('path').datum(pf).attr('fill','none').attr('d',lineExits(pf))
        .style('stroke',color).style('stroke-width','1.5px').style('stroke-dasharray','5,3');
      pf.forEach(d => {
        if (d.entries==null) return;
        const tip=`<b>${d3.timeFormat('%b %d')(new Date(d.date+'T12:00:00'))} — ${p}</b><br>Arrivals: ${d.entries}<br>Departures: ${d.exits}`;
        g.append('circle').attr('cx',x(d.date)).attr('cy',y(d.entries)).attr('r',2.5)
          .style('fill',color).attr('pointer-events','none');
        g.append('circle').attr('cx',x(d.date)).attr('cy',y(d.entries)).attr('r',7).attr('fill','transparent')
          .on('mouseover',ev=>showTip(tip,ev))
          .on('mousemove',(ev)=>{tooltip.style.left=(ev.clientX+14)+'px';tooltip.style.top=(ev.clientY-10)+'px';})
          .on('mouseout',hideTip);
      });
    });
    const tickDates = allDates.filter((_,i)=>i%7===0);
    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x).tickValues(tickDates).tickFormat(d=>d3.timeFormat('%b %d')(new Date(d+'T12:00:00'))))
      .selectAll('text').attr('fill',T_DIM);
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(4)).selectAll('text').attr('fill',T_DIM);
    // Legend
    const lg=svg.append('g').attr('transform',`translate(${W-margin.right+8},${margin.top})`);
    [['Arrivals','solid'],['Departures','dashed']].forEach(([label,style],i)=>{
      const row=lg.append('g').attr('transform',`translate(0,${i*15})`);
      row.append('line').attr('x1',0).attr('x2',14).attr('y1',5).attr('y2',5)
        .attr('stroke',T_MID).attr('stroke-width',style==='dashed'?1.5:2)
        .attr('stroke-dasharray',style==='dashed'?'5,3':null);
      row.append('text').attr('x',18).attr('y',9).attr('font-size','9px').attr('fill',T_MID).text(label);
    });
    if (activePorts.length>1) activePorts.forEach((p,i)=>{
      const color=PORT_COLORS[ports.indexOf(p)%PORT_COLORS.length];
      const row=lg.append('g').attr('transform',`translate(0,${(i+2)*15+6})`);
      row.append('rect').attr('width',9).attr('height',9).attr('rx',2).attr('fill',color);
      row.append('text').attr('x',13).attr('y',8).attr('font-size','9px').attr('fill',T_MID)
        .text(p.charAt(0).toUpperCase()+p.slice(1));
    });
  }

  // ── Movement chart — % stationary by hour ───────────────────────────────
  function renderMovement(activePorts) {
    const el = document.getElementById('cp-chart-movement');
    el.innerHTML = '';
    const relevant = data.movement_behaviour.filter(d => activePorts.includes(d.port));
    if (!relevant.length) return;
    const W=el.getBoundingClientRect().width||400, H=195;
    const showLegend=activePorts.length>1;
    const margin={top:16,right:showLegend?110:20,bottom:30,left:44};
    const iW=W-margin.left-margin.right, iH=H-margin.top-margin.bottom;
    const x=d3.scaleLinear().domain([0,23]).range([0,iW]);
    const maxY=d3.max(relevant,d=>d.avg_pct_stationary)||100;
    const y=d3.scaleLinear().domain([0,Math.min(100,maxY*1.15)]).range([iH,0]).nice();
    const line=d3.line().x(d=>x(d.hour)).y(d=>y(d.avg_pct_stationary)).curve(d3.curveCatmullRom.alpha(0.5));
    const svg=d3.select(el).append('svg').attr('width',W).attr('height',H);
    const g=svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);
    g.append('g').attr('class','pa-svg-grid').call(d3.axisLeft(y).tickSize(-iW).tickFormat('').ticks(4))
      .selectAll('line').attr('stroke','rgba(255,255,255,0.07)');
    g.select('.pa-svg-grid .domain').remove();
    activePorts.forEach(p => {
      const color=PORT_COLORS[ports.indexOf(p)%PORT_COLORS.length];
      const portData=data.movement_behaviour.filter(d=>d.port===p).sort((a,b)=>a.hour-b.hour);
      g.append('path').datum(portData).attr('fill','none').attr('stroke',color).attr('stroke-width',2).attr('d',line);
      portData.forEach(d=>{
        g.append('circle').attr('cx',x(d.hour)).attr('cy',y(d.avg_pct_stationary)).attr('r',7).attr('fill','transparent')
          .on('mouseover',ev=>showTip(`<b>${String(d.hour).padStart(2,'0')}:00 — ${p}</b><br>${d.avg_pct_stationary}% stationary`,ev))
          .on('mousemove',(ev)=>{tooltip.style.left=(ev.clientX+14)+'px';tooltip.style.top=(ev.clientY-10)+'px';})
          .on('mouseout',hideTip);
        g.append('circle').attr('cx',x(d.hour)).attr('cy',y(d.avg_pct_stationary)).attr('r',2.5).attr('fill',color).attr('pointer-events','none');
      });
    });
    g.append('g').attr('class','pa-axis').attr('transform',`translate(0,${iH})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0,24,6)).tickFormat(h=>String(h).padStart(2,'0')+':00'))
      .selectAll('text').attr('fill',T_DIM);
    g.select('.pa-axis .domain').attr('stroke','rgba(255,255,255,0.12)');
    g.append('g').attr('class','pa-axis-y').call(d3.axisLeft(y).ticks(4).tickFormat(d=>d+'%'))
      .selectAll('text').attr('fill',T_DIM);
    if (showLegend) {
      const lg=svg.append('g').attr('transform',`translate(${W-margin.right+10},${margin.top})`);
      activePorts.forEach((p,i)=>{
        const color=PORT_COLORS[ports.indexOf(p)%PORT_COLORS.length];
        const row=lg.append('g').attr('transform',`translate(0,${i*16})`);
        row.append('rect').attr('width',10).attr('height',10).attr('rx',2).attr('fill',color);
        row.append('text').attr('x',14).attr('y',9).attr('font-size','10px').attr('fill',T_MID)
          .text(p.charAt(0).toUpperCase()+p.slice(1));
      });
    }
  }

  // ── Heatmap ──────────────────────────────────────────────────────────────
  function renderHeatmap(activePorts) {
    const el = document.getElementById('cp-chart-heatmap');
    el.innerHTML = '';
    const isStationary = heatmapMode==='stationary';
    const source = isStationary ? data.stationary_heatmap : data.congestion_heatmap;
    const valKey  = isStationary ? 'avg_pct_stationary' : 'avg_vessel_count';
    const cellMap = {};
    activePorts.forEach(p => {
      source.filter(d=>d.port===p).forEach(d=>{
        const key=`${d.dow}-${d.hour}`;
        if (!cellMap[key]) cellMap[key]={dow:d.dow,hour:d.hour,sum:0,n:0};
        cellMap[key].sum+=d[valKey]; cellMap[key].n++;
      });
    });
    const rows=Object.values(cellMap).map(d=>({...d,val:d.sum/d.n}));
    const minVal=d3.min(rows,d=>d.val), maxVal=d3.max(rows,d=>d.val);
    const W=el.getBoundingClientRect().width||900;
    const margin={top:6,right:20,bottom:32,left:40};
    const cellW=(W-margin.left-margin.right)/24, cellH=24;
    const H=7*cellH+margin.top+margin.bottom;
    const colorScale = isStationary
      ? d3.scaleSequential(d3.interpolateOrRd).domain([minVal,maxVal])
      : d3.scaleSequential(d3.interpolateRgb('#0f172a',
          activePorts.length===1 ? PORT_COLORS[ports.indexOf(activePorts[0])%PORT_COLORS.length] : '#38bdf8')
        ).domain([minVal,maxVal]);
    const svg=d3.select(el).append('svg').attr('width',W).attr('height',H);
    const g=svg.append('g').attr('transform',`translate(${margin.left},${margin.top})`);
    rows.forEach(d=>{
      g.append('rect')
        .attr('x',d.hour*cellW+1).attr('y',d.dow*cellH+1)
        .attr('width',cellW-2).attr('height',cellH-2).attr('rx',2).attr('fill',colorScale(d.val))
        .on('mouseover',ev=>showTip(`<b>${DOW[d.dow]}, ${String(d.hour).padStart(2,'0')}:00</b><br>${isStationary?`Avg ${d.val.toFixed(1)}% stationary`:`Avg ${d.val.toFixed(1)} vessels`}`,ev))
        .on('mousemove',(ev)=>{tooltip.style.left=(ev.clientX+14)+'px';tooltip.style.top=(ev.clientY-10)+'px';})
        .on('mouseout',hideTip);
    });
    d3.range(0,24,3).forEach(h=>{
      g.append('text').attr('x',h*cellW+cellW/2).attr('y',7*cellH+18)
        .attr('text-anchor','middle').attr('font-size','10px').attr('fill',T_DIM).text(String(h).padStart(2,'0')+':00');
    });
    DOW.forEach((d,i)=>{
      g.append('text').attr('x',-6).attr('y',i*cellH+cellH/2+4)
        .attr('text-anchor','end').attr('font-size','10px').attr('fill',T_DIM).text(d);
    });
  }

  // ── Master update ────────────────────────────────────────────────────────
  function update(activePorts) {
    renderKPIs(activePorts);
    renderTrend(activePorts);
    renderDonut(activePorts);
    renderFlow(activePorts);
    renderMovement(activePorts);
    renderHeatmap(activePorts);
  }

  update(activePorts);
})();
