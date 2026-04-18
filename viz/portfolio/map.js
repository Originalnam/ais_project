/**
 * map.js — Port Activity Analytics · portfolio build
 *
 * Mounts the Danish waters vessel playback map into document.getElementById('chart-map').
 * The host element must have an explicit height (e.g. height:600px).
 * Dependencies (load before this script):
 *   <link  rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
 *   <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
 *   <script src="data/vessels_2026-02-01.js"></script>  <!-- sets window.VESSEL_DATA -->
 *   <link  rel="stylesheet" href="viz.css">
 */
(function () {
  const mount = document.getElementById('chart-map');
  if (!mount) return;
  const data = window.VESSEL_DATA;
  if (!data) { mount.textContent = 'Vessel data not loaded (missing data/vessels_2026-02-01.js).'; return; }

  // ── Inject scoped styles (slider thumb can't be set inline) ─────────────
  const styleTag = document.createElement('style');
  styleTag.textContent = `
    #cm-slider::-webkit-slider-thumb { -webkit-appearance:none; width:14px; height:14px; border-radius:50%; background:#38bdf8; cursor:pointer; }
    #cm-slider::-moz-range-thumb     { width:14px; height:14px; border-radius:50%; background:#38bdf8; border:none; cursor:pointer; }
    .cm-port-label { background:rgba(15,23,42,.88)!important; border:1px solid rgba(56,189,248,.4)!important; border-radius:8px!important; color:#c8d8f0!important; font-size:12px!important; font-weight:500!important; padding:5px 10px!important; box-shadow:0 2px 8px rgba(0,0,0,.5)!important; white-space:nowrap; }
    .cm-port-label::before { display:none!important; }
    .cm-port-label strong  { color:#e8e8f8; font-weight:700; }
    .cm-bbox-label { background:rgba(255,209,102,.92)!important; border:none!important; border-radius:4px!important; color:#1a1a2e!important; font-size:10px!important; font-weight:700!important; padding:2px 6px!important; letter-spacing:.04em; text-transform:uppercase; box-shadow:0 1px 4px rgba(0,0,0,.4)!important; white-space:nowrap; pointer-events:none; }
    .cm-bbox-label::before { display:none!important; }
  `;
  document.head.appendChild(styleTag);

  // ── Inject DOM skeleton ──────────────────────────────────────────────────
  mount.style.position = 'relative';
  mount.style.overflow = 'hidden';
  mount.innerHTML = `
    <div class="pa-map-wrap" id="cm-wrap">
      <div id="cm-inner" style="position:absolute;top:0;left:48px;right:48px;bottom:0;overflow:hidden;">
        <div id="cm-map" style="width:100%;height:100%;"></div>
        <canvas id="cm-canvas" style="position:absolute;top:0;left:0;pointer-events:none;z-index:400;"></canvas>

        <!-- Header badge -->
        <div id="cm-header" style="position:absolute;top:18px;left:18px;z-index:1000;background:rgba(15,23,42,.85);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:12px 18px;color:#fff;font-family:inherit;">
          <div style="font-size:15px;font-weight:600;letter-spacing:.01em;color:#e8e8f0;">AIS Vessel Traffic</div>
          <div style="font-size:11px;color:rgba(255,255,255,.45);margin-top:3px;">Danish Waters &mdash; 2026-02-01</div>
          <div style="display:flex;align-items:center;gap:6px;margin-top:6px;font-size:11px;color:rgba(255,255,255,.55);">
            <span style="display:inline-block;width:18px;height:10px;border:1.5px dashed #ffd166;background:rgba(255,209,102,.10);border-radius:1px;flex-shrink:0;"></span>
            Dashed boxes = filter inclusion area
          </div>
        </div>

        <!-- Vessel count -->
        <div id="cm-vessel-count" style="position:absolute;top:18px;right:18px;z-index:1000;background:rgba(15,23,42,.85);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,.10);border-radius:10px;padding:9px 14px;color:rgba(255,255,255,.5);font-size:11px;">
          <strong id="cm-count-visible" style="color:#e8e8f0;font-variant-numeric:tabular-nums;">—</strong> vessels visible
        </div>

        <!-- Tooltip -->
        <div id="cm-tooltip" style="position:absolute;z-index:2000;pointer-events:none;background:rgba(15,23,42,.95);border:1px solid rgba(255,255,255,.14);border-radius:10px;padding:10px 14px;color:#fff;font-size:12px;line-height:1.7;max-width:220px;display:none;"></div>

        <!-- Controls -->
        <div id="cm-controls" style="position:absolute;bottom:22px;left:50%;transform:translateX(-50%);z-index:1000;background:rgba(15,23,42,.92);backdrop-filter:blur(14px);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:16px 22px 14px;color:#fff;min-width:600px;display:flex;flex-direction:column;gap:12px;font-family:inherit;">
          <div style="display:flex;align-items:center;gap:12px;">
            <button id="cm-btn-play" style="width:34px;height:34px;border-radius:50%;border:none;background:rgba(255,255,255,.12);color:#fff;font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;">&#9654;</button>
            <span id="cm-time-display" style="font-size:14px;font-weight:600;font-variant-numeric:tabular-nums;color:#c8c8e0;width:75px;flex-shrink:0;letter-spacing:.04em;">00:00 UTC</span>
            <input type="range" id="cm-slider" min="0" max="1439" value="0" step="1" style="flex:1;-webkit-appearance:none;appearance:none;height:4px;border-radius:2px;background:rgba(255,255,255,.18);outline:none;cursor:pointer;">
          </div>
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:38px;flex-shrink:0;">Trail</span>
            <div style="display:flex;gap:6px;flex-wrap:wrap;" id="cm-trail-options">
              <button class="cm-pill" data-trail="30">30 min</button>
              <button class="cm-pill cm-active" data-trail="60">1 hr</button>
              <button class="cm-pill" data-trail="180">3 hr</button>
              <button class="cm-pill" data-trail="Infinity">All day</button>
            </div>
          </div>
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:38px;flex-shrink:0;">Type</span>
            <div style="display:flex;gap:6px;flex-wrap:wrap;" id="cm-type-filters"></div>
          </div>
        </div>
      </div>
    </div>
  `;

  // ── Inline pill styles (scoped) ──────────────────────────────────────────
  const pillStyle = document.createElement('style');
  pillStyle.textContent = `
    .cm-pill      { padding:4px 11px; border-radius:20px; border:1px solid rgba(255,255,255,.18); background:transparent; color:rgba(255,255,255,.55); font-size:11px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cm-pill:hover { border-color:rgba(255,255,255,.4); color:rgba(255,255,255,.85); }
    .cm-pill.cm-active { background:rgba(255,255,255,.15); border-color:rgba(255,255,255,.4); color:#fff; }
    .cm-type-pill { display:flex; align-items:center; gap:5px; padding:4px 10px; border-radius:20px; border:1px solid rgba(255,255,255,.14); background:transparent; color:rgba(255,255,255,.5); font-size:11px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cm-type-pill:hover { border-color:rgba(255,255,255,.35); color:rgba(255,255,255,.8); }
    .cm-type-pill.cm-active { border-color:rgba(255,255,255,.32); color:#fff; }
    .cm-type-dot  { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
  `;
  document.head.appendChild(pillStyle);

  // ── Constants ────────────────────────────────────────────────────────────
  const TYPE_COLORS = {
    'Cargo':         '#1f77b4',
    'Tanker':        '#ff7f0e',
    'Passenger':     '#2ca02c',
    'Fishing':       '#d62728',
    'Tug':           '#9467bd',
    'Pleasure Craft':'#8c564b',
    'HSC':           '#e377c2',
    'SAR':           '#7f7f7f',
    'Other':         '#bcbd22',
  };

  // ── State ────────────────────────────────────────────────────────────────
  let currentTime = 0;
  let trailLength = 60;
  let activeTypes = new Set(Object.keys(TYPE_COLORS));
  let isPlaying   = false;
  let rafId       = null;
  let lastRafTs   = null;
  const SPEED     = 20;

  // ── Map ──────────────────────────────────────────────────────────────────
  const mapContainer = document.getElementById('cm-map');
  const leafMap = L.map(mapContainer, {
    center: [57.5, 12.0], zoom: 7,
    zoomControl: false, attributionControl: true,
  });
  L.control.zoom({ position: 'bottomleft' }).addTo(leafMap);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a> | DMA AIS',
    maxZoom: 19, subdomains: 'abcd',
  }).addTo(leafMap);

  // ── Canvas overlay ───────────────────────────────────────────────────────
  const canvas = document.getElementById('cm-canvas');
  const ctx    = canvas.getContext('2d');
  const dpr    = window.devicePixelRatio || 1;

  function resizeCanvas() {
    const w = mapContainer.offsetWidth;
    const h = mapContainer.offsetHeight;
    canvas.width  = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width  = w + 'px';
    canvas.style.height = h + 'px';
    draw();
  }

  leafMap.on('zoom move resize', draw);
  window.addEventListener('resize', resizeCanvas);

  // ── Port bboxes ──────────────────────────────────────────────────────────
  const portIcon = L.divIcon({
    className: '',
    html: `<svg width="26" height="32" viewBox="0 0 26 32"><path d="M13 1C7.477 1 3 5.477 3 11c0 7.5 10 20 10 20s10-12.5 10-20C23 5.477 18.523 1 13 1z" fill="#0f172a" stroke="rgba(56,189,248,.9)" stroke-width="1.8"/><text x="13" y="14.5" text-anchor="middle" font-size="10" fill="rgba(56,189,248,.95)" font-family="sans-serif" font-weight="700">⚓</text></svg>`,
    iconSize: [26, 32], iconAnchor: [13, 31], tooltipAnchor: [0, -32],
  });

  function drawPortBboxes(ports) {
    ports.forEach(p => {
      const [[latMin, lonMin], [latMax, lonMax]] = p.bbox;
      L.rectangle(p.bbox, {
        color: '#ffd166', weight: 2.5, dashArray: '8 6',
        fillColor: 'rgba(255,209,102,0.08)', fillOpacity: 1, interactive: false,
      }).addTo(leafMap);
      L.marker([p.lat, p.lon], { icon: portIcon })
        .bindTooltip(`<strong>${p.name}</strong>`, {
          permanent: false, direction: 'top', className: 'cm-port-label', offset: [0, -4],
        })
        .addTo(leafMap);
    });
  }

  // ── Draw ─────────────────────────────────────────────────────────────────
  let allVessels = [];

  function draw() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.save();
    ctx.scale(dpr, dpr);

    const bounds = leafMap.getBounds().pad(0.15);
    const zoom   = leafMap.getZoom();
    const dotR   = zoom >= 10 ? 5 : zoom >= 8 ? 4 : 3;
    const lineW  = zoom >= 10 ? 2 : 1.5;
    let visible  = 0;

    for (const vessel of allVessels) {
      if (!activeTypes.has(vessel.type)) continue;
      const track = vessel.track;
      let latestIdx = -1;
      for (let i = 0; i < track.length; i++) {
        if (track[i].t <= currentTime) latestIdx = i; else break;
      }
      if (latestIdx === -1) continue;
      const latest = track[latestIdx];
      if (!bounds.contains([latest.lat, latest.lon])) continue;
      visible++;

      const color = TYPE_COLORS[vessel.type] || '#bcbd22';
      const trailStart = trailLength === Infinity ? -Infinity : currentTime - trailLength;
      let startIdx = latestIdx;
      while (startIdx > 0 && track[startIdx - 1].t >= trailStart) startIdx--;

      if (latestIdx > startIdx) {
        ctx.beginPath();
        ctx.strokeStyle = color; ctx.lineWidth = lineW; ctx.globalAlpha = 0.38; ctx.lineJoin = 'round';
        const p0 = leafMap.latLngToContainerPoint([track[startIdx].lat, track[startIdx].lon]);
        ctx.moveTo(p0.x, p0.y);
        for (let i = startIdx + 1; i <= latestIdx; i++) {
          const p = leafMap.latLngToContainerPoint([track[i].lat, track[i].lon]);
          ctx.lineTo(p.x, p.y);
        }
        ctx.stroke();
      }

      ctx.globalAlpha = 0.92; ctx.fillStyle = color; ctx.beginPath();
      const pos = leafMap.latLngToContainerPoint([latest.lat, latest.lon]);
      ctx.arc(pos.x, pos.y, dotR, 0, Math.PI * 2); ctx.fill();
    }

    ctx.restore();
    document.getElementById('cm-count-visible').textContent = visible.toLocaleString();
  }

  // ── Animation ────────────────────────────────────────────────────────────
  function step(ts) {
    if (!isPlaying) return;
    if (lastRafTs !== null) {
      const dt = (ts - lastRafTs) / 1000;
      currentTime = Math.min(1439, currentTime + dt * SPEED);
      document.getElementById('cm-slider').value = Math.round(currentTime);
      updateTimeDisplay();
      draw();
      if (currentTime >= 1439) { isPlaying = false; updatePlayBtn(); return; }
    }
    lastRafTs = ts;
    rafId = requestAnimationFrame(step);
  }

  function play() {
    if (currentTime >= 1439) currentTime = 0;
    isPlaying = true; lastRafTs = null; updatePlayBtn();
    rafId = requestAnimationFrame(step);
  }

  function pause() {
    isPlaying = false; lastRafTs = null; updatePlayBtn();
    if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
  }

  function updatePlayBtn() {
    document.getElementById('cm-btn-play').innerHTML = isPlaying ? '&#9646;&#9646;' : '&#9654;';
  }

  function updateTimeDisplay() {
    const h = Math.floor(currentTime / 60), m = Math.floor(currentTime % 60);
    document.getElementById('cm-time-display').textContent =
      String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0') + ' UTC';
  }

  // ── Tooltip ──────────────────────────────────────────────────────────────
  const tooltip = document.getElementById('cm-tooltip');
  const HIT_RADIUS = 14;

  mapContainer.addEventListener('mousemove', e => {
    const rect = mapContainer.getBoundingClientRect();
    const mx = e.clientX - rect.left, my = e.clientY - rect.top;
    let closest = null, closestDist = Infinity;

    for (const vessel of allVessels) {
      if (!activeTypes.has(vessel.type)) continue;
      const track = vessel.track;
      let latestIdx = -1;
      for (let i = 0; i < track.length; i++) {
        if (track[i].t <= currentTime) latestIdx = i; else break;
      }
      if (latestIdx === -1) continue;
      const latest = track[latestIdx];
      const pos = leafMap.latLngToContainerPoint([latest.lat, latest.lon]);
      const dist = Math.sqrt((pos.x - mx) ** 2 + (pos.y - my) ** 2);
      if (dist < HIT_RADIUS && dist < closestDist) { closestDist = dist; closest = { vessel, point: latest }; }
    }

    if (closest) {
      const { vessel, point } = closest;
      const name = vessel.name && vessel.name !== 'nan' && vessel.name.trim() ? vessel.name : `MMSI ${vessel.mmsi}`;
      const sog  = point.sog != null ? point.sog.toFixed(1) + ' kn' : '—';
      tooltip.innerHTML = `<div style="font-weight:600;font-size:13px;color:#e0e0f8;margin-bottom:3px;">${name}</div><div style="color:rgba(255,255,255,.55);">Type &nbsp;&nbsp;<span style="color:rgba(255,255,255,.88);">${vessel.type}</span></div><div style="color:rgba(255,255,255,.55);">Speed <span style="color:rgba(255,255,255,.88);">${sog}</span></div><div style="color:rgba(255,255,255,.55);">MMSI &nbsp;<span style="color:rgba(255,255,255,.88);">${vessel.mmsi}</span></div>`;
      tooltip.style.display = 'block';
      const mw = document.getElementById('cm-inner').getBoundingClientRect();
      tooltip.style.left = Math.min(e.clientX - mw.left + 16, mw.width - 234) + 'px';
      tooltip.style.top  = Math.min(e.clientY - mw.top - 10, mw.height - 134) + 'px';
      mapContainer.style.cursor = 'crosshair';
    } else {
      tooltip.style.display = 'none';
      mapContainer.style.cursor = '';
    }
  });
  mapContainer.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

  // ── UI events ────────────────────────────────────────────────────────────
  document.getElementById('cm-btn-play').addEventListener('click', () => isPlaying ? pause() : play());

  document.getElementById('cm-slider').addEventListener('input', e => {
    pause(); currentTime = parseInt(e.target.value, 10); updateTimeDisplay(); draw();
  });

  document.getElementById('cm-trail-options').addEventListener('click', e => {
    const btn = e.target.closest('.cm-pill');
    if (!btn || !btn.dataset.trail) return;
    document.querySelectorAll('#cm-trail-options .cm-pill').forEach(b => b.classList.remove('cm-active'));
    btn.classList.add('cm-active');
    trailLength = btn.dataset.trail === 'Infinity' ? Infinity : parseInt(btn.dataset.trail, 10);
    draw();
  });

  function buildTypeFilters(shipTypes) {
    const container = document.getElementById('cm-type-filters');
    container.innerHTML = '';

    function syncUI() {
      container.querySelectorAll('.cm-type-pill[data-type]').forEach(p =>
        p.classList.toggle('cm-active', activeTypes.has(p.dataset.type))
      );
    }

    const allBtn = document.createElement('button');
    allBtn.className = 'cm-pill'; allBtn.textContent = 'All';
    allBtn.addEventListener('click', () => { activeTypes = new Set(shipTypes); syncUI(); draw(); });
    container.appendChild(allBtn);

    const noneBtn = document.createElement('button');
    noneBtn.className = 'cm-pill'; noneBtn.textContent = 'None';
    noneBtn.addEventListener('click', () => { activeTypes.clear(); syncUI(); draw(); });
    container.appendChild(noneBtn);

    shipTypes.forEach(type => {
      const color = TYPE_COLORS[type] || '#999';
      const btn = document.createElement('button');
      btn.className = 'cm-type-pill cm-active'; btn.dataset.type = type;
      btn.innerHTML = `<span class="cm-type-dot" style="background:${color}"></span>${type}`;
      btn.addEventListener('click', () => {
        if (activeTypes.has(type)) { activeTypes.delete(type); btn.classList.remove('cm-active'); }
        else { activeTypes.add(type); btn.classList.add('cm-active'); }
        draw();
      });
      container.appendChild(btn);
    });
  }

  // ── Data ready ───────────────────────────────────────────────────────────
  allVessels = data.vessels;
  if (Array.isArray(data.ports)) drawPortBboxes(data.ports);
  buildTypeFilters(data.ship_types);
  resizeCanvas();
  updateTimeDisplay();
  play();
})();
