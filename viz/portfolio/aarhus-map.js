/**
 * aarhus-map.js — Port Activity Analytics · portfolio build
 *
 * Mounts the Aarhus zone vessel playback map into document.getElementById('chart-aarhus-map').
 * The host element must have an explicit height (e.g. height:600px).
 * Dependencies (load before this script):
 *   <link  rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
 *   <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
 *   <script src="data/aarhus_vessels_feb2026.js"></script>  <!-- sets window.AARHUS_VESSEL_DATA -->
 *   <link  rel="stylesheet" href="viz.css">
 */
(function () {
  const mount = document.getElementById('chart-aarhus-map');
  if (!mount) return;
  const data = window.AARHUS_VESSEL_DATA;
  if (!data) { mount.textContent = 'Vessel data not loaded (missing data/aarhus_vessels_feb2026.js).'; return; }

  // ── Inject scoped styles ─────────────────────────────────────────────────
  const styleTag = document.createElement('style');
  styleTag.textContent = `
    #cam-slider::-webkit-slider-thumb { -webkit-appearance:none; width:14px; height:14px; border-radius:50%; background:#38bdf8; cursor:pointer; }
    #cam-slider::-moz-range-thumb     { width:14px; height:14px; border-radius:50%; background:#38bdf8; border:none; cursor:pointer; }
    .cam-speed-btn.cam-active { background:rgba(56,189,248,.25)!important; border-color:#38bdf8!important; color:#fff!important; }
    .cam-zone-label { background:rgba(15,23,42,.88)!important; border-radius:6px!important; color:#c8d8f0!important; font-size:11px!important; font-weight:600!important; padding:4px 8px!important; box-shadow:0 2px 8px rgba(0,0,0,.5)!important; white-space:nowrap; }
    .cam-zone-label::before { display:none!important; }
    .cam-pill       { padding:4px 11px; border-radius:20px; border:1px solid rgba(255,255,255,.18); background:transparent; color:rgba(255,255,255,.55); font-size:11px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cam-pill:hover { border-color:rgba(255,255,255,.4); color:rgba(255,255,255,.85); }
    .cam-pill.cam-active { background:rgba(255,255,255,.15); border-color:rgba(255,255,255,.4); color:#fff; }
    .cam-zone-pill  { display:flex; align-items:center; gap:5px; padding:4px 10px; border-radius:20px; border:1px solid rgba(255,255,255,.14); background:transparent; color:rgba(255,255,255,.5); font-size:11px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cam-zone-pill:hover  { border-color:rgba(255,255,255,.35); color:rgba(255,255,255,.8); }
    .cam-zone-pill.cam-active { border-color:var(--zc); color:#fff; background:rgba(255,255,255,.08); }
    .cam-zone-dot   { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
    .cam-type-pill  { display:flex; align-items:center; gap:5px; padding:4px 10px; border-radius:20px; border:1px solid rgba(255,255,255,.14); background:transparent; color:rgba(255,255,255,.5); font-size:11px; cursor:pointer; transition:all .15s; white-space:nowrap; }
    .cam-type-pill:hover  { border-color:rgba(255,255,255,.35); color:rgba(255,255,255,.8); }
    .cam-type-pill.cam-active { border-color:rgba(255,255,255,.32); color:#fff; }
    .cam-type-dot   { width:8px; height:8px; border-radius:50%; flex-shrink:0; }
  `;
  document.head.appendChild(styleTag);

  // ── Inject DOM skeleton ──────────────────────────────────────────────────
  mount.style.position = 'relative';
  mount.style.overflow = 'hidden';
  mount.innerHTML = `
    <div class="pa-map-wrap" id="cam-wrap">
      <div id="cam-inner" style="position:absolute;top:0;left:48px;right:48px;bottom:0;overflow:hidden;">
        <div id="cam-map" style="width:100%;height:100%;"></div>
        <canvas id="cam-canvas" style="position:absolute;top:0;left:0;pointer-events:none;z-index:400;"></canvas>

        <!-- Header badge -->
        <div id="cam-header" style="position:absolute;top:18px;left:18px;z-index:1000;background:rgba(15,23,42,.85);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,.10);border-radius:12px;padding:12px 18px;color:#fff;font-family:inherit;">
          <div style="font-size:15px;font-weight:600;letter-spacing:.01em;color:#e8e8f0;">Aarhus Port — Vessel Movements</div>
          <div style="font-size:11px;color:rgba(255,255,255,.45);margin-top:3px;">February 2026 &middot; Danish Maritime Authority AIS</div>
        </div>

        <!-- Vessel count -->
        <div id="cam-vessel-count" style="position:absolute;top:18px;right:18px;z-index:1000;background:rgba(15,23,42,.85);backdrop-filter:blur(10px);border:1px solid rgba(255,255,255,.10);border-radius:10px;padding:9px 14px;color:rgba(255,255,255,.5);font-size:11px;">
          <strong id="cam-count-visible" style="color:#e8e8f0;font-variant-numeric:tabular-nums;">—</strong> vessels visible
        </div>

        <!-- Tooltip -->
        <div id="cam-tooltip" style="position:absolute;z-index:2000;pointer-events:none;background:rgba(15,23,42,.95);border:1px solid rgba(255,255,255,.14);border-radius:10px;padding:10px 14px;color:#fff;font-size:12px;line-height:1.7;max-width:230px;display:none;"></div>

        <!-- Controls -->
        <div id="cam-controls" style="position:absolute;bottom:22px;left:50%;transform:translateX(-50%);z-index:1000;background:rgba(15,23,42,.55);backdrop-filter:blur(14px);border:1px solid rgba(255,255,255,.10);border-radius:16px;padding:16px 22px 14px;color:#fff;min-width:660px;display:flex;flex-direction:column;gap:11px;font-family:inherit;">
          <!-- Time row -->
          <div style="display:flex;align-items:center;gap:12px;">
            <button id="cam-btn-pause" style="width:34px;height:34px;border-radius:50%;border:none;background:rgba(255,255,255,.12);color:#fff;font-size:13px;cursor:pointer;display:flex;align-items:center;justify-content:center;flex-shrink:0;">&#9646;&#9646;</button>
            <div style="width:1px;height:20px;background:rgba(255,255,255,.15);flex-shrink:0;margin:0 2px;"></div>
            <div style="display:flex;gap:6px;" id="cam-speed-btns">
              <button class="cam-speed-btn" data-speed="30"  style="width:30px;height:30px;border-radius:50%;border:1.5px solid rgba(255,255,255,.2);background:transparent;color:rgba(255,255,255,.5);font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;" title="Slow">&#9655;</button>
              <button class="cam-speed-btn cam-active" data-speed="120" style="width:30px;height:30px;border-radius:50%;border:1.5px solid rgba(255,255,255,.2);background:transparent;color:rgba(255,255,255,.5);font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;" title="Normal">&#9654;</button>
              <button class="cam-speed-btn" data-speed="480" style="width:30px;height:30px;border-radius:50%;border:1.5px solid rgba(255,255,255,.2);background:transparent;color:rgba(255,255,255,.5);font-size:11px;cursor:pointer;display:flex;align-items:center;justify-content:center;" title="Fast">&#9654;&#9654;</button>
            </div>
            <span id="cam-time-display" style="font-size:13px;font-weight:600;font-variant-numeric:tabular-nums;color:#c8c8e0;width:110px;flex-shrink:0;letter-spacing:.03em;">Feb 01 · 00:00</span>
            <input type="range" id="cam-slider" min="0" max="40319" value="0" step="1" style="flex:1;-webkit-appearance:none;appearance:none;height:4px;border-radius:2px;background:rgba(255,255,255,.18);outline:none;cursor:pointer;">
          </div>
          <!-- Trail row -->
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:44px;flex-shrink:0;">Trail</span>
            <div style="display:flex;gap:6px;flex-wrap:wrap;" id="cam-trail-options">
              <button class="cam-pill" data-trail="60">1 hr</button>
              <button class="cam-pill cam-active" data-trail="360">6 hr</button>
              <button class="cam-pill" data-trail="1440">1 day</button>
              <button class="cam-pill" data-trail="Infinity">All</button>
            </div>
          </div>
          <!-- Zone filter row -->
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:44px;flex-shrink:0;">Zone</span>
            <div style="display:flex;gap:6px;flex-wrap:wrap;" id="cam-zone-filters"></div>
          </div>
          <!-- Type filter row -->
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:44px;flex-shrink:0;">Type</span>
            <div style="display:flex;gap:6px;flex-wrap:wrap;" id="cam-type-filters"></div>
          </div>
          <!-- Color mode row -->
          <div style="display:flex;align-items:center;gap:12px;">
            <span style="font-size:11px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.05em;width:44px;flex-shrink:0;">Color</span>
            <button id="cam-color-toggle" style="padding:4px 11px;border-radius:20px;border:1px solid rgba(255,255,255,.25);background:rgba(255,255,255,.08);color:rgba(255,255,255,.75);font-size:11px;cursor:pointer;">By type</button>
          </div>
        </div>
      </div>
    </div>
  `;

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
  const ZONE_COLORS     = ['#4a6cf7', '#ff7f0e', '#2ca02c', '#9467bd'];
  const ZONE_LABELS_ARR = ['Outer Approach', 'Anchorage', 'South Terminal', 'North Terminal'];
  const ZONE_POLYS = [
    [[56.10,10.15],[56.10,10.30],[56.14,10.30],[56.14,10.15]],
    [[56.14,10.15],[56.14,10.21],[56.17,10.25],[56.17,10.15]],
    [[56.14,10.21],[56.14,10.30],[56.17,10.30],[56.17,10.25]],
    [[56.17,10.18],[56.17,10.30],[56.22,10.30],[56.22,10.18]],
  ];

  // ── State ────────────────────────────────────────────────────────────────
  const TOTAL_MINUTES = 28 * 1440;
  let currentTime = 0;
  let trailLength = 360;
  let activeTypes = new Set(Object.keys(TYPE_COLORS));
  let activeZones = new Set([0, 1, 2, 3, null]);
  let colorByZone = false;
  let playSpeed   = 0;
  let lastSpeed   = 120;
  let rafId       = null;
  let lastRafTs   = null;
  const SPEED_SLOW = 30, SPEED_NORMAL = 120, SPEED_FAST = 480;

  // ── Map ──────────────────────────────────────────────────────────────────
  const mapContainer = document.getElementById('cam-map');
  const leafMap = L.map(mapContainer, {
    center: [56.155, 10.225], zoom: 13, zoomControl: false, attributionControl: true,
  });
  L.control.zoom({ position: 'bottomleft' }).addTo(leafMap);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a> | DMA AIS',
    maxZoom: 19, subdomains: 'abcd',
  }).addTo(leafMap);

  // ── Zone overlays ────────────────────────────────────────────────────────
  ZONE_POLYS.forEach((vertices, i) => {
    const color = ZONE_COLORS[i];
    L.polygon(vertices, {
      color, weight: 1.5, dashArray: '5 4', fillColor: color, fillOpacity: 0.06, interactive: false,
    }).addTo(leafMap);
    const lat = vertices.reduce((s, v) => s + v[0], 0) / vertices.length;
    const lon = vertices.reduce((s, v) => s + v[1], 0) / vertices.length;
    L.marker([lat, lon], { interactive: false, opacity: 0 })
      .bindTooltip(ZONE_LABELS_ARR[i], {
        permanent: true, direction: 'center', className: 'cam-zone-label', offset: [0, 0],
      })
      .addTo(leafMap);
  });

  // ── Canvas overlay ───────────────────────────────────────────────────────
  const canvas = document.getElementById('cam-canvas');
  const ctx    = canvas.getContext('2d');
  const dpr    = window.devicePixelRatio || 1;

  function resizeCanvas() {
    const w = mapContainer.offsetWidth, h = mapContainer.offsetHeight;
    canvas.width  = w * dpr; canvas.height = h * dpr;
    canvas.style.width = w + 'px'; canvas.style.height = h + 'px';
    draw();
  }

  leafMap.on('zoom move resize', draw);
  window.addEventListener('resize', resizeCanvas);

  // ── Draw ─────────────────────────────────────────────────────────────────
  let allVessels = [];

  function draw() {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.save(); ctx.scale(dpr, dpr);

    const bounds = leafMap.getBounds().pad(0.15);
    const zoom   = leafMap.getZoom();
    const dotR   = zoom >= 14 ? 5 : zoom >= 12 ? 4 : 3;
    const lineW  = zoom >= 14 ? 2 : 1.5;
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
      const currentZone = latest.z != null ? latest.z : null;
      if (!activeZones.has(currentZone)) continue;
      if (!bounds.contains([latest.lat, latest.lon])) continue;
      visible++;

      const color = colorByZone
        ? (currentZone != null ? ZONE_COLORS[currentZone] : '#888')
        : (TYPE_COLORS[vessel.type] || '#bcbd22');

      const trailStart = trailLength === Infinity ? -Infinity : currentTime - trailLength;
      let startIdx = latestIdx;
      while (startIdx > 0 && track[startIdx - 1].t >= trailStart) startIdx--;

      if (latestIdx > startIdx) {
        ctx.beginPath(); ctx.strokeStyle = color; ctx.lineWidth = lineW;
        ctx.globalAlpha = 0.35; ctx.lineJoin = 'round';
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
    document.getElementById('cam-count-visible').textContent = visible.toLocaleString();
  }

  // ── Animation ────────────────────────────────────────────────────────────
  function step(ts) {
    if (playSpeed === 0) return;
    if (lastRafTs !== null) {
      const dt = (ts - lastRafTs) / 1000;
      currentTime = Math.min(TOTAL_MINUTES - 1, currentTime + dt * playSpeed);
      document.getElementById('cam-slider').value = Math.round(currentTime);
      updateTimeDisplay(); draw();
      if (currentTime >= TOTAL_MINUTES - 1) { setSpeed(0); return; }
    }
    lastRafTs = ts; rafId = requestAnimationFrame(step);
  }

  function setSpeed(speed) {
    if (speed > 0) lastSpeed = speed;
    playSpeed = speed;
    if (speed > 0) {
      if (currentTime >= TOTAL_MINUTES - 1) currentTime = 0;
      lastRafTs = null;
      if (!rafId) rafId = requestAnimationFrame(step);
    } else {
      lastRafTs = null;
      if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
    }
    document.querySelectorAll('.cam-speed-btn').forEach(btn => {
      const active = parseInt(btn.dataset.speed) === playSpeed;
      btn.classList.toggle('cam-active', active);
    });
  }

  function updateTimeDisplay() {
    const totalMin = Math.round(currentTime);
    const dayNum   = Math.floor(totalMin / 1440) + 1;
    const minOfDay = totalMin % 1440;
    const h = Math.floor(minOfDay / 60), m = minOfDay % 60;
    document.getElementById('cam-time-display').textContent =
      'Feb ' + String(dayNum).padStart(2,'0') + ' · ' + String(h).padStart(2,'0') + ':' + String(m).padStart(2,'0');
  }

  // ── Tooltip ──────────────────────────────────────────────────────────────
  const tooltip = document.getElementById('cam-tooltip');
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
      const currentZone = latest.z != null ? latest.z : null;
      if (!activeZones.has(currentZone)) continue;
      const pos = leafMap.latLngToContainerPoint([latest.lat, latest.lon]);
      const dist = Math.sqrt((pos.x - mx) ** 2 + (pos.y - my) ** 2);
      if (dist < HIT_RADIUS && dist < closestDist) { closestDist = dist; closest = { vessel, point: latest }; }
    }

    if (closest) {
      const { vessel, point } = closest;
      const name = vessel.name && vessel.name !== 'nan' && vessel.name.trim() ? vessel.name : `MMSI ${vessel.mmsi}`;
      const sog  = point.sog != null ? point.sog.toFixed(1) + ' kn' : '—';
      const zoneStr = point.z != null ? ZONE_LABELS_ARR[point.z] : 'Unzoned';
      tooltip.innerHTML = `<div style="font-weight:600;font-size:13px;color:#e0e0f8;margin-bottom:3px;">${name}</div><div style="color:rgba(255,255,255,.55);">Type &nbsp;<span style="color:rgba(255,255,255,.88);">${vessel.type}</span></div><div style="color:rgba(255,255,255,.55);">Zone &nbsp;<span style="color:rgba(255,255,255,.88);">${zoneStr}</span></div><div style="color:rgba(255,255,255,.55);">Speed <span style="color:rgba(255,255,255,.88);">${sog}</span></div><div style="color:rgba(255,255,255,.55);">MMSI &nbsp;<span style="color:rgba(255,255,255,.88);">${vessel.mmsi}</span></div>`;
      tooltip.style.display = 'block';
      const mw = document.getElementById('cam-inner').getBoundingClientRect();
      tooltip.style.left = Math.min(e.clientX - mw.left + 16, mw.width - 244) + 'px';
      tooltip.style.top  = Math.min(e.clientY - mw.top - 10, mw.height - 144) + 'px';
      mapContainer.style.cursor = 'crosshair';
    } else {
      tooltip.style.display = 'none';
      mapContainer.style.cursor = '';
    }
  });
  mapContainer.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });

  // ── UI events ────────────────────────────────────────────────────────────
  document.getElementById('cam-btn-pause').addEventListener('click', () =>
    setSpeed(playSpeed > 0 ? 0 : lastSpeed)
  );

  document.getElementById('cam-speed-btns').addEventListener('click', e => {
    const btn = e.target.closest('.cam-speed-btn');
    if (btn) setSpeed(parseInt(btn.dataset.speed));
  });

  document.getElementById('cam-slider').addEventListener('input', e => {
    setSpeed(0);
    currentTime = parseInt(e.target.value, 10);
    updateTimeDisplay(); draw();
  });

  document.getElementById('cam-trail-options').addEventListener('click', e => {
    const btn = e.target.closest('.cam-pill');
    if (!btn || !btn.dataset.trail) return;
    document.querySelectorAll('#cam-trail-options .cam-pill').forEach(b => b.classList.remove('cam-active'));
    btn.classList.add('cam-active');
    trailLength = btn.dataset.trail === 'Infinity' ? Infinity : parseInt(btn.dataset.trail, 10);
    draw();
  });

  document.getElementById('cam-color-toggle').addEventListener('click', function () {
    colorByZone = !colorByZone;
    this.textContent = colorByZone ? 'By zone' : 'By type';
    draw();
  });

  function buildZoneFilters() {
    const container = document.getElementById('cam-zone-filters');
    const zoneKeys  = [...ZONE_COLORS.map((_, i) => i), null];

    function syncZoneUI() {
      container.querySelectorAll('.cam-zone-pill[data-zone]').forEach(p => {
        const key = p.dataset.zone === 'null' ? null : parseInt(p.dataset.zone, 10);
        p.classList.toggle('cam-active', activeZones.has(key));
      });
    }

    const allBtn = document.createElement('button');
    allBtn.className = 'cam-pill'; allBtn.textContent = 'All';
    allBtn.addEventListener('click', () => { activeZones = new Set(zoneKeys); syncZoneUI(); draw(); });
    container.appendChild(allBtn);

    const noneBtn = document.createElement('button');
    noneBtn.className = 'cam-pill'; noneBtn.textContent = 'None';
    noneBtn.addEventListener('click', () => { activeZones.clear(); syncZoneUI(); draw(); });
    container.appendChild(noneBtn);

    ZONE_COLORS.forEach((color, i) => {
      const btn = document.createElement('button');
      btn.className = 'cam-zone-pill cam-active'; btn.dataset.zone = i;
      btn.style.setProperty('--zc', color);
      btn.innerHTML = `<span class="cam-zone-dot" style="background:${color}"></span>${ZONE_LABELS_ARR[i]}`;
      btn.addEventListener('click', () => {
        if (activeZones.has(i)) { activeZones.delete(i); btn.classList.remove('cam-active'); }
        else { activeZones.add(i); btn.classList.add('cam-active'); }
        draw();
      });
      container.appendChild(btn);
    });

    const unzonedBtn = document.createElement('button');
    unzonedBtn.className = 'cam-zone-pill cam-active'; unzonedBtn.dataset.zone = 'null';
    unzonedBtn.style.setProperty('--zc', '#888');
    unzonedBtn.innerHTML = `<span class="cam-zone-dot" style="background:#888"></span>Unzoned`;
    unzonedBtn.addEventListener('click', () => {
      if (activeZones.has(null)) { activeZones.delete(null); unzonedBtn.classList.remove('cam-active'); }
      else { activeZones.add(null); unzonedBtn.classList.add('cam-active'); }
      draw();
    });
    container.appendChild(unzonedBtn);
  }

  function buildTypeFilters(shipTypes) {
    const container = document.getElementById('cam-type-filters');

    function syncTypeUI() {
      container.querySelectorAll('.cam-type-pill[data-type]').forEach(p =>
        p.classList.toggle('cam-active', activeTypes.has(p.dataset.type))
      );
    }

    const allBtn = document.createElement('button');
    allBtn.className = 'cam-pill'; allBtn.textContent = 'All';
    allBtn.addEventListener('click', () => { activeTypes = new Set(shipTypes); syncTypeUI(); draw(); });
    container.appendChild(allBtn);

    const noneBtn = document.createElement('button');
    noneBtn.className = 'cam-pill'; noneBtn.textContent = 'None';
    noneBtn.addEventListener('click', () => { activeTypes.clear(); syncTypeUI(); draw(); });
    container.appendChild(noneBtn);

    shipTypes.forEach(type => {
      const color = TYPE_COLORS[type] || '#999';
      const btn = document.createElement('button');
      btn.className = 'cam-type-pill cam-active'; btn.dataset.type = type;
      btn.innerHTML = `<span class="cam-type-dot" style="background:${color}"></span>${type}`;
      btn.addEventListener('click', () => {
        if (activeTypes.has(type)) { activeTypes.delete(type); btn.classList.remove('cam-active'); }
        else { activeTypes.add(type); btn.classList.add('cam-active'); }
        draw();
      });
      container.appendChild(btn);
    });
  }

  // ── Data ready ───────────────────────────────────────────────────────────
  allVessels = data.vessels;
  buildZoneFilters();
  buildTypeFilters(data.ship_types);
  resizeCanvas();
  updateTimeDisplay();
  setSpeed(SPEED_NORMAL);
})();
