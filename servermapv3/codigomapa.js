// HIRI Map Client-side JavaScript
// Global configuration object will be injected by server: CFG

(function(){
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const show = (el, on) => { el.style.display = on ? '' : 'none'; };
  const setStatus = (msg, type='info') => {
    const s = $('#status');
    if(s) s.textContent = msg;
    updateConnectionIndicator(type);
  };
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));

  function updateConnectionIndicator(type = 'info') {
    const indicator = $('#connectionStatus');
    if(!indicator) return;

    const colors = {
      'connected': '#22c55e',    // verde - WebSocket activo
      'polling': '#eab308',     // amarillo - solo polling
      'error': '#ef4444',       // rojo - error
      'info': '#6b7280'         // gris - info general
    };

    indicator.style.background = colors[type] || colors.info;
  }

  let totalDataPoints = 0;

  let map = null;
  function findMapVar(){
    // Find the Leaflet Map variable created by Folium (e.g., window.map_xxx)
    const keys = Object.keys(window).filter(k => k.startsWith('map_'));
    for(const k of keys){
      try{ if(window[k] && window[k].setView) return window[k]; }catch(e){}
    }
    return null;
  }

  async function waitForMap(maxMs=4000){
    const t0 = performance.now();
    while(!map){
      map = findMapVar();
      if(map) break;
      if(performance.now() - t0 > maxMs) throw new Error('Folium map variable not found.');
      await sleep(60);
    }
  }

  // Layers and state
  let pointLayer = null;      // L.LayerGroup of CircleMarkers
  let clusterLayer = null;    // L.MarkerClusterGroup for clustering
  let heatLayer = null;       // L.heatLayer
  let heatData = [];          // [[lat,lon,val], ...]
  let lastTs = null;          // last timestamp of current-day load (for Live)
  let currentDay = null;      // YYYY-MM-DD currently loaded
  let currentBBox = null;     // for fitBounds after updates
  let useCluster = false;     // toggle clustering based on point count

  // Palette helpers
  const BR = CFG.palette.breaks;
  const CL = CFG.palette.colors;

  function colorForPM(v){
    const x = Math.max(BR[0], Math.min(BR[BR.length-1], v));
    for(let i=BR.length-1; i>=0; i--){
      if(x >= BR[i]) return CL[Math.min(i, CL.length-1)];
    }
    return CL[0];
  }

  function clearLayers(){
    if(pointLayer){ pointLayer.clearLayers(); }
    if(clusterLayer){ clusterLayer.clearLayers(); }
    if(heatLayer){ heatData = []; heatLayer.setLatLngs(heatData); }
    currentBBox = null;
  }

  function ensureLayers(){
    if(!pointLayer){
      pointLayer = L.layerGroup();
      pointLayer.addTo(map); // Add to map by default
    }
    if(!clusterLayer && window.L && L.markerClusterGroup){
      clusterLayer = L.markerClusterGroup({
        maxClusterRadius: 50,
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true
      });
    }
    if(!heatLayer){
      if(!L.heatLayer){
        console.warn('leaflet.heat plugin not loaded; heat map disabled');
      }else{
        heatLayer = L.heatLayer([], {radius:12, blur:22, minOpacity:0.3, maxZoom:18}).addTo(map);
      }
    }
  }

  function switchToClusterMode(enable) {
    if(enable === useCluster) return; // no change needed

    if(enable && clusterLayer) {
      // Switch to cluster mode
      if(map.hasLayer(pointLayer)) map.removeLayer(pointLayer);
      if(!map.hasLayer(clusterLayer)) map.addLayer(clusterLayer);
      // Move all markers from pointLayer to clusterLayer
      pointLayer.eachLayer(layer => {
        pointLayer.removeLayer(layer);
        clusterLayer.addLayer(layer);
      });
      useCluster = true;
      console.log('Switched to cluster mode');
    } else {
      // Switch to regular mode
      if(map.hasLayer(clusterLayer)) map.removeLayer(clusterLayer);
      if(!map.hasLayer(pointLayer)) map.addLayer(pointLayer);
      // Move all markers from clusterLayer to pointLayer
      if(clusterLayer) {
        clusterLayer.eachLayer(layer => {
          clusterLayer.removeLayer(layer);
          pointLayer.addLayer(layer);
        });
      }
      useCluster = false;
      console.log('Switched to regular mode');
    }
  }

  function extendBBox(lat, lon){
    if(!currentBBox){ currentBBox = [[lat,lon],[lat,lon]]; return; }
    currentBBox[0][0] = Math.min(currentBBox[0][0], lat);
    currentBBox[0][1] = Math.min(currentBBox[0][1], lon);
    currentBBox[1][0] = Math.max(currentBBox[1][0], lat);
    currentBBox[1][1] = Math.max(currentBBox[1][1], lon);
  }

  function fitIfBounds(){
    if(currentBBox){ map.fitBounds(currentBBox, {padding:[20,20]}); }
  }

  function addRows(rows, replace){
    ensureLayers();
    if(replace) {
      clearLayers();
      totalDataPoints = 0; // reset counter
      currentBBox = null; // reset bbox
    }
    let added = 0;
    for(const r of rows){
      const lat = +r.lat, lon = +r.lon, pm25 = +r.pm25;
      if(!isFinite(lat) || !isFinite(lon) || !isFinite(pm25)) continue;
      const col = colorForPM(pm25);
      const popup = `
        <div style="font: 12px system-ui,sans-serif;">
          <b>Dispositivo:</b> ${r.device_code || '-'}<br>
          <b>PM2.5:</b> ${pm25.toFixed(1)} µg/m³<br>
          <b>Time:</b> ${r.time || '-'}<br>
          <b>Envíos #:</b> ${r.envio_n || '-'}<br>
          <b>Lat:</b> ${lat.toFixed(6)}, <b>Lon:</b> ${lon.toFixed(6)}<br>
          <hr style="margin:4px 0"/>
          <b>PM1:</b> ${r.pm1 ?? '-'} | <b>PM10:</b> ${r.pm10 ?? '-'}<br>
          <b>Temp PMS:</b> ${r.temp_pms ?? '-'} °C | <b>Hum:</b> ${r.hum ?? '-'} %<br>
          <b>VBat:</b> ${r.vbat ?? '-'} V<br>
          <b>CSQ:</b> ${r.csq ?? '-'} | <b>Sats:</b> ${r.sats ?? '-'} | <b>Speed:</b> ${r.speed_kmh ?? '-'} km/h
        </div>`;
      const m = L.circleMarker([lat,lon], {
        radius: 6, color: col, fillColor: col, weight: 1, fillOpacity: 0.85
      }).bindPopup(popup);

      // Add to appropriate layer
      if(useCluster && clusterLayer) {
        clusterLayer.addLayer(m);
      } else {
        pointLayer.addLayer(m);
      }
      heatData.push([lat,lon, Math.max(BR[0], Math.min(BR[BR.length-1], pm25))]);
      extendBBox(lat, lon);
      added++;
    }
    if(heatLayer) heatLayer.setLatLngs(heatData);

    // Update counter before clustering decision
    if(replace) {
      totalDataPoints = added;
    } else {
      totalDataPoints += added;
    }

    // Auto-switch clustering based on total point count
    const shouldCluster = totalDataPoints > 100;
    switchToClusterMode(shouldCluster);

    // Update counter display
    const counter = $('#dataCount');
    if(counter) {
      counter.textContent = `${totalDataPoints} puntos en mapa`;
    }

    // Fit bounds after all markers added
    if(replace && added > 0) {
      fitIfBounds();
    }

    return added;
  }

  // Fetch helpers
  async function fetchJSON(url){
    const r = await fetch(url, {cache:'no-store'});
    const txt = await r.text();
    try{
      return JSON.parse(txt);
    }catch(e){
      throw new Error(`Bad JSON from ${url}: ${txt.slice(0,180)}...`);
    }
  }

  // Day index
  async function refreshDayIndex(selectLatest=true){
    const qp = new URLSearchParams({project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value}).toString();
    const j = await fetchJSON('/api/day-index?'+qp);
    const sel = $('#daySelect');
    sel.innerHTML = '';
    (j.days || []).forEach(d=>{
      const opt = document.createElement('option'); opt.value = d; opt.textContent = d; sel.appendChild(opt);
    });
    if(selectLatest && (j.days || []).length){
      sel.value = j.days[j.days.length-1];
      currentDay = sel.value;
      return {days:j.days, selected:sel.value, cursor:j.cursor};
    }
    return {days:j.days, selected:sel.value || null, cursor:j.cursor};
  }

  function updateDayDownloads(day){
    const base = `${location.origin}/api/data?mode=day&day=${encodeURIComponent(day)}&project_id=${encodeURIComponent($('#project_id').value)}&device_code=${encodeURIComponent($('#device_code').value)}&tabla=${encodeURIComponent($('#tabla').value)}`;
    $('#dl-day-csv').href  = base;
    $('#dl-day-xlsx').href = base;
  }

  function updatePageDownloads(limit, offset){
    const base = `project_id=${encodeURIComponent($('#project_id').value)}&device_code=${encodeURIComponent($('#device_code').value)}&tabla=${encodeURIComponent($('#tabla').value)}&limite=${limit}&offset=${offset}&paginate=0`;
    $('#dl-raw-csv').href   = `/download/raw.csv?${base}`;
    $('#dl-raw-xlsx').href  = `/download/raw.xlsx?${base}`;
    $('#dl-plot-csv').href  = `/download/plotted.csv?${base}`;
    $('#dl-plot-xlsx').href = `/download/plotted.xlsx?${base}`;
  }

  // Loaders
  async function loadPage(replace=true){
    const limit  = +$('#limit').value;
    const offset = +$('#offset').value;
    const qp = new URLSearchParams({
      type:'plotted', project_id:$('#project_id').value, device_code:$('#device_code').value,
      tabla:$('#tabla').value, limite:String(limit), offset:String(offset), paginate:'0'
    }).toString();
    setStatus('Loading page …'); showSpin(true);
    try{
      const j = await fetchJSON('/api/data?'+qp);
      const added = addRows(j.rows||[], replace);
      updatePageDownloads(limit, offset);
      setStatus(`Page rows=${(j.rows||[]).length} added=${added}`);
    }catch(e){
      setStatus('Error: '+ e.message);
      console.error(e);
    }finally{
      showSpin(false);
    }
  }

  async function loadDay(day, replace=true){
    if(!day) return;
    const qp = new URLSearchParams({mode:'day', day:day, project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value}).toString();
    setStatus('Loading day '+day+' …'); showSpin(true);
    try{
      const j = await fetchJSON('/api/data?'+qp);
      if(replace) clearLayers();
      const added = addRows(j.rows||[], replace);
      lastTs = null; for(const r of (j.rows||[])){ if(r.time && (!lastTs || r.time > lastTs)) lastTs = r.time; }
      currentDay = day;
      updateDayDownloads(day);
      setStatus(`Day ${day}: rows=${(j.rows||[]).length} added=${added}`);
    }catch(e){ setStatus('Day load error: '+e.message); console.error(e); }
    finally{ showSpin(false); }
  }

  let liveInterval = null;
  let consecutiveEmptyPolls = 0;
  const BASE_POLL_INTERVAL = 10000; // 10 segundos base
  const MAX_POLL_INTERVAL = 60000;  // máximo 60 segundos

  async function pollLive(){
    if(!$('#chkLive').checked || !currentDay || !lastTs) return;
    try{
      const qp = new URLSearchParams({
        mode:'day', day:currentDay, since:lastTs,
        project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value
      }).toString();
      const j = await fetchJSON('/api/data?'+qp);
      const rows = j.rows || [];
      if(rows.length){
        const added = addRows(rows, false);
        for(const r of rows){ if(r.time && (!lastTs || r.time > lastTs)) lastTs = r.time; }
        setStatus(`Live +${rows.length} (added=${added}) - ${new Date().toLocaleTimeString()}`, wsConnected ? 'connected' : 'polling');
        consecutiveEmptyPolls = 0;
        adjustPollingInterval();
      } else {
        consecutiveEmptyPolls++;
        adjustPollingInterval();
        setStatus(`Live mode - Última actualización: ${new Date().toLocaleTimeString()}`, wsConnected ? 'connected' : 'polling');
      }
    }catch(e){
      setStatus(`Error de conexión: ${e.message}`, 'error');
      consecutiveEmptyPolls++;
    }
  }

  function adjustPollingInterval() {
    if(liveInterval) clearInterval(liveInterval);

    let interval = BASE_POLL_INTERVAL;
    if(consecutiveEmptyPolls > 0) {
      interval = Math.min(MAX_POLL_INTERVAL, BASE_POLL_INTERVAL * (1 + consecutiveEmptyPolls * 0.5));
    }

    liveInterval = setInterval(pollLive, interval);
    console.log(`Polling interval adjusted to ${interval/1000}s (empty polls: ${consecutiveEmptyPolls})`);
  }

  adjustPollingInterval();

  // ====== WebSocket Setup ======
  let socket = null;
  let wsConnected = false;

  function initWebSocket() {
    if(!window.io) {
      console.warn('Socket.IO not loaded, using polling fallback');
      return;
    }

    socket = io(location.origin);

    socket.on('connect', () => {
      console.log('WebSocket connected');
      wsConnected = true;
      setStatus('Conectado en tiempo real', 'connected');
      socket.emit('subscribe', {
        project_id: $('#project_id').value,
        device_code: $('#device_code').value,
        tabla: $('#tabla').value
      });
    });

    socket.on('disconnect', () => {
      console.log('WebSocket disconnected');
      wsConnected = false;
      setStatus('Desconectado - usando polling', 'polling');
    });

    socket.on('new_data', (data) => {
      console.log('Received new data via WebSocket:', data);
      if(data.rows && data.rows.length > 0 && $('#chkLive').checked) {
        const added = addRows(data.rows, false);
        for(const r of data.rows){
          if(r.time && (!lastTs || r.time > lastTs)) lastTs = r.time;
        }
        setStatus(`🟢 WebSocket +${data.count} nuevos - ${new Date().toLocaleTimeString()}`);
        consecutiveEmptyPolls = 0;
      }
    });

    socket.on('status', (data) => {
      console.log('WebSocket status:', data.message);
    });
  }

  // Spinner (minimal)
  function showSpin(on){
    if(on){
      if($('#spin')) return;
      const s = document.createElement('div');
      s.id='spin'; s.textContent = 'Loading…';
      s.style.cssText = 'position:fixed;left:50%;top:12px;transform:translateX(-50%);background:#fff;padding:4px 8px;border-radius:6px;border:1px solid #ddd;z-index:9999;font:13px system-ui';
      document.body.appendChild(s);
    }else{
      const s = $('#spin'); if(s) s.remove();
    }
  }

  // Logs panel
  async function refreshLogs(){
    try{
      const qp = new URLSearchParams({project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value, tail:'300'}).toString();
      const j = await fetchJSON('/admin/logs?'+qp);
      const box = $('#logs');
      box.innerHTML = (j.lines||[]).map(x => `<div>${x}</div>`).join('');
      box.scrollTop = box.scrollHeight;
    }catch(e){}
  }
  setInterval(()=>{ if($('#logs').style.display !== 'none') refreshLogs(); }, 5000);

  // Wire events
  $('#btnLoad').addEventListener('click', ()=>loadPage(true));
  $('#btnOlderAppend').addEventListener('click', ()=>{
    $('#offset').value = Math.max(0, (+$('#offset').value) + (+$('#limit').value));
    loadPage(false);
  });
  $('#btnOlder').addEventListener('click', ()=>{
    $('#offset').value = Math.max(0, (+$('#offset').value) + (+$('#limit').value));
    loadPage(true);
  });
  $('#btnNewer').addEventListener('click', ()=>{
    $('#offset').value = Math.max(0, (+$('#offset').value) - (+$('#limit').value));
    loadPage(true);
  });
  $('#btnReset').addEventListener('click', ()=>{ $('#offset').value = 0; loadPage(true); });

  $('#btnRefreshDays').addEventListener('click', async ()=>{
    const previousDay = currentDay || $('#daySelect').value;
    const di = await refreshDayIndex(true); // Select latest day
    if(di && di.days && di.days.length > 0){
      // If there's a new day, load it; otherwise reload the previous day
      const dayToLoad = di.selected || previousDay || di.days[di.days.length-1];
      if(dayToLoad){
        await loadDay(dayToLoad, true);
      }
    }
  });
  $('#btnLoadDay').addEventListener('click', ()=>{ const d=$('#daySelect').value; if(d){ loadDay(d, true); } });
  $('#btnPrevDay').addEventListener('click', ()=>{
    const s = $('#daySelect'); if(!s.value) return;
    const idx = Array.from(s.options).findIndex(o=>o.value===s.value);
    if(idx>0){ s.value = s.options[idx-1].value; loadDay(s.value,true); }
  });
  $('#btnNextDay').addEventListener('click', ()=>{
    const s = $('#daySelect'); if(!s.value) return;
    const idx = Array.from(s.options).findIndex(o=>o.value===s.value);
    if(idx>=0 && idx < s.options.length-1){ s.value = s.options[idx+1].value; loadDay(s.value,true); }
  });

  $('#btnAdminReindex').addEventListener('click', async ()=>{
    if(!confirm('Reindex now? This will (re)start the collector and may take time.')) return;
    const qp = new URLSearchParams({project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value, limit:$('#limit').value, reset:'1'}).toString();
    const j = await fetchJSON('/admin/reindex?'+qp);
    setStatus(j.message || 'Reindex started'); refreshLogs();
  });
  $('#btnAdminPurge').addEventListener('click', async ()=>{
    if(!confirm('Purge cache and stop collector?')) return;
    const qp = new URLSearchParams({project_id:$('#project_id').value, device_code:$('#device_code').value, tabla:$('#tabla').value}).toString();
    const j = await fetchJSON('/admin/purge?'+qp);
    setStatus(j.message || 'Purged'); await refreshDayIndex(true); clearLayers();
  });
  $('#btnToggleLogs').addEventListener('click', async ()=>{
    const box = $('#logs'); show(box, box.style.display === 'none'); if(box.style.display !== 'none') refreshLogs();
  });

  $('#btnApply').addEventListener('click', ()=>{
    const u = new URL(location.href);
    u.searchParams.set('project_id',$('#project_id').value);
    u.searchParams.set('device_code',$('#device_code').value);
    u.searchParams.set('tabla',$('#tabla').value);
    location.href = u.toString();
  });

  $('#btnCollapse').addEventListener('click', ()=>{
    const c = $('#controls');
    if(c.style.height === '28px'){ c.style.height = ''; } else { c.style.height = '28px'; }
  });

  // CSV Upload functionality
  $('#btnUploadCSV').addEventListener('click', ()=>{
    $('#csvFileInput').click();
  });

  $('#csvFileInput').addEventListener('change', async (event)=>{
    const file = event.target.files[0];
    if(!file) return;

    if(!file.name.toLowerCase().endsWith('.csv')){
      alert('Por favor selecciona un archivo CSV válido');
      return;
    }

    const formData = new FormData();
    formData.append('csvfile', file);

    try{
      setStatus('Subiendo CSV...', 'info');
      
      const response = await fetch('/upload-csv', {
        method: 'POST',
        body: formData
      });

      const result = await response.json();

      if(result.status === 'success'){
        setStatus(`CSV procesado: ${result.valid_rows} puntos válidos`, 'connected');
        
        // Show success dialog with download option
        const download = confirm(
          `¡CSV procesado exitosamente!\n\n` +
          `Archivo: ${result.filename}\n` +
          `Total filas: ${result.total_rows}\n` +
          `Puntos válidos: ${result.valid_rows}\n` +
          `Columnas: ${result.columns.join(', ')}\n\n` +
          `¿Deseas descargar el mapa HTML ahora?`
        );

        if(download){
          // Create download link and trigger it
          const downloadUrl = `/generate-map/${result.upload_id}`;
          const link = document.createElement('a');
          link.href = downloadUrl;
          link.download = `mapa_pm25_${new Date().toISOString().slice(0,19).replace(/[T:]/g, '_')}.html`;
          document.body.appendChild(link);
          link.click();
          document.body.removeChild(link);
          
          setStatus('Mapa HTML descargado', 'connected');
        }
      } else {
        setStatus(`Error: ${result.message}`, 'error');
        alert(`Error procesando CSV:\n${result.message}`);
      }
    } catch(error) {
      setStatus(`Error de conexión: ${error.message}`, 'error');
      alert(`Error de conexión:\n${error.message}`);
    }

    // Reset file input
    event.target.value = '';
  });

  // Boot
  (async ()=>{
    try{
      await waitForMap();
      setStatus('Map ready.');
      initWebSocket();
      const di = await refreshDayIndex(true);
      if(di && di.selected){ await loadDay(di.selected, true); }
      updatePageDownloads($('#limit').value, $('#offset').value);
    }catch(e){
      setStatus('Init error: '+e.message);
      console.error(e);
    }
  })();
})();
