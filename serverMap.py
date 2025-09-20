# app.py
# -*- coding: utf-8 -*-
"""
HIRIPRO-01 Flask server with:
- V2 upstream fetch (limit/offset)
- Robust retries/timeouts
- Per-day server-side cache (incremental prefetch, deduplicated)
- Folium map UI with:
  * Page mode (limit/offset) + downloads
  * Day mode (load one day, prev/next, append next day)
  * Background auto-prefetch ▶ that fills the per-day cache page by page
- HeatMap and circle markers sharing the same color scale

Run:
  python app.py
Open:
  http://127.0.0.1:5000/map
"""

import io
import json
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flask import Flask, request, Response, send_file, redirect, url_for, jsonify
import folium
from folium.plugins import HeatMap, Fullscreen, MiniMap
from branca.colormap import LinearColormap
from branca.element import Element

# =========================
# ========= CONFIG ========
# =========================

UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2"

DEFAULT_TABLA = "datos"
DEFAULT_PROJECT_ID = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"

# Default pagination (page mode)
DEFAULT_LIMIT = 500
DEFAULT_OFFSET = 0

# Networking defaults
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_READ_TIMEOUT = 60
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF = 0.5

# PM2.5 scale
PM_BREAKS = [0, 12, 35, 55, 150, 250]
PM_COLORS = ["#2ecc71", "#a3d977", "#f1c40f", "#e67e22", "#e74c3c", "#7f1d1d"]
COLORBAR_CAPTION = "PM2.5 (µg/m³)"

# Marker/Heat
MARKER_RADIUS = 6
MARKER_OPACITY = 0.85
HEAT_RADIUS = 12
HEAT_BLUR = 22
HEAT_MIN_OP = 0.30

# UI boxes
TOOLBAR_CSS = (
    "position: fixed; top: 10px; right: 10px; z-index: 9999;"
    "background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 8px;"
    "box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 14px;"
)
HEADER_CSS = (
    "position: fixed; top: 10px; left: 10px; z-index: 9999;"
    "background: rgba(255,255,255,0.98); padding: 10px 12px; border-radius: 10px;"
    "box-shadow: 0 2px 10px rgba(0,0,0,0.18); font-family: system-ui, sans-serif; font-size: 14px;"
    "display:flex; flex-wrap:wrap; gap:8px; align-items:center;"
)

# Upstream schema keys
KEY_TIME    = "fecha"
KEY_PM25    = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
KEY_PM1     = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10    = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM     = "PMS5003 [Humedad (%)]"
KEY_TEMP    = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT    = "Divisor de Voltaje [Voltaje (V)]"
KEY_SIM_LAT   = "SIM7600G [Latitud (°)]"
KEY_SIM_LON   = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ   = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS  = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"
KEY_META_LAT  = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON  = "Metadatos Estacion [Longitud (°)]"
KEY_NUM_ENV   = "Metadatos Estacion [Numero de envios (Numeral)]"

DEFAULT_HEADERS = {"User-Agent": "HIRIMap/1.0 (Flask) requests"}

# Safety
MAX_PAGES_API = 500

# =========================
# ====== APP & STATE ======
# =========================

app = Flask(__name__)

# Cursor & caches are keyed by (project_id, device_code, tabla)
# Pagination state
PAG_CURSORS: Dict[Tuple[str, str, str], Dict[str, Any]] = {}   # {key: {"next_offset": int, "finished": bool, "limit": int}}
# Per-day caches
DAY_CACHE_STORE: Dict[Tuple[str, str, str], Dict[str, Dict[str, Any]]] = {}   # {key: {day: {"plotted": [dict], "count": int}}}
# Global dedupe per key (avoids re-adding same row while prefetching)
DEDUP_KEYS: Dict[Tuple[str, str, str], set] = {}

# =========================
# ======== UTILS ==========
# =========================

def build_upstream_url(project_id: str, device_code: str, tabla: str, limit: int, offset: int) -> str:
    return (
        f"{UPSTREAM_BASE}?tabla={tabla}"
        f"&disp.id_proyecto={project_id}"
        f"&disp.codigo_interno={device_code}"
        f"&limite={int(limit)}&offset={int(offset)}"
    )

def make_session(retries: int, backoff: float) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=max(0, int(retries)),
        connect=max(0, int(retries)),
        read=max(0, int(retries)),
        status=max(0, int(retries)),
        backoff_factor=float(backoff),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(DEFAULT_HEADERS)
    return s

def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            return float(x) if not (isinstance(x, float) and math.isnan(x)) else None
        s = str(x).strip().replace(",", ".")
        if s == "" or s.lower() in {"nan", "null", "none"}:
            return None
        for tok in ["µg/m³", "ug/m3", "km/h", "V", "%", "°C"]:
            s = s.replace(tok, "").strip()
        return float(s)
    except Exception:
        return None

def choose_coords(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    lat = to_float(row.get(KEY_SIM_LAT)); lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None:
        return lat, lon
    lat = to_float(row.get(KEY_META_LAT)); lon = to_float(row.get(KEY_META_LON))
    return lat, lon

def process_rows_to_plotted(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert upstream raw rows to the 'plotted' schema used by the map and caches."""
    out: List[Dict[str, Any]] = []
    for row in rows:
        lat, lon = choose_coords(row)
        pm25 = to_float(row.get(KEY_PM25))
        if lat is None or lon is None or pm25 is None:
            continue
        out.append({
            "time": row.get(KEY_TIME),
            "envio_n": row.get(KEY_NUM_ENV),
            "lat": lat,
            "lon": lon,
            "pm25": pm25,
            "pm1": to_float(row.get(KEY_PM1)),
            "pm10": to_float(row.get(KEY_PM10)),
            "temp_pms": to_float(row.get(KEY_TEMP)),
            "hum": to_float(row.get(KEY_HUM)),
            "vbat": to_float(row.get(KEY_VBAT)),
            "csq": to_float(row.get(KEY_SIM_CSQ)),
            "sats": to_float(row.get(KEY_SIM_SATS)),
            "speed_kmh": to_float(row.get(KEY_SIM_SPEED)),
        })
    return out

def day_of(r: Dict[str, Any]) -> Optional[str]:
    """Return 'YYYY-MM-DD' from r['time'] which is 'YYYY-MM-DDTHH:MM:SS'."""
    t = r.get("time")
    if not t or not isinstance(t, str) or "T" not in t:
        return None
    return t.split("T", 1)[0]

def key_tuple(project_id: str, device_code: str, tabla: str) -> Tuple[str, str, str]:
    return (str(project_id), str(device_code), str(tabla))

def get_structs(k: Tuple[str, str, str]):
    if k not in PAG_CURSORS:
        PAG_CURSORS[k] = {"next_offset": 0, "finished": False, "limit": DEFAULT_LIMIT}
    if k not in DAY_CACHE_STORE:
        DAY_CACHE_STORE[k] = {}
    if k not in DEDUP_KEYS:
        DEDUP_KEYS[k] = set()
    return PAG_CURSORS[k], DAY_CACHE_STORE[k], DEDUP_KEYS[k]

# =========================
# ======== API ============ 
# =========================

def fetch_one_page(project_id: str, device_code: str, tabla: str,
                   limit: int, offset: int,
                   connect_timeout: float, read_timeout: float,
                   retries: int, backoff: float, verify: bool=True) -> List[Dict[str, Any]]:
    """Fetch one page from V2 API and return raw rows (dicts)."""
    sess = make_session(retries, backoff)
    url = build_upstream_url(project_id, device_code, tabla, limit, offset)
    resp = sess.get(url, timeout=(connect_timeout, read_timeout), verify=verify)
    resp.raise_for_status()
    payload = resp.json()
    rows = payload.get("data", {}).get("tableData", [])
    return [r for r in rows if isinstance(r, dict)]

@app.route("/api/prefetch")
def api_prefetch():
    """
    Incrementally fetch ONE page from upstream and push into per-day server cache.
    Query:
      project_id, device_code, tabla
      limit (default 500)
      connect_timeout/read_timeout/retries/backoff/verify
    Returns:
      { updated_days: {day: added_count}, next_offset, finished, total_days }
    """
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)
    limit = int(request.args.get("limit", DEFAULT_LIMIT))
    connect_timeout = float(request.args.get("connect_timeout", DEFAULT_CONNECT_TIMEOUT))
    read_timeout = float(request.args.get("read_timeout", DEFAULT_READ_TIMEOUT))
    retries = int(request.args.get("retries", DEFAULT_RETRIES))
    backoff = float(request.args.get("backoff", DEFAULT_BACKOFF))
    verify_tls = request.args.get("verify", "1") != "0"

    k = key_tuple(project_id, device_code, tabla)
    cursor, day_cache, dedup = get_structs(k)
    # If limit changes, update cursor limit (affects next step calculation)
    cursor["limit"] = limit

    if cursor.get("finished", False):
        return jsonify({"status": "ok", "message": "finished", "next_offset": cursor["next_offset"], "finished": True,
                        "updated_days": {}, "total_days": len(day_cache)})

    offset = cursor["next_offset"]
    updated: Dict[str, int] = {}
    error_msg = None

    try:
        rows_raw = fetch_one_page(project_id, device_code, tabla, limit, offset,
                                  connect_timeout, read_timeout, retries, backoff, verify_tls)
        plotted = process_rows_to_plotted(rows_raw)

        # Deduplicate and push into per-day buckets
        for r in plotted:
            d = day_of(r)
            if not d:
                continue
            unique = f"{r.get('time','')}|{r.get('envio_n','')}|{r.get('lat','')}|{r.get('lon','')}"
            if unique in dedup:
                continue
            dedup.add(unique)
            if d not in day_cache:
                day_cache[d] = {"plotted": [], "count": 0}
            day_cache[d]["plotted"].append(r)
            day_cache[d]["count"] += 1
            updated[d] = updated.get(d, 0) + 1

        # Advance cursor
        n = len(rows_raw)
        cursor["next_offset"] = offset + n
        if n < limit or cursor["next_offset"] >= MAX_PAGES_API * limit:
            cursor["finished"] = True

        return jsonify({
            "status": "ok",
            "updated_days": updated,
            "fetched_raw": n,
            "added_plotted": sum(updated.values()),
            "next_offset": cursor["next_offset"],
            "finished": cursor["finished"],
            "total_days": len(day_cache)
        })

    except requests.exceptions.RequestException as e:
        error_msg = f"{type(e).__name__}: {e}"
        return jsonify({"status": "error", "error": error_msg, "next_offset": offset}), 502

@app.route("/api/day-index")
def api_day_index():
    """Return the list of cached days with counts."""
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)
    k = key_tuple(project_id, device_code, tabla)
    _, day_cache, _ = get_structs(k)
    # Sorted newest first
    days = sorted(day_cache.keys())
    return jsonify({"status": "ok", "days": [{"day": d, "count": day_cache[d]["count"]} for d in days]})

@app.route("/api/data")
def api_data():
    """
    Modes:
      - Page mode (default): limit/offset, paginate=0 (for downloads & quick checks)
      - Day mode: mode=day&day=YYYY-MM-DD  -> returns cached day (plotted)
    """
    mode = request.args.get("mode", "page")
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)
    verify_tls = request.args.get("verify", "1") != "0"

    if mode == "day":
        day = request.args.get("day")
        if not day:
            return jsonify({"status": "error", "error": "Missing 'day'"}), 400
        k = key_tuple(project_id, device_code, tabla)
        _, day_cache, _ = get_structs(k)
        rows = day_cache.get(day, {}).get("plotted", [])
        return jsonify({"status": "success", "mode": "day", "day": day, "rows": rows})

    # page mode (single page)
    limit = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    connect_timeout = float(request.args.get("connect_timeout", DEFAULT_CONNECT_TIMEOUT))
    read_timeout = float(request.args.get("read_timeout", DEFAULT_READ_TIMEOUT))
    retries = int(request.args.get("retries", DEFAULT_RETRIES))
    backoff = float(request.args.get("backoff", DEFAULT_BACKOFF))

    try:
        rows_raw = fetch_one_page(project_id, device_code, tabla, limit, offset,
                                  connect_timeout, read_timeout, retries, backoff, verify_tls)
        plotted = process_rows_to_plotted(rows_raw)
        return jsonify({"status": "success", "mode": "page", "rows": plotted,
                        "meta": {"limit": limit, "offset": offset, "count": len(plotted)}})
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "error", "error": f"{type(e).__name__}: {e}"}), 502

@app.route("/download/day/<day>.<ext>")
def download_day(day: str, ext: str):
    """Download plotted rows for a cached day."""
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)
    k = key_tuple(project_id, device_code, tabla)
    _, day_cache, _ = get_structs(k)
    rows = day_cache.get(day, {}).get("plotted", [])
    df = pd.DataFrame(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"plotted_{day}_{ts}.{ext}"
    if ext == "csv":
        bio = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        return send_file(bio, as_attachment=True, download_name=filename, mimetype="text/csv; charset=utf-8")
    elif ext == "xlsx":
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        bio.seek(0)
        return send_file(bio, as_attachment=True, download_name=filename,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return Response("Invalid extension", status=400)

# Keep the old page-mode downloads for compatibility
@app.route("/download/<kind>.<ext>")
def download_page(kind: str, ext: str):
    if kind not in {"raw", "plotted"} or ext not in {"csv", "xlsx"}:
        return Response("Invalid path", status=400)
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    limit = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    connect_timeout = float(request.args.get("connect_timeout", DEFAULT_CONNECT_TIMEOUT))
    read_timeout = float(request.args.get("read_timeout", DEFAULT_READ_TIMEOUT))
    retries = int(request.args.get("retries", DEFAULT_RETRIES))
    backoff = float(request.args.get("backoff", DEFAULT_BACKOFF))
    verify_tls = request.args.get("verify", "1") != "0"

    try:
        rows_raw = fetch_one_page(project_id, device_code, tabla, limit, offset,
                                  connect_timeout, read_timeout, retries, backoff, verify_tls)
    except requests.exceptions.RequestException as e:
        return Response(f"Upstream error: {e}", status=502, mimetype="text/plain")

    if kind == "raw":
        df = pd.DataFrame(rows_raw)
    else:
        df = pd.DataFrame(process_rows_to_plotted(rows_raw))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{kind}_{ts}.{ext}"
    if ext == "csv":
        bio = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        return send_file(bio, as_attachment=True, download_name=filename, mimetype="text/csv; charset=utf-8")
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# =========================
# ========= MAP =========== 
# =========================

@app.route("/")
def index():
    return redirect(url_for("map_view"))

@app.route("/map")
def map_view():
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    # Initialize cursor struct for this key so auto-prefetch starts at 0
    k = key_tuple(project_id, device_code, tabla)
    cursor, _, _ = get_structs(k)
    cursor["next_offset"] = 0
    cursor["finished"] = False
    cursor["limit"] = DEFAULT_LIMIT

    # Base map (center will move)
    fmap = folium.Map(location=[-33.45, -70.65], zoom_start=12, control_scale=True)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)

    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    cmap.add_to(fmap)

    # Toolbar (downloads for "page mode" + for "day mode")
    toolbar_html = f"""
    <div style="{TOOLBAR_CSS}">
      <b>Downloads</b><br>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Page mode:</span>
        <a id="dl-raw-csv" href="#">CSV</a> |
        <a id="dl-raw-xlsx" href="#">Excel</a> |
        <a id="dl-plot-csv" href="#">Plotted CSV</a> |
        <a id="dl-plot-xlsx" href="#">Plotted Excel</a>
      </div>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Day mode:</span>
        <a id="dl-day-csv" href="#">CSV</a> |
        <a id="dl-day-xlsx" href="#">Excel</a>
      </div>
    </div>
    """

    # Header control
    header_html = f"""
    <div id="controls" style="{HEADER_CSS}">
      <div style="font-weight:700;">HIRI Map • {device_code}</div>

      <!-- PAGE MODE -->
      <label>Limit:
        <input type="number" id="limit" value="{DEFAULT_LIMIT}" min="50" max="2000" step="50" style="width:90px;margin-left:6px;">
      </label>
      <label>Offset:
        <input type="number" id="offset" value="{DEFAULT_OFFSET}" min="0" step="10" style="width:100px;margin-left:6px;">
      </label>
      <button id="btnLoad">Load</button>
      <button id="btnOlderAppend">Append older</button>
      <button id="btnOlderReplace">Older</button>
      <button id="btnNewerReplace">Newer</button>
      <button id="btnReset">Reset</button>

      <!-- DAY MODE -->
      <span style="margin-left:10px;font-weight:600;">Day:</span>
      <select id="daySelect" style="min-width:130px;"></select>
      <button id="btnLoadDay">Load day</button>
      <button id="btnPrevDay">&#9664; Prev</button>
      <button id="btnNextDay">Next &#9654;</button>

      <!-- BACKGROUND PREFETCH -->
      <button id="btnAutoStart" title="Auto prefetch per-day cache">Auto prefetch ▶</button>
      <button id="btnAutoStop"  title="Stop auto prefetch" disabled>Stop ⏹</button>

      <span id="status" style="margin-left:10px;color:#333;">Ready.</span>
    </div>
    """

    root = fmap.get_root()
    root.html.add_child(Element(toolbar_html))
    root.html.add_child(Element(header_html))
    root.html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>'))

    js_cfg = {
        "project_id": project_id,
        "device_code": device_code,
        "tabla": tabla,
        "pm_breaks": PM_BREAKS,
        "pm_colors": PM_COLORS,
        "pm_vmax": PM_BREAKS[-1],
        "marker_radius": MARKER_RADIUS,
        "marker_opacity": MARKER_OPACITY,
        "heat_radius": HEAT_RADIUS,
        "heat_blur": HEAT_BLUR,
        "heat_min_opacity": HEAT_MIN_OP,
        "default_limit": DEFAULT_LIMIT,
        "default_offset": DEFAULT_OFFSET,
        "connect_timeout": DEFAULT_CONNECT_TIMEOUT,
        "read_timeout": DEFAULT_READ_TIMEOUT,
        "retries": DEFAULT_RETRIES,
        "backoff": DEFAULT_BACKOFF,
        "verify": 1,
        "map_var_name": fmap.get_name(),
        # Auto prefetch settings
        "autostart_prefetch": True,
        "prefetch_limit": 500,          # per request to /api/prefetch
        "prefetch_max_cycles": 400,     # safety (500 * 400 = 200k raw rows)
        "prefetch_delay_ms": 250        # interval between cycles
    }
    root.script.add_child(Element('window.CFG=%s;' % json.dumps(js_cfg)))
    # Main JS (no braces interpolation by Python)
    root.script.add_child(Element(r"""
(function(){
  const log = (...a)=>console.debug("[HIRI]", ...a);
  const err = (...a)=>console.error("[HIRI]", ...a);

  function makeGradient(colors){ const g={}; const n=colors.length; for(let i=0;i<n;i++) g[i/(n-1)]=colors[i]; return g; }
  function colorForPM(v){
    const b=CFG.pm_breaks, c=CFG.pm_colors, vmax = CFG.pm_vmax || b[b.length-1];
    const x=Math.max(b[0], Math.min(vmax, Number(v)));
    for(let i=b.length-1;i>=0;i--) if(x>=b[i]) return c[i];
    return c[0];
  }

  function bootstrap(MAP){
    const status = (t)=>{ const e=document.getElementById('status'); if(e) e.textContent=t; };
    const pointsLayer = L.layerGroup().addTo(MAP);
    let heatLayer = null, heatPoints = [];
    if (L.heatLayer) {
      heatLayer = L.heatLayer([], {
        radius: CFG.heat_radius, blur: CFG.heat_blur, minOpacity: CFG.heat_min_opacity,
        gradient: makeGradient(CFG.pm_colors), max: 1.0
      }).addTo(MAP);
    }
    L.control.layers(null, heatLayer? {"PM2.5 points":pointsLayer, "HeatMap PM2.5":heatLayer} :
                                   {"PM2.5 points":pointsLayer}, {collapsed:false}).addTo(MAP);

    let currentLimit  = Number(document.getElementById('limit').value)  || CFG.default_limit;
    let currentOffset = Number(document.getElementById('offset').value) || CFG.default_offset;
    let totalPlotted = 0, allLatLngs = [];
    let dayList = []; let currentDay = null;

    function updatePageDownloads(){
      const qp = new URLSearchParams({
        project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla,
        limite: currentLimit, offset: currentOffset, paginate: 0,
        connect_timeout: CFG.connect_timeout, read_timeout: CFG.read_timeout,
        retries: CFG.retries, backoff: CFG.backoff, verify: CFG.verify
      }).toString();
      [["dl-raw-csv","/download/raw.csv?"],["dl-raw-xlsx","/download/raw.xlsx?"],["dl-plot-csv","/download/plotted.csv?"],["dl-plot-xlsx","/download/plotted.xlsx?"]]
        .forEach(([id, base])=>{ const a=document.getElementById(id); if(a) a.href = base + qp; });
    }
    function updateDayDownloads(){
      if(!currentDay) return;
      const qp = new URLSearchParams({project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla}).toString();
      const base = `/download/day/${currentDay}.`;
      const a1=document.getElementById("dl-day-csv"); if(a1) a1.href = base+"csv?"+qp;
      const a2=document.getElementById("dl-day-xlsx"); if(a2) a2.href = base+"xlsx?"+qp;
    }
    function clearLayers(){
      pointsLayer.clearLayers(); heatPoints=[]; if(heatLayer) heatLayer.setLatLngs([]);
      totalPlotted=0; allLatLngs=[];
    }
    function addRows(rows, replace){
      const vmax = CFG.pm_vmax || CFG.pm_breaks[CFG.pm_breaks.length-1];
      let added=0;
      for (const r of rows){
        if (r.lat==null || r.lon==null || r.pm25==null) continue;
        const col = colorForPM(r.pm25);
        const m = L.circleMarker([r.lat, r.lon], {radius: CFG.marker_radius, color: col, weight:1, fillColor: col, fillOpacity: CFG.marker_opacity});
        m.bindPopup(`<b>PM2.5:</b> ${Number(r.pm25).toFixed(1)} µg/m³<br>
<b>Time:</b> ${r.time ?? '-'}<br>
<b>Envíos #:</b> ${r.envio_n ?? '-'}<br>
<b>Lat:</b> ${Number(r.lat).toFixed(6)}, <b>Lon:</b> ${Number(r.lon).toFixed(6)}<br>
<hr style="margin:4px 0"/>
<b>PM1:</b> ${r.pm1 ?? '-'} | <b>PM10:</b> ${r.pm10 ?? '-'}<br>
<b>Temp PMS:</b> ${r.temp_pms ?? '-'} °C | <b>Hum:</b> ${r.hum ?? '-'} %<br>
<b>VBat:</b> ${r.vbat ?? '-'} V<br>
<b>CSQ:</b> ${r.csq ?? '-'} | <b>Sats:</b> ${r.sats ?? '-'} | <b>Speed:</b> ${r.speed_kmh ?? '-'} km/h`);
        pointsLayer.addLayer(m);
        const intensity = Math.max(0, Math.min(vmax, Number(r.pm25))) / vmax;
        if (heatLayer) heatPoints.push([r.lat, r.lon, intensity]);
        allLatLngs.push([r.lat, r.lon]); totalPlotted++; added++;
      }
      if (heatLayer) heatLayer.setLatLngs(heatPoints);
      if (replace && allLatLngs.length) MAP.fitBounds(L.latLngBounds(allLatLngs), {padding:[20,20]});
      return added;
    }

    // ---- PAGE MODE ----
    async function fetchPage(limit, offset){
      const qp = new URLSearchParams({
        mode:'page', project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla,
        limite: limit, offset: offset, connect_timeout: CFG.connect_timeout, read_timeout: CFG.read_timeout,
        retries: CFG.retries, backoff: CFG.backoff, verify: CFG.verify
      }).toString();
      const url = '/api/data?'+qp;
      const t0 = performance.now();
      const resp = await fetch(url);
      if(!resp.ok) throw new Error('HTTP '+resp.status);
      const j = await resp.json();
      const ms = performance.now() - t0;
      log("page fetched:", j.meta?.count, "ms:", ms.toFixed(0));
      return j.rows || [];
    }
    async function loadReplace(limit, offset){
      status('Loading…');
      try{
        const rows = await fetchPage(limit, offset);
        clearLayers();
        const added = addRows(rows, true);
        currentLimit=limit; currentOffset=offset;
        document.getElementById('limit').value = currentLimit;
        document.getElementById('offset').value = currentOffset;
        updatePageDownloads();
        status(`Loaded page: rows=${rows.length} (added=${added}) limit=${limit} offset=${offset}`);
      }catch(e){ status('Load error: '+e.message); err(e); }
    }
    async function appendOlder(){
      const nextOffset = currentOffset + currentLimit;
      status('Appending older…');
      try{
        const rows = await fetchPage(currentLimit, nextOffset);
        const added = addRows(rows, false);
        currentOffset = nextOffset; document.getElementById('offset').value = currentOffset;
        updatePageDownloads();
        status(`Appended page: +${rows.length} (added=${added}) total=${totalPlotted} offset=${currentOffset}`);
      }catch(e){ status('Append error: '+e.message); err(e); }
    }

    // ---- DAY MODE ----
    async function refreshDayIndex(selectNewestIfEmpty=true){
      const qp = new URLSearchParams({project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla}).toString();
      const resp = await fetch('/api/day-index?'+qp);
      if(!resp.ok) return;
      const j = await resp.json();
      dayList = (j.days||[]).map(x=>x.day).sort(); // ascending
      const sel = document.getElementById('daySelect');
      const prev = sel.value;
      sel.innerHTML = "";
      for(const d of dayList){
        const opt = document.createElement('option'); opt.value=d; opt.textContent=d;
        sel.appendChild(opt);
      }
      if(prev && dayList.includes(prev)) sel.value = prev;
      else if(selectNewestIfEmpty && dayList.length) sel.value = dayList[dayList.length-1];
    }
    async function loadDay(day, replace=true){
      if(!day) return;
      status('Loading day '+day+' …');
      const qp = new URLSearchParams({
        mode:'day', day: day, project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla
      }).toString();
      const resp = await fetch('/api/data?'+qp);
      if(!resp.ok){ status('Day load error '+resp.status); return; }
      const j = await resp.json();
      if(replace) clearLayers();
      const added = addRows(j.rows||[], replace);
      currentDay = day; updateDayDownloads();
      status(`Day loaded: ${day} rows=${(j.rows||[]).length} (added=${added})`);
    }

    // ---- BACKGROUND AUTO-PREFETCH (build day cache) ----
    let autoFlag=false, autoCycles=0, autoTimer=null;
    async function prefetchOnce(){
      const qp = new URLSearchParams({
        project_id: CFG.project_id, device_code: CFG.device_code, tabla: CFG.tabla,
        limit: CFG.prefetch_limit, connect_timeout: CFG.connect_timeout, read_timeout: CFG.read_timeout,
        retries: CFG.retries, backoff: CFG.backoff, verify: CFG.verify
      }).toString();
      const resp = await fetch('/api/prefetch?'+qp);
      if(!resp.ok) { status('Prefetch HTTP '+resp.status); return {stop:true}; }
      const j = await resp.json();
      if (j.status !== 'ok'){ status('Prefetch error'); return {stop:true}; }
      await refreshDayIndex(false);
      return {stop: !!j.finished};
    }
    function startAuto(){
      if (autoFlag) return;
      autoFlag = true; autoCycles=0;
      document.getElementById('btnAutoStart').disabled = true;
      document.getElementById('btnAutoStop').disabled = false;
      status('Auto prefetching…');
      const loop = async ()=>{
        if(!autoFlag) return;
        autoCycles++;
        const res = await prefetchOnce();
        if(res.stop || autoCycles>=CFG.prefetch_max_cycles){
          stopAuto();
          status('Auto prefetch stopped.');
          return;
        }
        autoTimer = setTimeout(loop, CFG.prefetch_delay_ms);
      };
      loop();
    }
    function stopAuto(){
      autoFlag=false;
      document.getElementById('btnAutoStart').disabled = false;
      document.getElementById('btnAutoStop').disabled = true;
      if(autoTimer){ clearTimeout(autoTimer); autoTimer=null; }
    }

    // Wire UI
    document.getElementById('btnLoad').addEventListener('click', ()=>{
      const limit  = Math.max(50, Number(document.getElementById('limit').value)  || CFG.default_limit);
      const offset = Math.max(0,  Number(document.getElementById('offset').value) || CFG.default_offset);
      loadReplace(limit, offset);
    });
    document.getElementById('btnOlderAppend').addEventListener('click', appendOlder);
    document.getElementById('btnOlderReplace').addEventListener('click', ()=>{
      const limit = Math.max(50, Number(document.getElementById('limit').value)||CFG.default_limit);
      loadReplace(limit, currentOffset + limit);
    });
    document.getElementById('btnNewerReplace').addEventListener('click', ()=>{
      const limit = Math.max(50, Number(document.getElementById('limit').value)||CFG.default_limit);
      loadReplace(limit, Math.max(0, currentOffset - limit));
    });
    document.getElementById('btnReset').addEventListener('click', ()=>loadReplace(currentLimit, 0));

    document.getElementById('btnLoadDay').addEventListener('click', ()=>{
      const d = document.getElementById('daySelect').value; loadDay(d, true);
    });
    document.getElementById('btnPrevDay').addEventListener('click', ()=>{
      if(!dayList.length) return; const d = document.getElementById('daySelect').value;
      const i = dayList.indexOf(d); if(i>0){ document.getElementById('daySelect').value = dayList[i-1]; loadDay(dayList[i-1], true); }
    });
    document.getElementById('btnNextDay').addEventListener('click', ()=>{
      if(!dayList.length) return; const d = document.getElementById('daySelect').value;
      const i = dayList.indexOf(d); if(i>=0 && i<dayList.length-1){ document.getElementById('daySelect').value = dayList[i+1]; loadDay(dayList[i+1], true); }
    });

    document.getElementById('btnAutoStart').addEventListener('click', startAuto);
    document.getElementById('btnAutoStop').addEventListener('click', stopAuto);

    // Initials
    updatePageDownloads();
    refreshDayIndex().then(()=>{
      if (dayList.length){ document.getElementById('daySelect').value = dayList[dayList.length-1]; loadDay(dayList[dayList.length-1], true); }
    });
    if (CFG.autostart_prefetch) startAuto();
  }

  (function waitForMap(){
    const want = CFG.map_var_name; const start = performance.now();
    (function poll(){
      const m = window[want];
      if(m && m instanceof L.Map){ bootstrap(m); return; }
      if(performance.now()-start > 5000){ err("Map variable not found:", want); return; }
      setTimeout(poll, 50);
    })();
  })();
})();
    """))

    return Response(fmap.get_root().render(), mimetype="text/html")

# =========================
# ======== HEALTH =========
# =========================

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})

# =========================
# ========= MAIN ==========
# =========================

if __name__ == "__main__":
    # For production: use gunicorn/uvicorn and set host to 0.0.0.0
    app.run(host="127.0.0.1", port=5000, debug=True)
