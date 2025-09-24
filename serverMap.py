# app.py
# -*- coding: utf-8 -*-
"""
HIRIPRO-01 Flask server (API V2) with:
- Background collector (daemon thread) that paginates forever with retries/backoff.
- Per-day cache in memory AND on disk (JSONL per day).
- Deduplication across the whole device/session.
- Map UI with "Day mode" (preferred) and "Page mode" (diagnostics).
- Consistent PM2.5 color scale for points/heatmap/legend.
- Robust client JS (no Leaflet heat getLatLngs error).

Run:
  pip install flask requests pandas folium branca
  python app.py
Open:
  http://127.0.0.1:5000/map
"""

import io
import os
import json
import math
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, Response, send_file, redirect, url_for, jsonify

import folium
from branca.colormap import LinearColormap
from branca.element import Element
from folium.plugins import HeatMap, Fullscreen, MiniMap

# =========================
# ========= CONFIG ========
# =========================

UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2"

DEFAULT_TABLA       = "datos"
DEFAULT_PROJECT_ID  = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"

# Page defaults for upstream calls (collector and /api/data?page)
DEFAULT_LIMIT   = 500
DEFAULT_OFFSET  = 0

# Network/retry defaults
CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 60
RETRIES         = 3
BACKOFF         = 0.7

# Safety: max pages before we voluntarily pause (avoid infinite loops on bad APIs)
MAX_PAGES_SOFT_CAP = 1_000_000  # effectively unlimited

# Disk cache base directory
CACHE_DIR = Path("cache")

# PM2.5 scale (fixed)
PM_BREAKS  = [0, 12, 35, 55, 150, 250]
PM_COLORS  = ["#2ecc71", "#a3d977", "#f1c40f", "#e67e22", "#e74c3c", "#7f1d1d"]
PM_CAPTION = "PM2.5 (µg/m³)"

# Marker/HeatMap styling
MARK_RADIUS   = 6
MARK_OPACITY  = 0.85
HEAT_RADIUS   = 12
HEAT_BLUR     = 22
HEAT_MIN_OP   = 0.30

# UI CSS boxes
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

DEFAULT_HEADERS = {"User-Agent": "HIRIMap/1.0 (Flask collector) requests"}

# =========================
# ====== APP & STATE ======
# =========================

app = Flask(__name__)

# In-memory per-key structures
# Key tuple = (project_id, device_code, tabla)
DayCache: Dict[Tuple[str,str,str], Dict[str, Dict[str, Any]]] = {}  # {key: {day: {"plotted":[rows], "count":int}}}
DedupSet: Dict[Tuple[str,str,str], set] = {}                        # {key: set(unique_key)}
Cursor:   Dict[Tuple[str,str,str], Dict[str, Any]] = {}             # {key: {"offset":int, "finished":bool, "pages":int}}

# Background collector thread control
CollectorThread: Optional[threading.Thread] = None
CollectorStop   = threading.Event()

# =========================
# ======== UTILS ==========
# =========================

def key_tuple(project_id: str, device_code: str, tabla: str) -> Tuple[str,str,str]:
    return (str(project_id), str(device_code), str(tabla))

def key_dir(key: Tuple[str,str,str]) -> Path:
    p, d, t = key
    safe = f"{p}__{d}__{t}".replace("/", "_")
    return CACHE_DIR / safe

def load_cursor(key: Tuple[str,str,str]) -> Dict[str, Any]:
    """Load cursor from disk (if exists)."""
    kd = key_dir(key); kd.mkdir(parents=True, exist_ok=True)
    f = kd / "cursor.json"
    if f.exists():
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"offset": 0, "finished": False, "pages": 0, "limit": DEFAULT_LIMIT}

def save_cursor(key: Tuple[str,str,str], cur: Dict[str,Any]) -> None:
    kd = key_dir(key); kd.mkdir(parents=True, exist_ok=True)
    (kd / "cursor.json").write_text(json.dumps(cur, ensure_ascii=False, indent=2), encoding="utf-8")

def append_jsonl(path: Path, rows: List[Dict[str,Any]]) -> None:
    """Append rows as JSONL; ensure dir exists."""
    if not rows: return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

def read_jsonl(path: Path) -> List[Dict[str,Any]]:
    if not path.exists(): return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try: rows.append(json.loads(line))
            except Exception: continue
    return rows

def to_float(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        if isinstance(x, (int, float)):
            return float(x) if not (isinstance(x, float) and math.isnan(x)) else None
        s = str(x).strip().replace(",", ".")
        if s == "" or s.lower() in {"nan","null","none"}: return None
        for tok in ["µg/m³","ug/m3","km/h","V","%","°C"]:
            s = s.replace(tok, "").strip()
        return float(s)
    except Exception:
        return None

def choose_coords(row: Dict[str,Any]) -> Tuple[Optional[float], Optional[float]]:
    lat = to_float(row.get(KEY_SIM_LAT)); lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None: return lat, lon
    lat = to_float(row.get(KEY_META_LAT)); lon = to_float(row.get(KEY_META_LON))
    return lat, lon

def process_rows_to_plotted(rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out = []
    for row in rows:
        lat, lon = choose_coords(row)
        pm25 = to_float(row.get(KEY_PM25))
        if lat is None or lon is None or pm25 is None:  # only keep valid points
            continue
        out.append({
            "time": row.get(KEY_TIME),
            "envio_n": row.get(KEY_NUM_ENV),
            "lat": lat, "lon": lon, "pm25": pm25,
            "pm1":  to_float(row.get(KEY_PM1)),
            "pm10": to_float(row.get(KEY_PM10)),
            "temp_pms": to_float(row.get(KEY_TEMP)),
            "hum": to_float(row.get(KEY_HUM)),
            "vbat": to_float(row.get(KEY_VBAT)),
            "csq":  to_float(row.get(KEY_SIM_CSQ)),
            "sats": to_float(row.get(KEY_SIM_SATS)),
            "speed_kmh": to_float(row.get(KEY_SIM_SPEED)),
        })
    return out

def day_of(r: Dict[str,Any]) -> Optional[str]:
    t = r.get("time")
    if not t or not isinstance(t, str) or "T" not in t: return None
    return t.split("T",1)[0]

def make_session() -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=RETRIES, connect=RETRIES, read=RETRIES, status=RETRIES,
        backoff_factor=BACKOFF, status_forcelist=[429,500,502,503,504],
        allowed_methods={"GET"}, raise_on_status=False,
    )
    a = HTTPAdapter(max_retries=r)
    s.mount("https://", a); s.mount("http://", a)
    s.headers.update(DEFAULT_HEADERS)
    return s

def build_url(project_id: str, device_code: str, tabla: str, limit: int, offset: int) -> str:
    return (f"{UPSTREAM_BASE}?tabla={tabla}"
            f"&disp.id_proyecto={project_id}"
            f"&disp.codigo_interno={device_code}"
            f"&limite={int(limit)}&offset={int(offset)}")

def ensure_structs(key: Tuple[str,str,str]) -> None:
    if key not in DayCache: DayCache[key] = {}
    if key not in DedupSet: DedupSet[key] = set()
    if key not in Cursor:   Cursor[key] = load_cursor(key)

def add_to_day_cache(key: Tuple[str,str,str], plotted: List[Dict[str,Any]]) -> Dict[str,int]:
    """Add plotted rows to in-memory cache + append to disk JSONL per day (dedup in-memory)."""
    ensure_structs(key)
    updated: Dict[str,int] = {}
    for r in plotted:
        d = day_of(r)
        if not d: continue
        ukey = f"{r.get('time','')}|{r.get('envio_n','')}|{r.get('lat','')}|{r.get('lon','')}"
        if ukey in DedupSet[key]:  # already cached
            continue
        DedupSet[key].add(ukey)
        # in-memory
        if d not in DayCache[key]: DayCache[key][d] = {"plotted": [], "count": 0}
        DayCache[key][d]["plotted"].append(r)
        DayCache[key][d]["count"] += 1
        updated[d] = updated.get(d, 0) + 1
    # persist to disk per-day
    if updated:
        for d in updated.keys():
            day_path = key_dir(key) / f"{d}.jsonl"
            # append only the new rows of that day
            new_rows = [r for r in plotted if day_of(r) == d]
            append_jsonl(day_path, new_rows)
    return updated

def load_days_from_disk(key: Tuple[str,str,str]) -> None:
    """Warm in-memory cache from disk at startup (ensures dedup too)."""
    ensure_structs(key)
    kd = key_dir(key)
    if not kd.exists(): return
    for f in kd.glob("*.jsonl"):
        day = f.stem
        rows = read_jsonl(f)
        # rebuild dedup while filling memory
        for r in rows:
            ukey = f"{r.get('time','')}|{r.get('envio_n','')}|{r.get('lat','')}|{r.get('lon','')}"
            if ukey in DedupSet[key]: continue
            DedupSet[key].add(ukey)
            if day not in DayCache[key]:
                DayCache[key][day] = {"plotted": [], "count": 0}
            DayCache[key][day]["plotted"].append(r)
            DayCache[key][day]["count"] += 1

# =========================
# === BACKGROUND COLLECT ==
# =========================

def collector_loop(project_id: str, device_code: str, tabla: str, limit: int):
    """Run forever: paginate, cache per-day, dedupe, and expose status."""
    key = key_tuple(project_id, device_code, tabla)
    ensure_structs(key)
    load_days_from_disk(key)
    session = make_session()

    while not CollectorStop.is_set():
        cur = Cursor[key]
        offset = int(cur.get("offset", 0))
        pages  = int(cur.get("pages", 0))

        if pages >= MAX_PAGES_SOFT_CAP:
            cur["last_error"] = f"Soft cap reached: MAX_PAGES_SOFT_CAP={MAX_PAGES_SOFT_CAP}"
            save_cursor(key, cur)
            time.sleep(5.0)
            continue

        url = build_url(project_id, device_code, tabla, limit, offset)
        try:
            resp = session.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=True)
            resp.raise_for_status()
            payload  = resp.json()
            raw_rows = payload.get("data", {}).get("tableData", [])
            raw_rows = [r for r in raw_rows if isinstance(r, dict)]
            n = len(raw_rows)

            plotted = process_rows_to_plotted(raw_rows)
            updated = add_to_day_cache(key, plotted)

            cur["offset"]     = offset + n
            cur["pages"]      = pages + 1
            cur["finished"]   = (n < limit)
            cur["last_ok_ts"] = time.time()
            cur["last_error"] = None
            cur["last_url"]   = url
            save_cursor(key, cur)

            print(f"[collector] page#{cur['pages']} offset={offset} got={n} "
                  f"plotted+={sum(updated.values())} days+={list(updated.keys())}")

            time.sleep(30.0 if cur["finished"] else 0.2)

        except requests.exceptions.RequestException as e:
            cur["last_error"] = f"{type(e).__name__}: {e}"
            cur["last_url"]   = url
            save_cursor(key, cur)
            print(f"[collector] error: {cur['last_error']}; sleeping 5s")
            time.sleep(5.0)

def start_collector(project_id: str, device_code: str, tabla: str, limit: int=DEFAULT_LIMIT):
    """Start background collector if not running."""
    global CollectorThread
    if CollectorThread and CollectorThread.is_alive():
        return
    CollectorStop.clear()
    t = threading.Thread(target=collector_loop, args=(project_id, device_code, tabla, limit), daemon=True)
    CollectorThread = t
    t.start()

def stop_collector():
    CollectorStop.set()

# =========================
# ========= ROUTES ========
# =========================

@app.route("/")
def index():
    return redirect(url_for("map_view"))

# ---- Admin controls ----
@app.route("/admin/prefetch/start")
def admin_start():
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)
    limit      = int(request.args.get("limit", DEFAULT_LIMIT))
    start_collector(project_id, device_code, tabla, limit)
    return jsonify({"status":"ok","message":"collector started","limit":limit})

@app.route("/admin/prefetch/stop")
def admin_stop():
    stop_collector()
    return jsonify({"status":"ok","message":"collector stop requested"})


@app.route("/admin/prefetch/status")
def admin_status():
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)
    key = key_tuple(project_id, device_code, tabla)
    ensure_structs(key)
    kd   = key_dir(key)
    cur  = Cursor.get(key, {"offset":0,"finished":False,"pages":0})
    days = sorted([p.stem for p in kd.glob("*.jsonl")]) if kd.exists() else []
    return jsonify({
        "status": "ok",
        "cursor": {
            "offset":   cur.get("offset", 0),
            "pages":    cur.get("pages", 0),
            "finished": bool(cur.get("finished", False)),
            "last_ok_ts": cur.get("last_ok_ts"),
            "last_error": cur.get("last_error"),
            "last_url":   cur.get("last_url"),
            "limit":      cur.get("limit", DEFAULT_LIMIT)
        },
        "days_cached": days,
        "days_count": len(days)
    })

# ---- Data APIs ----
@app.route("/api/day-index")
def api_day_index():
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)
    key = key_tuple(project_id, device_code, tabla)
    ensure_structs(key); load_days_from_disk(key)
    days = sorted(DayCache[key].keys())
    return jsonify({"status":"ok","days":[{"day":d,"count":DayCache[key][d]["count"]} for d in days]})

@app.route("/api/data")
def api_data():
    mode = request.args.get("mode","page")
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)
    key = key_tuple(project_id, device_code, tabla)
    ensure_structs(key)

    if mode == "day":
        day = request.args.get("day")
        if not day: return jsonify({"status":"error","error":"missing day"}), 400
        # Ensure memory contains disk content
        load_days_from_disk(key)
        rows = DayCache[key].get(day, {}).get("plotted", [])
        return jsonify({"status":"success","mode":"day","day":day,"rows":rows})

    # page mode (single page direct from upstream)
    limit  = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    url = build_url(project_id, device_code, tabla, limit, offset)
    try:
        s = make_session()
        r = s.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=True)
        r.raise_for_status()
        payload = r.json()
        raw = payload.get("data", {}).get("tableData", [])
        raw = [x for x in raw if isinstance(x, dict)]
        rows = process_rows_to_plotted(raw)
        return jsonify({"status":"success","mode":"page","meta":{"limit":limit,"offset":offset,"count":len(rows)},"rows":rows})
    except requests.exceptions.RequestException as e:
        return jsonify({"status":"error","error":f"{type(e).__name__}: {e}"}), 502

# ---- Downloads ----
@app.route("/download/day/<day>.<ext>")
def download_day(day: str, ext: str):
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)
    key = key_tuple(project_id, device_code, tabla)
    load_days_from_disk(key)
    rows = DayCache.get(key, {}).get(day, {}).get("plotted", [])
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

@app.route("/download/<kind>.<ext>")
def download_page(kind:str, ext:str):
    """For diagnostics: download the current page directly from upstream."""
    if kind not in {"raw","plotted"} or ext not in {"csv","xlsx"}:
        return Response("Invalid path", status=400)
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)

    limit  = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))

    try:
        s = make_session()
        url = build_url(project_id, device_code, tabla, limit, offset)
        r = s.get(url, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=True)
        r.raise_for_status()
        payload = r.json()
        raw = payload.get("data", {}).get("tableData", [])
        raw = [x for x in raw if isinstance(x, dict)]
    except requests.exceptions.RequestException as e:
        return Response(f"Upstream error: {e}", status=502, mimetype="text/plain")

    df = pd.DataFrame(raw) if kind == "raw" else pd.DataFrame(process_rows_to_plotted(raw))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{kind}_{ts}.{ext}"
    if ext == "csv":
        return send_file(io.BytesIO(df.to_csv(index=False).encode("utf-8")), as_attachment=True,
                         download_name=filename, mimetype="text/csv; charset=utf-8")
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    bio.seek(0)
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---- Map ----
@app.route("/map")
@app.route("/map")
def map_view():
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code= request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla      = request.args.get("tabla", DEFAULT_TABLA)

    # start background collector
    start_collector(project_id, device_code, tabla, DEFAULT_LIMIT)

    # base map
    fmap = folium.Map(location=[-33.45, -70.65], zoom_start=12, control_scale=True)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)

    # legend
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = PM_CAPTION
    cmap.add_to(fmap)

    # toolbars (simple style attributes: seguros dentro de f-strings)
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
    header_html = f"""
    <div id="controls" style="{HEADER_CSS}">
      <div style="font-weight:700;">HIRI Map • {device_code}</div>
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

      <span style="margin-left:10px;font-weight:600;">Day:</span>
      <select id="daySelect" style="min-width:130px;"></select>
      <button id="btnLoadDay">Load day</button>
      <button id="btnPrevDay">&#9664; Prev</button>
      <button id="btnNextDay">Next &#9654;</button>

      <span id="status" style="margin-left:10px;color:#333;">Waiting for data…</span>
      <span id="spin" class="spin" title="loading" style="margin-left:6px;"></span>
      <span id="cstat" style="margin-left:10px;color:#666;"></span>
      <span id="cerr"  style="margin-left:6px;color:#b91c1c;"></span>
    </div>
    """

    root = fmap.get_root()
    root.html.add_child(Element(toolbar_html))
    root.html.add_child(Element(header_html))

    # CSS con llaves -> NO f-string
    root.html.add_child(Element('''
<style>
  .spin {
    width: 12px; height: 12px; border: 2px solid #999; border-top-color: transparent;
    border-radius: 50%; display:inline-block; animation: spin 0.9s linear infinite; visibility:hidden;
  }
  @keyframes spin { from { transform: rotate(0deg);} to { transform: rotate(360deg);} }
</style>
'''))

    # leaflet-heat
    root.html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>'))

    # config visible en window
    js_cfg = {
        "project_id": project_id, "device_code": device_code, "tabla": tabla,
        "pm_breaks": PM_BREAKS, "pm_colors": PM_COLORS, "pm_vmax": PM_BREAKS[-1],
        "marker_radius": MARK_RADIUS, "marker_opacity": MARK_OPACITY,
        "heat_radius": HEAT_RADIUS, "heat_blur": HEAT_BLUR, "heat_min_opacity": HEAT_MIN_OP,
        "default_limit": DEFAULT_LIMIT, "default_offset": DEFAULT_OFFSET,
        "map_var_name": fmap.get_name()
    }
    root.script.add_child(Element('window.CFG=%s;' % json.dumps(js_cfg)))

    # JS robusto (poll de status y day-index, spinner, errores controlados)
    root.script.add_child(Element(r"""
(function(){
  const log = (...a)=>console.debug("[HIRI]", ...a);
  const err = (...a)=>console.error("[HIRI]", ...a);
  function makeGradient(colors){ const g={}; const n=colors.length; for(let i=0;i<n;i++) g[i/(n-1)]=colors[i]; return g; }
  function colorForPM(v){ const b=CFG.pm_breaks, c=CFG.pm_colors, vmax = CFG.pm_vmax||b[b.length-1]; const x=Math.max(b[0], Math.min(vmax, Number(v))); for(let i=b.length-1;i>=0;i--) if(x>=b[i]) return c[i]; return c[0]; }

  function boot(MAP){
    const $ = (id)=>document.getElementById(id);
    const showSpin = (on)=>{ const s=$('spin'); if(s) s.style.visibility = on ? 'visible' : 'hidden'; };
    const setStatus = (t)=>{ const e=$('status'); if(e) e.textContent=t; };

    // layers
    const pointsLayer = L.layerGroup().addTo(MAP);
    let heatLayer = null, heatPoints = [];
    if (L.heatLayer) {
      heatLayer = L.heatLayer([], { radius: CFG.heat_radius, blur: CFG.heat_blur, minOpacity: CFG.heat_min_opacity, gradient: makeGradient(CFG.pm_colors), max: 1.0 }).addTo(MAP);
    }
    L.control.layers(null, heatLayer? {"PM2.5 points":pointsLayer, "HeatMap PM2.5":heatLayer} :
                                   {"PM2.5 points":pointsLayer}, {collapsed:false}).addTo(MAP);

    let currentLimit  = Number($('limit').value)||CFG.default_limit;
    let currentOffset = Number($('offset').value)||CFG.default_offset;
    let allLatLngs = [];

    function updatePageDownloads(){
      const qp = new URLSearchParams({ project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla, limite:currentLimit, offset:currentOffset }).toString();
      [["dl-raw-csv","/download/raw.csv?"],["dl-raw-xlsx","/download/raw.xlsx?"],["dl-plot-csv","/download/plotted.csv?"],["dl-plot-xlsx","/download/plotted.xlsx?"]]
        .forEach(([id, base])=>{ const a=$(id); if(a) a.href = base + qp; });
    }
    function updateDayDownloads(day){
      if(!day) return;
      const qp = new URLSearchParams({project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla}).toString();
      const base = `/download/day/${day}.`;
      const a1=$("dl-day-csv");  if(a1) a1.href = base+"csv?"+qp;
      const a2=$("dl-day-xlsx"); if(a2) a2.href = base+"xlsx?"+qp;
    }
    function clearLayers(){ pointsLayer.clearLayers(); heatPoints=[]; if(heatLayer) heatLayer.setLatLngs([]); allLatLngs=[]; }
    function addRows(rows, replaceBounds){
      const vmax = CFG.pm_vmax || CFG.pm_breaks[CFG.pm_breaks.length-1];
      let added=0;
      for(const r of rows){
        if (r.lat==null || r.lon==null || r.pm25==null) continue;
        const col = colorForPM(r.pm25);
        const m = L.circleMarker([r.lat, r.lon], {radius: CFG.marker_radius, color: col, weight:1, fillColor: col, fillOpacity: CFG.marker_opacity});
        m.bindPopup(`<b>PM2.5:</b> ${Number(r.pm25).toFixed(1)} µg/m³<br>
<b>Time:</b> ${r.time ?? '-'}<br><b>Envíos #:</b> ${r.envio_n ?? '-'}<br>
<b>Lat:</b> ${Number(r.lat).toFixed(6)}, <b>Lon:</b> ${Number(r.lon).toFixed(6)}<br>
<hr style="margin:4px 0"/>
<b>PM1:</b> ${r.pm1 ?? '-'} | <b>PM10:</b> ${r.pm10 ?? '-'}<br>
<b>Temp PMS:</b> ${r.temp_pms ?? '-'} °C | <b>Hum:</b> ${r.hum ?? '-'} %<br>
<b>VBat:</b> ${r.vbat ?? '-'} V<br>
<b>CSQ:</b> ${r.csq ?? '-'} | <b>Sats:</b> ${r.sats ?? '-'} | <b>Speed:</b> ${r.speed_kmh ?? '-'} km/h`);
        pointsLayer.addLayer(m);
        const intensity = Math.max(0, Math.min(vmax, Number(r.pm25))) / vmax;
        if (heatLayer) heatPoints.push([r.lat, r.lon, intensity]);
        allLatLngs.push([r.lat, r.lon]); added++;
      }
      if (heatLayer) heatLayer.setLatLngs(heatPoints);
      if (replaceBounds && allLatLngs.length) MAP.fitBounds(L.latLngBounds(allLatLngs), {padding:[20,20]});
      return added;
    }

    async function fetchJSON(url){ const r = await fetch(url); if(!r.ok) throw new Error("HTTP "+r.status); return r.json(); }

    // Page mode
    async function fetchPage(limit, offset){
      const qp = new URLSearchParams({mode:'page', project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla, limite:limit, offset:offset}).toString();
      return (await fetchJSON('/api/data?'+qp)).rows || [];
    }
    async function loadReplace(limit, offset){
      setStatus('Loading page…'); showSpin(true);
      try{
        const rows = await fetchPage(limit, offset);
        clearLayers();
        const added = addRows(rows, true);
        currentLimit=limit; currentOffset=offset;
        $('limit').value = currentLimit; $('offset').value = currentOffset;
        updatePageDownloads();
        setStatus(`Loaded page: rows=${rows.length} added=${added}`);
      }catch(e){ setStatus('Load error: '+e.message); err(e); }
      finally{ showSpin(false); }
    }
    async function appendOlder(){
      const nextOffset = currentOffset + currentLimit;
      setStatus('Appending…'); showSpin(true);
      try{
        const rows = await fetchPage(currentLimit, nextOffset);
        const added = addRows(rows, false);
        currentOffset = nextOffset; $('offset').value = currentOffset;
        updatePageDownloads();
        setStatus(`Appended: +${rows.length} (added=${added})`);
      }catch(e){ setStatus('Append error: '+e.message); err(e); }
      finally{ showSpin(false); }
    }

    // Day mode
    async function refreshDayIndex(selectNewestIfEmpty=true){
      const qp = new URLSearchParams({project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla}).toString();
      const j  = await fetchJSON('/api/day-index?'+qp);
      const days = (j.days||[]).map(x=>x.day).sort();
      const sel = $('daySelect'); const prev = sel.value;
      sel.innerHTML="";
      for(const d of days){ const o=document.createElement('option'); o.value=d; o.textContent=d; sel.appendChild(o); }
      if(prev && days.includes(prev)) sel.value=prev;
      else if(selectNewestIfEmpty && days.length) sel.value=days[days.length-1];
      return { selected: sel.value || null, days, j };
    }
    async function loadDay(day, replace=true){
      if(!day) return;
      setStatus('Loading day '+day+' …'); showSpin(true);
      try{
        const qp = new URLSearchParams({mode:'day', day:day, project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla}).toString();
        const j = await fetchJSON('/api/data?'+qp);
        if(replace) clearLayers();
        const added = addRows(j.rows||[], replace);
        updateDayDownloads(day);
        setStatus(`Day ${day}: rows=${(j.rows||[]).length} added=${added}`);
      }catch(e){ setStatus('Day load error: '+e.message); err(e); }
      finally{ showSpin(false); }
    }

    // Wire UI
    $('btnLoad').addEventListener('click', ()=>{ const Lm=Math.max(50, Number($('limit').value)||CFG.default_limit); const Of=Math.max(0, Number($('offset').value)||CFG.default_offset); loadReplace(Lm, Of); });
    $('btnOlderAppend').addEventListener('click', appendOlder);
    $('btnOlderReplace').addEventListener('click', ()=>{ const Lm=Math.max(50, Number($('limit').value)||CFG.default_limit); loadReplace(Lm, currentOffset+Lm); });
    $('btnNewerReplace').addEventListener('click', ()=>{ const Lm=Math.max(50, Number($('limit').value)||CFG.default_limit); loadReplace(Lm, Math.max(0, currentOffset-Lm)); });
    $('btnReset').addEventListener('click', ()=>loadReplace(currentLimit, 0));
    $('btnLoadDay').addEventListener('click', ()=>loadDay($('daySelect').value, true));
    $('btnPrevDay').addEventListener('click', ()=>{ const sel=$('daySelect'); const days=[...sel.options].map(o=>o.value); const i=days.indexOf(sel.value); if(i>0){ sel.value=days[i-1]; loadDay(sel.value,true);} });
    $('btnNextDay').addEventListener('click', ()=>{ const sel=$('daySelect'); const days=[...sel.options].map(o=>o.value); const i=days.indexOf(sel.value); if(i>=0 && i<days.length-1){ sel.value=days[i+1]; loadDay(sel.value,true);} });

    // Poll admin status (pages/offset/finished) and refresh day index while crece
    async function pollAdmin(){
      try{
        const qp = new URLSearchParams({project_id:CFG.project_id, device_code:CFG.device_code, tabla:CFG.tabla}).toString();
        const j  = await fetchJSON('/admin/prefetch/status?'+qp);
        const c  = j.cursor||{};
        const last = c.last_ok_ts ? new Date(c.last_ok_ts*1000).toLocaleTimeString() : "-";
        $('cstat').textContent = `Collector pages=${c.pages||0}, offset=${c.offset||0}, finished=${!!c.finished}, last=${last}`;
        $('cerr').textContent  = c.last_error ? `Error: ${c.last_error}` : '';
        // si aún no hay días en UI y el colector avanza, intenta recargar day-index
        const sel = $('daySelect');
        if(!sel.value){
          const di = await refreshDayIndex(true);
          if(di && di.selected) loadDay(di.selected, true);
        }
      }catch(e){ $('cerr').textContent = 'Admin poll error: '+e.message; }
    }
    setInterval(pollAdmin, 5000);

    // Primer arranque: mostrar spinner y esperar día más reciente
    (async ()=>{
      showSpin(true); setStatus('Waiting for data…');
      const di = await refreshDayIndex(true);
      if (di && di.selected) await loadDay(di.selected, true);
      updatePageDownloads();
      showSpin(false);
    })();
  }

  // Esperar a que Folium cree el mapa
  (function waitMap(){
    const want = CFG.map_var_name; const start = performance.now();
    (function poll(){
      const m = window[want];
      if(m && m instanceof L.Map){ boot(m); return; }
      if(performance.now()-start > 8000){ console.error("[HIRI] Map var not found:", want); return; }
      setTimeout(poll, 50);
    })();
  })();
})();
    """))

    return Response(fmap.get_root().render(), mimetype="text/html")

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})

# =========================
# ========= MAIN ==========
# =========================

if __name__ == "__main__":
    # Start collector on boot so the server keeps collecting even without clients
    start_collector(DEFAULT_PROJECT_ID, DEFAULT_DEVICE_CODE, DEFAULT_TABLA, DEFAULT_LIMIT)
    # Dev server
    app.run(host="127.0.0.1", port=5000, debug=True)
