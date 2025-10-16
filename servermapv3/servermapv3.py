# -*- coding: utf-8 -*-
"""
HIRI map server (V3 - Refactored) with separated HTML/CSS/JS files.

Features
- /map: interactive Leaflet/Folium map + resizable control panel
- /api/data: day cache and page data endpoints
- /api/day-index: list of cached days + collector status
- /download/<raw|plotted>.<csv|xlsx>: exports current page/day
- /admin/reindex: start/restart background collector
- /admin/purge: purge cache
- /admin/logs: collector logs
- /healthz

New in V3:
- HTML template in index.html
- CSS in styles.css
- JavaScript in codigomapa.js
- Python handles only backend logic and serves static files
"""

import os
import io
import json
import math
import time
import shutil
import threading
import re
import unicodedata

from collections import deque, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from flask import Flask, request, Response, send_file, redirect, url_for, jsonify, render_template_string
from flask_socketio import SocketIO, emit

import folium
from folium.plugins import Fullscreen, MiniMap, HeatMap
from branca.colormap import LinearColormap
from branca.element import Element

# =========================
# ======== CONFIG =========
# =========================

UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2"

DEFAULT_TABLA = "datos"
DEFAULT_PROJECT_ID = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"

DEFAULT_LIMIT = 500
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_READ_TIMEOUT = 60
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.5

MAX_PAGES_SAFE = 500
HEAD_POLL_SECONDS = 30

# Schema
KEY_TIME = "fecha"
KEY_DEVICE_CODE = "codigo_interno"
KEY_PM25 = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
KEY_PM1 = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10 = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM = "PMS5003 [Humedad (%)]"
KEY_TEMP = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT = "Divisor de Voltaje [Voltaje (V)]"

KEY_SIM_LAT = "SIM7600G [Latitud (°)]"
KEY_SIM_LON = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"
KEY_META_LAT = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON = "Metadatos Estacion [Longitud (°)]"
KEY_NUM_ENV = "Metadatos Estacion [Numero de envios (Numeral)]"

# PM2.5 palette
PM_BREAKS = [0, 12, 35, 55, 150, 250]
PM_COLORS = ["#2ecc71", "#a3d977", "#f1c40f", "#e67e22", "#e74c3c", "#7f1d1d"]
COLORBAR_CAPTION = "PM2.5 (µg/m³)"

# Storage
CACHE_ROOT = os.path.abspath("./cache")

# HTTP headers
DEFAULT_HEADERS = {"User-Agent": "HIRIMap/1.1 (requests)"}

# =========================
# ====== APP SETUP ========
# =========================

app = Flask(__name__, static_folder='servermapv3', static_url_path='/static')
app.config['SECRET_KEY'] = 'hiripro_websocket_secret_2024'
socketio = SocketIO(app, cors_allowed_origins="*", logger=True, engineio_logger=True)

# Runtime state
Logs = deque(maxlen=2000)

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    Logs.append(line)

# Per-device structures
Days: Dict[Tuple[str,str,str], List[str]] = defaultdict(list)
DayRows: Dict[Tuple[str,str,str], Dict[str, List[Dict[str,Any]]]] = defaultdict(lambda: defaultdict(list))
DayFP: Dict[Tuple[str,str,str], Dict[str, set]] = defaultdict(lambda: defaultdict(set))
Cursor: Dict[Tuple[str,str,str], Dict[str, Any]] = defaultdict(dict)
CollectorThreads: Dict[Tuple[str,str,str], Dict[str, Any]] = {}

# =========================
# ====== UTILITIES ========
# =========================

def make_session(retries=DEFAULT_RETRIES, backoff=DEFAULT_BACKOFF) -> requests.Session:
    s = requests.Session()
    r = Retry(
        total=int(retries), connect=int(retries), read=int(retries), status=int(retries),
        backoff_factor=float(backoff),
        status_forcelist=[429,500,502,503,504],
        allowed_methods={"GET"},
        raise_on_status=False
    )
    a = HTTPAdapter(max_retries=r)
    s.mount("https://", a); s.mount("http://", a)
    s.headers.update(DEFAULT_HEADERS)
    return s

def to_float(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        if isinstance(x, (int,float)):
            return float(x) if not (isinstance(x,float) and math.isnan(x)) else None
        s = str(x).strip().replace(",", ".")
        if s == "" or s.lower() in {"nan","null","none"}: return None
        for tok in ["µg/m³","ug/m3","km/h","V","%","°C"]:
            s = s.replace(tok,"").strip()
        return float(s)
    except Exception:
        return None

def choose_coords(row: Dict[str,Any]) -> Tuple[Optional[float], Optional[float]]:
    lat = to_float(row.get(KEY_SIM_LAT)); lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None: return lat, lon
    lat = to_float(row.get(KEY_META_LAT)); lon = to_float(row.get(KEY_META_LON))
    return lat, lon

def build_upstream_url(project_id: str, device_code: str, tabla: str, limite: int, offset: int) -> str:
    return (
        f"{UPSTREAM_BASE}?tabla={tabla}"
        f"&disp.id_proyecto={project_id}"
        f"{f'&disp.codigo_interno={device_code}' if device_code else ''}"
        f"&limite={int(limite)}&offset={int(offset)}"
    )

def extract_rows(payload: Dict[str,Any]) -> List[Dict[str,Any]]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    rows = data.get("tableData", [])
    return [r for r in rows if isinstance(r, dict)]

def is_no_records_payload(payload: dict) -> bool:
    if not isinstance(payload, dict): return False
    if str(payload.get("status","")).lower() != "fail": return False
    msg = (payload.get("error") or payload.get("message") or "").lower()
    return "no hay registros" in msg

# ---- Cache helpers ----

def key_tuple(project_id: str, device_code: str, tabla: str) -> Tuple[str,str,str]:
    return (str(project_id), str(device_code), str(tabla))

def cache_dir(key: Tuple[str,str,str]) -> str:
    p, d, t = key
    path = os.path.join(CACHE_ROOT, f"{p}_{d}_{t}")
    os.makedirs(path, exist_ok=True)
    return path

def day_from_time(ts: str) -> Optional[str]:
    if not ts: return None
    try:
        return str(ts)[:10]
    except Exception:
        return None

def ensure_structs(key: Tuple[str,str,str]) -> None:
    _ = cache_dir(key)
    if key not in Cursor:
        Cursor[key] = {"offset": 0, "pages": 0, "finished": False, "last_ok_ts": None, "last_error": None, "last_url": ""}

def load_day_from_disk(key: Tuple[str,str,str], day: str) -> None:
    ensure_structs(key)
    if day in DayRows[key]:
        return
    path = os.path.join(cache_dir(key), f"{day}.jsonl")
    rows, fps = [], set()
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    fp = f"{r.get('time','')}|{r.get('envio_n','')}"
                    if fp in fps:
                        continue
                    fps.add(fp)
                    rows.append(r)
                except Exception:
                    continue
    DayRows[key][day] = rows
    DayFP[key][day] = fps
    if day not in Days[key]:
        Days[key].append(day)
        Days[key] = sorted(Days[key])

def add_to_day_cache(key: Tuple[str,str,str], plotted: List[Dict[str,Any]]) -> Dict[str,int]:
    ensure_structs(key)
    added_per_day: Dict[str,int] = defaultdict(int)

    for r in plotted:
        d = day_from_time(r.get("time"))
        if not d:
            continue
        load_day_from_disk(key, d)
        fp = f"{r.get('time','')}|{r.get('envio_n','')}"
        if fp in DayFP[key][d]:
            continue
        DayFP[key][d].add(fp)
        DayRows[key][d].append(r)
        added_per_day[d] += 1

    for d, n in added_per_day.items():
        if n <= 0: continue
        path = os.path.join(cache_dir(key), f"{d}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            for r in DayRows[key][d][-n:]:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    if added_per_day:
        Days[key] = sorted(set(Days[key]) | set(added_per_day.keys()))
    return added_per_day

def process_raw_to_plotted(raw_rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out = []
    for row in raw_rows:
        lat, lon = choose_coords(row)
        pm25 = to_float(row.get(KEY_PM25))
        if lat is None or lon is None or pm25 is None:
            continue
        out.append({
            "device_code": row.get(KEY_DEVICE_CODE),
            "time": row.get(KEY_TIME),
            "envio_n": row.get(KEY_NUM_ENV),
            "lat": lat, "lon": lon, "pm25": pm25,
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

# =========================
# ===== CSV MAP GENERATOR =
# =========================

def clamp_pm25(v: float) -> float:
    """Clamp PM2.5 to the configured colormap domain to avoid out-of-range artifacts."""
    return max(PM_BREAKS[0], min(PM_BREAKS[-1], float(v)))

def build_popup_from_plotted(row: Dict[str, Any], lat: float, lon: float, pm25_val: float) -> str:
    """HTML popup content for plotted data."""
    def safe_val(v: Any) -> str:
        return "-" if v in (None, "", "null") else str(v)

    return (
        f"<b>Dispositivo:</b> {safe_val(row.get('device_code'))}<br>"
        f"<b>PM2.5:</b> {pm25_val:.1f} µg/m³<br>"
        f"<b>Time:</b> {safe_val(row.get('time'))}<br>"
        f"<b>Envíos #:</b> {safe_val(row.get('envio_n'))}<br>"
        f"<b>Lat:</b> {lat:.6f}, <b>Lon:</b> {lon:.6f}<br>"
        f"<hr style='margin:4px 0'/>"
        f"<b>PM1:</b> {safe_val(row.get('pm1'))} | "
        f"<b>PM10:</b> {safe_val(row.get('pm10'))}<br>"
        f"<b>Temp PMS:</b> {safe_val(row.get('temp_pms'))} °C | "
        f"<b>Hum:</b> {safe_val(row.get('hum'))} %<br>"
        f"<b>VBat:</b> {safe_val(row.get('vbat'))} V<br>"
        f"<b>CSQ:</b> {safe_val(row.get('csq'))} | "
        f"<b>Sats:</b> {safe_val(row.get('sats'))} | "
        f"<b>Speed:</b> {safe_val(row.get('speed_kmh'))} km/h"
    )

def gradient_from_cmap(cm: LinearColormap, steps: int = 256) -> dict:
    """Build a 0..1 gradient dict for Leaflet.Heat from the same colormap."""
    return {i/(steps-1): cm(cm.vmin + (cm.vmax-cm.vmin)*i/(steps-1))
            for i in range(steps)}

def generate_html_map_from_csv_data(plotted_records: List[Dict[str, Any]], title: str = "HIRI PM2.5 Map") -> str:
    """Generate a complete HTML map from plotted CSV data."""
    if not plotted_records:
        raise ValueError("No valid data points to plot")

    df = pd.DataFrame(plotted_records)
    
    # Color map: one scale for points, legend, and heatmap
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    heat_gradient = gradient_from_cmap(cmap, steps=8)

    # Create Folium map
    fmap = folium.Map(
        location=[df["lat"].iloc[0], df["lon"].iloc[0]], 
        zoom_start=16, 
        control_scale=True
    )

    # Points layer
    fg_points = folium.FeatureGroup(name="PM2.5 points", overlay=True, control=True)
    for _, r in df.iterrows():
        val = clamp_pm25(float(r["pm25"]))
        color = cmap(val)

        popup_html = build_popup_from_plotted(
            r.to_dict(), float(r["lat"]), float(r["lon"]), float(r["pm25"])
        )

        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=6,
            popup=folium.Popup(popup_html, max_width=360),
            color=color,
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=0.85,
        ).add_to(fg_points)

    fg_points.add_to(fmap)

    # HeatMap layer
    HeatMap(
        df.assign(pm25=df["pm25"].apply(clamp_pm25))[["lat", "lon", "pm25"]].values.tolist(),
        name="HeatMap PM2.5",
        min_opacity=0.30,
        radius=12,
        blur=22,
        max_zoom=18,
        gradient=heat_gradient,
    ).add_to(fmap)

    # Legend + controls
    cmap.add_to(fmap)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    # Fit to all points (auto-zoom)
    sw = [float(df["lat"].min()), float(df["lon"].min())]
    ne = [float(df["lat"].max()), float(df["lon"].max())]
    fmap.fit_bounds([sw, ne], padding=(20, 20))

    # Add title
    title_html = f"""
    <div style="position: fixed; top: 10px; left: 50%; transform: translateX(-50%); z-index: 9999;
                background: rgba(255,255,255,0.9); padding: 8px 16px; border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-family: system-ui, sans-serif;">
        <h3 style="margin: 0; color: #333;">{title}</h3>
        <p style="margin: 2px 0 0 0; font-size: 12px; color: #666;">
            {len(df)} puntos de datos - Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
    </div>
    """
    Element(title_html).add_to(fmap)

    return fmap.get_root().render()

# =========================
# ===== COLLECTOR =========
# =========================

def collector_loop(key: Tuple[str,str,str], limit: int,
                   connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                   read_timeout=DEFAULT_READ_TIMEOUT,
                   verify_tls=True):
    p, d, t = key
    ensure_structs(key)
    session = make_session()
    stop = CollectorThreads[key]["stop"]

    while not stop.is_set():
        cur = Cursor[key]
        try:
            if not cur.get("finished", False):
                offset = int(cur.get("offset", 0))
                url = build_upstream_url(p, d, t, limit, offset)
                resp = session.get(url, timeout=(connect_timeout, read_timeout), verify=verify_tls, stream=False)

                payload = {}
                try:
                    payload = resp.json()
                except Exception:
                    pass

                if resp.status_code == 400 and is_no_records_payload(payload):
                    cur["finished"] = True
                    cur["last_ok_ts"] = time.time()
                    cur["last_error"] = None
                    cur["last_url"] = url
                    log(f"[collector] end (no records) {key}")
                    time.sleep(HEAD_POLL_SECONDS)
                    continue

                resp.raise_for_status()
                if not payload:
                    payload = resp.json()

                raw_rows = extract_rows(payload)
                n = len(raw_rows)
                plotted = process_raw_to_plotted(raw_rows)
                added = add_to_day_cache(key, plotted)

                cur["offset"] = offset + n
                cur["pages"] = int(cur.get("pages", 0)) + 1
                cur["finished"] = (n < limit)
                cur["last_ok_ts"] = time.time()
                cur["last_error"] = None
                cur["last_url"] = url
                log(f"[collector] page#{cur['pages']} offset={offset} got={n} plotted+={sum(added.values())} days+={list(added.keys())}")
                time.sleep(0.2 if not cur["finished"] else HEAD_POLL_SECONDS)
                continue

            # Head polling
            url = build_upstream_url(p, d, t, limit, 0)
            resp = session.get(url, timeout=(connect_timeout, read_timeout), verify=verify_tls, stream=False)
            payload = {}
            try:
                payload = resp.json()
            except Exception:
                resp.raise_for_status()
            if resp.status_code == 400 and is_no_records_payload(payload):
                time.sleep(HEAD_POLL_SECONDS)
                continue

            resp.raise_for_status()
            raw_rows = extract_rows(payload)
            plotted = process_raw_to_plotted(raw_rows)
            added = add_to_day_cache(key, plotted)
            if sum(added.values()) > 0:
                log(f"[collector] head append +{sum(added.values())} rows days+={list(added.keys())}")
                try:
                    socketio.emit('new_data', {
                        'key': {'project_id': p, 'device_code': d, 'tabla': t},
                        'rows': plotted,
                        'count': sum(added.values()),
                        'days': list(added.keys())
                    }, namespace='/')
                except Exception as e:
                    log(f"[websocket] Error emitting: {e}")
            time.sleep(HEAD_POLL_SECONDS)

        except requests.exceptions.RequestException as e:
            Cursor[key]["last_error"] = f"{type(e).__name__}: {e}"
            log(f"[collector] error {Cursor[key]['last_error']}; sleep 5s")
            time.sleep(5.0)

def start_collector(project_id: str, device_code: str, tabla: str, limit: int, reset=False):
    key = key_tuple(project_id, device_code, tabla)
    ensure_structs(key)
    if reset:
        purge_cache(project_id, device_code, tabla, keep_structs=True)
    if key in CollectorThreads and CollectorThreads[key]["thread"].is_alive():
        return
    stop_evt = threading.Event()
    th = threading.Thread(target=collector_loop, args=(key, int(limit)), daemon=True)
    CollectorThreads[key] = {"thread": th, "stop": stop_evt}
    th.start()
    log(f"[collector] started {key} with limit={limit}")

def stop_collector(project_id: str, device_code: str, tabla: str):
    key = key_tuple(project_id, device_code, tabla)
    info = CollectorThreads.get(key)
    if not info: return
    info["stop"].set()
    log(f"[collector] stop requested {key}")

def purge_cache(project_id: str, device_code: str, tabla: str, keep_structs=False):
    key = key_tuple(project_id, device_code, tabla)
    stop_collector(project_id, device_code, tabla)
    if not keep_structs:
        Days.pop(key, None)
        DayRows.pop(key, None)
        DayFP.pop(key, None)
        Cursor.pop(key, None)
    else:
        Days[key].clear()
        DayRows[key].clear()
        DayFP[key].clear()
        Cursor[key] = {"offset": 0, "pages": 0, "finished": False, "last_ok_ts": None, "last_error": None, "last_url": ""}

    folder = cache_dir(key)
    try:
        shutil.rmtree(folder)
    except Exception:
        pass
    os.makedirs(folder, exist_ok=True)
    log(f"[admin] purged cache {key}")

def scan_and_load_all_devices(project_id: str, tabla: str) -> List[str]:
    """Scan cache directory and load all days for all devices found."""
    if not os.path.exists(CACHE_ROOT):
        return []

    prefix = f"{project_id}_"
    suffix = f"_{tabla}"
    devices_found = []

    for dirname in os.listdir(CACHE_ROOT):
        if dirname.startswith(prefix) and dirname.endswith(suffix):
            device = dirname[len(prefix):-len(suffix)]
            if not device:  # Skip if device_code is empty
                continue

            devices_found.append(device)
            key = key_tuple(project_id, device, tabla)
            ensure_structs(key)
            folder = cache_dir(key)

            # Load all days from disk for this device
            days_loaded = []
            if os.path.exists(folder):
                for name in os.listdir(folder):
                    if name.endswith(".jsonl") and len(name) >= 10:
                        day = name[:10]
                        if day not in Days[key]:
                            Days[key].append(day)
                            days_loaded.append(day)
                        # Pre-load the day data into memory
                        load_day_from_disk(key, day)

                Days[key] = sorted(Days[key])
                if days_loaded:
                    log(f"[startup] Loaded {len(days_loaded)} days for device {device}: {sorted(days_loaded)}")

    if devices_found:
        log(f"[startup] Found and loaded {len(devices_found)} devices: {devices_found}")

    return devices_found

def start_collectors_for_all_devices(project_id: str, tabla: str, limit: int):
    """Start collectors for all known devices."""
    devices = scan_and_load_all_devices(project_id, tabla)
    for device in devices:
        start_collector(project_id, device, tabla, limit, reset=False)
    return devices

# =========================
# ====== FLASK ROUTES =====
# =========================

@app.route("/")
def index():
    return redirect(url_for("map_view"))

@app.route("/map")
def map_view():
    from branca.element import Element

    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", "")  # Empty by default to show all devices
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    # Only start collector if device_code is specified
    if device_code:
        start_collector(project_id, device_code, tabla, DEFAULT_LIMIT, reset=False)

    # Create Folium map with plugins
    fmap = folium.Map(location=[-33.45, -70.65], zoom_start=12, control_scale=True, prefer_canvas=True)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)

    # Add colormap legend
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    cmap.add_to(fmap)

    # Configuration object for JavaScript
    cfg = {
        "project_id": project_id,
        "device_code": device_code,
        "tabla": tabla,
        "palette": {"breaks": PM_BREAKS, "colors": PM_COLORS},
        "exports_base": "/download",
    }

    # Add external CSS
    fmap.get_root().html.add_child(Element('<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.3/MarkerCluster.css" />'))
    fmap.get_root().html.add_child(Element('<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.3/MarkerCluster.Default.css" />'))
    fmap.get_root().html.add_child(Element('<link rel="stylesheet" href="/static/styles.css">'))

    # Control panel HTML
    header_html = f"""
    <div id="controls">
      <div style="font-weight:700;">HIRI Map</div>

      <label>Project:
        <input type="text" id="project_id" value="{project_id}" style="width:70px;margin-left:4px;">
      </label>

      <label>Device:
        <input id="device_code" list="deviceList" value="{device_code}" style="width:120px;margin-left:4px;">
        <datalist id="deviceList">
          <option value="HIRIPRO-01">
          <option value="HIRIPRO-02">
          <option value="HIRIPRO-03">
          <option value="HIRIPRO-04">
          <option value="HIRIPRO-05">
        </datalist>
      </label>

      <label>Tabla:
        <input type="text" id="tabla" value="{tabla}" style="width:80px;margin-left:4px;">
      </label>

      <button id="btnApply">Apply</button>
      <button id="btnCollapse" title="Collapse/expand panel">⇕</button>

      <span style="flex-basis:100%; height:0;"></span>

      <label>Limit:
        <input type="number" id="limit" value="{DEFAULT_LIMIT}" min="50" max="2000" step="50" style="width:80px;margin-left:6px;">
      </label>
      <label>Offset:
        <input type="number" id="offset" value="0" min="0" step="10" style="width:90px;margin-left:6px;">
      </label>
      <button id="btnLoad">Load</button>
      <button id="btnOlderAppend">Append older</button>
      <button id="btnOlder">Older</button>
      <button id="btnNewer">Newer</button>
      <button id="btnReset">Reset</button>

      <span style="flex-basis:100%; height:0;"></span>

      <label>Day:
        <select id="daySelect" style="min-width:140px;"></select>
      </label>
      <button id="btnLoadDay">Load day</button>
      <button id="btnPrevDay">◀ Prev</button>
      <button id="btnNextDay">Next ▶</button>
      <label style="margin-left:8px;">
        <input type="checkbox" id="chkLive"> Live
      </label>
      <button id="btnRefreshDays">Refresh days</button>

      <span style="flex-basis:100%; height:0;"></span>

      <button id="btnAdminReindex" title="Rebuild cache in background">Admin: Reindex</button>
      <button id="btnAdminPurge" title="Purge on-disk/in-memory cache">Admin: Purge cache</button>
      <button id="btnToggleLogs" title="Show/hide logs">Logs</button>

      <div style="margin-left:10px; display:flex; align-items:center; gap:8px;">
        <div id="connectionStatus" style="width:12px; height:12px; border-radius:50%; background:#6b7280;" title="Estado de conexión"></div>
        <span id="status" style="color:#333;">Ready.</span>
        <span id="dataCount" style="font-size:12px; color:#666;"></span>
      </div>

      <div id="logs" style="display:none; flex-basis:100%; max-height:220px; overflow:auto; background:#fafafa; border:1px solid #ddd; padding:6px; border-radius:6px;"></div>
    </div>
    """
    fmap.get_root().html.add_child(Element(header_html))

    # Downloads toolbar
    toolbar_html = f"""
    <div id="dlbar">
      <b>Downloads</b><br>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Page mode:</span>
        <a id="dl-raw-csv" href="#">CSV</a> |
        <a id="dl-raw-xlsx" href="#">Excel</a> |
        <a id="dl-plot-csv" href="#">Plotted CSV</a> |
        <a id="dl-plot-xlsx" href="#">Plotted Excel</a>
      </div>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Day exports:</span>
        <a id="dl-day-csv" href="#">CSV</a> |
        <a id="dl-day-xlsx" href="#">Excel</a>
      </div>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">CSV to Map:</span>
        <input type="file" id="csvFileInput" accept=".csv" style="display:none;">
        <button id="btnUploadCSV" style="font-size:12px; padding:4px 8px;">Cargar CSV</button>
        <br><small style="color:#666;">Genera mapa HTML descargable</small>
      </div>
    </div>
    """
    fmap.get_root().html.add_child(Element(toolbar_html))

    # Add scripts
    fmap.get_root().html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>'))
    fmap.get_root().html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.markercluster/1.5.3/leaflet.markercluster.js"></script>'))
    fmap.get_root().html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.2/socket.io.js"></script>'))

    # Add configuration
    script_head = "<script>const CFG = " + json.dumps(cfg) + ";</script>"
    fmap.get_root().html.add_child(Element(script_head))

    # Add main JS
    with open(os.path.join(os.path.dirname(__file__), 'codigomapa.js'), 'r', encoding='utf-8') as f:
        js_code = f.read()
    fmap.get_root().html.add_child(Element(f'<script>{js_code}</script>'))

    html = fmap.get_root().render()
    return Response(html, mimetype="text/html")

# ---- Data APIs ----

@app.route("/api/day-index")
def api_day_index():
    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    t = request.args.get("tabla", DEFAULT_TABLA)
    d = request.args.get("device_code")

    if not d:
        all_days = set()
        last_cursor = {}
        prefix = f"{p}_"
        suffix = f"_{t}"
        for dirname in os.listdir(CACHE_ROOT):
            if dirname.startswith(prefix) and dirname.endswith(suffix):
                device = dirname[len(prefix):-len(suffix)]
                key = key_tuple(p, device, t)
                ensure_structs(key)
                folder = cache_dir(key)
                log(f"[day-index] Scanning folder (no device): {folder}")
                for name in os.listdir(folder):
                    if name.endswith(".jsonl") and len(name) >= 10:
                        day = name[:10]
                        if day not in Days[key]:
                            Days[key].append(day)
                        all_days.add(day)
                Days[key] = sorted(Days[key])
                last_cursor = Cursor.get(key, {})
        log(f"[day-index] Total days (all devices): {len(all_days)} - {sorted(all_days)}")
        return jsonify({
            "days": sorted(all_days),
            "cursor": last_cursor
        })
    else:
        key = key_tuple(p, d, t)
        ensure_structs(key)
        folder = cache_dir(key)
        log(f"[day-index] Scanning folder for device {d}: {folder}")
        days_found = []
        for name in os.listdir(folder):
            if name.endswith(".jsonl") and len(name) >= 10:
                day = name[:10]
                days_found.append(day)
                if day not in Days[key]:
                    Days[key].append(day)
        Days[key] = sorted(Days[key])
        cur = Cursor.get(key, {})
        log(f"[day-index] Found {len(days_found)} days on disk: {sorted(days_found)}, returning {len(Days[key])} days: {Days[key]}")
        return jsonify({"days": Days[key], "cursor": cur})

@app.route("/api/data")
def api_data():
    mode = request.args.get("mode")
    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    d = request.args.get("device_code")
    t = request.args.get("tabla", DEFAULT_TABLA)

    key = key_tuple(p, d, t)

    if mode == "day":
        day = request.args.get("day")
        since = request.args.get("since")
        if not day:
            return jsonify({"status":"fail","error":"day required"}), 400

        def to_epoch(s: str) -> float:
            try:
                if not s: return 0.0
                if s.isdigit():
                    return float(s)
                return datetime.fromisoformat(s.replace("Z","")).timestamp()
            except Exception:
                return 0.0

        rows: List[Dict[str,Any]] = []
        if not d:
            prefix = f"{p}_"
            suffix = f"_{t}"
            for dirname in os.listdir(CACHE_ROOT):
                if dirname.startswith(prefix) and dirname.endswith(suffix):
                    device = dirname[len(prefix):-len(suffix)]
                    dkey = key_tuple(p, device, t)
                    load_day_from_disk(dkey, day)
                    day_rows = DayRows[dkey].get(day, [])
                    for r in day_rows:
                        if "device_code" not in r:
                            r = dict(r)
                            r["device_code"] = device
                        rows.append(r)
        else:
            load_day_from_disk(key, day)
            rows = DayRows[key].get(day, [])

        if since:
            th = to_epoch(since)
            filtered = []
            for r in rows:
                ts = r.get("time")
                if not ts:
                    continue
                try:
                    te = datetime.fromisoformat(str(ts).replace("Z","")).timestamp()
                except Exception:
                    continue
                if te > th:
                    filtered.append(r)
            rows = filtered

        try:
            rows.sort(key=lambda x: x.get("time") or "")
        except Exception:
            pass
        return jsonify({"status":"success","type":"plotted","rows":rows, "aggregated": (not d), "day": day, "since": since})

    # Page mode
    limite = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", 0))
    url = build_upstream_url(p,d,t,limite,offset)
    s = make_session()
    try:
        r = s.get(url, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), verify=True, stream=False)
        payload = {}
        try:
            payload = r.json()
        except Exception:
            pass
        if r.status_code == 400 and is_no_records_payload(payload):
            return jsonify({"status":"success","type":"plotted","rows":[],"meta":{"note":"no records"}})
        r.raise_for_status()
        if not payload:
            payload = r.json()
        raw = extract_rows(payload)
        plotted = process_raw_to_plotted(raw)
        return jsonify({"status":"success","type":"plotted","rows":plotted})
    except requests.exceptions.RequestException as e:
        return jsonify({"status":"fail","error":f"{type(e).__name__}: {e}", "url":url}), 502

# ---- Downloads ----

def df_from_rows(rows: List[Dict[str,Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows)

@app.route("/download/<kind>.<ext>")
def download(kind: str, ext: str):
    if kind not in {"raw","plotted"} or ext not in {"csv","xlsx"}:
        return Response("Bad request.", status=400)

    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    d = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    t = request.args.get("tabla", DEFAULT_TABLA)
    limite = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", 0))
    paginate = request.args.get("paginate","0") == "1";

    rows_all: List[Dict[str,Any]] = []
    s = make_session()
    pages = 0
    cur_offset = offset
    while True:
        url = build_upstream_url(p,d,t,limite,cur_offset)
        r = s.get(url, timeout=(DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT), verify=True, stream=False)
        payload = {}
        try:
            payload = r.json()
        except Exception:
            pass
        if r.status_code == 400 and is_no_records_payload(payload):
            break
        r.raise_for_status()
        if not payload:
            payload = r.json()
        raw = extract_rows(payload)
        if kind == "raw":
            rows_all.extend(raw)
        else:
            rows_all.extend(process_raw_to_plotted(raw))
        pages += 1
        if not paginate or len(raw) < limite or pages >= MAX_PAGES_SAFE:
            break
        cur_offset += limite

    df = df_from_rows(rows_all)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S");
    fname = f"{kind}_{ts}.{ext}";

    if ext == "csv":
        bio = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        return send_file(bio, as_attachment=True, download_name=fname, mimetype="text/csv; charset=utf-8")
    else:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        bio.seek(0)
        return send_file(bio, as_attachment=True, download_name=fname,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---- Admin ----

@app.route("/admin/reindex")
def admin_reindex():
    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    d = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    t = request.args.get("tabla", DEFAULT_TABLA)
    limit = int(request.args.get("limit", DEFAULT_LIMIT))
    reset = request.args.get("reset","0") == "1"
    start_collector(p,d,t,limit,reset=reset)
    return jsonify({"ok": True, "message": f"collector started for {(p,d,t)} reset={reset}, limit={limit}"})

@app.route("/admin/purge")
def admin_purge():
    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    d = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    t = request.args.get("tabla", DEFAULT_TABLA)
    purge_cache(p,d,t, keep_structs=False)
    return jsonify({"ok": True, "message": f"purged cache for {(p,d,t)}"})

@app.route("/admin/logs")
def admin_logs():
    tail = int(request.args.get("tail", 200))
    lines = list(Logs)[-tail:]
    return jsonify({"lines": lines})

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})

# ---- CSV Upload and Map Generation ----

@app.route("/upload-csv", methods=["POST"])
def upload_csv():
    """Handle CSV file upload and return processed data info."""
    try:
        if 'csvfile' not in request.files:
            return jsonify({"status": "error", "message": "No file uploaded"}), 400
        
        file = request.files['csvfile']
        if file.filename == '':
            return jsonify({"status": "error", "message": "No file selected"}), 400
        
        if not file.filename.lower().endswith('.csv'):
            return jsonify({"status": "error", "message": "File must be a CSV"}), 400

        # Read CSV
        try:
            df = pd.read_csv(file)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Error reading CSV: {str(e)}"}), 400

        # Validate required columns
        required_cols = ['lat', 'lon', 'pm25']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return jsonify({
                "status": "error", 
                "message": f"Missing required columns: {missing_cols}. Available: {list(df.columns)}"
            }), 400

        # Filter valid rows
        df_valid = df.dropna(subset=required_cols)
        df_valid = df_valid[(df_valid['lat'].between(-90, 90)) & 
                           (df_valid['lon'].between(-180, 180)) & 
                           (df_valid['pm25'] >= 0)]

        if len(df_valid) == 0:
            return jsonify({
                "status": "error", 
                "message": "No valid data points found (need lat, lon, pm25 with valid values)"
            }), 400

        # Store in session or temporary storage for map generation
        # For simplicity, we'll generate a unique ID and store temporarily
        import uuid
        upload_id = str(uuid.uuid4())
        
        # Store in a simple dict (in production, use Redis or database)
        if not hasattr(app, 'csv_uploads'):
            app.csv_uploads = {}
        
        app.csv_uploads[upload_id] = {
            'data': df_valid.to_dict('records'),
            'filename': file.filename,
            'uploaded_at': datetime.now(),
            'total_rows': len(df),
            'valid_rows': len(df_valid)
        }

        return jsonify({
            "status": "success",
            "upload_id": upload_id,
            "filename": file.filename,
            "total_rows": len(df),
            "valid_rows": len(df_valid),
            "columns": list(df.columns),
            "message": f"CSV processed successfully. {len(df_valid)} valid data points ready for mapping."
        })

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

@app.route("/generate-map/<upload_id>")
def generate_map_from_csv(upload_id: str):
    """Generate and download HTML map from uploaded CSV data."""
    try:
        # Check if upload exists
        if not hasattr(app, 'csv_uploads') or upload_id not in app.csv_uploads:
            return Response("Upload not found or expired", status=404)

        upload_data = app.csv_uploads[upload_id]
        plotted_records = upload_data['data']
        filename = upload_data['filename']

        # Generate HTML map
        title = f"Mapa PM2.5 - {filename}"
        html_content = generate_html_map_from_csv_data(plotted_records, title)

        # Create filename for download
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        map_filename = f"mapa_pm25_{ts}.html"

        # Clean up old uploads (keep last 10)
        if len(app.csv_uploads) > 10:
            oldest_keys = sorted(app.csv_uploads.keys(), 
                               key=lambda k: app.csv_uploads[k]['uploaded_at'])[:5]
            for old_key in oldest_keys:
                del app.csv_uploads[old_key]

        return Response(
            html_content,
            mimetype='text/html',
            headers={
                'Content-Disposition': f'attachment; filename="{map_filename}"'
            }
        )

    except Exception as e:
        log(f"[map-gen] Error generating map for {upload_id}: {e}")
        return Response(f"Error generating map: {str(e)}", status=500)

@app.route("/csv-upload-form")
def csv_upload_form():
    """Simple upload form for testing."""
    form_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>CSV to Map Generator</title>
        <style>
            body { font-family: system-ui, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
            .form-group { margin: 15px 0; }
            label { display: block; margin-bottom: 5px; font-weight: bold; }
            input[type="file"] { width: 100%; padding: 8px; }
            button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
            button:hover { background: #0056b3; }
            .info { background: #f8f9fa; padding: 15px; border-radius: 4px; margin: 15px 0; }
            .result { margin-top: 20px; padding: 15px; border-radius: 4px; }
            .success { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
            .error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
        </style>
    </head>
    <body>
        <h1>📊 Generador de Mapas desde CSV</h1>
        
        <div class="info">
            <h3>📋 Requisitos del CSV:</h3>
            <ul>
                <li><strong>Columnas obligatorias:</strong> <code>lat</code>, <code>lon</code>, <code>pm25</code></li>
                <li><strong>Columnas opcionales:</strong> <code>time</code>, <code>device_code</code>, <code>envio_n</code>, 
                    <code>pm1</code>, <code>pm10</code>, <code>temp_pms</code>, <code>hum</code>, <code>vbat</code>, 
                    <code>csq</code>, <code>sats</code>, <code>speed_kmh</code></li>
                <li><strong>Formato:</strong> Archivo .csv con encabezados</li>
                <li><strong>Datos válidos:</strong> lat (-90 a 90), lon (-180 a 180), pm25 ≥ 0</li>
            </ul>
        </div>

        <form id="csvForm" enctype="multipart/form-data">
            <div class="form-group">
                <label for="csvfile">Seleccionar archivo CSV:</label>
                <input type="file" id="csvfile" name="csvfile" accept=".csv" required>
            </div>
            <button type="submit">📤 Cargar y Generar Mapa</button>
        </form>

        <div id="result"></div>

        <script>
        document.getElementById('csvForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData();
            const fileInput = document.getElementById('csvfile');
            formData.append('csvfile', fileInput.files[0]);
            
            const resultDiv = document.getElementById('result');
            resultDiv.innerHTML = '<p>⏳ Procesando archivo...</p>';
            
            try {
                const response = await fetch('/upload-csv', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (result.status === 'success') {
                    resultDiv.innerHTML = `
                        <div class="result success">
                            <h3>✅ ¡Archivo procesado exitosamente!</h3>
                            <p><strong>Archivo:</strong> ${result.filename}</p>
                            <p><strong>Total filas:</strong> ${result.total_rows}</p>
                            <p><strong>Puntos válidos:</strong> ${result.valid_rows}</p>
                            <p><strong>Columnas:</strong> ${result.columns.join(', ')}</p>
                            <br>
                            <a href="/generate-map/${result.upload_id}" download>
                                <button>🗺️ Descargar Mapa HTML</button>
                            </a>
                        </div>
                    `;
                } else {
                    resultDiv.innerHTML = `
                        <div class="result error">
                            <h3>❌ Error</h3>
                            <p>${result.message}</p>
                        </div>
                    `;
                }
            } catch (error) {
                resultDiv.innerHTML = `
                    <div class="result error">
                        <h3>❌ Error de conexión</h3>
                        <p>${error.message}</p>
                    </div>
                `;
            }
        });
        </script>
    </body>
    </html>
    """
    return Response(form_html, mimetype='text/html')

@app.route("/csv-info")
def csv_info():
    """API endpoint with CSV format information."""
    return jsonify({
        "csv_format": {
            "required_columns": ["lat", "lon", "pm25"],
            "optional_columns": [
                "time", "device_code", "envio_n", "pm1", "pm10", 
                "temp_pms", "hum", "vbat", "csq", "sats", "speed_kmh"
            ],
            "data_types": {
                "lat": "float (-90 to 90)",
                "lon": "float (-180 to 180)", 
                "pm25": "float (>= 0)",
                "time": "string (ISO format preferred)",
                "device_code": "string",
                "envio_n": "integer",
                "pm1": "float",
                "pm10": "float",
                "temp_pms": "float",
                "hum": "float (0-100)",
                "vbat": "float",
                "csq": "integer",
                "sats": "integer",
                "speed_kmh": "float"
            },
            "example_csv": """lat,lon,pm25,time,device_code,pm1,pm10,temp_pms,hum
-33.4569,-70.6483,25.3,2025-10-16T10:30:00,HIRIPRO-01,18.2,32.1,22.5,65.2
-33.4571,-70.6485,28.7,2025-10-16T10:31:00,HIRIPRO-01,20.1,35.4,22.3,64.8"""
        }
    })

# ---- WebSocket events ----

@socketio.on('connect')
def handle_connect():
    log(f"[websocket] Client connected: {request.sid}")
    emit('status', {'message': 'Connected to HIRI live updates'})

@socketio.on('disconnect')
def handle_disconnect():
    log(f"[websocket] Client disconnected: {request.sid}")

@socketio.on('subscribe')
def handle_subscribe(data):
    project_id = data.get('project_id')
    device_code = data.get('device_code')
    tabla = data.get('tabla')
    log(f"[websocket] Client {request.sid} subscribed to {project_id}/{device_code}/{tabla}")
    emit('subscribed', {'project_id': project_id, 'device_code': device_code, 'tabla': tabla})

# =========================
# ========= MAIN ==========
# =========================

if __name__ == "__main__":
    os.makedirs(CACHE_ROOT, exist_ok=True)

    # Scan and load ALL devices found in cache, start collectors for each
    log("[startup] Scanning cache for all devices...")
    devices = start_collectors_for_all_devices(DEFAULT_PROJECT_ID, DEFAULT_TABLA, DEFAULT_LIMIT)

    if not devices:
        # If no devices found in cache, start default collector
        log(f"[startup] No devices found in cache, starting default collector for {DEFAULT_DEVICE_CODE}")
        start_collector(DEFAULT_PROJECT_ID, DEFAULT_DEVICE_CODE, DEFAULT_TABLA, DEFAULT_LIMIT, reset=False)

    socketio.run(app, host="127.0.0.1", port=5000, debug=True, allow_unsafe_werkzeug=True)
