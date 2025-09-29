# -*- coding: utf-8 -*-
"""
HIRI map server (V2 API) with day cache, background collector, admin actions and rich UI.

Features
- /map: interactive Leaflet/Folium map + resizable control panel (50% width by default)
- /api/data:
    * mode=day&day=YYYY-MM-DD                -> plotted rows of a cached day
    * mode=day&day=YYYY-MM-DD&since=...      -> only rows newer than 'since' (ISO or epoch)
    * mode=page&limite&offset                -> single upstream page (debug)
- /api/day-index: list of cached days + collector status
- /download/<raw|plotted>.<csv|xlsx>: exports current page/day
- /admin/reindex: start/restart background collector for a device (optionally reset cache)
- /admin/purge:   purge on-disk/in-memory cache for a device
- /admin/logs:    last N collector log lines
- /healthz

Notes
- Upstream: https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2
- We persist per-day JSONL files under ./cache/<key>/{YYYY-MM-DD}.jsonl
- Dedup key = (time, envio_n)
- When pagination ends (empty page OR 400 with "No hay registros"), collector switches to "head polling"
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

from flask import Flask, request, Response, send_file, redirect, url_for, jsonify
from branca.element import Element  # added for explicit HTML injection like in serverMap.py

import folium
from folium.plugins import Fullscreen, MiniMap
from branca.colormap import LinearColormap

# =========================
# ======== CONFIG =========
# =========================

UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2"

DEFAULT_TABLA = "datos"
DEFAULT_PROJECT_ID = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"

DEFAULT_LIMIT = 500           # page size for collector and manual loads
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_READ_TIMEOUT = 60
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.5

MAX_PAGES_SAFE = 500
HEAD_POLL_SECONDS = 30        # after finish, poll offset=0 periodically for new rows

# Schema
KEY_TIME    = "fecha"
KEY_DEVICE_CODE  = "codigo_interno"
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

# PM2.5 palette (legend + markers)
PM_BREAKS = [0, 12, 35, 55, 150, 250]
PM_COLORS = ["#2ecc71", "#a3d977", "#f1c40f", "#e67e22", "#e74c3c", "#7f1d1d"]
COLORBAR_CAPTION = "PM2.5 (µg/m³)"

# UI CSS
HEADER_CSS = (
    "position:fixed; top:10px; left:50px; z-index:9999;"
    "width:50vw; min-width:360px; max-width:95vw;"
    "background:rgba(255,255,255,0.98); padding:10px 12px; border-radius:10px;"
    "box-shadow:0 2px 10px rgba(0,0,0,0.18); font-family:system-ui,sans-serif; font-size:14px;"
    "display:flex; flex-wrap:wrap; gap:8px; align-items:center;"
    "resize:horizontal; overflow:auto;"
)
    # "background:rgba(0,0,0,0.95); padding:8px 10px; border-radius:8px;"
TOOLBAR_CSS = (
    "position:fixed; top:50px; right:10px; z-index:9999;"
    "background:rgba(255,255,255,0.95); padding:8px 10px; border-radius:8px;"
    "box-shadow:0 2px 8px rgba(0,0,0,0.15); font-family:system-ui,sans-serif; font-size:14px;"
)

# Storage
CACHE_ROOT = os.path.abspath("./cache")

# HTTP headers
DEFAULT_HEADERS = {"User-Agent": "HIRIMap/1.1 (requests)"}

# =========================
# ====== APP SETUP ========
# =========================

app = Flask(__name__)

# Runtime state
Logs = deque(maxlen=2000)  # small rolling log for /admin/logs

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    Logs.append(line)

# Per-device structures
# Key = (project_id, device_code, tabla)
Days: Dict[Tuple[str,str,str], List[str]] = defaultdict(list)           # sorted list of days available
DayRows: Dict[Tuple[str,str,str], Dict[str, List[Dict[str,Any]]]] = defaultdict(lambda: defaultdict(list))
DayFP: Dict[Tuple[str,str,str], Dict[str, set]] = defaultdict(lambda: defaultdict(set))  # (time|envio)
Cursor: Dict[Tuple[str,str,str], Dict[str, Any]] = defaultdict(dict)
CollectorThreads: Dict[Tuple[str,str,str], Dict[str, Any]] = {}  # {"thread":Thread, "stop":Event}

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

def clamp_pm25(v: float) -> float:
    return max(PM_BREAKS[0], min(PM_BREAKS[-1], float(v)))

def build_popup(row: Dict[str, Any], lat: float, lon: float, pm25_val: float) -> str:
    def sv(v: Any) -> str: return "-" if v in (None,"","null") else str(v)
    print(sv)
    return (
        f"<b>Dispositivo:</b> {sv(row.get(KEY_DEVICE_CODE))} µg/m³<br>"
        f"<b>PM2.5:</b> {pm25_val:.1f} µg/m³<br>"
        f"<b>Time:</b> {sv(row.get(KEY_TIME))}<br>"
        f"<b>Envíos #:</b> {sv(row.get(KEY_NUM_ENV))}<br>"
        f"<b>Lat:</b> {lat:.6f}, <b>Lon:</b> {lon:.6f}<br>"
        f"<hr style='margin:4px 0'/>"
        f"<b>PM1:</b> {sv(row.get(KEY_PM1))} | "
        f"<b>PM10:</b> {sv(row.get(KEY_PM10))}<br>"
        f"<b>Temp PMS:</b> {sv(row.get(KEY_TEMP))} °C | "
        f"<b>Hum:</b> {sv(row.get(KEY_HUM))} %<br>"
        f"<b>VBat:</b> {sv(row.get(KEY_VBAT))} V<br>"
        f"<b>CSQ:</b> {sv(row.get(KEY_SIM_CSQ))} | "
        f"<b>Sats:</b> {sv(row.get(KEY_SIM_SATS))} | "
        f"<b>Speed:</b> {sv(row.get(KEY_SIM_SPEED))} km/h"
    )

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
# ---------- Schema autodetect ----------

def _norm(s: str) -> str:
    """Normalize header names: lowercase, remove accents and units, keep alnum."""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # remove bracketed units or extra info
    s = re.sub(r"\[.*?\]", " ", s)
    s = s.replace("µ", "u")
    s = s.replace("°", "")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _find_key(candidates: List[str], *needles: str) -> Optional[str]:
    """Return first column whose normalized text contains all needles."""
    ns = [ _norm(c) for c in candidates ]
    for ci, nval in enumerate(ns):
        ok = True
        for needle in needles:
            if needle not in nval:
                ok = False; break
        if ok:
            return candidates[ci]
    return None

def detect_schema(rows: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """
    Intenta mapear todas las llaves relevantes a partir del primer registro.
    Devuelve dict con claves: time, pm25, pm1, pm10, hum, temp, vbat,
    sim_lat, sim_lon, meta_lat, meta_lon, envio, sim_csq, sim_sats, sim_speed
    """
    schema = {k: None for k in [
        "time","pm25","pm1","pm10","hum","temp","vbat",
        "sim_lat","sim_lon","meta_lat","meta_lon","envio",
        "sim_csq","sim_sats","sim_speed"
    ]}
    if not rows:
        return schema
    cols = list(rows[0].keys())

    # time / envio
    schema["time"]  = _find_key(cols, "fecha") or _find_key(cols, "time")
    schema["envio"] = _find_key(cols, "numero", "envio") or _find_key(cols, "envio")

    # pm
    schema["pm25"]  = (_find_key(cols, "pm", "2", "5") or
                      _find_key(cols, "pm2 5") or
                      _find_key(cols, "pm25"))
    schema["pm1"]   = _find_key(cols, "pm", "1") or _find_key(cols, "pm1")
    schema["pm10"]  = _find_key(cols, "pm", "10") or _find_key(cols, "pm10")

    # hum / temp / vbat
    schema["hum"]   = _find_key(cols, "humedad") or _find_key(cols, "hum")
    schema["temp"]  = _find_key(cols, "grados", "celcius") or _find_key(cols, "temperatura") or _find_key(cols, "temp")
    schema["vbat"]  = _find_key(cols, "voltaje") or _find_key(cols, "vbat") or _find_key(cols, "bateria")

    # sim7600 lat/lon (preferidos)
    schema["sim_lat"]   = _find_key(cols, "sim7600g", "latitud") or _find_key(cols, "sim7600", "latitud")
    schema["sim_lon"]   = _find_key(cols, "sim7600g", "longitud") or _find_key(cols, "sim7600", "longitud")
    schema["sim_csq"]   = _find_key(cols, "sim7600", "intensidad", "senal") or _find_key(cols, "csq")
    schema["sim_sats"]  = _find_key(cols, "sim7600", "satelites") or _find_key(cols, "sats")
    schema["sim_speed"] = _find_key(cols, "sim7600", "velocidad") or _find_key(cols, "velocidad", "km h")

    # metadata lat/lon (fallback)
    schema["meta_lat"]  = (_find_key(cols, "metadatos", "latitud") or
                           _find_key(cols, "estacion", "latitud") or
                           _find_key(cols, "meta", "latitud"))
    schema["meta_lon"]  = (_find_key(cols, "metadatos", "longitud") or
                           _find_key(cols, "estacion", "longitud") or
                           _find_key(cols, "meta", "longitud"))

    return schema
# ---------- Cache management ----------

def key_tuple(project_id: str, device_code: str, tabla: str) -> Tuple[str,str,str]:
    return (str(project_id), str(device_code), str(tabla))

def cache_dir(key: Tuple[str,str,str]) -> str:
    p, d, t = key
    path = os.path.join(CACHE_ROOT, f"{p}_{d}_{t}")
    os.makedirs(path, exist_ok=True)
    return path

def day_from_time(ts: str) -> Optional[str]:
    # Accepts "YYYY-MM-DDTHH:MM:SS" or similar
    if not ts: return None
    try:
        return str(ts)[:10]
    except Exception:
        return None

def ensure_structs(key: Tuple[str,str,str]) -> None:
    _ = cache_dir(key)  # ensure folder
    if key not in Cursor:
        Cursor[key] = {"offset": 0, "pages": 0, "finished": False, "last_ok_ts": None, "last_error": None, "last_url": ""}

def load_day_from_disk(key: Tuple[str,str,str], day: str) -> None:
    """Lazy load day jsonl into memory (with dedup set)."""
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
                    if fp in fps:  # dedup
                        continue
                    fps.add(fp)
                    rows.append(r)
                except Exception:
                    continue
    DayRows[key][day] = rows
    DayFP[key][day] = fps
    if day not in Days[key]:
        Days[key].append(day)
        Days[key] = sorted(Days[key])  # keep sorted

def add_to_day_cache(key: Tuple[str,str,str], plotted: List[Dict[str,Any]]) -> Dict[str,int]:
    """Append plotted rows into day caches (memory + disk) with dedup. Returns {day: added_count}."""
    ensure_structs(key)
    added_per_day: Dict[str,int] = defaultdict(int)

    for r in plotted:
        d = day_from_time(r.get("time"))
        if not d:
            continue
        load_day_from_disk(key, d)  # ensure structures
        fp = f"{r.get('time','')}|{r.get('envio_n','')}"
        if fp in DayFP[key][d]:
            continue
        DayFP[key][d].add(fp)
        DayRows[key][d].append(r)
        added_per_day[d] += 1

    # Persist incrementally by day
    for d, n in added_per_day.items():
        if n <= 0: continue
        path = os.path.join(cache_dir(key), f"{d}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            for r in DayRows[key][d][-n:]:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

    if added_per_day:
        # refresh index
        Days[key] = sorted(set(Days[key]) | set(added_per_day.keys()))
    return added_per_day

def process_raw_to_plotted(raw_rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    out = []
    for row in raw_rows:
        # print(row)
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
# ===== COLLECTOR =========
# =========================

def collector_loop(key: Tuple[str,str,str], limit: int,
                   connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                   read_timeout=DEFAULT_READ_TIMEOUT,
                   verify_tls=True):
    """
    Background pagination towards older data; when finished, poll head for new data.
    """
    p, d, t = key
    ensure_structs(key)
    session = make_session()
    stop = CollectorThreads[key]["stop"]

    while not stop.is_set():
        cur = Cursor[key]
        try:
            # Continue pagination while not finished
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

            # Finished -> poll head (offset=0)
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
        return  # already running
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
    # Clear memory
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

    # Clear disk
    folder = cache_dir(key)
    try:
        shutil.rmtree(folder)
    except Exception:
        pass
    os.makedirs(folder, exist_ok=True)
    log(f"[admin] purged cache {key}")

# =========================
# ====== FLASK ROUTES =====
# =========================

@app.route("/")
def index():
    return redirect(url_for("map_view"))

@app.route("/map")
def map_view():
    """Renders Folium base map and injects a resizable control panel & JS client."""
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    # Start collector for this device (non-blocking). No reset here.
    start_collector(project_id, device_code, tabla, DEFAULT_LIMIT, reset=False)

    # Folium map (base only; layers are added by client JS)
    fmap = folium.Map(location=[-33.45, -70.65], zoom_start=12, control_scale=True, prefer_canvas=True)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)

    # Legend (static; client uses same palette)
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    cmap.add_to(fmap)
    html_demo = """
    <div style='position:fixed; top:20px; left:20px; z-index:9999; background:white; padding:10px; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,0.15); font-size:14px;'>
      <b>Este es un cuadro de texto en Folium</b><br>
      Puedes poner cualquier HTML aquí.
    </div>
    """
    folium.Element(html_demo).add_to(fmap)
    # Downloads toolbar (right side)  (inject via root.html to ensure it renders in final HTML)
    toolbar_html = f"""
    <div id=\"dlbar\" style=\"{TOOLBAR_CSS}\">
      <b>Downloads</b><br>
      <div style=\"margin-top:6px;\">
        <span style=\"font-weight:600;\">Page mode:</span>
        <a id=\"dl-raw-csv\" href=\"#\">CSV</a> |
        <a id=\"dl-raw-xlsx\" href=\"#\">Excel</a> |
        <a id=\"dl-plot-csv\" href=\"#\">Plotted CSV</a> |
        <a id=\"dl-plot-xlsx\" href=\"#\">Plotted Excel</a>
      </div>
      <div style=\"margin-top:6px;\">
        <span style=\"font-weight:600;\">Day exports:</span>
        <a id=\"dl-day-csv\" href=\"#\">CSV</a> |
        <a id=\"dl-day-xlsx\" href=\"#\">Excel</a>
      </div>
    </div>
    """
    fmap.get_root().html.add_child(Element(toolbar_html))

    # Include leaflet.heat plugin (needed for L.heatLayer)
    fmap.get_root().html.add_child(Element('<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.heat/0.2.0/leaflet-heat.js"></script>'))

    # Resizable control header (left, 50% width)
    # Device selector + admin buttons + paging controls + day/live controls
    header_html = f"""
    <div id="controls" style="{HEADER_CSS}">
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

      <span style="flex-basis:100%; height:0;"></span>  <!-- line break -->

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

      <span style="flex-basis:100%; height:0;"></span>  <!-- line break -->

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

      <span style="flex-basis:100%; height:0;"></span>  <!-- line break -->

      <button id="btnAdminReindex" title="Rebuild cache in background">Admin: Reindex</button>
      <button id="btnAdminPurge" title="Purge on-disk/in-memory cache">Admin: Purge cache</button>
      <button id="btnToggleLogs" title="Show/hide logs">Logs</button>
      <span id="status" style="margin-left:10px;color:#333;">Ready.</span>

      <div id="logs" style="display:none; flex-basis:100%; max-height:220px; overflow:auto; background:#fafafa; border:1px solid #ddd; padding:6px; border-radius:6px;"></div>
    </div>
    """
    fmap.get_root().html.add_child(Element(header_html))

    # Client configuration object (embedded safely)
    cfg = {
        "project_id": project_id,
        "device_code": device_code,
        "tabla": tabla,
        "palette": {"breaks": PM_BREAKS, "colors": PM_COLORS},
        "exports_base": "/download",
    }
    script_head = "<script>const CFG = " + json.dumps(cfg) + ";</script>"
    fmap.get_root().html.add_child(Element(script_head))

    # Client JS (static string, uses CFG; finds folium map dynamically)
    client_js = r"""
    <script>
    (function(){
      const $ = (sel)=>document.querySelector(sel);
      const $$ = (sel)=>Array.from(document.querySelectorAll(sel));
      const show = (el, on)=>{ el.style.display = on ? '' : 'none'; };
      const setStatus = (msg)=>{ const s = $('#status'); if(s) s.textContent = msg; };
      const sleep = (ms)=>new Promise(r=>setTimeout(r,ms));

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
      let heatLayer = null;       // L.heatLayer
      let heatData = [];          // [[lat,lon,val], ...]
      let lastTs = null;          // last timestamp of current-day load (for Live)
      let currentDay = null;      // YYYY-MM-DD currently loaded
      let currentBBox = null;     // for fitBounds after updates

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
        if(heatLayer){ heatData = []; heatLayer.setLatLngs(heatData); }
        currentBBox = null;
      }
      function ensureLayers(){
        if(!pointLayer){ pointLayer = L.layerGroup().addTo(map); }
        if(!heatLayer){
          if(!L.heatLayer){
            console.warn('leaflet.heat plugin not loaded; heat map disabled');
          }else{
            heatLayer = L.heatLayer([], {radius:12, blur:22, minOpacity:0.3, maxZoom:18}).addTo(map);
          }
        }
      }
      function extendBBox(lat, lon){
        if(!currentBBox){ currentBBox = [[lat,lon],[lat,lon]]; return; }
        currentBBox[0][0] = Math.min(currentBBox[0][0], lat);
        currentBBox[0][1] = Math.min(currentBBox[0][1], lon);
        currentBBox[1][0] = Math.max(currentBBox[1][0], lat);
        currentBBox[1][1] = Math.max(currentBBox[1][1], lon);
      }
      function fitIfBBox(){
        if(currentBBox){ map.fitBounds(currentBBox, {padding:[20,20]}); }
      }

      function addRows(rows, replace){
        ensureLayers();
        if(replace) clearLayers();
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
          m.addTo(pointLayer);
          heatData.push([lat,lon, Math.max(BR[0], Math.min(BR[BR.length-1], pm25))]);
          extendBBox(lat, lon);
          added++;
        }
        if(heatLayer) heatLayer.setLatLngs(heatData);
        if(replace) fitIfBBox();
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
        // Export current day using /download based on /api/data?mode=day
        const base = `${location.origin}/api/data?mode=day&day=${encodeURIComponent(day)}&project_id=${encodeURIComponent($('#project_id').value)}&device_code=${encodeURIComponent($('#device_code').value)}&tabla=${encodeURIComponent($('#tabla').value)}`;
        // Day CSV/XLSX are produced client-side via /api/data -> DataFrame requires new endpoints if needed.
        $('#dl-day-csv').href  = base;   // keep link (raw JSON); could be replaced by a real CSV endpoint if desired
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
            setStatus(`Live +${rows.length} (added=${added})`);
          }
        }catch(e){ /* silent */ }
      }
      setInterval(pollLive, 15000);

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
        const di = await refreshDayIndex(false);
        if(di && di.selected){ await loadDay(di.selected, true); }
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
        // reload /map with new base params
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

      // Boot
      (async ()=>{
        try{
          await waitForMap();
          setStatus('Map ready.');
          const di = await refreshDayIndex(true);
          if(di && di.selected){ await loadDay(di.selected, true); }
          updatePageDownloads($('#limit').value, $('#offset').value);
        }catch(e){
          setStatus('Init error: '+e.message);
          console.error(e);
        }
      })();
    })();
    </script>
    """
    fmap.get_root().html.add_child(Element(client_js))

    html = fmap.get_root().render()
    # DEBUG: write a copy to disk so user can inspect if elements are missing
    try:
        with open("_last_map_debug.html", "w", encoding="utf-8") as _dbg:
            _dbg.write(html)
    except Exception:
        pass
    if toolbar_html not in html:
        log("[debug] toolbar_html not found in rendered HTML")
    if "id=\"controls\"" not in html:
        log("[debug] controls div not found in rendered HTML")
    return Response(html, mimetype="text/html")

# ---- Data APIs ----

@app.route("/api/day-index")
def api_day_index():
    p = request.args.get("project_id", DEFAULT_PROJECT_ID)
    t = request.args.get("tabla", DEFAULT_TABLA)
    d = request.args.get("device_code")

    if not d:
      # Agrupar todos los días de todos los dispositivos en un solo array
      all_days = set()
      last_cursor = {}
      # Iterar sobre directorios en CACHE_ROOT que coincidan con el proyecto y tabla
      prefix = f"{p}_"
      suffix = f"_{t}"
      for dirname in os.listdir(CACHE_ROOT):
        if dirname.startswith(prefix) and dirname.endswith(suffix):
          device = dirname[len(prefix):-len(suffix)]
          key = key_tuple(p, device, t)
          ensure_structs(key)
          folder = cache_dir(key)
          for name in os.listdir(folder):
            if name.endswith(".jsonl") and len(name) >= 10:
              day = name[:10]
              if day not in Days[key]:
                Days[key].append(day)
              all_days.add(day)
          Days[key] = sorted(Days[key])
          # Tomar el cursor del último dispositivo (puedes ajustar esto si quieres combinar de otra forma)
          last_cursor = Cursor.get(key, {})
      return jsonify({
        "days": sorted(all_days),
        "cursor": last_cursor
      })
        
    else:
        key = key_tuple(p, d, t)
        ensure_structs(key)
        folder = cache_dir(key)
        for name in os.listdir(folder):
            if name.endswith(".jsonl") and len(name) >= 10:
                day = name[:10]
                
                if day not in Days[key]:
                    Days[key].append(day)
        Days[key] = sorted(Days[key])
        print(Days[key])
        cur = Cursor.get(key, {})
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
              return datetime.fromisoformat(s.replace("Z","")) .timestamp()
          except Exception:
              return 0.0

      rows: List[Dict[str,Any]] = []
      if not d:  # agregamos todos los dispositivos para ese proyecto/tabla
          prefix = f"{p}_"
          suffix = f"_{t}"
          for dirname in os.listdir(CACHE_ROOT):
              if dirname.startswith(prefix) and dirname.endswith(suffix):
                  device = dirname[len(prefix):-len(suffix)]
                  dkey = key_tuple(p, device, t)
                  load_day_from_disk(dkey, day)
                  day_rows = DayRows[dkey].get(day, [])
                  # Añadir device_code en la fila para diferenciación
                  for r in day_rows:
                      if "device_code" not in r:
                          r = dict(r)
                          r["device_code"] = device
                      rows.append(r)
      else:
          load_day_from_disk(key, day)
          rows = DayRows[key].get(day, [])

      # Filtrar por 'since' si corresponde
      if since:
          th = to_epoch(since)
          filtered = []
          for r in rows:
              ts = r.get("time")
              if not ts:
                  continue
              try:
                  te = datetime.fromisoformat(str(ts).replace("Z","")) .timestamp()
              except Exception:
                  continue
              if te > th:
                  filtered.append(r)
          rows = filtered

      # Ordenar por tiempo si existe el campo
      try:
          rows.sort(key=lambda x: x.get("time") or "")
      except Exception:
          pass
      return jsonify({"status":"success","type":"plotted","rows":rows, "aggregated": (not d), "day": day, "since": since})

  # default: page mode (single upstream page via fetch_rows_v2-lite)
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

    # Single page or simple pagination (safe cap)
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

# =========================
# ========= MAIN ==========
# =========================

if __name__ == "__main__":
    os.makedirs(CACHE_ROOT, exist_ok=True)
    # Start default collector on boot
    start_collector(DEFAULT_PROJECT_ID, DEFAULT_DEVICE_CODE, DEFAULT_TABLA, DEFAULT_LIMIT, reset=False)
    app.run(host="127.0.0.1", port=5000, debug=True)
