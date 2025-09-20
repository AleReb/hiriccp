# app.py
# -*- coding: utf-8 -*-
"""
Flask server for HIRIPRO-01 PM2.5 map and data exports (V2 API with retries, timeouts, pagination, diagnostics).

Endpoints:
- /map                      -> Folium map (consistent colors, auto-zoom, HeatMap, popup with "Número de envíos")
- /download/raw.csv         -> All rows/columns from upstream (no filtering)
- /download/raw.xlsx        -> Same as above, Excel
- /download/plotted.csv     -> Only rows with valid (lat, lon, pm25) used in the map
- /download/plotted.xlsx    -> Same as above, Excel
- /api/data                 -> JSON API (type=raw|plotted)
- /diag                     -> Connectivity diagnostics (one request or paginated sample)
- /healthz                  -> health check

Query params (for /map, /api/data, /download/*, /diag):
- project_id   (default: 18)
- device_code  (default: HIRIPRO-01)
- tabla        (default: datos)
- limite       (default: 1000)   -> page size per V2 request
- offset       (default: 0)      -> initial offset
- paginate     (default: 1)      -> 1: auto-fetch all pages; 0: single page only
- connect_timeout (default: 10)  -> seconds for TCP connect
- read_timeout    (default: 60)  -> seconds for server response read
- retries         (default: 3)   -> total retry attempts per request
- backoff         (default: 0.5) -> exponential backoff factor (0.5 => 0.5s,1s,2s,...)

Notes:
- If a timeout happens mid-pagination, partial pages are returned and a warning banner appears on the map.
"""

import io
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


# =========================
# ========= CONFIG ========
# =========================

# Upstream API base (V2)
UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2"

# Defaults for upstream query
DEFAULT_TABLA = "datos"
DEFAULT_PROJECT_ID = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"
DEFAULT_LIMIT = 1000
DEFAULT_OFFSET = 0
DEFAULT_PAGINATE = 1  # 1=fetch all pages; 0=single page

# Schema field names
KEY_TIME    = "fecha"
KEY_PM25    = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
KEY_PM1     = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10    = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM     = "PMS5003 [Humedad (%)]"
KEY_TEMP    = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT    = "Divisor de Voltaje [Voltaje (V)]"

# Coordinates: prefer SIM7600G; fallback station metadata
KEY_SIM_LAT   = "SIM7600G [Latitud (°)]"
KEY_SIM_LON   = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ   = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS  = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"
KEY_META_LAT  = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON  = "Metadatos Estacion [Longitud (°)]"
KEY_NUM_ENV   = "Metadatos Estacion [Numero de envios (Numeral)]"  # shown in popup

# PM2.5 color scale (fixed, coherent across points/legend/heatmap)
PM_BREAKS = [0, 12, 35, 55, 150, 250]  # add 500 if needed
PM_COLORS = [
    "#2ecc71",  # Good
    "#a3d977",  # Mod-Good
    "#f1c40f",  # Moderate
    "#e67e22",  # Unhealthy (SG)
    "#e74c3c",  # Unhealthy
    "#7f1d1d",  # Hazardous
]
COLORBAR_CAPTION = "PM2.5 (µg/m³)"

# Marker/HeatMap styling
MARKER_RADIUS   = 6
MARKER_OPACITY  = 0.85
HEAT_RADIUS     = 12
HEAT_BLUR       = 22
HEAT_MIN_OP     = 0.30
HEAT_MAX_ZOOM   = 18
HEAT_STEPS      = 8   # 6..16 is OK

# Toolbar CSS (download buttons box)
TOOLBAR_CSS = (
    "position: fixed; top: 10px; right: 10px; z-index: 9999;"
    "background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 8px;"
    "box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 14px;"
)

# Simple in-memory cache (seconds)
CACHE_TTL_SECONDS = 60
_cache: Dict[str, Dict[str, Any]] = {}  # key -> {"ts": float, "rows": List[Dict]}

# Pagination safety cap
MAX_PAGES = 500

# Default HTTP headers for upstream
DEFAULT_HEADERS = {
    "User-Agent": "HIRIMap/1.0 (+github.com/your-org) requests"
}


# =========================
# ====== APP SETUP ========
# =========================

app = Flask(__name__)


# =========================
# ====== UTILITIES ========
# =========================

def build_upstream_url(project_id: str, device_code: str, tabla: str, limite: int, offset: int) -> str:
    """Build the V2 API URL with query params."""
    return (
        f"{UPSTREAM_BASE}"
        f"?tabla={tabla}"
        f"&disp.id_proyecto={project_id}"
        f"&disp.codigo_interno={device_code}"
        f"&limite={int(limite)}"
        f"&offset={int(offset)}"
    )


def make_session(retries: int, backoff: float) -> requests.Session:
    """
    Build a requests Session with retry/backoff policy.
    Retries on 429/5xx and on connect/read errors.
    """
    session = requests.Session()
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
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(DEFAULT_HEADERS)
    return session


def to_float(x: Any) -> Optional[float]:
    """Convert strings like '4.0', '4,0', '4.0 µg/m³' to float. Return None if not parseable."""
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
    """Priority: SIM7600G; fallback: station metadata."""
    lat = to_float(row.get(KEY_SIM_LAT)); lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None:
        return lat, lon
    lat = to_float(row.get(KEY_META_LAT)); lon = to_float(row.get(KEY_META_LON))
    return lat, lon


def clamp_pm25(v: float) -> float:
    """Clamp PM2.5 to the color scale domain."""
    return max(PM_BREAKS[0], min(PM_BREAKS[-1], float(v)))


def build_popup(row: Dict[str, Any], lat: float, lon: float, pm25_val: float) -> str:
    """HTML popup content."""
    ts = row.get(KEY_TIME, "N/A")
    num_env = row.get(KEY_NUM_ENV, "-")

    def safe_val(v: Any) -> str:
        return "-" if v in (None, "", "null") else str(v)

    html = (
        f"<b>PM2.5:</b> {pm25_val:.1f} µg/m³<br>"
        f"<b>Time:</b> {safe_val(ts)}<br>"
        f"<b>Envíos #:</b> {safe_val(num_env)}<br>"
        f"<b>Lat:</b> {lat:.6f}, <b>Lon:</b> {lon:.6f}<br>"
        f"<hr style='margin:4px 0'/>"
        f"<b>PM1:</b> {safe_val(row.get(KEY_PM1))} | "
        f"<b>PM10:</b> {safe_val(row.get(KEY_PM10))}<br>"
        f"<b>Temp PMS:</b> {safe_val(row.get(KEY_TEMP))} °C | "
        f"<b>Hum:</b> {safe_val(row.get(KEY_HUM))} %<br>"
        f"<b>VBat:</b> {safe_val(row.get(KEY_VBAT))} V<br>"
        f"<b>CSQ:</b> {safe_val(row.get(KEY_SIM_CSQ))} | "
        f"<b>Sats:</b> {safe_val(row.get(KEY_SIM_SATS))} | "
        f"<b>Speed:</b> {safe_val(row.get(KEY_SIM_SPEED))} km/h"
    )
    return html


def gradient_from_cmap(cm: LinearColormap, steps: int = 256) -> dict:
    """Build a 0..1 gradient dict for Leaflet.Heat from the same colormap."""
    return {i/(steps-1): cm(cm.vmin + (cm.vmax - cm.vmin) * i/(steps-1)) for i in range(steps)}


def extract_rows_from_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract list of dicts at data.tableData from V2 payload."""
    if not isinstance(payload, dict):
        return []
    data = payload.get("data", {})
    rows = data.get("tableData", [])
    return [r for r in rows if isinstance(r, dict)]


def fetch_rows_v2(project_id: str, device_code: str, tabla: str,
                  limite: int, offset: int, paginate: bool,
                  connect_timeout: float, read_timeout: float,
                  retries: int, backoff: float,
                  verify_tls: bool = True,
                  use_cache: bool = True) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Fetch rows with retries and optional pagination.
    Returns (rows_all, meta) where meta includes:
      {"partial": bool, "pages": int, "last_url": str, "error": Optional[str], "elapsed_sec": float}
    If a timeout/error occurs mid-fetch and at least one page was downloaded, returns partial=True.
    """
    cache_key = f"v2|{tabla}|{project_id}|{device_code}|{limite}|{offset}|{int(paginate)}|{connect_timeout}|{read_timeout}|{retries}|{backoff}"
    now = time.time()

    if use_cache and CACHE_TTL_SECONDS > 0:
        entry = _cache.get(cache_key)
        if entry and (now - entry["ts"] <= CACHE_TTL_SECONDS):
            return entry["rows"], entry["meta"]

    session = make_session(retries=retries, backoff=backoff)
    timeout_tuple = (float(connect_timeout), float(read_timeout))

    rows_all: List[Dict[str, Any]] = []
    pages = 0
    last_url = ""
    meta_error = None
    start = time.time()

    try:
        if paginate:
            current_offset = offset
            while True:
                last_url = build_upstream_url(project_id, device_code, tabla, limite, current_offset)
                resp = session.get(last_url, timeout=timeout_tuple, verify=verify_tls, stream=False)
                resp.raise_for_status()
                payload = resp.json()
                rows_page = extract_rows_from_payload(payload)
                if not rows_page:
                    break
                rows_all.extend(rows_page)
                pages += 1
                if len(rows_page) < limite:
                    break
                current_offset += limite
                if pages >= MAX_PAGES:
                    meta_error = f"Reached MAX_PAGES={MAX_PAGES}, stopping for safety."
                    break
        else:
            last_url = build_upstream_url(project_id, device_code, tabla, limite, offset)
            resp = session.get(last_url, timeout=timeout_tuple, verify=verify_tls, stream=False)
            resp.raise_for_status()
            payload = resp.json()
            rows_all = extract_rows_from_payload(payload)

    except requests.exceptions.RequestException as e:
        meta_error = f"{type(e).__name__}: {e}"

    elapsed = time.time() - start
    meta = {
        "partial": meta_error is not None and len(rows_all) > 0,
        "pages": pages if pages > 0 else (1 if rows_all else 0),
        "last_url": last_url,
        "error": meta_error,
        "elapsed_sec": round(elapsed, 3),
    }

    if use_cache and CACHE_TTL_SECONDS > 0 and rows_all:
        _cache[cache_key] = {"ts": now, "rows": rows_all, "meta": meta}

    return rows_all, meta


def build_dataframes(rows_raw: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """df_raw = all rows; df_plot = rows with valid (lat, lon, pm25)."""
    df_raw = pd.DataFrame(rows_raw)
    plotted_records: List[Dict[str, Any]] = []
    for row in rows_raw:
        lat, lon = choose_coords(row)
        pm25 = to_float(row.get(KEY_PM25))
        if lat is None or lon is None or pm25 is None:
            continue
        plotted_records.append({
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
    df_plot = pd.DataFrame(plotted_records)
    return df_raw, df_plot


def build_map_html(df_plot: pd.DataFrame, query_string: str, warnings: Optional[List[str]] = None) -> str:
    """Generate Folium map HTML + toolbar; optionally show warning banners."""
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    heat_gradient = gradient_from_cmap(cmap, steps=HEAT_STEPS)

    fmap = folium.Map(location=[df_plot["lat"].iloc[0], df_plot["lon"].iloc[0]], zoom_start=16, control_scale=True)

    fg_points = folium.FeatureGroup(name="PM2.5 points", overlay=True, control=True)
    for _, r in df_plot.iterrows():
        val = clamp_pm25(float(r["pm25"]))
        color = cmap(val)
        popup_html = build_popup(
            {
                KEY_TIME: r["time"],
                KEY_NUM_ENV: r["envio_n"],
                KEY_PM1: r["pm1"],
                KEY_PM10: r["pm10"],
                KEY_TEMP: r["temp_pms"],
                KEY_HUM: r["hum"],
                KEY_VBAT: r["vbat"],
                KEY_SIM_CSQ: r["csq"],
                KEY_SIM_SATS: r["sats"],
                KEY_SIM_SPEED: r["speed_kmh"],
            },
            float(r["lat"]), float(r["lon"]), float(r["pm25"])
        )
        folium.CircleMarker(
            location=[r["lat"], r["lon"]],
            radius=MARKER_RADIUS,
            popup=folium.Popup(popup_html, max_width=360),
            color=color,
            weight=1,
            fill=True,
            fill_color=color,
            fill_opacity=MARKER_OPACITY,
        ).add_to(fg_points)
    fg_points.add_to(fmap)

    HeatMap(
        df_plot.assign(pm25=df_plot["pm25"].apply(clamp_pm25))[["lat", "lon", "pm25"]].values.tolist(),
        name="HeatMap PM2.5",
        min_opacity=HEAT_MIN_OP,
        radius=HEAT_RADIUS,
        blur=HEAT_BLUR,
        max_zoom=HEAT_MAX_ZOOM,
        gradient=heat_gradient,
    ).add_to(fmap)

    cmap.add_to(fmap)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    sw = [float(df_plot["lat"].min()), float(df_plot["lon"].min())]
    ne = [float(df_plot["lat"].max()), float(df_plot["lon"].max())]
    fmap.fit_bounds([sw, ne], padding=(20, 20))

    # Downloads toolbar
    toolbar_html = f"""
    <div style="{TOOLBAR_CSS}">
      <b>Downloads</b><br>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Raw:</span>
        <a href="/download/raw.csv?{query_string}">CSV</a> |
        <a href="/download/raw.xlsx?{query_string}">Excel</a>
      </div>
      <div style="margin-top:4px;">
        <span style="font-weight:600;">Plotted:</span>
        <a href="/download/plotted.csv?{query_string}">CSV</a> |
        <a href="/download/plotted.xlsx?{query_string}">Excel</a>
      </div>
    </div>
    """
    folium.Element(toolbar_html).add_to(fmap)

    # Optional warning banners (e.g., partial data)
    if warnings:
        banner = """
        <div style="
            position: fixed; bottom: 10px; left: 50%; transform: translateX(-50%);
            z-index: 9999; background: rgba(255,220,180,0.95); color:#5a2;
            padding: 8px 12px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            font-family: system-ui, sans-serif; font-size: 14px;">
            {}
        </div>
        """.format(" &nbsp;|&nbsp; ".join(warnings))
        folium.Element(banner).add_to(fmap)

    return fmap.get_root().render()


# =========================
# ========= ROUTES ========
# =========================

@app.route("/")
def index():
    return redirect(url_for("map_view"))


@app.route("/map")
def map_view():
    # Core params
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    # Pagination/timeout/retries params
    limite = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    paginate = int(request.args.get("paginate", DEFAULT_PAGINATE)) == 1

    connect_timeout = float(request.args.get("connect_timeout", 10))
    read_timeout = float(request.args.get("read_timeout", 60))
    retries = int(request.args.get("retries", 3))
    backoff = float(request.args.get("backoff", 0.5))
    verify_tls = request.args.get("verify", "1") != "0"  # leave TLS ON by default

    # Fetch
    rows, meta = fetch_rows_v2(
        project_id, device_code, tabla,
        limite, offset, paginate,
        connect_timeout, read_timeout,
        retries, backoff,
        verify_tls=verify_tls,
        use_cache=True
    )

    # Error handling
    if not rows and meta.get("error"):
        msg = (
            f"<h3>Upstream error: {meta['error']}</h3>"
            f"<p>URL: {meta.get('last_url','-')}</p>"
            f"<p>Try: lower <code>limite</code>, increase <code>read_timeout</code>, set <code>paginate=0</code>.</p>"
        )
        return Response(msg, status=504, mimetype="text/html")

    df_raw, df_plot = build_dataframes(rows)
    if df_plot.empty:
        warn = []
        if meta.get("error"):
            warn.append("No points; upstream error occurred.")
        html = (
            "<h3>No valid points (lat/lon/pm25) to plot.</h3>"
            f"<p>Rows fetched: {len(rows)} | Elapsed: {meta.get('elapsed_sec','-')}s</p>"
        )
        return Response(html, mimetype="text/html")

    # Partial banner if something went wrong mid-way
    warnings = []
    if meta.get("partial"):
        warnings.append(
            f"Partial data: fetched {len(rows)} rows in {meta.get('elapsed_sec','-')}s; "
            f"last request failed. Consider lower 'limite' or higher 'read_timeout'."
        )

    html = build_map_html(df_plot, query_string=request.query_string.decode("utf-8"), warnings=warnings)
    return Response(html, mimetype="text/html")


@app.route("/api/data")
def api_data():
    data_type = request.args.get("type", "plotted")
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    limite = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    paginate = int(request.args.get("paginate", DEFAULT_PAGINATE)) == 1

    connect_timeout = float(request.args.get("connect_timeout", 10))
    read_timeout = float(request.args.get("read_timeout", 60))
    retries = int(request.args.get("retries", 3))
    backoff = float(request.args.get("backoff", 0.5))
    verify_tls = request.args.get("verify", "1") != "0"

    rows, meta = fetch_rows_v2(
        project_id, device_code, tabla,
        limite, offset, paginate,
        connect_timeout, read_timeout,
        retries, backoff,
        verify_tls=verify_tls,
        use_cache=True
    )
    df_raw, df_plot = build_dataframes(rows)

    if data_type == "raw":
        return jsonify({"status": "success", "type": "raw", "rows": df_raw.to_dict(orient="records"), "meta": meta})
    else:
        return jsonify({"status": "success", "type": "plotted", "rows": df_plot.to_dict(orient="records"), "meta": meta})


@app.route("/download/<kind>.<ext>")
def download(kind: str, ext: str):
    if kind not in {"raw", "plotted"}:
        return Response("Invalid kind (use raw|plotted).", status=400)
    if ext not in {"csv", "xlsx"}:
        return Response("Invalid extension (use csv|xlsx).", status=400)

    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    limite = int(request.args.get("limite", DEFAULT_LIMIT))
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    paginate = int(request.args.get("paginate", DEFAULT_PAGINATE)) == 1

    connect_timeout = float(request.args.get("connect_timeout", 10))
    read_timeout = float(request.args.get("read_timeout", 60))
    retries = int(request.args.get("retries", 3))
    backoff = float(request.args.get("backoff", 0.5))
    verify_tls = request.args.get("verify", "1") != "0"

    rows, meta = fetch_rows_v2(
        project_id, device_code, tabla,
        limite, offset, paginate,
        connect_timeout, read_timeout,
        retries, backoff,
        verify_tls=verify_tls,
        use_cache=True
    )
    df_raw, df_plot = build_dataframes(rows)
    df = df_raw if kind == "raw" else df_plot

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{kind}_{ts}.{ext}"

    if ext == "csv":
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        bio = io.BytesIO(csv_bytes)
        return send_file(bio, as_attachment=True, download_name=filename, mimetype="text/csv; charset=utf-8")
    else:
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        bio.seek(0)
        return send_file(
            bio,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


@app.route("/diag")
def diag():
    """
    Quick connectivity diagnostics:
    - Makes one request (or a couple if paginate=1) and reports status, elapsed time, row counts.
    """
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    limite = int(request.args.get("limite", 10))  # smaller by default for diag
    offset = int(request.args.get("offset", 0))
    paginate = int(request.args.get("paginate", 0)) == 1  # default: single page

    connect_timeout = float(request.args.get("connect_timeout", 10))
    read_timeout = float(request.args.get("read_timeout", 60))
    retries = int(request.args.get("retries", 2))
    backoff = float(request.args.get("backoff", 0.5))
    verify_tls = request.args.get("verify", "1") != "0"

    rows, meta = fetch_rows_v2(
        project_id, device_code, tabla,
        limite, offset, paginate,
        connect_timeout, read_timeout,
        retries, backoff,
        verify_tls=verify_tls,
        use_cache=False
    )

    sample = rows[:3] if rows else []
    return jsonify({
        "ok": bool(rows),
        "rows_fetched": len(rows),
        "meta": meta,
        "sample_rows": sample,
        "tip": "If timeouts occur: lower 'limite', increase 'read_timeout', or set 'paginate=0'."
    })


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


# =========================
# ======== MAIN ===========
# =========================

if __name__ == "__main__":
    # Dev server; use gunicorn/uvicorn in production
    app.run(host="127.0.0.1", port=5000, debug=True)
