# app.py
# -*- coding: utf-8 -*-
"""
Flask server for HIRIPRO-01 PM2.5 map and data exports.

Features:
- /map               -> Folium map HTML (consistent colors, auto-zoom, HeatMap, popup with "Número de envíos")
- /download/raw.csv  -> All rows/columns as returned by the upstream API (no filtering)
- /download/raw.xlsx -> Same as above, Excel
- /download/plotted.csv  -> Only rows with valid (lat, lon, pm25) used in the map
- /download/plotted.xlsx -> Same as above, Excel
- /api/data          -> JSON API (type=raw|plotted) with the same query params as /map
- Simple in-memory cache with TTL to avoid hammering the upstream API

Customize: see "CONFIG" and inline comments.
"""

import io
import math
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from flask import Flask, request, Response, send_file, redirect, url_for, jsonify
import folium
from folium.plugins import HeatMap, Fullscreen, MiniMap
from branca.colormap import LinearColormap


# =========================
# ========= CONFIG ========
# =========================

# Upstream API base (query params are added below)
UPSTREAM_BASE = "https://api-sensores.cmasccp.cl/listarDatosEstructurados"

# Default query params for upstream API
DEFAULT_TABLA = "datos"
DEFAULT_PROJECT_ID = "18"
DEFAULT_DEVICE_CODE = "HIRIPRO-01"

# Schema field names (adjust to your API keys centrally here)
KEY_TIME    = "fecha"
KEY_PM25    = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
KEY_PM1     = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10    = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM     = "PMS5003 [Humedad (%)]"
KEY_TEMP    = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT    = "Divisor de Voltaje [Voltaje (V)]"

# Preferred GNSS source (SIM7600G first); fallback to station metadata
KEY_SIM_LAT   = "SIM7600G [Latitud (°)]"
KEY_SIM_LON   = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ   = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS  = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"
KEY_META_LAT  = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON  = "Metadatos Estacion [Longitud (°)]"
KEY_NUM_ENV   = "Metadatos Estacion [Numero de envios (Numeral)]"  # Show in popup

# PM2.5 color scale (fixed, coherent across points/legend/heatmap)
# You can add 500 if needed -> [0, 12, 35, 55, 150, 250, 500]
PM_BREAKS = [0, 12, 35, 55, 150, 250]
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

# Simple in-memory cache to avoid refetching too often
CACHE_TTL_SECONDS = 60  # change to 0 to disable caching


# =========================
# ====== APP SETUP ========
# =========================

app = Flask(__name__)

_cache: Dict[str, Dict[str, Any]] = {}  # key -> {"ts": float, "rows": List[Dict]}


# =========================
# ====== UTILITIES ========
# =========================

def build_upstream_url(project_id: str, device_code: str, tabla: str) -> str:
    """
    Build the upstream API URL with query params.
    Example:
    https://.../listarDatosEstructurados?tabla=datos&disp.id_proyecto=18&disp.codigo_interno=HIRIPRO-01
    """
    return (
        f"{UPSTREAM_BASE}"
        f"?tabla={tabla}"
        f"&disp.id_proyecto={project_id}"
        f"&disp.codigo_interno={device_code}"
    )


def to_float(x: Any) -> Optional[float]:
    """
    Convert values like '4.0', '4,0', '4.0 µg/m³' to float.
    Return None if not parseable.
    """
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
    """
    Priority: SIM7600G coordinates; fallback to station metadata.
    To invert priority, swap the two blocks.
    """
    lat = to_float(row.get(KEY_SIM_LAT))
    lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None:
        return lat, lon

    lat = to_float(row.get(KEY_META_LAT))
    lon = to_float(row.get(KEY_META_LON))
    return lat, lon


def clamp_pm25(v: float) -> float:
    """Clamp PM2.5 into the colormap domain to avoid out-of-range artifacts."""
    return max(PM_BREAKS[0], min(PM_BREAKS[-1], float(v)))


def build_popup(row: Dict[str, Any], lat: float, lon: float, pm25_val: float) -> str:
    """HTML popup content. Adjust fields/text here if needed."""
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
    """
    Build a 0..1 gradient dict for Leaflet.Heat from the same colormap.
    Reduce `steps` if you prefer fewer discrete levels in the heatmap.
    """
    return {
        i / (steps - 1): cm(cm.vmin + (cm.vmax - cm.vmin) * i / (steps - 1))
        for i in range(steps)
    }


def fetch_rows(project_id: str, device_code: str, tabla: str, use_cache: bool = True) -> List[Dict[str, Any]]:
    """
    Fetch upstream rows (list of dicts) from data.tableData.
    Uses a simple memory cache (TTL) to reduce repeated calls.
    """
    cache_key = f"{tabla}|{project_id}|{device_code}"
    now = time.time()

    if use_cache and CACHE_TTL_SECONDS > 0:
        entry = _cache.get(cache_key)
        if entry and (now - entry["ts"] <= CACHE_TTL_SECONDS):
            return entry["rows"]

    url = build_upstream_url(project_id, device_code, tabla)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    rows = [row for row in j.get("data", {}).get("tableData", []) if isinstance(row, dict)]

    if use_cache and CACHE_TTL_SECONDS > 0:
        _cache[cache_key] = {"ts": now, "rows": rows}

    return rows


def build_dataframes(rows_raw: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build:
      - df_raw: all rows/columns as returned (no filtering)
      - df_plot: only rows with valid lat, lon, pm25 (used in the map)
    """
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


def build_map_html(df_plot: pd.DataFrame, query_string: str) -> str:
    """
    Build a complete HTML document containing the Folium map and a download toolbar.
    The toolbar links back to this Flask server with the same query string to preserve context.
    """
    # One colormap for everything (points/legend/heatmap)
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    heat_gradient = gradient_from_cmap(cmap, steps=HEAT_STEPS)

    # Create map (center will be overridden by fit_bounds)
    fmap = folium.Map(location=[df_plot["lat"].iloc[0], df_plot["lon"].iloc[0]], zoom_start=16, control_scale=True)

    # Points layer
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

    # HeatMap layer (same palette)
    HeatMap(
        df_plot.assign(pm25=df_plot["pm25"].apply(clamp_pm25))[["lat", "lon", "pm25"]].values.tolist(),
        name="HeatMap PM2.5",
        min_opacity=HEAT_MIN_OP,
        radius=HEAT_RADIUS,
        blur=HEAT_BLUR,
        max_zoom=HEAT_MAX_ZOOM,
        gradient=heat_gradient,
    ).add_to(fmap)

    # Legend + controls
    cmap.add_to(fmap)
    Fullscreen(position="topleft").add_to(fmap)
    MiniMap(toggle_display=True).add_to(fmap)
    folium.LayerControl(collapsed=False).add_to(fmap)

    # Auto-zoom to all points
    sw = [float(df_plot["lat"].min()), float(df_plot["lon"].min())]
    ne = [float(df_plot["lat"].max()), float(df_plot["lon"].max())]
    fmap.fit_bounds([sw, ne], padding=(20, 20))

    # Toolbar (download links preserve the same query string)
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

    # Return the full HTML document
    return fmap.get_root().render()


# =========================
# ========= ROUTES ========
# =========================

@app.route("/")
def index():
    # Redirect to map with defaults to make it friendly
    return redirect(url_for("map_view"))


@app.route("/map")
def map_view():
    """
    Render the Folium map. Query params:
      - project_id (default: 18)
      - device_code (default: HIRIPRO-01)
      - tabla (default: datos)
    """
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    # Fetch + build dataframes
    rows = fetch_rows(project_id, device_code, tabla, use_cache=True)
    df_raw, df_plot = build_dataframes(rows)

    if df_plot.empty:
        return (
            "<h3>No valid points found (lat/lon/pm25 missing).</h3>"
            "<p>Try another device or check upstream data.</p>",
            200,
            {"Content-Type": "text/html; charset=utf-8"},
        )

    html = build_map_html(df_plot, query_string=request.query_string.decode("utf-8"))
    return Response(html, mimetype="text/html")


@app.route("/api/data")
def api_data():
    """
    Return JSON data. Query params:
      - type=raw|plotted  (default: plotted)
      - project_id, device_code, tabla   (same defaults as /map)
    """
    data_type = request.args.get("type", "plotted")
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    rows = fetch_rows(project_id, device_code, tabla, use_cache=True)
    df_raw, df_plot = build_dataframes(rows)

    if data_type == "raw":
        return jsonify({"status": "success", "type": "raw", "rows": df_raw.to_dict(orient="records")})
    else:
        return jsonify({"status": "success", "type": "plotted", "rows": df_plot.to_dict(orient="records")})


@app.route("/download/<kind>.<ext>")
def download(kind: str, ext: str):
    """
    Download endpoints:
      /download/raw.csv
      /download/raw.xlsx
      /download/plotted.csv
      /download/plotted.xlsx

    Same query params as /map to keep context.
    """
    project_id = request.args.get("project_id", DEFAULT_PROJECT_ID)
    device_code = request.args.get("device_code", DEFAULT_DEVICE_CODE)
    tabla = request.args.get("tabla", DEFAULT_TABLA)

    rows = fetch_rows(project_id, device_code, tabla, use_cache=True)
    df_raw, df_plot = build_dataframes(rows)

    # Choose dataset
    if kind not in {"raw", "plotted"}:
        return Response("Invalid kind (use raw|plotted).", status=400)
    df = df_raw if kind == "raw" else df_plot

    # Timestamped filename
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{kind}_{ts}.{ext}"

    # CSV / Excel in-memory
    if ext == "csv":
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        bio = io.BytesIO(csv_bytes)
        return send_file(
            bio,
            as_attachment=True,
            download_name=filename,
            mimetype="text/csv; charset=utf-8",
        )
    elif ext == "xlsx":
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
    else:
        return Response("Invalid extension (use csv|xlsx).", status=400)


@app.route("/healthz")
def healthz():
    """Simple health check."""
    return jsonify({"ok": True})


# =========================
# ======== MAIN ===========
# =========================

if __name__ == "__main__":
    # Dev server; use gunicorn/uvicorn in production
    app.run(host="127.0.0.1", port=5000, debug=True)
