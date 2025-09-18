# -*- coding: utf-8 -*-
"""
HIRIPRO-01 PM2.5 Map
- Single, coherent color scale (points, HeatMap, legend)
- Auto-zoom to all points
- Popup includes "Número de envíos" for DB lookup
- RAW (all rows/columns) + PLOTTED (map points) exports to CSV/XLSX
- Download buttons embedded in the HTML (for all exports)

Adjustable settings are grouped near the top. Code is heavily commented.
"""

import sys
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd
import folium
from folium.plugins import HeatMap, Fullscreen, MiniMap
from branca.colormap import LinearColormap


# =========================
# ==== CONFIGURABLES ======
# =========================

# --- API endpoint (change project/device here) ---
API_URL_DEFAULT = (
    "https://api-sensores.cmasccp.cl/listarDatosEstructurados"
    "?tabla=datos&disp.id_proyecto=18&disp.codigo_interno=HIRIPRO-01"
)

# --- Schema field names (match your API keys; rename safely here) ---
KEY_TIME    = "fecha"
KEY_PM25    = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
KEY_PM1     = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10    = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM     = "PMS5003 [Humedad (%) ]" if False else "PMS5003 [Humedad (%) ]"  # placeholder to show how you'd change
# If your exact key is "PMS5003 [Humedad (%)]", set it explicitly:
KEY_HUM     = "PMS5003 [Humedad (%)]"
KEY_TEMP    = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT    = "Divisor de Voltaje [Voltaje (V)]"

# Preferred GNSS source (SIM7600G first)
KEY_SIM_LAT   = "SIM7600G [Latitud (°)]"
KEY_SIM_LON   = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ   = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS  = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"

# Fallback station metadata for coordinates
KEY_META_LAT = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON = "Metadatos Estacion [Longitud (°)]"

# Extra ID for cross-reference in DB (shown in popup)
KEY_NUM_ENV = "Metadatos Estacion [Numero de envios (Numeral)]"

# --- Color scale (CHANGE HERE to adjust legend/points/heatmap consistently) ---
# Breakpoints (µg/m³) and colors (hex). These are OMS/EPA-like buckets.
PM_BREAKS = [0, 12, 35, 55, 100]  # you can add 500 if needed -> [0,12,35,55,150,250,500]
PM_COLORS = [
    "#2ecc71",  # Good
    "#a3d977",  # Mod-Good
    "#f1c40f",  # Moderate
    "#e67e22",  # Unhealthy (SG)
    "#e74c3c",  # Unhealthy
    "#7f1d1d",  # Hazardous
]
# Legend caption text
COLORBAR_CAPTION = "PM2.5 (µg/m³)"

# --- Marker rendering ---
MARKER_RADIUS = 6           # change to 4..10 depending density
MARKER_OPACITY = 0.85       # fill opacity

# --- HeatMap rendering ---
HEAT_RADIUS = 12
HEAT_BLUR = 22
HEAT_MIN_OPACITY = 0.30
HEAT_MAX_ZOOM = 18
HEAT_GRADIENT_STEPS = 8      # 6..16 is usually fine

# --- HTML toolbar (download buttons) position/style ---
TOOLBAR_CSS = """
position: fixed; top: 10px; right: 10px; z-index: 9999;
background: rgba(255,255,255,0.94); padding: 8px 10px; border-radius: 8px;
box-shadow: 0 2px 8px rgba(0,0,0,0.15); font-family: system-ui, sans-serif; font-size: 14px;
"""

# =========================
# ==== UTIL FUNCTIONS =====
# =========================

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


def fetch_rows(url: str) -> List[Dict[str, Any]]:
    """GET API and extract the list under data.tableData (adjust here if backend changes)."""
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    j = r.json()
    return [row for row in j.get("data", {}).get("tableData", []) if isinstance(row, dict)]


def choose_coords(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Priority: SIM7600G coords; fallback: Station Metadata.
    To invert priority, swap the blocks below.
    """
    lat = to_float(row.get(KEY_SIM_LAT))
    lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None:
        return lat, lon

    lat = to_float(row.get(KEY_META_LAT))
    lon = to_float(row.get(KEY_META_LON))
    return lat, lon


def clamp_pm25(v: float) -> float:
    """Clamp PM2.5 to the configured colormap domain to avoid out-of-range artifacts."""
    return max(PM_BREAKS[0], min(PM_BREAKS[-1], float(v)))


def build_popup(row: Dict[str, Any], lat: float, lon: float, pm25_val: float) -> str:
    """HTML popup content. Adjust fields/text here."""
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
    return {i/(steps-1): cm(cm.vmin + (cm.vmax-cm.vmin)*i/(steps-1))
            for i in range(steps)}


# =========================
# ======== MAIN ===========
# =========================

def main():
    # CLI: python script.py [url] [out_html]
    url = API_URL_DEFAULT
    out_html = None

    if len(sys.argv) >= 2 and sys.argv[1].startswith("http"):
        url = sys.argv[1]
    if len(sys.argv) >= 3 and sys.argv[2] != "-":
        out_html = sys.argv[2]

    print(f"[INFO] Fetching: {url}")
    rows_raw = fetch_rows(url)
    if not rows_raw:
        print("[ERROR] No rows under data.tableData")
        sys.exit(1)

    # ---------- Exports: RAW (all rows/columns as-is) ----------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_csv  = f"hiripro_raw_{ts}.csv"
    raw_xlsx = f"hiripro_raw_{ts}.xlsx"
    pd.DataFrame(rows_raw).to_csv(raw_csv, index=False)
    pd.DataFrame(rows_raw).to_excel(raw_xlsx, index=False)

    # ---------- Build PLOTTED dataset (requires lat, lon, pm25) ----------
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

    if not plotted_records:
        print("[WARN] No valid (lat, lon, pm25) -> map will not be created.")
        print(f"[OK] RAW exported: {raw_csv}, {raw_xlsx}")
        sys.exit(0)

    df = pd.DataFrame(plotted_records)

    # ---------- Color map: one scale for points, legend, and heatmap ----------
    # If you prefer continuous, use LinearColormap(PM_COLORS, vmin=..., vmax=...) without .to_step()
    cmap = LinearColormap(colors=PM_COLORS, vmin=PM_BREAKS[0], vmax=PM_BREAKS[-1]).to_step(index=PM_BREAKS)
    cmap.caption = COLORBAR_CAPTION
    heat_gradient = gradient_from_cmap(cmap, steps=HEAT_GRADIENT_STEPS)

    # ---------- Folium map ----------
    # Initial center (will be overridden by fit_bounds)
    fmap = folium.Map(location=[df["lat"].iloc[0], df["lon"].iloc[0]], zoom_start=16, control_scale=True)

    # Points layer
    fg_points = folium.FeatureGroup(name="PM2.5 points", overlay=True, control=True)
    for _, r in df.iterrows():
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
            color=color,                 # stroke color matches legend
            weight=1,
            fill=True,
            fill_color=color,            # fill color matches legend
            fill_opacity=MARKER_OPACITY,
        ).add_to(fg_points)

    fg_points.add_to(fmap)

    # HeatMap layer (same scale via gradient)
    HeatMap(
        df.assign(pm25=df["pm25"].apply(clamp_pm25))[["lat", "lon", "pm25"]].values.tolist(),
        name="HeatMap PM2.5",
        min_opacity=HEAT_MIN_OPACITY,
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

    # Fit to all points (auto-zoom)
    sw = [float(df["lat"].min()), float(df["lon"].min())]
    ne = [float(df["lat"].max()), float(df["lon"].max())]
    fmap.fit_bounds([sw, ne], padding=(20, 20))

    # ---------- Exports: PLOTTED ----------
    plotted_csv  = f"hiripro_plotted_{ts}.csv"
    plotted_xlsx = f"hiripro_plotted_{ts}.xlsx"
    df.to_csv(plotted_csv, index=False)
    df.to_excel(plotted_xlsx, index=False)

    # ---------- Download toolbar (embedded links) ----------
    toolbar_html = f"""
    <div style="{TOOLBAR_CSS}">
      <b>Downloads</b><br>
      <div style="margin-top:6px;">
        <span style="font-weight:600;">Raw:</span>
        <a href='{raw_csv}' download>CSV</a> |
        <a href='{raw_xlsx}' download>Excel</a>
      </div>
      <div style="margin-top:4px;">
        <span style="font-weight:600;">Plotted:</span>
        <a href='{plotted_csv}' download>CSV</a> |
        <a href='{plotted_xlsx}' download>Excel</a>
      </div>
    </div>
    """
    folium.Element(toolbar_html).add_to(fmap)

    # ---------- Save HTML ----------
    if not out_html:
        out_html = f"map_pm25_{ts}.html"
    fmap.save(out_html)

    print(f"[OK] Map    : {out_html}")
    print(f"[OK] RAW    : {raw_csv} | {raw_xlsx}")
    print(f"[OK] PLOTTED: {plotted_csv} | {plotted_xlsx}")
    print(f"[INFO] Points plotted: {len(df)} / Rows total: {len(rows_raw)}")


if __name__ == "__main__":
    main()
