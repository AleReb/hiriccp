# -*- coding: utf-8 -*-
"""
Fetch structured sensor data (HIRIPRO-01) and render a Folium map with PM2.5 markers.
Author: cmasu

- Pulls JSON from:
  https://api-sensores.cmasccp.cl/listarDatosEstructurados?tabla=datos&disp.id_proyecto=18&disp.codigo_interno=HIRIPRO-01
- Expects payload like:
  {"status":"success","data":{"tableData":[ {...}, {...}, ... ]}}

Notes:
- Coordinates priority: use SIM7600G lat/lon if present; otherwise use Metadatos Estacion lat/lon.
- PM2.5 key: "PMS5003 [Material particulado PM 2.5 (µg/m³)]"
- Time key: "fecha" (ISO string)
"""

import sys
import json
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
import folium

API_URL_DEFAULT = (
    "https://api-sensores.cmasccp.cl/listarDatosEstructurados"
    "?tabla=datos&disp.id_proyecto=18&disp.codigo_interno=HIRIPRO-01"
)

# --- Fixed keys present in the provided schema ---
KEY_TIME = "fecha"
KEY_PM25 = "PMS5003 [Material particulado PM 2.5 (µg/m³)]"

# Preferred GNSS source
KEY_SIM_LAT = "SIM7600G [Latitud (°)]"
KEY_SIM_LON = "SIM7600G [Longitud (°)]"
KEY_SIM_CSQ = "SIM7600G [Intensidad señal telefónica (Adimensional)]"
KEY_SIM_SATS = "SIM7600G [Satelites (int)]"
KEY_SIM_SPEED = "SIM7600G [Velocidad_km/h (km/h)]"

# Fallback station metadata
KEY_META_LAT = "Metadatos Estacion [Latitud (°)]"
KEY_META_LON = "Metadatos Estacion [Longitud (°)]"

# (Optional) extra context keys
KEY_PM1 = "PMS5003 [Material particulado PM 1.0 (µg/m³)]"
KEY_PM10 = "PMS5003 [Material particulado PM 10 (µg/m³)]"
KEY_HUM = "PMS5003 [Humedad (%)]"
KEY_TEMP = "PMS5003 [Grados celcius (°C)]"
KEY_VBAT = "Divisor de Voltaje [Voltaje (V)]"


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)):
            if isinstance(x, float) and math.isnan(x):
                return None
            return float(x)
        s = str(x).strip().replace(",", ".")
        if s == "" or s.lower() in {"nan", "null", "none"}:
            return None
        # remove common units if any slipped in
        for tok in ["µg/m³", "ug/m3", "ug m3", "km/h", "V", "%", "°C"]:
            s = s.replace(tok, "").strip()
        return float(s)
    except Exception:
        return None


def to_int(x: Any) -> Optional[int]:
    f = to_float(x)
    return int(round(f)) if f is not None else None


def parse_epoch_ms(iso_or_epoch: Any) -> Optional[int]:
    """Return epoch ms from ISO string or numeric epoch."""
    if iso_or_epoch is None:
        return None
    # numeric epoch path
    if isinstance(iso_or_epoch, (int, float)):
        v = float(iso_or_epoch)
        if v > 1e12:   # already ms
            return int(v)
        if v > 1e6:    # seconds
            return int(v * 1000)
        return None
    # string path
    s = str(iso_or_epoch).strip()
    if s.isdigit():
        return parse_epoch_ms(float(s))
    # Try ISO formats
    try:
        # Python is flexible with fromisoformat for 'YYYY-MM-DDTHH:MM:SS'
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    except Exception:
        pass
    # Fallback known patterns
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.timestamp() * 1000)
        except Exception:
            continue
    return None


def pm25_color(pm25: int) -> str:
    """AQI-like buckets for PM2.5 (µg/m³). Adjust thresholds if needed."""
    if pm25 <= 12:
        return "green"
    elif pm25 <= 35:
        return "lightgreen"
    elif pm25 <= 55:
        return "orange"
    elif pm25 <= 150:
        return "red"
    else:
        return "darkred"


def fetch_payload(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def extract_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Expected path: payload["data"]["tableData"] -> list of dicts
    try:
        data = payload.get("data", {})
        rows = data.get("tableData", [])
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return []
    except Exception:
        return []


def choose_coords(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str]:
    """Return (lat, lon, source_label). Prefer SIM7600G; else fall back to Metadatos."""
    lat = to_float(row.get(KEY_SIM_LAT))
    lon = to_float(row.get(KEY_SIM_LON))
    if lat is not None and lon is not None:
        return lat, lon, "SIM7600G"

    lat = to_float(row.get(KEY_META_LAT))
    lon = to_float(row.get(KEY_META_LON))
    if lat is not None and lon is not None:
        return lat, lon, "Metadatos"

    return None, None, "N/A"


def format_popup(row: Dict[str, Any], ts_ms: Optional[int], lat: float, lon: float, pm25_val: int) -> str:
    ts_str = datetime.fromtimestamp(ts_ms / 1000).isoformat(sep=" ") if ts_ms else str(row.get(KEY_TIME, "N/A"))

    pm1 = row.get(KEY_PM1)
    pm10 = row.get(KEY_PM10)
    hum = row.get(KEY_HUM)
    temp = row.get(KEY_TEMP)
    vbat = row.get(KEY_VBAT)

    csq = row.get(KEY_SIM_CSQ)
    sats = row.get(KEY_SIM_SATS)
    spd = row.get(KEY_SIM_SPEED)

    def safe(s: Any) -> str:
        return "-" if s in (None, "", "null") else str(s)

    html = (
        f"<b>PM2.5:</b> {pm25_val} µg/m³<br>"
        f"<b>Time:</b> {ts_str}<br>"
        f"<b>Lat:</b> {lat:.6f}, <b>Lon:</b> {lon:.6f}<br>"
        f"<hr style='margin:4px 0'/>"
        f"<b>PM1:</b> {safe(pm1)} | <b>PM10:</b> {safe(pm10)}<br>"
        f"<b>Temp PMS:</b> {safe(temp)} °C | <b>Hum:</b> {safe(hum)} %<br>"
        f"<b>VBat:</b> {safe(vbat)} V<br>"
        f"<b>CSQ:</b> {safe(csq)} | <b>Sats:</b> {safe(sats)} | <b>Speed:</b> {safe(spd)} km/h"
    )
    return html


def build_map(points: List[Tuple[int, float, float, int, Dict[str, Any]]], zoom_start: int = 14) -> folium.Map:
    center_lat, center_lon = points[0][1], points[0][2]
    fmap = folium.Map(location=[center_lat, center_lon], zoom_start=zoom_start, control_scale=True)

    for ts_ms, lat, lon, pm25_val, row in points:
        color = pm25_color(pm25_val)
        popup_html = format_popup(row, ts_ms, lat, lon, pm25_val)

        folium.CircleMarker(
            location=[lat, lon],
            radius=6,
            popup=folium.Popup(popup_html, max_width=320),
            color=color,
            fill=True,
            fill_opacity=0.85,
            weight=1,
        ).add_to(fmap)

    return fmap


def main():
    url = API_URL_DEFAULT
    out_name = None
    limit_last = None  # e.g., 500 to plot last N points

    # Simple CLI: python script.py [url] [out_name] [limit_last]
    if len(sys.argv) >= 2 and sys.argv[1].startswith("http"):
        url = sys.argv[1]
    if len(sys.argv) >= 3 and sys.argv[2] != "-":
        out_name = sys.argv[2]
    if len(sys.argv) >= 4:
        try:
            limit_last = int(sys.argv[3])
        except Exception:
            limit_last = None

    print(f"[INFO] Fetching: {url}")
    payload = fetch_payload(url)
    rows = extract_records(payload)

    if not rows:
        print("[ERROR] No records found at data.tableData")
        sys.exit(1)

    points: List[Tuple[int, float, float, int, Dict[str, Any]]] = []
    for row in rows:
        # Coordinates
        lat, lon, _src = choose_coords(row)
        if lat is None or lon is None:
            continue

        # PM2.5
        pm25_val_f = to_float(row.get(KEY_PM25))
        if pm25_val_f is None:
            continue
        pm25_val = int(round(pm25_val_f))

        # Time
        ts_ms = parse_epoch_ms(row.get(KEY_TIME))

        points.append((ts_ms or 0, lat, lon, pm25_val, row))

    if not points:
        print("[ERROR] No valid (lat, lon, pm2.5) tuples parsed.")
        sys.exit(1)

    # Sort by time, then optionally keep last N
    points.sort(key=lambda x: x[0])
    if limit_last is not None and limit_last > 0 and len(points) > limit_last:
        points = points[-limit_last:]

    fmap = build_map(points, zoom_start=14)

    if not out_name:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"map_pm25_{ts}.html"

    fmap.save(out_name)
    print(f"[OK] Map generated: {out_name}")
    print(f"[INFO] Plotted points: {len(points)}")


if __name__ == "__main__":
    main()
