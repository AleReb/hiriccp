# HIRI CCP - Air Quality Monitoring System

Real-time air quality monitoring and visualization system for PM2.5 concentration data from HIRI sensor network.

## Description

This project provides a comprehensive web-based platform for monitoring and analyzing air quality data from multiple sensor devices. The system features real-time data collection, interactive map visualization, and multi-device support.

## Features

- **Real-time Data Collection**: Automatic background polling from sensor API
- **Interactive Map Visualization**: Leaflet-based maps with:
  - Marker clustering for large datasets
  - Heatmap layer
  - Color-coded PM2.5 visualization
  - Fullscreen and minimap controls
- **Multi-Device Support**: View data from all devices or filter by specific sensor
- **WebSocket Updates**: Live data streaming with automatic map updates
- **Day-based Cache**: Efficient JSONL-based storage system
- **Data Export**: Download data in CSV or Excel format
- **Responsive UI**: Resizable control panel with comprehensive controls

## Project Structure

```
hiriccp/
├── servermapv3/              # Main application (v3 - Refactored)
│   ├── servermapv3.py        # Flask backend with SocketIO
│   ├── codigomapa.js         # Client-side JavaScript
│   ├── styles.css            # CSS styling
│   ├── index.html            # HTML template reference
│   └── cache/                # Day-based data cache (JSONL files)
├── serverMapv2.py            # Previous version (monolithic)
├── mapgenerator.py           # Static map generator
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/AleReb/hiriccp.git
   cd hiriccp
   ```

2. **Create a virtual environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Server

```bash
cd servermapv3
python servermapv3.py
```

The server will:
- Start on `http://127.0.0.1:5000`
- Automatically scan and load all devices from cache
- Start background collectors for each device
- Provide WebSocket support for real-time updates

### Accessing the Web Interface

Open your browser and navigate to:
- **All devices**: `http://127.0.0.1:5000/map`
- **Specific device**: `http://127.0.0.1:5000/map?device_code=HIRIPRO-01`

### Control Panel Features

- **Project/Device/Table Filters**: Configure data source
- **Limit/Offset Controls**: Pagination for page mode
- **Day Selector**: Choose specific days or load latest
- **Live Mode**: Enable real-time updates with adaptive polling
- **Admin Controls**: Reindex cache, purge data, view logs
- **Downloads**: Export current page or full day data

## Architecture

### Backend (servermapv3.py)

- **Flask + SocketIO**: Web server with WebSocket support
- **Background Collectors**: Per-device threads for continuous data fetching
- **Day Cache System**: JSONL files organized by project/device/table/day
- **REST API Endpoints**:
  - `/map`: Main application view
  - `/api/day-index`: List available days
  - `/api/data`: Fetch data (page or day mode)
  - `/download/<kind>.<ext>`: Export data
  - `/admin/*`: Administrative functions

### Frontend (codigomapa.js)

- **Leaflet Integration**: Interactive maps with plugins
- **Marker Clustering**: Auto-switches at 100+ points
- **WebSocket Client**: Real-time updates via Socket.IO
- **Adaptive Polling**: Intelligent fallback with backoff
- **Dynamic UI**: Responsive controls and status indicators

## Configuration

Edit `servermapv3.py` to configure:

```python
DEFAULT_PROJECT_ID = "18"
DEFAULT_TABLA = "datos"
DEFAULT_LIMIT = 500
HEAD_POLL_SECONDS = 30  # Real-time polling interval
```

## API Integration

The system connects to:
```
https://api-sensores.cmasccp.cl/listarDatosEstructuradosV2
```

Data schema includes:
- PM2.5, PM1.0, PM10 concentrations
- GPS coordinates (SIM7600G)
- Temperature and humidity
- Battery voltage
- Signal quality

## Development

### Version History

- **v3 (Current)**: Modular architecture with auto-loading
  - Separated code into distinct files
  - Automatic device discovery
  - Enhanced multi-device support
  - Fixed data loading issues

- **v2**: Monolithic server with manual device selection

- **v1**: Static map generator

### Key Improvements in v3

- Pre-loads all devices and days on startup
- Eliminates need for manual device selection
- Unified view of all sensors
- Better debugging with detailed logging
- Fixed clustering and counter bugs
- Improved "Refresh days" functionality

## License

This project is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License. See the [LICENSE](LICENSE) file for details.

## Contributors

- **Ale Rebolledo** (arebolledo@udd.cl)
- Co-Authored-By: todos los que le dan fork y son remergeados al reppo!

## Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
