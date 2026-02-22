# Getting Started with Slicks Telemetry

Welcome to the **Slicks Telemetry** package. This guide will help you install the package and write your first script to analyze race data.

## 1. Installation

### For Users
To install the latest stable version directly from GitHub:

```bash
pip install slicks
```

### For Developers (Contributing)
If you want to modify the package source code:

```bash
git clone https://github.com/Western-Formula-Racing/wfr-telemetry.git
cd wfr-telemetry
pip install -e .
```

---

## 2. Quick Start

Here is the minimal code needed to connect to the database and download data for a specific sensor.

### Step 1: Import and Configure
The package connects to the InfluxDB database automatically using defaults, but you can configure it explicitly.

```python
import slicks
from datetime import datetime

# Optional: Configure manually (or use .env file / defaults)
slicks.connect_influxdb3(
    url="http://your-influx-server:8086",
    token="your-token-here", # Ask Data Lead for your token
    org="Docs",
    db="WFR25"
)
```

### Step 2: Define Time Range
Always use Python's `datetime` objects.

```python
start = datetime(2025, 9, 28, 12, 0, 0) # Sept 28, 2025 at 12:00 PM
end   = datetime(2025, 9, 28, 14, 0, 0) # Sept 28, 2025 at 02:00 PM
```

### Step 3: Fetch Data
You can request a single sensor or a list of sensors.

```python
# Fetch Motor Speed
df = slicks.fetch_telemetry(start, end, "INV_Motor_Speed")

if df is not None:
    print(df.head())
    print(f"Average Speed: {df['INV_Motor_Speed'].mean():.2f}")
else:
    print("No data found for this range.")
```

---

## 3. Key Concepts

### Movement Filtering (Default: On)
By default, `fetch_telemetry` filters out data when the car is stationary (idling in the pits). This ensures your averages (like average speed or temp) represent **driving conditions**.

To see raw data (including pit/idle time), pass `filter_movement=False`:

```python
df_raw = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", filter_movement=False)
```

### Auto-Resampling
By default, data is aligned to a **1-second frequency** (`1s`). This makes it easy to plot multiple sensors on the same graph without worrying about mismatched timestamps.

To get **raw data without resampling**, pass `resample=None`:

```python
df_raw = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample=None)
```

You can also specify a **custom frequency** using any pandas frequency string:

```python
# High-resolution data (100ms)
df_fast = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample="100ms")

# Lower resolution (5 seconds)
df_slow = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample="5s")
```

---

## 4. What's Next?

Once you're comfortable fetching and filtering data, explore the analysis modules:

- **[Data Scanner](scanner.md)** — discover when data exists in your database with interactive calendar views.
- **[Battery Analysis](advanced_usage.md#5-battery-pack-analysis)** — monitor cell voltages, detect weak cells, and assess pack health via `slicks.battery`.
- **[Vehicle Dynamics](advanced_usage.md#6-vehicle-dynamics-calculations)** — calculate G-force and estimate speed from RPM via `slicks.calculations`.
- **[Advanced Usage](advanced_usage.md)** — bulk exports, environment management, movement detection tuning, and more.
- **[API Reference](api_reference.md)** — full function signatures and return types.
