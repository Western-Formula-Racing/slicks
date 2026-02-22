# Advanced Usage & Workflows

## 1. Dynamic Sensor Discovery
Not sure what sensors are available for a specific test day? Don't guess. Use the discovery tool.

```python
from slicks import discover_sensors
from datetime import datetime

start = datetime(2025, 9, 28)
end = datetime(2025, 9, 30)

# This physically queries the DB to find what tags exist
available_sensors = discover_sensors(start, end)

print(f"Found {len(available_sensors)} sensors:")
for sensor in available_sensors:
    print(f" - {sensor}")
```

## 2. Managing Environments
You often need to switch between `Development`, `Testing`, and `Production` databases, or switch to a local replay server.

### Configuration Precedence

Settings are resolved in the following order (highest priority wins):

1. **Runtime calls** — `slicks.connect_influxdb3(url=..., db=...)` overrides everything.
2. **Environment variables** — `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_DB`, `INFLUX_SCHEMA`, `INFLUX_TABLE` (loaded from a `.env` file or your shell environment).
3. **Package defaults** — hardcoded fallbacks in `config.py` (localhost, WFR25, etc.).

### Option A: Environment Variables (Best for CI/CD)
Set these in your shell or `.env` file before running python:
```bash
export INFLUX_URL="http://production-server:8086"
export INFLUX_DB="Season2026_Final"
```

### Option B: Runtime Configuration (Best for Scripts/Notebooks)
```python
import slicks

slicks.connect_influxdb3(
    url="http://192.168.1.50:9000",
    db="DynoTest_Day1"
)
```

### Schema & Table Overrides

By default, queries target the table `iox.<database_name>`. If your InfluxDB instance uses a different schema or table name, override them:

```python
slicks.connect_influxdb3(
    db="WFR25",
    schema="public",
    table="telemetry_raw"
)
```

Or via environment variables:
```bash
export INFLUX_SCHEMA="public"
export INFLUX_TABLE="telemetry_raw"
```

## 3. Bulk Export for CSV Analysis
If you need to hand off data to the aerodynamics team who uses Excel/MATLAB, use the bulk fetcher. It handles day-by-day chunking to avoid crashing the computer.

```python
from slicks import bulk_fetch_season

# Exports entire date range to a single CSV
bulk_fetch_season(start, end, output_file="full_weekend_data.csv")
```

## 4. Customizing Movement Detection
If you are analyzing **Charging** or **Static Testing**, the default movement filter will hide your data. Disable it:

```python
# Fetch Battery Current even when car is stopped
df = slicks.fetch_telemetry(
    start, end,
    signals="PackCurrent",
    filter_movement=False
)
```

The movement detector uses `INV_Motor_Speed > 100.0` as its default threshold. You can adjust this when calling the movement functions directly:

```python
from slicks import get_movement_segments, detect_movement_ratio

# Use a lower threshold for slow-speed testing
segments = get_movement_segments(df, threshold=50.0)
ratio = detect_movement_ratio(df, threshold=50.0)
```

## 5. Battery Pack Analysis

The `slicks.battery` module provides tools for monitoring cell-level health across the battery pack. It works on any DataFrame that contains cell voltage columns matching the pattern `M*_Cell*_Voltage`.

### Fetching Cell Voltage Data

First, discover and fetch the cell voltage signals:

```python
import slicks
from datetime import datetime

start = datetime(2025, 9, 28, 14, 0)
end = datetime(2025, 9, 28, 16, 0)

# Find all cell voltage sensors
all_sensors = slicks.discover_sensors(start, end)
cell_sensors = [s for s in all_sensors if "Cell" in s and "Voltage" in s]

print(f"Found {len(cell_sensors)} cell voltage sensors")

# Fetch with movement filter off (battery data matters at all times)
df = slicks.fetch_telemetry(start, end, signals=cell_sensors, filter_movement=False)
```

### Cell Statistics

Get min, max, average, and imbalance at every timestamp:

```python
from slicks import battery

stats = battery.get_cell_statistics(df)

print(stats.head())
#                      min_cell_voltage  max_cell_voltage  avg_cell_voltage  pack_imbalance lowest_cell_name
# time
# 2025-09-28 14:00:01             3.42              3.65              3.55            0.23  M3_Cell12_Voltage
# 2025-09-28 14:00:02             3.41              3.64              3.54            0.23  M3_Cell12_Voltage
```

### Identifying Weak Cells

Find which cells are most frequently the lowest in the pack:

```python
weak = battery.identify_weak_cells(df)
print(weak.head())
#           cell_name  count  percentage
# 0  M3_Cell12_Voltage   1820       45.5
# 1   M1_Cell8_Voltage    640       16.0
# 2   M5_Cell3_Voltage    540       13.5
```

A cell that appears at the top with a high percentage is a candidate for physical inspection.

### Pack Health Summary

Get a single-call overview:

```python
health = battery.get_pack_health(df)
print(health)
# {
#     'max_imbalance': 0.35,
#     'avg_imbalance': 0.12,
#     'weakest_cell': 'M3_Cell12_Voltage',
#     'min_pack_voltage': 2.95
# }
```

## 6. Vehicle Dynamics Calculations

The `slicks.calculations` module derives physical quantities from raw sensor data.

### G-Force (Friction Circle)

Calculate combined lateral and longitudinal G-force from accelerometer data:

```python
from slicks import calculations

df = slicks.fetch_telemetry(start, end, signals=["Accel_X", "Accel_Y"])

g_force = calculations.calculate_g_sum(df)

print(f"Peak G-force: {g_force.max():.2f} G")
print(f"Average G-force: {g_force.mean():.2f} G")
```

The default scaling assumes DBC-scaled values (`lsb_per_g=81.92`). If your accelerometer data is raw LSBs without DBC scaling, pass `lsb_per_g=8192.0`:

```python
g_force = calculations.calculate_g_sum(df, lsb_per_g=8192.0)
```

### Speed from RPM

Estimate vehicle speed when a dedicated speed sensor is not available:

```python
from slicks import calculations

df = slicks.fetch_telemetry(start, end, signals=["INV_Motor_Speed"])

# Example: 8-inch tire radius, 3.5:1 final drive
speed_mps = calculations.estimate_speed_from_rpm(
    df,
    tire_radius_m=0.2032,
    gear_ratio=3.5,
    rpm_col="INV_Motor_Speed"
)

# Convert m/s to km/h
speed_kph = speed_mps * 3.6
print(f"Top speed: {speed_kph.max():.1f} km/h")
```

The function automatically falls back to `INV_Motor_Speed` if the specified `rpm_col` is not found in the DataFrame.

## 7. Resampling Control

By default, `fetch_telemetry` resamples data to **1-second** intervals. You can change this behavior:

```python
# High-resolution data (100ms intervals)
df_fast = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample="100ms")

# Lower resolution (5-second intervals)
df_slow = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample="5s")

# Raw data — no resampling, original timestamps preserved
df_raw = slicks.fetch_telemetry(start, end, "INV_Motor_Speed", resample=None)
```

Any valid [pandas frequency string](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases) is accepted.
