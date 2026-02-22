# API Reference

This document details the functions available in the `slicks` package.

## Core Functions

### `slicks.connect_influxdb3`

Updates the global InfluxDB connection settings dynamically.

```python
slicks.connect_influxdb3(url=None, token=None, org=None, db=None, schema=None, table=None)
```
- **url** *(str)*: The InfluxDB host URL (e.g., `"http://localhost:8086"`).
- **token** *(str)*: Authentication token.
- **org** *(str)*: Organization name (default: `"Docs"`).
- **db** *(str)*: Database/Bucket name (default: `"WFR25"`).
- **schema** *(str)*: IOx schema name (default: `"iox"`).
- **table** *(str)*: IOx table name (default: same as `db`).

**Configuration Precedence:**

Settings are resolved in the following order (highest priority first):

1. **Runtime** — values passed to `connect_influxdb3()`.
2. **Environment variables** — `INFLUX_URL`, `INFLUX_TOKEN`, `INFLUX_ORG`, `INFLUX_DB`, `INFLUX_SCHEMA`, `INFLUX_TABLE` (loaded from `.env` or shell).
3. **Defaults** — hardcoded fallbacks in `config.py`.

---

### `slicks.fetch_telemetry`

The primary function to retrieve data. It handles querying, pivoting, resampling, and movement filtering.

```python
slicks.fetch_telemetry(start_time, end_time, signals=None, client=None, filter_movement=True, resample="1s")
```

- **start_time** *(datetime)*: Start of the query range.
- **end_time** *(datetime)*: End of the query range.
- **signals** *(str or list[str])*: A single sensor name or a list of sensor names to fetch. Defaults to standard configuration if `None`.
- **client** *(InfluxDBClient3, optional)*: An existing client instance (advanced use).
- **filter_movement** *(bool)*: If `True` (default), strips out rows where the car is stationary. If `False`, returns all raw data.
- **resample** *(str or None)*: Pandas frequency string for resampling (e.g., `"1s"`, `"100ms"`, `"5s"`). Set to `None` to disable resampling and return raw timestamps. Default: `"1s"`.

**Returns:** `pandas.DataFrame` indexed by time, with columns for each requested signal. Returns `None` if no data is found.

---

### `slicks.discover_sensors`

Scans the database to find which sensors actually recorded data during a time period.

```python
slicks.discover_sensors(start_time, end_time, chunk_size_days=1)
```

- **start_time** *(datetime)*: Start of scan.
- **end_time** *(datetime)*: End of scan.
- **chunk_size_days** *(int)*: How many days to query at once (prevents timeouts).

**Returns:** `list[str]` of unique sensor names sorted alphabetically.

---

### `slicks.scan_data_availability`

Scans the database to discover when telemetry data exists. Returns an interactive result object. See the [Data Scanner](scanner.md) page for full details.

```python
slicks.scan_data_availability(start, end, timezone="UTC", table=None, bin_size="hour", include_counts=True, show_progress=True)
```

**Returns:** `ScanResult` object with interactive Jupyter display, export methods, and calendar heatmap.

---

### `slicks.bulk_fetch_season`

Exports telemetry data day-by-day to a single CSV file. Handles chunking automatically to avoid memory issues on large date ranges.

```python
slicks.bulk_fetch_season(start_date, end_date, output_file="telemetry_season.csv")
```

- **start_date** *(datetime)*: First day to export.
- **end_date** *(datetime)*: Last day (exclusive) to export.
- **output_file** *(str)*: Path for the output CSV file.

---

## Analysis Tools

### `slicks.get_movement_segments`

Identifies distinct "laps" or driving sessions by detecting gaps in movement.

```python
slicks.get_movement_segments(df, speed_column="INV_Motor_Speed", threshold=100.0, max_gap_seconds=60.0)
```

- **df** *(pd.DataFrame)*: DataFrame containing at least a speed column.
- **speed_column** *(str)*: Column name representing speed. Default: `"INV_Motor_Speed"`.
- **threshold** *(float)*: Speed value above which the car is considered moving. Default: `100.0` (motor RPM with DBC scaling applied).
- **max_gap_seconds** *(float)*: Time in seconds to wait before declaring a new "segment" (default: 60s).

**Returns:** `pandas.DataFrame` with columns `start_time`, `end_time`, `duration`, `duration_sec`, `state` ("Moving"/"Idle"), `mean_speed`, and `rows`.

---

### `slicks.detect_movement_ratio`

Calculates the percentage of time the car was active.

```python
slicks.detect_movement_ratio(df, speed_column="INV_Motor_Speed", threshold=100.0)
```

- **df** *(pd.DataFrame)*: DataFrame containing telemetry data.
- **speed_column** *(str)*: Column name representing speed.
- **threshold** *(float)*: Speed value above which the car is considered moving.

**Returns:** `dict` containing `total_rows`, `moving_rows`, `idle_rows`, and `movement_ratio` (0.0 - 1.0).

---

### `slicks.filter_data_in_movement`

Filters a DataFrame to keep only rows where the car is actively moving.

```python
slicks.filter_data_in_movement(df, speed_column="INV_Motor_Speed", threshold=100.0)
```

- **df** *(pd.DataFrame)*: DataFrame containing telemetry data.
- **speed_column** *(str)*: Column name representing speed.
- **threshold** *(float)*: Speed value above which the car is considered moving.

**Returns:** `pandas.DataFrame` containing only rows where `speed_column > threshold`.

---

## Battery Analysis — `slicks.battery`

Tools for monitoring battery pack health. These functions operate on DataFrames that contain cell voltage columns matching the pattern `M*_Cell*_Voltage` (e.g., `M1_Cell1_Voltage`, `M5_Cell20_Voltage`).

### `slicks.battery.get_cell_statistics`

Calculates per-timestamp statistics across all cell voltage columns in the pack.

```python
slicks.battery.get_cell_statistics(df)
```

- **df** *(pd.DataFrame)*: DataFrame containing telemetry data with cell voltage columns.

**Returns:** `pandas.DataFrame` with columns:

| Column | Description |
|--------|-------------|
| `min_cell_voltage` | Lowest cell voltage at each timestamp |
| `max_cell_voltage` | Highest cell voltage at each timestamp |
| `avg_cell_voltage` | Mean voltage across all cells |
| `pack_imbalance` | Difference between max and min (delta) |
| `lowest_cell_name` | Name of the cell with the lowest voltage |

---

### `slicks.battery.identify_weak_cells`

Determines which cells spend the most time as the lowest voltage in the pack.

```python
slicks.battery.identify_weak_cells(df)
```

- **df** *(pd.DataFrame)*: DataFrame containing telemetry data.

**Returns:** `pandas.DataFrame` with columns:

| Column | Description |
|--------|-------------|
| `cell_name` | Cell identifier (e.g., `M3_Cell12_Voltage`) |
| `count` | Number of timestamps where this cell was the lowest |
| `percentage` | Percentage of total time spent as the lowest cell |

Rows are sorted by frequency (most common offender first).

---

### `slicks.battery.get_pack_health`

Returns a high-level summary of the battery pack's health over a data window.

```python
slicks.battery.get_pack_health(df)
```

- **df** *(pd.DataFrame)*: DataFrame containing telemetry data.

**Returns:** `dict` with keys:

| Key | Description |
|-----|-------------|
| `max_imbalance` | Highest recorded voltage difference between cells |
| `avg_imbalance` | Average voltage difference over the window |
| `weakest_cell` | The cell that was lowest most often |
| `min_pack_voltage` | The lowest single-cell voltage recorded |

Returns an empty `dict` if no cell voltage columns are found.

---

## Vehicle Dynamics — `slicks.calculations`

Tools for deriving physical quantities from raw sensor data.

### `slicks.calculations.calculate_g_sum`

Calculates the combined G-force magnitude (friction circle usage) from accelerometer data.

```python
slicks.calculations.calculate_g_sum(df, x_col="Accel_X", y_col="Accel_Y", lsb_per_g=81.92)
```

- **df** *(pd.DataFrame)*: DataFrame containing accelerometer data.
- **x_col** *(str)*: Column name for longitudinal acceleration. Default: `"Accel_X"`.
- **y_col** *(str)*: Column name for lateral acceleration. Default: `"Accel_Y"`.
- **lsb_per_g** *(float)*: Scaling factor to convert raw values to G. Default: `81.92`.
    - **Derivation:** 8192 LSB/g (from a +/-4G accelerometer range) × 0.01 (DBC scaling factor) = 81.92.
    - If your data is pure LSBs without DBC scaling, use `8192.0`.

**Returns:** `pandas.Series` representing the vector sum of G-forces (`sqrt(x² + y²)`).

---

### `slicks.calculations.estimate_speed_from_rpm`

Estimates vehicle speed from motor or wheel RPM data.

```python
slicks.calculations.estimate_speed_from_rpm(df, tire_radius_m, gear_ratio=1.0, rpm_col="Right_RPM")
```

- **df** *(pd.DataFrame)*: DataFrame containing RPM data.
- **tire_radius_m** *(float)*: Radius of the tire in meters. **Required.**
- **gear_ratio** *(float)*: Final drive ratio (Motor RPM → Wheel RPM). Use `1.0` if the input column is already wheel RPM. Example: `3.5` means the motor spins 3.5× faster than the wheels. Default: `1.0`.
- **rpm_col** *(str)*: Column name for RPM data. Falls back to `"INV_Motor_Speed"` if the specified column is not found. Default: `"Right_RPM"`.

**Returns:** `pandas.Series` representing estimated speed in **meters per second** (m/s).

---

## Utility Functions

### `slicks.get_influx_client`

Returns a raw `InfluxDBClient3` instance. Useful for advanced queries or direct database access.

```python
slicks.get_influx_client(url=None, token=None, org=None, db=None)
```

**Returns:** `InfluxDBClient3` configured with the current connection settings.

---

### `slicks.list_target_sensors`

Returns the default sensor list configured in `config.py`.

```python
slicks.list_target_sensors()
```

**Returns:** `list[str]` of default sensor names (e.g., `["PackCurrent", "SOC", "INV_Motor_Speed", ...]`).
