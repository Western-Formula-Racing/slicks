# API Reference

This document details the functions available in the `slicks` package.

## Core Functions

### `slicks.connect_timescaledb`

Updates the global TimescaleDB connection settings dynamically.

```python
slicks.connect_timescaledb(dsn=None, None, org=None, table=None)
```
- **url** *(str)*: The TimescaleDB host URL (e.g., `"http://localhost:8086"`).
- **token** *(str)*: Authentication token.
- **org** *(str)*: Organization name (default: `"Docs"`).
- **db** *(str)*: Database/Bucket name (default: `"WFR25"`).

---

### `slicks.fetch_telemetry`

The primary function to retrieve data. It handles querying, resampling, and movement filtering.

```python
slicks.fetch_telemetry(start_time, end_time, signals=None, client=None,
                       filter_movement=True, resample="1s", schema="wide")
```

- **start_time** *(datetime)*: Start of the query range.
- **end_time** *(datetime)*: End of the query range.
- **signals** *(str or list[str])*: A single sensor name or a list of sensor names to fetch. Defaults to standard configuration if `None`.
- **client** *(TimescaleDBClient3, optional)*: An existing client instance (advanced use).
- **filter_movement** *(bool)*: If `True` (default), strips out rows where the car is stationary.
- **resample** *(str or None)*: Pandas frequency string for resampling (e.g. `"1s"`, `"100ms"`). Pass `None` for raw data.
- **schema** *(str)*: `"wide"` (default, columnar — each signal is a column) or `"narrow"` (legacy EAV — requires pivot).

**Returns:** `pandas.DataFrame` indexed by time. Returns `None` if no data is found.

---

### `slicks.fetch_telemetry_chunked`

Same interface as `fetch_telemetry`, but splits large date ranges into chunks and runs them in parallel. Handles server resource limits automatically via adaptive query bisection.

```python
slicks.fetch_telemetry_chunked(start_time, end_time, signals=None, client=None,
                                filter_movement=True, resample="1s", schema="wide",
                                chunk_size=timedelta(hours=6), max_workers=4)
```

- **chunk_size** *(timedelta)*: Window size per chunk (default: 6 hours).
- **max_workers** *(int)*: Number of parallel threads (default: 4).

**Returns:** `pandas.DataFrame` concatenated from all chunks, or `None`.

---

### `slicks.discover_sensors`

Returns the list of available sensor/signal names.

```python
slicks.discover_sensors(start_time, end_time, chunk_size_days=7,
                        client=None, show_progress=True, schema="wide")
```

- **start_time** *(datetime)*: Start of scan (used only in `"narrow"` schema).
- **end_time** *(datetime)*: End of scan (used only in `"narrow"` schema).
- **chunk_size_days** *(int)*: Days per chunk for narrow schema scans (default: 7).
- **schema** *(str)*: `"wide"` performs an instant metadata lookup (`information_schema.columns`) — no time range required. `"narrow"` scans actual data rows.

**Returns:** `list[str]` of unique sensor names sorted alphabetically.

---

## Analysis Tools

### `slicks.get_movement_segments`

Identifies distinct "laps" or driving sessions by detecting gaps in movement.

```python
slicks.get_movement_segments(df, speed_column="INV_Motor_Speed", threshold=100.0, max_gap_seconds=60.0)
```

- **df** *(pd.DataFrame)*: DataFrame containing at least a speed column.
- **max_gap_seconds** *(float)*: Time in seconds to wait before declaring a new "segment" (default: 60s).

**Returns:** `pandas.DataFrame` with columns `start_time`, `end_time`, `duration`, `state` ("Moving"/"Idle"), and `mean_speed`.

### `slicks.detect_movement_ratio`

Calculates the percentage of time the car was active.

```python
slicks.detect_movement_ratio(df, speed_column="INV_Motor_Speed")
```

**Returns:** `dict` containing `total_rows`, `moving_rows`, `idle_rows`, and `movement_ratio` (0.0 - 1.0).

---

## Wide Format Writing

### `slicks.WideWriter`

Encodes CAN frames to TimescaleDB wide format line protocol and writes them in batches.

```python
from slicks import WideWriter

writer = WideWriter(
    url,                    # TimescaleDB URL
    token,                  # Auth token
    bucket,                 # Bucket/database name (e.g. "WFR26")
    measurement,            # Measurement name (e.g. "WFR26")
    dbc_path=None,          # Path to DBC file (or set WFR_DBC_PATH env var)
    batch_size=5000,        # Points per write batch
)
```

**Methods:**

- `decode_and_queue(can_id, data, ts_ns)` — Decode raw CAN bytes and queue for batch write.
- `write_lines(lines)` — Write pre-formatted line protocol strings directly.
- `flush()` — Flush the pending batch.
- `close()` — Flush and close the connection.

**Line protocol format:**
```
WFR26,messageName=BMS_Status,canId=512 PackCurrent=-3264.0,SOC=85.0 1700000000000000000
```

---

## CAN Decoding

### `slicks.decode_frame`

Decodes a raw CAN frame into named signals using a DBC database.

```python
from slicks import load_dbc, decode_frame

db = load_dbc("path/to/WFR26.dbc")
frame = decode_frame(db, can_id, raw_bytes)  # → DecodedFrame or None
```

**`DecodedFrame` fields:**
- `message_name` *(str)*: CAN message name from the DBC.
- `can_id` *(int)*: CAN frame ID.
- `signals` *(dict[str, float])*: Decoded signal values.

### `slicks.frame_to_line_protocol`

Converts a `DecodedFrame` to an TimescaleDB line protocol string.

```python
from slicks import frame_to_line_protocol

line = frame_to_line_protocol(frame, measurement="WFR26", timestamp_ns=ts)
```
