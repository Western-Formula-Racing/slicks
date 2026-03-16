# Advanced Usage & Workflows

## 1. Wide vs Narrow Format

The database stores telemetry in **wide format**: each CAN signal is its own column. This is faster to query and requires no pivot step.

```python
import slicks
from datetime import datetime

start = datetime(2025, 9, 28, 12, 0)
end   = datetime(2025, 9, 28, 14, 0)

# Wide format (default, preferred) — direct column access
df = slicks.fetch_telemetry(start, end, ["INV_Motor_Speed", "PackCurrent"], schema="wide")

# Narrow format (legacy EAV) — only for old data that was never migrated
df = slicks.fetch_telemetry(start, end, ["INV_Motor_Speed", "PackCurrent"], schema="narrow")
```

Use `schema="wide"` for all new work.

---

## 2. Dynamic Sensor Discovery
Not sure what sensors are available? With wide format, discovery is instant — it reads column metadata rather than scanning data rows.

```python
from slicks import discover_sensors

# Wide: instant metadata lookup (no time range needed)
available_sensors = discover_sensors(None, None, schema="wide")

print(f"Found {len(available_sensors)} sensors:")
for sensor in available_sensors:
    print(f" - {sensor}")
```

## 3. Managing Environments
You often need to switch between `Development`, `Testing`, and `Production` databases, or switch to a local replay server.

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

## 4. Bulk Export for CSV Analysis
If you need to hand off data to the aerodynamics team who uses Excel/MATLAB, use the bulk fetcher. It handles day-by-day chunking to avoid crashing the computer.

```python
from slicks import bulk_fetch_season

# Exports entire date range to a single CSV
bulk_fetch_season(start, end, output_file="full_weekend_data.csv")
```

## 5. Writing CAN Data (Wide Format)

If you're ingesting raw CAN bus data (e.g., from a replay script or live logger), use `WideWriter`. It decodes CAN frames using a DBC file and writes them as wide format line protocol.

```python
from slicks import WideWriter

writer = WideWriter(
    url="http://localhost:8086",
    token="my-token",
    bucket="WFR26",
    measurement="WFR26",
    dbc_path="path/to/WFR26.dbc",
)

# Decode and queue a CAN frame
writer.decode_and_queue(can_id=0x200, data=bytes([0x01, 0x02, ...]), ts_ns=timestamp_ns)

# Flush remaining data when done
writer.close()
```

Each decoded CAN message becomes one row with all of its signals as fields:
```
WFR26,messageName=BMS_Status,canId=512 PackCurrent=-3264.0,SOC=85.0 1700000000000000000
```

## 6. Customizing Movement Detection
If you are analyzing **Charging** or **Static Testing**, the default movement filter will hide your data. Disable it:

```python
# Fetch Battery Current even when car is stopped
df = slicks.fetch_telemetry(
    start, end,
    signals="PackCurrent",
    filter_movement=False
)
```
