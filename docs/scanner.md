# Data Availability Scanner

The `scan_data_availability` function helps you discover when telemetry data exists in your database. It's especially useful in Jupyter notebooks where it provides an interactive, collapsible view of data windows organized by month and day.

## Quick Start

```python
import slicks
from datetime import datetime

# Configure connection first
slicks.connect_influxdb3(
    url="http://your-server:9000",
    token="your-token",
    db="WFR25"
)

# Scan for data availability
result = slicks.scan_data_availability(
    start=datetime(2025, 1, 1),
    end=datetime(2026, 1, 1),
    timezone="America/Toronto"
)

# Display interactive view (just type the variable name in Jupyter)
result
```

## Interactive Views

### Collapsible Tree View

When you display `result` in Jupyter, you get a nested, collapsible view:

- **Months** are shown expanded by default
- **Days** can be clicked to expand/collapse
- **Time windows** show start/end times and row counts

![Inline Calendar View](assets/images/inline_calendar.png)

### Calendar Heatmap

Visualize data density across the year with a GitHub-style calendar:

```python
result.calendar_view()
```

This creates a 12-month grid where darker green means more data was recorded that day.

![Calendar Heatmap](assets/images/heatmap_calendar.png)

## Function Reference

### `slicks.scan_data_availability`

```python
slicks.scan_data_availability(
    start: datetime,
    end: datetime,
    timezone: str = "UTC",
    table: str = None,
    bin_size: str = "hour",
    include_counts: bool = True,
    show_progress: bool = True
) -> ScanResult
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start` | `datetime` | *required* | Start of scan range (UTC or timezone-aware) |
| `end` | `datetime` | *required* | End of scan range |
| `timezone` | `str` | `"UTC"` | Timezone for display (e.g., `"America/Toronto"`) |
| `table` | `str` | `None` | Table to scan (defaults to `"iox.{INFLUX_DB}"`) |
| `bin_size` | `str` | `"hour"` | Granularity: `"hour"` or `"day"` |
| `include_counts` | `bool` | `True` | Include row counts (slightly slower if `True`) |
| `show_progress` | `bool` | `True` | Show progress bar during scan |

**Returns:** `ScanResult` object with interactive display

## ScanResult Methods

### `.to_dict()`

Export as nested dictionary:

```python
data = result.to_dict()
# {'2025-01-15': [{'start_utc': '...', 'end_utc': '...', 'row_count': 1500}, ...], ...}
```

### `.to_dataframe()`

Flatten to pandas DataFrame:

```python
df = result.to_dataframe()
```

| date | start_utc | end_utc | start_local | end_local | row_count | duration_hours |
|------|-----------|---------|-------------|-----------|-----------|----------------|
| 2025-01-15 | 2025-01-15T14:00:00+00:00 | 2025-01-15T16:00:00+00:00 | ... | ... | 1500 | 2.0 |

### `.calendar_view(year=None)`

Generate a heatmap calendar:

```python
fig = result.calendar_view()  # Auto-detects year from data
fig = result.calendar_view(year=2025)  # Specific year
```

### Properties

- `result.days` - List of dates with data (e.g., `['2025-01-15', '2025-01-16', ...]`)
- `result.total_rows` - Total row count across all windows
- `len(result)` - Number of days with data

## Performance Tips

### For Large Date Ranges

Use day-level granularity for faster scans:

```python
result = slicks.scan_data_availability(
    start=datetime(2025, 1, 1),
    end=datetime(2026, 1, 1),
    bin_size="day"  # Much faster than "hour"
)
```

### Skip Row Counts

If you only need to know *when* data exists (not how much):

```python
result = slicks.scan_data_availability(
    start=datetime(2025, 1, 1),
    end=datetime(2025, 2, 1),
    include_counts=False  # Faster
)
```

### Scan Smaller Ranges

For dense months, scan one month at a time:

```python
result = slicks.scan_data_availability(
    start=datetime(2025, 6, 1),
    end=datetime(2025, 7, 1),
    timezone="America/Toronto"
)
```

## Terminal Usage

The scanner also works in regular Python scripts with a text-based display:

```python
result = slicks.scan_data_availability(...)
print(result)
```

Output:
```
Data Availability (America/Toronto)
========================================

📆 January 2025 (3 days, 4,500 rows)
   📅 Day 15 (2 windows, 1,500 rows)
      └─ 09:00 → 11:00 (1,200 rows)
      └─ 14:30 → 16:00 (300 rows)
   📅 Day 16 (1 window, 3,000 rows)
      └─ 10:00 → 14:00 (3,000 rows)

========================================
Total: 3 days, 4,500 rows
```
