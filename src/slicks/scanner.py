"""
Scanner module for discovering data availability windows.

Provides an interactive way to browse what time ranges have telemetry data.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import md5
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from influxdb_client_3 import InfluxDBClient3
from tqdm.auto import tqdm
from zoneinfo import ZoneInfo

from . import config

UTC = timezone.utc


def _quote_table(table: str) -> str:
    """Quote table name for SQL, handling schema.table format."""
    parts = table.split(".", 1)
    if len(parts) == 2:
        return f'"{parts[0]}"."{parts[1]}"'
    return f'"{table}"'


@dataclass
class TimeWindow:
    """A contiguous time window with data."""
    start_utc: datetime
    end_utc: datetime
    start_local: datetime
    end_local: datetime
    row_count: int
    bins: int
    
    def to_dict(self) -> dict:
        return {
            "start_utc": self.start_utc.isoformat(),
            "end_utc": self.end_utc.isoformat(),
            "start_local": self.start_local.isoformat(),
            "end_local": self.end_local.isoformat(),
            "row_count": self.row_count,
            "bins": self.bins,
        }


class ScanResult:
    """
    Holds scan results with environment-aware display.
    
    - In Jupyter: displays as collapsible HTML sections (Month → Day → Windows)
    - In terminal: displays as formatted text tree
    - Programmatic: use .to_dict() or .to_dataframe()
    - Visualization: use .calendar_view() for GitHub-style heatmap
    """
    
    def __init__(self, data: Dict[str, List[TimeWindow]], timezone_name: str):
        self._data = data  # {"2025-01-15": [TimeWindow, ...], ...}
        self._timezone = timezone_name
    
    def __repr__(self) -> str:
        """Terminal/script display - formatted text tree."""
        if not self._data:
            return "No data found in the specified time range."
        
        lines = [f"Data Availability ({self._timezone})", "=" * 40]
        total_rows = 0
        
        # Group by month
        months: Dict[str, Dict[str, List[TimeWindow]]] = defaultdict(dict)
        for day in sorted(self._data.keys()):
            month_key = day[:7]  # "2025-01"
            months[month_key][day] = self._data[day]
        
        for month in sorted(months.keys()):
            days_in_month = months[month]
            month_rows = sum(w.row_count for d in days_in_month.values() for w in d)
            total_rows += month_rows
            
            # Parse month for display
            from datetime import datetime as dt
            month_name = dt.strptime(month, "%Y-%m").strftime("%B %Y")
            lines.append(f"\n📆 {month_name} ({len(days_in_month)} days, {month_rows:,} rows)")
            
            for day in sorted(days_in_month.keys()):
                windows = days_in_month[day]
                day_rows = sum(w.row_count for w in windows)
                day_num = day.split("-")[2]
                lines.append(f"   📅 Day {day_num} ({len(windows)} window{'s' if len(windows) != 1 else ''}, {day_rows:,} rows)")
                
                for w in windows:
                    start_time = w.start_local.strftime("%H:%M")
                    end_time = w.end_local.strftime("%H:%M")
                    lines.append(f"      └─ {start_time} → {end_time} ({w.row_count:,} rows)")
        
        lines.append(f"\n{'=' * 40}")
        lines.append(f"Total: {len(self._data)} days, {total_rows:,} rows")
        return "\n".join(lines)
    
    def _repr_html_(self) -> str:
        """Jupyter display - nested collapsible: Month → Day → Windows."""
        if not self._data:
            return "<p>No data found in the specified time range.</p>"
        
        total_rows = sum(w.row_count for windows in self._data.values() for w in windows)
        
        # Group by month
        months: Dict[str, Dict[str, List[TimeWindow]]] = defaultdict(dict)
        for day in sorted(self._data.keys()):
            month_key = day[:7]  # "2025-01"
            months[month_key][day] = self._data[day]
        
        html_parts = [
            "<div style='font-family: -apple-system, BlinkMacSystemFont, sans-serif; padding: 10px;'>",
            f"<h3>📊 Data Availability ({self._timezone})</h3>",
            f"<p><strong>{len(self._data)} days</strong> with data, <strong>{total_rows:,}</strong> total rows</p>",
        ]
        
        from datetime import datetime as dt
        
        for month in sorted(months.keys()):
            days_in_month = months[month]
            month_rows = sum(w.row_count for d in days_in_month.values() for w in d)
            month_name = dt.strptime(month, "%Y-%m").strftime("%B %Y")
            
            # Build day sections
            day_sections = []
            for day in sorted(days_in_month.keys()):
                windows = days_in_month[day]
                day_rows = sum(w.row_count for w in windows)
                day_display = dt.strptime(day, "%Y-%m-%d").strftime("%a %d")
                
                window_items = []
                for w in windows:
                    start_time = w.start_local.strftime("%H:%M")
                    end_time = w.end_local.strftime("%H:%M")
                    duration = (w.end_utc - w.start_utc).total_seconds() / 3600
                    window_items.append(
                        f"<li style='padding: 2px 0;'>"
                        f"<code style='background: #f0f0f0; padding: 2px 4px; border-radius: 3px;'>{start_time}</code> → "
                        f"<code style='background: #f0f0f0; padding: 2px 4px; border-radius: 3px;'>{end_time}</code> "
                        f"<span style='color: #888;'>({duration:.1f}h, {w.row_count:,} rows)</span></li>"
                    )
                
                day_sections.append(
                    f"<details style='margin: 3px 0 3px 20px; padding: 3px;'>"
                    f"<summary style='cursor: pointer;'>"
                    f"📅 <strong>{day_display}</strong> "
                    f"<span style='color: #666;'>({len(windows)} window{'s' if len(windows) != 1 else ''}, {day_rows:,} rows)</span>"
                    f"</summary>"
                    f"<ul style='margin: 5px 0 0 15px; padding: 0; list-style: none;'>{''.join(window_items)}</ul>"
                    f"</details>"
                )
            
            html_parts.append(
                f"<details style='margin: 8px 0; padding: 8px; border-left: 4px solid #2196F3; background: #f8f9fa;' open>"
                f"<summary style='cursor: pointer; font-size: 1.1em;'>"
                f"📆 <strong>{month_name}</strong> "
                f"<span style='color: #666; font-weight: normal;'>({len(days_in_month)} days, {month_rows:,} rows)</span>"
                f"</summary>"
                f"<div style='margin-top: 5px;'>{''.join(day_sections)}</div>"
                f"</details>"
            )
        
        html_parts.append("</div>")
        return "".join(html_parts)
    
    def calendar_view(self, year: Optional[int] = None):
        """
        Display a GitHub-style calendar heatmap.
        Darker colors = more data that day.
        
        Args:
            year: Year to display (auto-detected if None)
        
        Returns:
            matplotlib Figure (displays inline in Jupyter)
        """
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import numpy as np
        from datetime import datetime as dt
        import calendar
        
        # Aggregate row counts per day
        day_counts = {}
        for day, windows in self._data.items():
            day_counts[day] = sum(w.row_count for w in windows)
        
        if not day_counts:
            print("No data to display.")
            return None
        
        # Auto-detect year from data
        if year is None:
            years = set(d[:4] for d in day_counts.keys())
            year = int(max(years))  # Use most recent year
        
        # Create figure - one row per month
        fig, axes = plt.subplots(4, 3, figsize=(14, 10))
        fig.suptitle(f"Data Availability Heatmap - {year} ({self._timezone})", 
                     fontsize=14, fontweight='bold', y=0.98)
        
        # Color settings
        max_count = max(day_counts.values()) if day_counts else 1
        
        for month_idx in range(12):
            ax = axes[month_idx // 3, month_idx % 3]
            month = month_idx + 1
            month_name = calendar.month_abbr[month]
            
            # Get calendar for this month
            cal = calendar.Calendar(firstweekday=6)  # Sunday start
            month_days = cal.monthdayscalendar(year, month)
            
            # Create heatmap grid
            grid = np.zeros((len(month_days), 7))
            grid[:] = np.nan  # NaN for empty cells
            
            for week_idx, week in enumerate(month_days):
                for day_idx, day in enumerate(week):
                    if day == 0:
                        continue
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    if date_str in day_counts:
                        # Normalize to 0-1 scale (log scale for better visibility)
                        count = day_counts[date_str]
                        grid[week_idx, day_idx] = np.log1p(count) / np.log1p(max_count)
                    else:
                        grid[week_idx, day_idx] = 0
            
            # Plot heatmap
            cmap = plt.cm.Greens
            cmap.set_bad(color='white')
            
            im = ax.imshow(grid, cmap=cmap, aspect='equal', vmin=0, vmax=1)
            
            # Add day numbers
            for week_idx, week in enumerate(month_days):
                for day_idx, day in enumerate(week):
                    if day != 0:
                        date_str = f"{year}-{month:02d}-{day:02d}"
                        color = 'white' if date_str in day_counts and day_counts[date_str] > max_count * 0.3 else 'black'
                        ax.text(day_idx, week_idx, str(day), ha='center', va='center', 
                               fontsize=7, color=color)
            
            ax.set_title(month_name, fontsize=11, fontweight='bold')
            ax.set_xticks(range(7))
            ax.set_xticklabels(['S', 'M', 'T', 'W', 'T', 'F', 'S'], fontsize=8)
            ax.set_yticks([])
            ax.set_xlim(-0.5, 6.5)
            ax.set_ylim(len(month_days) - 0.5, -0.5)
            
            # Remove frame
            for spine in ax.spines.values():
                spine.set_visible(False)
        
        plt.tight_layout(rect=[0, 0.02, 1, 0.96])
        
        # Add legend
        fig.text(0.5, 0.01, 
                f"Total: {len(self._data)} days with data | Darker = more data | Max: {max_count:,} rows/day",
                ha='center', fontsize=10, style='italic')
        
        return fig
    
    def __iter__(self):
        """Iterate over (day, windows) pairs."""
        for day in sorted(self._data.keys()):
            yield day, self._data[day]
    
    def __len__(self) -> int:
        """Number of days with data."""
        return len(self._data)
    
    def to_dict(self) -> Dict[str, List[dict]]:
        """Export as nested dictionary."""
        return {
            day: [w.to_dict() for w in windows]
            for day, windows in self._data.items()
        }
    
    def to_dataframe(self):
        """Flatten to pandas DataFrame with one row per time window."""
        import pandas as pd
        
        rows = []
        for day, windows in self._data.items():
            for w in windows:
                rows.append({
                    "date": day,
                    "start_utc": w.start_utc,
                    "end_utc": w.end_utc,
                    "start_local": w.start_local,
                    "end_local": w.end_local,
                    "row_count": w.row_count,
                    "duration_hours": (w.end_utc - w.start_utc).total_seconds() / 3600,
                })
        
        return pd.DataFrame(rows)
    
    @property
    def days(self) -> List[str]:
        """List of dates with data."""
        return sorted(self._data.keys())
    
    @property
    def total_rows(self) -> int:
        """Total row count across all windows."""
        return sum(w.row_count for windows in self._data.values() for w in windows)


def scan_data_availability(
    start: datetime,
    end: datetime,
    timezone: str = "UTC",
    table: Optional[str] = None,
    bin_size: str = "hour",
    include_counts: bool = True,
    show_progress: bool = True,
) -> ScanResult:
    """
    Scan the database for data availability windows.
    
    Args:
        start: Start datetime (timezone-aware or naive UTC)
        end: End datetime (timezone-aware or naive UTC)
        timezone: Timezone for display (e.g., "America/Toronto", "UTC")
        table: Table to scan (defaults to "iox.{INFLUX_DB}")
        bin_size: Granularity for scanning - "hour" or "day"
        include_counts: Whether to include row counts (slightly slower)
        show_progress: Show progress bar (works in Jupyter and terminal)
    
    Returns:
        ScanResult: Interactive result object grouped by day
    
    Example:
        >>> import slicks
        >>> slicks.connect_influxdb3(url="...", token="...", db="WFR25")
        >>> result = slicks.scan_data_availability(
        ...     start=datetime(2025, 1, 1),
        ...     end=datetime(2025, 1, 31),
        ...     timezone="America/Toronto"
        ... )
        >>> result  # displays interactive view in Jupyter
        >>> result.to_dataframe()  # for programmatic access
    """
    # Ensure datetimes are UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    else:
        start = start.astimezone(UTC)
    
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    else:
        end = end.astimezone(UTC)
    
    # Setup timezone
    tz = ZoneInfo(timezone)
    
    # Default table
    if table is None:
        table = f"iox.{config.INFLUX_DB}"
    
    table_ref = _quote_table(table)
    
    # Determine bin settings
    interval = "1 day" if bin_size == "day" else "1 hour"
    step = timedelta(days=1) if bin_size == "day" else timedelta(hours=1)
    
    # Calculate total chunks for progress bar
    initial_chunk_days = 31
    total_chunks = ((end - start).days + initial_chunk_days - 1) // initial_chunk_days
    
    # Fetch bins with progress bar
    bins = list(_fetch_bins_adaptive(
        start=start,
        end=end,
        table_ref=table_ref,
        interval=interval,
        step=step,
        initial_chunk_days=initial_chunk_days,
        show_progress=show_progress,
        total_chunks=total_chunks,
    ))
    
    if not bins:
        return ScanResult({}, timezone)
    
    # Compress into windows
    windows = _compress_bins(bins, step)
    
    # Group by day with local timezone
    grouped: Dict[str, List[TimeWindow]] = defaultdict(list)
    
    for start_utc, end_utc, bins_cnt, rows_cnt in windows:
        start_local = start_utc.astimezone(tz)
        end_local = end_utc.astimezone(tz)
        
        day_key = start_local.strftime("%Y-%m-%d")
        
        grouped[day_key].append(TimeWindow(
            start_utc=start_utc,
            end_utc=end_utc,
            start_local=start_local,
            end_local=end_local,
            row_count=rows_cnt if include_counts else 0,
            bins=bins_cnt,
        ))
    
    return ScanResult(dict(grouped), timezone)


def _fetch_bins_adaptive(
    start: datetime,
    end: datetime,
    table_ref: str,
    interval: str,
    step: timedelta,
    initial_chunk_days: int = 31,
    show_progress: bool = True,
    total_chunks: int = 1,
) -> Iterable[Tuple[datetime, int]]:
    """Iterate over bucket start times with counts using adaptive chunking."""
    
    def query_grouped_bins(client: InfluxDBClient3, t0: datetime, t1: datetime) -> Sequence[Tuple[datetime, int]]:
        sql = f"""
            SELECT
                DATE_BIN(INTERVAL '{interval}', time, TIMESTAMP '{t0.isoformat()}') AS bucket,
                COUNT(*) AS n
            FROM {table_ref}
            WHERE time >= TIMESTAMP '{t0.isoformat()}'
              AND time <  TIMESTAMP '{t1.isoformat()}'
            GROUP BY bucket
            HAVING COUNT(*) > 0
            ORDER BY bucket
        """
        tbl = client.query(sql)
        rows: List[Tuple[datetime, int]] = []
        for i in range(tbl.num_rows):
            bucket = tbl.column("bucket")[i].as_py()
            n = tbl.column("n")[i].as_py()
            if bucket.tzinfo is None:
                bucket = bucket.replace(tzinfo=UTC)
            else:
                bucket = bucket.astimezone(UTC)
            rows.append((bucket, int(n)))
        return rows

    def query_exists_per_bin(client: InfluxDBClient3, t0: datetime, t1: datetime) -> List[Tuple[datetime, int]]:
        cur = t0
        rows: List[Tuple[datetime, int]] = []
        while cur < t1:
            nxt = min(cur + step, t1)
            sql = f"""
                SELECT 1
                FROM {table_ref}
                WHERE time >= TIMESTAMP '{cur.isoformat()}'
                  AND time <  TIMESTAMP '{nxt.isoformat()}'
                LIMIT 1
            """
            try:
                tbl = client.query(sql)
                if tbl.num_rows > 0:
                    rows.append((cur, 1))
            except Exception:
                pass
            cur = nxt
        return rows

    def process_range(client: InfluxDBClient3, t0: datetime, t1: datetime, chunk_days: float):
        min_exists_span = step * 4
        if (t1 - t0) <= min_exists_span:
            for pair in query_exists_per_bin(client, t0, t1):
                yield pair
            return
        try:
            for pair in query_grouped_bins(client, t0, t1):
                yield pair
            return
        except Exception:
            mid = t0 + (t1 - t0) / 2
            if mid <= t0 or mid >= t1:
                for pair in query_exists_per_bin(client, t0, t1):
                    yield pair
                return
            for pair in process_range(client, t0, mid, chunk_days / 2):
                yield pair
            for pair in process_range(client, mid, t1, chunk_days / 2):
                yield pair

    with InfluxDBClient3(
        host=config.INFLUX_URL,
        token=config.INFLUX_TOKEN,
        database=config.INFLUX_DB,
    ) as client:
        cur = start
        
        # Setup progress bar (works in Jupyter and terminal)
        pbar = tqdm(
            total=total_chunks,
            desc="Scanning",
            unit="chunk",
            disable=not show_progress,
        )
        
        try:
            while cur < end:
                nxt = min(cur + timedelta(days=initial_chunk_days), end)
                pbar.set_postfix_str(f"{cur.strftime('%b %d')} - {nxt.strftime('%b %d')}")
                
                for pair in process_range(client, cur, nxt, initial_chunk_days):
                    yield pair
                
                pbar.update(1)
                cur = nxt
        finally:
            pbar.close()


def _compress_bins(
    pairs: Sequence[Tuple[datetime, int]], 
    step: timedelta
) -> List[Tuple[datetime, datetime, int, int]]:
    """Merge consecutive buckets into contiguous windows."""
    sorted_pairs = sorted(pairs, key=lambda row: row[0])
    windows: List[Tuple[datetime, datetime, int, int]] = []
    cur_start = cur_end = None
    bins_in = rows_in = 0

    for bucket_start, n in sorted_pairs:
        if cur_start is None:
            cur_start = bucket_start
            cur_end = bucket_start + step
            bins_in = 1
            rows_in = n
            continue
        if bucket_start == cur_end:
            cur_end += step
            bins_in += 1
            rows_in += n
        else:
            windows.append((cur_start, cur_end, bins_in, rows_in))
            cur_start = bucket_start
            cur_end = bucket_start + step
            bins_in = 1
            rows_in = n

    if cur_start is not None:
        windows.append((cur_start, cur_end, bins_in, rows_in))
    return windows
