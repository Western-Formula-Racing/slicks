"""
Scanner module for discovering data availability windows in TimescaleDB.

Provides an interactive way to browse what time ranges have telemetry data.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import pandas as pd

from zoneinfo import ZoneInfo

from . import config
from .fetcher import get_db_engine

UTC = timezone.utc

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
    """
    
    def __init__(self, data: Dict[str, List[TimeWindow]], timezone_name: str):
        self._data = data 
        self._timezone = timezone_name
    
    def __repr__(self) -> str:
        if not self._data:
            return "No data found in the specified time range."
        
        lines = [f"Data Availability ({self._timezone})", "=" * 40]
        total_rows = 0
        
        months: Dict[str, Dict[str, List[TimeWindow]]] = defaultdict(dict)
        for day in sorted(self._data.keys()):
            month_key = day[:7]
            months[month_key][day] = self._data[day]
        
        from datetime import datetime as dt
        for month in sorted(months.keys()):
            days_in_month = months[month]
            month_rows = sum(w.row_count for d in days_in_month.values() for w in d)
            total_rows += month_rows
            
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
        lines.append(f"Total: {len(self._data)} days, {total_rows:,} rows")
        return "\n".join(lines)
        
    def __len__(self) -> int:
        return len(self._data)
        
    @property
    def days(self) -> list[str]:
        return sorted(self._data.keys())


def scan_data_availability(
    start: datetime,
    end: datetime,
    timezone: str = "UTC",
    table: Optional[str] = None,
    bin_size: str = "hour",
    include_counts: bool = True,
    show_progress: bool = True,
    max_workers: int = 4, # Kept for bw-compat
) -> ScanResult:
    """
    Scan the database for data availability windows using TimescaleDB time_bucket.
    """
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    else:
        start = start.astimezone(UTC)
    
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    else:
        end = end.astimezone(UTC)
    
    tz = ZoneInfo(timezone)
    table_ref = table or config.POSTGRES_TABLE
    
    interval = "1 day" if bin_size == "day" else "1 hour"
    step = timedelta(days=1) if bin_size == "day" else timedelta(hours=1)
    
    engine = get_db_engine()
    
    # In PostgreSQL with TimescaleDB, time_bucket aggregates incredibly fast.
    sql = f"""
        SELECT
            time_bucket('{interval}', time) AS bucket,
            COUNT(*) AS n
        FROM {table_ref}
        WHERE time >= '{start.strftime('%Y-%m-%d %H:%M:%S%z')}'
          AND time < '{end.strftime('%Y-%m-%d %H:%M:%S%z')}'
        GROUP BY bucket
        ORDER BY bucket ASC
    """
    
    if show_progress:
        print(f"Scanning data from {start.date()} to {end.date()} by {bin_size}...")

    try:
        df = pd.read_sql(sql, engine)
    except Exception as e:
        raise RuntimeError(f"Scan aborted due to error: {e}") from e

    if df.empty:
        return ScanResult({}, timezone)

    df['bucket'] = pd.to_datetime(df['bucket'], utc=True)
    
    bins = []
    for _, row in df.iterrows():
        b = row['bucket']
        n = int(row['n'])
        if n > 0:
            bins.append((b, n))
            
    if not bins:
         return ScanResult({}, timezone)

    windows = _compress_bins(bins, step)
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
