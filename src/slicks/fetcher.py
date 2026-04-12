import os
import warnings
from datetime import datetime, timedelta
from typing import Any, List, Optional
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

from . import config
from .query_utils import quote_table
from .movement_detector import filter_data_in_movement

# SQLAlchemy engine cache
_ENGINE = None

def get_db_engine():
    """
    Returns a SQLAlchemy engine configured for the TimescaleDB host.
    Utilizes PostgreSQL connection pooling.
    """
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(config.POSTGRES_DSN)
    return _ENGINE


def list_target_sensors():
    """
    Returns the list of DEFAULT sensors configured in config.py.
    """
    return config.SIGNALS


def fetch_telemetry(start_time, end_time, signals=None, engine=None, filter_movement=True, resample="1s", schema="wide"):
    if schema != "wide":
        raise ValueError("slicks ONLY supports schema='wide' for TimescaleDB. 'narrow' is deprecated and unsupported.")

    if signals is None:
        signals = config.SIGNALS

    if isinstance(signals, str):
        signals = [signals]

    if not signals:
        print("Error: No signals specified for fetching.")
        return None

    if engine is None:
        engine = get_db_engine()

    table = config.POSTGRES_TABLE

    signal_cols = ", ".join(f'"{s}"' for s in signals)
    query = (
        f"SELECT time, {signal_cols} "
        f"FROM {table} "
        f"WHERE time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' "
        f"AND time < '{end_time.strftime('%Y-%m-%d %H:%M:%S.%f%z')}' "
        f"ORDER BY time ASC"
    )
    
    print(f"Executing TimescaleDB query for range: {start_time} to {end_time}...")
    try:
        df = pd.read_sql(query, con=engine)
        if df.empty:
            print("No data found for this range.")
            return None
            
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.set_index("time")
        
        if resample:
            df = df.resample(resample).mean().dropna(how="all")
        if filter_movement:
            df = filter_data_in_movement(df)
        print(f"Fetched {len(df)} rows{' (filtered)' if filter_movement else ''}.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def fetch_telemetry_chunked(
    start_time: datetime,
    end_time: datetime,
    signals=None,
    engine=None,
    filter_movement: bool = True,
    resample: Optional[str] = "1s",
    chunk_size: timedelta = timedelta(hours=6),
    show_progress: bool = True,
    schema: str = "wide",
):
    """
    Fetch telemetry with time-splitting limits for RAM preservation.
    """
    if signals is None:
        signals = config.SIGNALS
    if isinstance(signals, str):
        signals = [signals]
    if not signals:
        return None

    if engine is None:
        engine = get_db_engine()

    chunks: List[tuple] = []
    t = start_time
    while t < end_time:
        chunks.append((t, min(t + chunk_size, end_time)))
        t += chunk_size

    if show_progress:
        print(f"Fetching {len(chunks)} chunk(s) from {start_time.date()} to {end_time.date()}...")

    all_dfs: List[pd.DataFrame] = []

    for i, (t0, t1) in enumerate(chunks):
        if show_progress:
            print(f"  chunk {i + 1}/{len(chunks)}: {t0} → {t1}")
        
        # We recursively call `fetch_telemetry` but WITHOUT resampling/filtering yet 
        # so we can apply them cleanly after concatenation to avoid boundary artifacts.
        chunk_df = fetch_telemetry(
            t0, t1, signals=signals, engine=engine, 
            filter_movement=False, resample=None, schema=schema
        )
        if chunk_df is not None and not chunk_df.empty:
            all_dfs.append(chunk_df)

    if not all_dfs:
        if show_progress:
            print("No data found.")
        return None

    df = pd.concat(all_dfs).sort_index()
    # Remove duplicate timestamps from chunk boundaries
    df = df[~df.index.duplicated(keep="first")]

    if resample:
        df = df.resample(resample).mean().dropna(how="all")

    if filter_movement:
        df = filter_data_in_movement(df)

    if show_progress:
        print(f"Fetched {len(df)} rows.")
    return df


def bulk_fetch_season(start_date, end_date, output_file="telemetry_season.csv"):
    """
    Fetch data day-by-day.
    """
    current = start_date
    first_write = not os.path.exists(output_file) if not output_file else True
    
    total_rows = 0
    engine = get_db_engine()
    
    if output_file and os.path.dirname(output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    while current < end_date:
        next_day = current + timedelta(days=1)
        print(f"Fetching {current.date()}...")
        
        df = fetch_telemetry(current, next_day, engine=engine)
        
        if df is not None and not df.empty:
            mode = 'w' if first_write else 'a'
            header = first_write
            df.to_csv(output_file, mode=mode, header=header)
            
            rows = len(df)
            total_rows += rows
            print(f"  -> Added {rows} rows. Total: {total_rows}")
            first_write = False
        else:
            print("  -> No driving data found.")
            
        current = next_day
        
    print(f"Bulk fetch complete. Saved {total_rows} rows to {output_file}.")