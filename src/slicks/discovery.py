"""
Sensor discovery module for PostgreSQL / TimescaleDB.

Scans the database schema for all unique sensor names within a wide table.
"""

from __future__ import annotations

import warnings
from datetime import datetime
from typing import List
import pandas as pd

from . import config
from .fetcher import get_db_engine

NON_SIGNAL_COLS = {'time', 'message_name', 'can_id'}

def discover_sensors(
    start_time: datetime = None,
    end_time: datetime = None,
    chunk_size_days: int = 7,  # Kept for bw-compat API signatures
    client=None,               # Kept for bw-compat
    show_progress: bool = True,
    schema: str = "wide",
) -> List[str]:
    """
    Scan the database for ALL unique sensor names via information_schema metadata.
    
    In TimescaleDB, the wide format is permanent and we can securely retrieve 
    the list of sensors without touching billions of data rows natively!
    """
    if schema != "wide":
        warnings.warn(
            "schema='narrow' logic is depreciated and unsupported for TimescaleDB.",
            DeprecationWarning,
            stacklevel=2,
        )

    table = config.POSTGRES_TABLE
    # Strip quotes internally to check pure schema names just in case
    raw_table = table.strip('"')

    engine = get_db_engine()
    sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{raw_table}'
    """
    
    try:
        df = pd.read_sql(sql, con=engine)
        if df.empty:
            return []
            
        columns = df['column_name'].tolist()
        return sorted(
            c for c in columns 
            if c not in NON_SIGNAL_COLS
        )
    except Exception as e:
        print(f"Error discovering sensors: {e}")
        return []
