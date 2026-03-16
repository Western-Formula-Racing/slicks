"""
Sensor discovery module.

Scans the database for all unique sensor names within a time range.
Uses adaptive chunking with parallel execution for narrow schema.
For wide schema, uses instant information_schema.columns metadata lookup.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta
from typing import List, Optional

from tqdm.auto import tqdm

from . import config
from .fetcher import get_influx_client
from .query_utils import adaptive_query, run_chunks_parallel, PermanentQueryError, quote_table
from .writer import NON_SIGNAL_COLS


def discover_sensors(
    start_time: datetime,
    end_time: datetime,
    chunk_size_days: int = 7,
    client=None,
    show_progress: bool = True,
    schema: str = "narrow",
) -> List[str]:
    """
    Scan the database for ALL unique sensor names within the time range.

    For ``schema="wide"``, uses an instant ``information_schema.columns`` metadata
    lookup (no data scan, no adaptive bisection, ignores time range and chunk params).

    For ``schema="narrow"`` (default), uses adaptive chunking with parallel execution
    to handle server resource limits efficiently.

    Args:
        start_time: Start of scan range (narrow schema only).
        end_time: End of scan range (narrow schema only).
        chunk_size_days: Days per chunk (default 7, narrow schema only).
        client: Ignored (kept for backward compatibility).
        show_progress: Show progress bar (default True, narrow schema only).
        schema: "narrow" (legacy EAV) or "wide" (one field per signal).

    Returns:
        Sorted list of unique sensor name strings.
    """
    db_schema = config.INFLUX_SCHEMA or "iox"
    table = config.INFLUX_TABLE or config.INFLUX_DB

    if schema == "wide":
        # Instant metadata lookup — no data scan needed
        cli = get_influx_client()
        sql = (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{db_schema}' AND table_name = '{table}'"
        )
        result = cli.query(query=sql)
        if result.num_rows == 0:
            return []
        col = result.column("column_name")
        return sorted(
            v.as_py()
            for v in col
            if v.as_py() is not None and v.as_py() not in NON_SIGNAL_COLS
        )

    # --- narrow (legacy EAV) path ---

    def _make_client():
        return get_influx_client()

    def _query_distinct(
        client: InfluxDBClient3, t0: datetime, t1: datetime,
    ) -> List[str]:
        table_ref = quote_table(db_schema, table)
        sql = f"""
        SELECT DISTINCT "signalName"
        FROM {table_ref}
        WHERE time >= '{t0.isoformat()}Z'
        AND time < '{t1.isoformat()}Z'
        """
        tbl = client.query(query=sql)
        if tbl.num_rows == 0:
            return []
        col = tbl.column("signalName")
        return [v.as_py() for v in col if v.as_py() is not None]

    def _process_chunk(
        client: InfluxDBClient3, t0: datetime, t1: datetime,
    ) -> List[str]:
        return adaptive_query(
            client=client,
            t0=t0,
            t1=t1,
            primary_fn=_query_distinct,
            fallback_fn=None,
            min_span=timedelta(seconds=10),
            max_depth=5,
        )

    # Build chunk list
    chunks = []
    cur = start_time
    while cur < end_time:
        nxt = min(cur + timedelta(days=chunk_size_days), end_time)
        if nxt <= cur:
            break
        chunks.append((cur, nxt))
        cur = nxt

    pbar = tqdm(
        total=len(chunks),
        desc="Discovering sensors",
        unit="chunk",
        disable=not show_progress,
    )
    pbar_lock = threading.Lock()

    def on_chunk_done(idx: int) -> None:
        with pbar_lock:
            pbar.update(1)

    try:
        all_names = run_chunks_parallel(
            client_factory=_make_client,
            chunks=chunks,
            query_fn=_process_chunk,
            max_workers=4,
            on_chunk_done=on_chunk_done,
        )
    except PermanentQueryError as e:
        raise RuntimeError(f"Sensor discovery aborted: {e}") from e
    finally:
        pbar.close()

    unique = sorted(set(all_names))
    return unique
