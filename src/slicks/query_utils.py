"""
Shared utilities for adaptive chunked querying against InfluxDB 3.x (IOx).

Provides:
- Error classification (recoverable vs permanent)
- Parallel chunk execution via ThreadPoolExecutor
- Adaptive recursive splitting on resource-limit failures
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Callable, List, Optional, Sequence, Tuple, TypeVar

from influxdb_client_3 import InfluxDBClient3

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

_PERMANENT_ERROR_PATTERNS = (
    "table not found",
    "not found",
    "unauthorized",
    "unauthenticated",
    "permission denied",
    "invalid token",
    "database not found",
    "bucket not found",
    "syntax error",
)


def quote_table(schema: str, table: str) -> str:
    """Quote table name for SQL, handling schema.table format."""
    # If the table already contains a dot (and isn't just the schema), assume it might differ?
    # Actually, InfluxDB 3 usually expects "schema"."table"
    return f'"{schema}"."{table}"'


class PermanentQueryError(Exception):
    """An error that will not resolve by splitting the time range."""


def is_permanent_error(exc: Exception) -> bool:
    """Classify an exception as permanent (non-retryable) vs recoverable."""
    msg = str(exc).lower()
    return any(pattern in msg for pattern in _PERMANENT_ERROR_PATTERNS)


# ---------------------------------------------------------------------------
# Adaptive recursive query
# ---------------------------------------------------------------------------

def adaptive_query(
    client: InfluxDBClient3,
    t0: datetime,
    t1: datetime,
    primary_fn: Callable[[InfluxDBClient3, datetime, datetime], List[T]],
    fallback_fn: Optional[Callable[[InfluxDBClient3, datetime, datetime], List[T]]] = None,
    min_span: Optional[timedelta] = None,
    max_depth: int = 10,
    _depth: int = 0,
) -> List[T]:
    """
    Execute *primary_fn* on [t0, t1).  On a recoverable failure the range is
    split in half and each half is retried recursively.

    When the remaining span is smaller than *min_span* (or *max_depth* is
    reached) *fallback_fn* is used instead — if provided — otherwise an empty
    list is returned.

    Raises ``PermanentQueryError`` immediately for non-retryable errors such
    as authentication failures or missing tables.
    """
    if min_span and (t1 - t0) <= min_span:
        if fallback_fn:
            return fallback_fn(client, t0, t1)
        return []

    if _depth > max_depth:
        if fallback_fn:
            return fallback_fn(client, t0, t1)
        return []

    try:
        return primary_fn(client, t0, t1)
    except Exception as exc:
        if is_permanent_error(exc):
            raise PermanentQueryError(str(exc)) from exc

        mid = t0 + (t1 - t0) / 2
        if mid <= t0 or mid >= t1:
            if fallback_fn:
                return fallback_fn(client, t0, t1)
            return []

        left = adaptive_query(
            client, t0, mid, primary_fn, fallback_fn,
            min_span, max_depth, _depth + 1,
        )
        right = adaptive_query(
            client, mid, t1, primary_fn, fallback_fn,
            min_span, max_depth, _depth + 1,
        )
        return left + right


# ---------------------------------------------------------------------------
# Parallel chunk execution
# ---------------------------------------------------------------------------

def run_chunks_parallel(
    client_factory: Callable[[], InfluxDBClient3],
    chunks: Sequence[Tuple[datetime, datetime]],
    query_fn: Callable[[InfluxDBClient3, datetime, datetime], List[T]],
    max_workers: int = 4,
    on_chunk_done: Optional[Callable[[int], None]] = None,
) -> List[T]:
    """
    Execute *query_fn* across time-range *chunks* in parallel.

    Each worker thread receives its own ``InfluxDBClient3`` instance
    (via *client_factory*) because the client is not guaranteed thread-safe.

    Results are returned in chunk order regardless of completion order.

    Raises ``PermanentQueryError`` immediately, cancelling remaining work.
    """
    if not chunks:
        return []

    results: dict[int, List[T]] = {}
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_idx: dict = {}
        clients: list[InfluxDBClient3] = []

        for idx, (t0, t1) in enumerate(chunks):
            client = client_factory()
            clients.append(client)
            future = pool.submit(query_fn, client, t0, t1)
            future_to_idx[future] = idx

        try:
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                result = future.result()  # raises on exception
                with lock:
                    results[idx] = result
                if on_chunk_done:
                    on_chunk_done(idx)
        except PermanentQueryError:
            for f in future_to_idx:
                f.cancel()
            raise
        finally:
            for c in clients:
                try:
                    c.close()
                except Exception:
                    pass

    ordered: List[T] = []
    for idx in sorted(results.keys()):
        ordered.extend(results[idx])
    return ordered
