import os
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

import pandas as pd
import psycopg2

from . import config
from .movement_detector import filter_data_in_movement
from .query_utils import adaptive_query, quote_table, run_chunks_parallel


class _Scalar:
    """Wraps a Python value to mimic a pyarrow scalar (.as_py() interface)."""

    __slots__ = ("_v",)

    def __init__(self, value: Any):
        self._v = value

    def as_py(self) -> Any:
        return self._v


class _ArrowLike:
    """Pandas-backed result with a tiny Arrow-like interface used by callers."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def num_rows(self) -> int:
        return len(self._df)

    def column(self, name: str) -> List[_Scalar]:
        if name not in self._df.columns:
            return []
        return [_Scalar(v) for v in self._df[name].tolist()]


class TimescaleClient:
    """Thin SQL client that exposes .query() API compatible with old call sites."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    def query(self, query: str, mode: Optional[str] = None, **_: Any) -> Any:
        with psycopg2.connect(self._dsn) as conn:
            df = pd.read_sql_query(query, conn)
        if mode == "pandas":
            return df
        return _ArrowLike(df)

    def close(self) -> None:
        pass

    def __enter__(self) -> "TimescaleClient":
        return self

    def __exit__(self, *_: object) -> None:
        pass


def get_timescale_client(dsn: Optional[str] = None) -> TimescaleClient:
    return TimescaleClient(dsn=dsn or config.POSTGRES_DSN)


def get_influx_client(url=None, token=None, org=None, db=None):
    """Backward-compatible alias for older code paths."""
    return get_timescale_client()


def list_target_sensors() -> List[str]:
    return config.SIGNALS


def _fmt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_telemetry(
    start_time,
    end_time,
    signals=None,
    client=None,
    filter_movement=True,
    resample="1s",
    schema="wide",
):
    """Fetch telemetry data from TimescaleDB for selected signals and time range."""
    if signals is None:
        signals = config.SIGNALS
    if isinstance(signals, str):
        signals = [signals]
    if not signals:
        print("Error: No signals specified for fetching.")
        return None

    if client is None:
        client = get_timescale_client()

    db_schema = config.TIMESCALE_SCHEMA or "public"
    table = config.TIMESCALE_TABLE
    table_ref = quote_table(db_schema, table)

    if schema == "wide":
        signal_cols = ", ".join(f'"{s}"' for s in signals)
        query = (
            f"SELECT time, {signal_cols} "
            f"FROM {table_ref} "
            f"WHERE time >= '{_fmt(start_time)}' "
            f"AND time < '{_fmt(end_time)}' "
            f"ORDER BY time ASC"
        )
        print(f"Executing wide query for range: {start_time} to {end_time}...")
        try:
            df = client.query(query=query, mode="pandas")
            if df.empty:
                print("No data found for this range.")
                return None
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.set_index("time")
            if resample:
                df = df.resample(resample).mean().dropna(how="all")
            if filter_movement:
                df = filter_data_in_movement(df)
            print(f"Fetched {len(df)} rows{' (filtered)' if filter_movement else ''}.")
            return df
        except Exception as exc:
            print(f"Error fetching data: {exc}")
            return None

    warnings.warn(
        "schema='narrow' is deprecated and will be removed in a future release. "
        "WFR has moved to wide schema - use schema='wide' (default).",
        DeprecationWarning,
        stacklevel=2,
    )

    signal_list = "', '".join(signals)
    query = f"""
    SELECT
        time,
        "signalName",
        "sensorReading"
    FROM {table_ref}
    WHERE
        "signalName" IN ('{signal_list}')
        AND time >= '{_fmt(start_time)}'
        AND time < '{_fmt(end_time)}'
    ORDER BY time ASC
    """

    print(f"Executing query for range: {start_time} to {end_time}...")
    try:
        table_df = client.query(query=query, mode="pandas")
        if table_df.empty:
            print("No data found for this range.")
            return None

        df = table_df.pivot_table(
            index="time",
            columns="signalName",
            values="sensorReading",
            aggfunc="mean",
        )

        if resample:
            df = df.resample(resample).mean().dropna()

        if filter_movement:
            df = filter_data_in_movement(df)

        print(f"Fetched {len(df)} rows{' (filtered)' if filter_movement else ''}.")
        return df

    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return None


def fetch_telemetry_chunked(
    start_time: datetime,
    end_time: datetime,
    signals=None,
    client=None,
    filter_movement: bool = True,
    resample: Optional[str] = "1s",
    chunk_size: timedelta = timedelta(hours=6),
    max_workers: int = 1,
    show_progress: bool = True,
    schema: str = "wide",
) -> Optional[pd.DataFrame]:
    """Fetch telemetry with adaptive recursive chunking on query failures."""
    if signals is None:
        signals = config.SIGNALS
    if isinstance(signals, str):
        signals = [signals]
    if not signals:
        return None

    if client is None:
        client = get_timescale_client()

    db_schema = config.TIMESCALE_SCHEMA or "public"
    table = config.TIMESCALE_TABLE
    table_ref = quote_table(db_schema, table)

    if schema == "wide":
        signal_cols = ", ".join(f'"{s}"' for s in signals)

        def _fetch_chunk(cli: Any, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
            query = (
                f"SELECT time, {signal_cols} "
                f"FROM {table_ref} "
                f"WHERE time >= '{_fmt(t0)}' AND time < '{_fmt(t1)}' "
                f"ORDER BY time ASC"
            )
            raw = cli.query(query=query, mode="pandas")
            if raw.empty:
                return []
            raw["time"] = pd.to_datetime(raw["time"], utc=True)
            df = raw.set_index("time")
            return [df]
    else:
        signal_list = "', '".join(signals)

        def _fetch_chunk(cli: Any, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
            query = (
                f"SELECT time, \"signalName\", \"sensorReading\" "
                f"FROM {table_ref} "
                f"WHERE \"signalName\" IN ('{signal_list}') "
                f"AND time >= '{_fmt(t0)}' AND time < '{_fmt(t1)}' "
                f"ORDER BY time ASC"
            )
            raw = cli.query(query=query, mode="pandas")
            if raw.empty:
                return []
            df = raw.pivot_table(
                index="time",
                columns="signalName",
                values="sensorReading",
                aggfunc="mean",
            )
            return [df]

    chunks: List[tuple] = []
    t = start_time
    while t < end_time:
        chunks.append((t, min(t + chunk_size, end_time)))
        t += chunk_size

    if show_progress:
        print(f"Fetching {len(chunks)} chunk(s) from {start_time.date()} to {end_time.date()}...")

    all_dfs: List[pd.DataFrame] = []

    def _fetch_adaptive(cli: Any, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
        return adaptive_query(
            client=cli,
            t0=t0,
            t1=t1,
            primary_fn=_fetch_chunk,
            min_span=timedelta(minutes=1),
        )

    if max_workers > 1:
        all_dfs = run_chunks_parallel(
            client_factory=get_timescale_client,
            chunks=chunks,
            query_fn=_fetch_adaptive,
            max_workers=max_workers,
        )
    else:
        for i, (t0, t1) in enumerate(chunks):
            if show_progress:
                print(f"  chunk {i + 1}/{len(chunks)}: {t0} -> {t1}")
            results = _fetch_adaptive(client, t0, t1)
            all_dfs.extend(results)

    if not all_dfs:
        if show_progress:
            print("No data found.")
        return None

    df = pd.concat(all_dfs).sort_index()
    df = df[~df.index.duplicated(keep="first")]

    if resample:
        df = df.resample(resample).mean().dropna(how="all")

    if filter_movement:
        df = filter_data_in_movement(df)

    if show_progress:
        print(f"Fetched {len(df)} rows.")
    return df


def bulk_fetch_season(start_date, end_date, output_file="telemetry_season.csv"):
    """Fetch data day-by-day and append to CSV."""
    current = start_date
    first_write = not os.path.exists(output_file) if not output_file else True

    total_rows = 0
    client = get_timescale_client()

    if output_file and os.path.dirname(output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    while current < end_date:
        next_day = current + timedelta(days=1)
        print(f"Fetching {current.date()}...")

        df = fetch_telemetry(current, next_day, client=client)

        if df is not None and not df.empty:
            mode = "w" if first_write else "a"
            header = first_write
            df.to_csv(output_file, mode=mode, header=header)
            total_rows += len(df)
            first_write = False
            print(f"  Wrote {len(df)} rows")
        else:
            print("  No data")

        current = next_day

    print(f"Done. Total rows written: {total_rows}")
    return total_rows
