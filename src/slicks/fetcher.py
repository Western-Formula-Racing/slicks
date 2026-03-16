import os
from datetime import datetime, timedelta
from typing import Any, List, Optional
import pandas as pd
from influxdb_client_3 import InfluxDBClient3
from . import config
from .query_utils import quote_table, adaptive_query, run_chunks_parallel
from .movement_detector import filter_data_in_movement


class _Scalar:
    """Wraps a Python value to mimic a pyarrow scalar (.as_py() interface)."""
    __slots__ = ("_v",)

    def __init__(self, v):
        self._v = v

    def as_py(self):
        return self._v


class _ArrowLike:
    """
    Wraps a pandas DataFrame to expose the minimal pyarrow Table interface
    used by this codebase (.num_rows, .column(name)).

    Allows HttpInfluxClient.query() to be a drop-in for InfluxDBClient3.query()
    in callers that iterate over result columns with .as_py().
    """

    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def num_rows(self) -> int:
        return len(self._df)

    def column(self, name: str) -> List[_Scalar]:
        if name not in self._df.columns:
            return []
        return [_Scalar(v) for v in self._df[name]]


class HttpInfluxClient:
    """
    HTTP-based InfluxDB 3 client using /api/v3/query_sql with Parquet responses.

    Drop-in replacement for InfluxDBClient3 for environments where gRPC/Arrow Flight
    is blocked (e.g. HTTPS Cloudflare tunnels). Implements the same .query() interface.

    Auto-selected by get_influx_client() when the host URL starts with https://.
    """

    def __init__(self, host: str, token: str, database: str):
        self._host = host.rstrip("/")
        self._token = token
        self._database = database

    def query(self, query: str, mode: str = None, database: str = None, **kwargs) -> Any:
        import httpx

        db = database or self._database
        resp = httpx.post(
            f"{self._host}/api/v3/query_sql",
            headers={
                "Authorization": f"Token {self._token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json={"db": db, "q": query},
            timeout=120.0,
        )
        if not resp.is_success:
            try:
                err = resp.json().get("error", resp.text)
            except Exception:
                err = resp.text
            raise Exception(f"InfluxDB HTTP query error [{resp.status_code}]: {err}")

        rows = resp.json() if resp.content else []
        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        if mode == "pandas":
            return df
        return _ArrowLike(df)

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


def get_influx_client(url=None, token=None, org=None, db=None):
    """
    Returns an InfluxDB client appropriate for the configured host.

    - https:// hosts → HttpInfluxClient (REST /api/v3/query_sql, Parquet)
      Used for remote Cloudflare-tunnelled servers where gRPC/Flight is blocked.
    - http:// hosts  → InfluxDBClient3 (Arrow Flight SQL / gRPC)
      Used for local Docker stacks with direct access.
    """
    host = url or config.INFLUX_URL
    tok = token or config.INFLUX_TOKEN
    database = db or config.INFLUX_DB

    if host.startswith("https://"):
        return HttpInfluxClient(host=host, token=tok, database=database)

    return InfluxDBClient3(
        host=host,
        token=tok,
        org=org or config.INFLUX_ORG,
        database=database,
    )


def list_target_sensors():
    """
    Returns the list of DEFAULT sensors configured in config.py.
    """
    return config.SIGNALS


def fetch_telemetry(start_time, end_time, signals=None, client=None, filter_movement=True, resample="1s", schema="narrow"):
    """
    Fetch telemetry data for specified signals within a time range.

    Args:
        start_time (datetime): Start of the query range.
        end_time (datetime): End of the query range.
        signals (list or str, optional): List of sensor names or a single sensor name.
                                         Defaults to config.SIGNALS if None.
        client (InfluxDBClient3, optional): Existing client instance.
        filter_movement (bool): If True, applies movement detection filtering. Defaults to True.
        resample (str or None): Pandas frequency string for resampling (e.g. "1s", "100ms", "5s").
                                Set to None to disable resampling and get raw data. Defaults to "1s".
        schema (str): "narrow" (legacy EAV with signalName tag) or "wide" (one field per signal).
                      Defaults to "narrow" for backwards compatibility.
    """
    if signals is None:
        signals = config.SIGNALS

    # Handle single string input for convenience
    if isinstance(signals, str):
        signals = [signals]

    if not signals:
        print("Error: No signals specified for fetching.")
        return None

    if client is None:
        client = get_influx_client()

    # Ensure safe defaults if config vars are missing or empty
    db_schema = config.INFLUX_SCHEMA or "iox"
    table = config.INFLUX_TABLE or config.INFLUX_DB
    table_ref = quote_table(db_schema, table)

    if schema == "wide":
        # Wide format: each signal is its own column — SELECT directly, no pivot needed
        signal_cols = ", ".join(f'"{s}"' for s in signals)
        query = (
            f"SELECT time, {signal_cols} "
            f"FROM {table_ref} "
            f"WHERE time >= '{start_time.isoformat()}Z' "
            f"AND time < '{end_time.isoformat()}Z' "
            f"ORDER BY time ASC"
        )
        print(f"Executing wide query for range: {start_time} to {end_time}...")
        try:
            df = client.query(query=query, mode="pandas")
            if df.empty:
                print("No data found for this range.")
                return None
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

    # --- narrow (legacy EAV) path ---
    signal_list = "', '".join(signals)

    query = f"""
    SELECT
        time,
        "signalName",
        "sensorReading"
    FROM {table_ref}
    WHERE
        "signalName" IN ('{signal_list}')
        AND time >= '{start_time.isoformat()}Z'
        AND time < '{end_time.isoformat()}Z'
    ORDER BY time ASC
    """

    print(f"Executing query for range: {start_time} to {end_time}...")
    try:
        table = client.query(query=query, mode="pandas")
        if table.empty:
            print("No data found for this range.")
            return None

        # Pivot the data
        df = table.pivot_table(
            index="time",
            columns="signalName",
            values="sensorReading",
            aggfunc='mean'
        )

        # Resample to common frequency (if specified)
        if resample:
            df = df.resample(resample).mean().dropna()

        # Use the movement detector tool to filter
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
    client=None,
    filter_movement: bool = True,
    resample: Optional[str] = "1s",
    chunk_size: timedelta = timedelta(hours=6),
    max_workers: int = 1,
    show_progress: bool = True,
    schema: str = "narrow",
) -> Optional[pd.DataFrame]:
    """
    Fetch telemetry with automatic time-splitting when InfluxDB's per-query
    file limit is exceeded.

    Identical interface to ``fetch_telemetry`` but uses ``adaptive_query``
    internally: if a time window hits the server's parquet-file cap the range
    is recursively halved until each sub-query succeeds, then results are
    concatenated.  Suitable for ranges that span many test sessions.

    Args:
        start_time: Start of the query range.
        end_time:   End of the query range.
        signals:    Sensor names (defaults to config.SIGNALS).
        client:     Existing InfluxDBClient3 instance (creates one if None).
        filter_movement: Apply movement-detection filtering to the final result.
        resample:   Pandas frequency string, e.g. "1s", "100ms", or None for raw.
        chunk_size: Initial time window per adaptive-query call.  Each chunk is
                    split further on file-limit errors. Default: 6 hours.
        max_workers: Parallel workers for top-level chunks (1 = sequential).
        show_progress: Print progress messages.
        schema: "narrow" (legacy EAV) or "wide" (one field per signal).

    Returns:
        Combined DataFrame with DatetimeIndex, or None if no data found.
    """
    if signals is None:
        signals = config.SIGNALS
    if isinstance(signals, str):
        signals = [signals]
    if not signals:
        return None

    if client is None:
        client = get_influx_client()

    db_schema = config.INFLUX_SCHEMA or "iox"
    table = config.INFLUX_TABLE or config.INFLUX_DB
    table_ref = quote_table(db_schema, table)

    def _fmt(dt: datetime) -> str:
        """Format datetime as UTC ISO string for SQL, safe for both naive and tz-aware."""
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"

    if schema == "wide":
        signal_cols = ", ".join(f'"{s}"' for s in signals)

        def _fetch_chunk(cli: InfluxDBClient3, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
            """Fetch one wide time window; return list-of-DataFrame for adaptive_query."""
            query = (
                f"SELECT time, {signal_cols} "
                f"FROM {table_ref} "
                f"WHERE time >= '{_fmt(t0)}' AND time < '{_fmt(t1)}' "
                f"ORDER BY time ASC"
            )
            raw = cli.query(query=query, mode="pandas")
            if raw.empty:
                return []
            df = raw.set_index("time")
            return [df]
    else:
        signal_list = "', '".join(signals)

        def _fetch_chunk(cli: InfluxDBClient3, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
            """Fetch one narrow time window; return list-of-DataFrame for adaptive_query."""
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

    # Split full range into top-level chunks, then use adaptive_query per chunk
    chunks: List[tuple] = []
    t = start_time
    while t < end_time:
        chunks.append((t, min(t + chunk_size, end_time)))
        t += chunk_size

    if show_progress:
        print(f"Fetching {len(chunks)} chunk(s) from {start_time.date()} to {end_time.date()}...")

    all_dfs: List[pd.DataFrame] = []

    def _fetch_adaptive(cli: InfluxDBClient3, t0: datetime, t1: datetime) -> List[pd.DataFrame]:
        return adaptive_query(
            client=cli,
            t0=t0,
            t1=t1,
            primary_fn=_fetch_chunk,
            min_span=timedelta(minutes=1),
        )

    if max_workers > 1:
        all_dfs = run_chunks_parallel(
            client_factory=get_influx_client,
            chunks=chunks,
            query_fn=_fetch_adaptive,
            max_workers=max_workers,
        )
    else:
        for i, (t0, t1) in enumerate(chunks):
            if show_progress:
                print(f"  chunk {i + 1}/{len(chunks)}: {t0} → {t1}")
            results = _fetch_adaptive(client, t0, t1)
            all_dfs.extend(results)

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
    client = get_influx_client()
    
    # Ensure directory exists
    if output_file and os.path.dirname(output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    while current < end_date:
        next_day = current + timedelta(days=1)
        print(f"Fetching {current.date()}...")
        
        df = fetch_telemetry(current, next_day, client=client)
        
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