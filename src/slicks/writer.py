"""Wide-format TimescaleDB writer - one row per CAN message, all signals as columns."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

from . import config
from .can_decode import DecodedFrame, decode_frame, load_dbc

logger = logging.getLogger(__name__)

#: Metadata columns present in wide tables - not telemetry signals.
NON_SIGNAL_COLS: frozenset[str] = frozenset(
    {"time", "message_name", "can_id", "messageName", "canId", "iox::measurement"}
)

_LP_ESCAPE = str.maketrans({" ": r"\ ", ",": r"\,", "=": r"\="})


def _esc(val: str) -> str:
    return val.translate(_LP_ESCAPE)


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def frame_to_line_protocol(
    measurement: str,
    frame: DecodedFrame,
    ts_ns: int,
    include_tags: bool = True,
) -> str:
    """Legacy helper kept for compatibility with existing callers/tests."""
    if not frame.signals:
        raise ValueError(f"Frame {frame.message_name} has no numeric signals")

    fields = ",".join(f"{_esc(k)}={v}" for k, v in frame.signals.items())
    tags = (
        f",messageName={_esc(frame.message_name)},canId={frame.can_id}"
        if include_tags
        else ""
    )
    return f"{_esc(measurement)}{tags} {fields} {ts_ns}"


class WideWriter:
    """Decode CAN frames and upsert wide rows into a TimescaleDB table."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        table: Optional[str] = None,
        measurement: Optional[str] = None,
        schema: Optional[str] = None,
        url: str = "",
        token: str = "",
        bucket: str = "",
        org: str = "",
        batch_size: int = 5000,
        flush_interval_ms: int = 1000,
        dbc_path: Optional[Path] = None,
    ) -> None:
        del url, token, org, flush_interval_ms

        self._dsn = dsn or config.POSTGRES_DSN
        self._schema = schema or config.TIMESCALE_SCHEMA or "public"
        self._table = table or bucket or measurement or config.TIMESCALE_TABLE
        self._measurement = measurement or self._table
        self._batch_size = batch_size
        self._db = load_dbc(dbc_path)
        self._rows: list[tuple[datetime, str, int, dict[str, float]]] = []
        self._known_columns: set[str] = set()

        logger.info("WideWriter: loaded DBC (%d messages)", len(self._db.messages))
        self._ensure_base_table()

    @property
    def _table_ref(self) -> str:
        return f"{_qident(self._schema)}.{_qident(self._table)}"

    def _ensure_base_table(self) -> None:
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._table_ref} (
                        time TIMESTAMPTZ NOT NULL,
                        message_name TEXT,
                        can_id INTEGER
                    )
                    """
                )
                cur.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {_qident(self._table + '_dedup_idx')} "
                    f"ON {self._table_ref} (time, message_name)"
                )
                # Best-effort hypertable conversion if Timescale extension is available.
                try:
                    cur.execute(
                        "SELECT create_hypertable(%s, 'time', if_not_exists => TRUE)",
                        (f"{self._schema}.{self._table}",),
                    )
                except Exception:
                    conn.rollback()
                    with conn.cursor() as cur2:
                        cur2.execute("SELECT 1")
                conn.commit()

    def _ensure_signal_columns(self, signal_names: set[str]) -> None:
        new_cols = sorted(signal_names - self._known_columns)
        if not new_cols:
            return
        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                for col in new_cols:
                    cur.execute(
                        f"ALTER TABLE {self._table_ref} "
                        f"ADD COLUMN IF NOT EXISTS {_qident(col)} DOUBLE PRECISION"
                    )
            conn.commit()
        self._known_columns.update(new_cols)

    @staticmethod
    def _ns_to_utc(ts_ns: int) -> datetime:
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)

    def decode_and_queue(self, can_id: int, data: bytes, ts_ns: int) -> int:
        frame = decode_frame(self._db, can_id, data)
        if frame is None or not frame.signals:
            return 0
        self._rows.append((self._ns_to_utc(ts_ns), frame.message_name, can_id, frame.signals))
        if len(self._rows) >= self._batch_size:
            self.flush()
        return 1

    def write_lines(self, lines: list[str]) -> None:
        del lines
        raise NotImplementedError(
            "write_lines() is not supported in Timescale mode. Use decode_and_queue()."
        )

    def flush(self) -> None:
        if not self._rows:
            return

        all_signal_cols: set[str] = set()
        for _, _, _, signals in self._rows:
            all_signal_cols.update(signals.keys())
        self._ensure_signal_columns(all_signal_cols)

        signal_cols = sorted(all_signal_cols)
        insert_cols = ["time", "message_name", "can_id", *signal_cols]

        values = []
        for ts, msg, can_id, signals in self._rows:
            row = [ts, msg, can_id]
            row.extend(signals.get(c) for c in signal_cols)
            values.append(tuple(row))

        update_targets = [c for c in insert_cols if c not in {"time", "message_name"}]
        set_clause = ", ".join(
            f"{_qident(c)} = EXCLUDED.{_qident(c)}" for c in update_targets
        )
        insert_cols_sql = ", ".join(_qident(c) for c in insert_cols)

        sql_query = (
            f"INSERT INTO {self._table_ref} ({insert_cols_sql}) VALUES %s "
            f"ON CONFLICT (time, message_name) DO UPDATE SET {set_clause}"
        )

        with psycopg2.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql_query, values)
            conn.commit()

        self._rows.clear()

    def close(self) -> None:
        self.flush()

    def __enter__(self) -> "WideWriter":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
