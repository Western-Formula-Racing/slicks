"""Wide-format InfluxDB writer — one point per CAN message, all signals as fields."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from influxdb_client import InfluxDBClient, WriteOptions

from .can_decode import DecodedFrame, decode_frame, load_dbc

logger = logging.getLogger(__name__)

#: Metadata columns present in wide tables — not telemetry signals.
NON_SIGNAL_COLS: frozenset[str] = frozenset({"time", "messageName", "canId", "iox::measurement"})

_LP_ESCAPE = str.maketrans({" ": r"\ ", ",": r"\,", "=": r"\="})


def _esc(val: str) -> str:
    return val.translate(_LP_ESCAPE)


def frame_to_line_protocol(
    measurement: str,
    frame: DecodedFrame,
    ts_ns: int,
    include_tags: bool = True,
) -> str:
    """
    Convert a DecodedFrame to a wide InfluxDB line protocol string.

    Format::

        measurement[,messageName=X,canId=Y] sig1=v1,sig2=v2 timestamp_ns

    Example::

        WFR26,messageName=BMS_Status,canId=512 PackCurrent=-3264.0,SOC=85.0 1700000000000000000

    Raises ValueError if the frame has no numeric signals.
    """
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
    """
    Decode CAN frames and write wide-format points to InfluxDB.

    Each CAN message produces one line protocol point — all decoded signals
    become fields, with ``messageName`` and ``canId`` as optional provenance tags.
    Uses ``influxdb_client`` v2-compat WriteApi for batching.
    """

    def __init__(
        self,
        url: str,
        token: str,
        bucket: str,
        measurement: Optional[str] = None,
        org: str = "",
        batch_size: int = 5000,
        flush_interval_ms: int = 1000,
        dbc_path: Optional[Path] = None,
    ) -> None:
        self._bucket = bucket
        self._org = org
        self._measurement = measurement or bucket
        self._db = load_dbc(dbc_path)
        logger.info("WideWriter: loaded DBC (%d messages)", len(self._db.messages))

        self._client = InfluxDBClient(url=url, token=token or None, org=org)
        self._write_api = self._client.write_api(
            write_options=WriteOptions(
                batch_size=batch_size,
                flush_interval=flush_interval_ms,
                jitter_interval=500,
                retry_interval=5_000,
            )
        )
        logger.info(
            "WideWriter: → %s  bucket=%s  measurement=%s  batch=%d",
            url, bucket, self._measurement, batch_size,
        )

    def decode_and_queue(self, can_id: int, data: bytes, ts_ns: int) -> int:
        """
        Decode a CAN frame and queue its wide point for writing.

        Returns 1 if a point was queued, 0 if the CAN ID is not in the DBC
        or the frame contains no numeric signals.
        """
        frame = decode_frame(self._db, can_id, data)
        if frame is None or not frame.signals:
            return 0
        try:
            line = frame_to_line_protocol(self._measurement, frame, ts_ns)
        except ValueError:
            return 0
        self._write_api.write(bucket=self._bucket, org=self._org, record=line)
        return 1

    def write_lines(self, lines: list[str]) -> None:
        """Write pre-formatted line protocol strings directly (bypass DBC decode)."""
        if lines:
            self._write_api.write(bucket=self._bucket, org=self._org, record=lines)

    def flush(self) -> None:
        """Flush any pending batched writes."""
        self._write_api.flush()

    def close(self) -> None:
        """Flush pending writes and close the underlying InfluxDB client."""
        self._write_api.close()
        self._client.close()

    def __enter__(self) -> "WideWriter":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
