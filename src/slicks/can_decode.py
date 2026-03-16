"""CAN frame decoding — shared logic for all telemetry writers."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import cantools


@dataclass
class DecodedFrame:
    message_name: str
    can_id: int
    signals: dict[str, float] = field(default_factory=dict)


def resolve_dbc_path(env_var: str = "DBC_FILE_PATH", fallback: str = "example.dbc") -> Path:
    """Resolve DBC file path from environment variable or common locations."""
    env_val = os.getenv(env_var, fallback)
    env_path = Path(env_val)
    if env_path.exists():
        return env_path

    candidates = [
        Path("/app/example.dbc"),
        Path("/installer/example.dbc"),
        Path(__file__).parent.parent.parent / "example.dbc",
    ]
    for c in candidates:
        if c.exists():
            return c

    # Try newest .dbc in current directory
    dbcs = sorted(Path(".").glob("*.dbc"), key=lambda p: p.stat().st_mtime, reverse=True)
    if dbcs:
        return dbcs[0]

    raise FileNotFoundError(
        f"Could not find DBC file. Set {env_var} or place example.dbc in /app/."
    )


def load_dbc(path: Optional[Path] = None) -> cantools.Database:
    """Load a cantools DBC database, resolving path if not provided."""
    if path is None:
        path = resolve_dbc_path()
    return cantools.database.load_file(str(path))


def decode_frame(db: cantools.Database, can_id: int, data: bytes) -> Optional[DecodedFrame]:
    """
    Decode a CAN frame using a loaded DBC database.

    Handles:
    - Extended CAN IDs (bit 31 flag stripped before lookup)
    - NamedSignalValue enums (converted to float)
    - Non-numeric signal values (skipped)

    Returns DecodedFrame with only numeric signals, or None if CAN ID not in DBC.
    """
    effective_id = can_id & 0x1FFFFFFF  # Strip extended CAN ID flag

    try:
        message = db.get_message_by_frame_id(effective_id)
    except KeyError:
        return None

    try:
        raw = message.decode(data)
    except Exception:
        return None

    signals: dict[str, float] = {}
    for name, val in raw.items():
        if hasattr(val, "value") and hasattr(val, "name"):
            # NamedSignalValue enum from cantools
            try:
                signals[name] = float(val.value)
            except (ValueError, TypeError):
                continue
        elif isinstance(val, (int, float)):
            signals[name] = float(val)
        # else: skip non-numeric values

    return DecodedFrame(message_name=message.name, can_id=can_id, signals=signals)
