"""
Shared utilities for querying against TimescaleDB.

Provides:
- Error classification (recoverable vs permanent)
- Quoting utilities
"""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

_PERMANENT_ERROR_PATTERNS = (
    "relation does not exist",
    "password authentication failed",
    "permission denied",
    "database does not exist",
    "syntax error",
)

def quote_table(schema: str, table: str) -> str:
    """Quote table name for SQL, handling schema.table format."""
    # Since Timescale natively respects dot notation, we don't strictly *need* to quote 
    # if it's already "schema.table", but PostgreSQL standard is strictly `schema`.`table` or just `table`
    if schema:
        return f'"{schema}"."{table}"'
    return f'"{table}"'

class PermanentQueryError(Exception):
    """An error that will not resolve by splitting the time range."""

def is_permanent_error(exc: Exception) -> bool:
    """Classify an exception as permanent (non-retryable) vs recoverable."""
    msg = str(exc).lower()
    return any(pattern in msg for pattern in _PERMANENT_ERROR_PATTERNS)
    
# We strip parallel chunk execution here largely because SQLAlchemy and Pandas 
# handle execution threading natively behind engines, and TimescaleDB 
# doesn't suffer the restrictive Parquet file size cap that forced chunk bisection.
