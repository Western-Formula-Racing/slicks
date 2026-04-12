import os
import warnings
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DSN: str = os.getenv("POSTGRES_DSN", "postgresql://wfr:wfr_password@localhost:5432/wfr")
POSTGRES_TABLE: str = os.getenv("POSTGRES_TABLE", "wfr26")

def connect_timescaledb(
    dsn: Optional[str] = None,
    table: Optional[str] = None,
) -> None:
    """
    Update the global configuration settings for TimescaleDB connection.
    
    Call this before using any slicks functions to configure your database.
    
    Args:
        dsn: PostgreSQL connection string (e.g., "postgresql://user:pass@host:5432/db")
        table: The timescale hypertable to query (e.g., "wfr26")
    
    Example:
        >>> import slicks
        >>> slicks.connect_timescaledb(
        ...     dsn="postgresql://wfr:wfr_password@127.0.0.1:5432/wfr",
        ...     table="wfr26test"
        ... )
    """
    global POSTGRES_DSN, POSTGRES_TABLE
    if dsn: POSTGRES_DSN = dsn
    if table: POSTGRES_TABLE = table

def connect_influxdb3(*args, **kwargs) -> None:
    warnings.warn(
        "connect_influxdb3 is deprecated in favor of connect_timescaledb. "
        "Slicks now connects to TimescaleDB via psycopg2 natively.",
        DeprecationWarning,
        stacklevel=2,
    )
    if kwargs.get("db") or kwargs.get("table"):
        connect_timescaledb(table=kwargs.get("db") or kwargs.get("table"))

# Default Sensor Registry
SIGNALS = [
    "PackCurrent",         # Primary heat source
    "M1_Thermistor1",      # Module 1 Temp
    "M3_Thermistor1",      # Module 3 Temp (Middle)
    "M5_Thermistor1",      # Module 5 Temp
    "SOC",                 # State of Charge
    "INV_Motor_Speed",     # Motion context
    "VCU_INV_Torque_Command", # Load context
    "Throttle",            # Driver input (if available)
    "Brake_Percent"        # Driver input (if available)
]