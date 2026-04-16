import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

POSTGRES_DSN: str = os.getenv(
    "POSTGRES_DSN",
    "postgresql://wfr:wfr_password@localhost:5432/wfr",
)
TIMESCALE_SCHEMA: str = os.getenv("TIMESCALE_SCHEMA", "public")
TIMESCALE_TABLE: str = os.getenv("TIMESCALE_TABLE") or os.getenv("TIMESCALE_SEASON", "wfr25")

# Backward compatibility aliases (legacy naming)
INFLUX_URL: str = os.getenv("INFLUX_URL", "")
INFLUX_TOKEN: str = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG: str = os.getenv("INFLUX_ORG", "")
INFLUX_DB: str = TIMESCALE_TABLE
INFLUX_SCHEMA: str = TIMESCALE_SCHEMA
INFLUX_TABLE: str = TIMESCALE_TABLE


def _sync_legacy_aliases() -> None:
    """Keep legacy INFLUX_* globals aligned for callers not yet migrated."""
    global INFLUX_DB, INFLUX_SCHEMA, INFLUX_TABLE
    INFLUX_DB = TIMESCALE_TABLE
    INFLUX_SCHEMA = TIMESCALE_SCHEMA
    INFLUX_TABLE = TIMESCALE_TABLE


def connect_timescaledb(
    dsn: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> None:
    """Update Timescale/Postgres connection settings used by slicks."""
    global POSTGRES_DSN, TIMESCALE_SCHEMA, TIMESCALE_TABLE
    if dsn:
        POSTGRES_DSN = dsn
    if schema:
        TIMESCALE_SCHEMA = schema
    if table:
        TIMESCALE_TABLE = table
    _sync_legacy_aliases()

def connect_influxdb3(
    url: Optional[str] = None,
    token: Optional[str] = None,
    org: Optional[str] = None,
    db: Optional[str] = None,
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> None:
    """
    Backward-compatible wrapper that maps legacy Influx-style kwargs to
    Timescale configuration.
    
    Args:
        url/token/org are accepted for compatibility and ignored.
        db: Legacy alias for table name.
        schema: Database schema name.
        table: Timescale table name.
    
    Example:
        >>> import slicks
        >>> slicks.connect_influxdb3(
        ...     url="https://us-east-1-1.aws.cloud2.influxdata.com",
        ...     token="your-api-token",
        ...     db="WFR25",
        ...     table="my_custom_table"
        ... )
    """
    global INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG
    if url:
        INFLUX_URL = url
    if token:
        INFLUX_TOKEN = token
    if org:
        INFLUX_ORG = org

    effective_table = table or db
    connect_timescaledb(schema=schema, table=effective_table)


# Default Sensor Registry
# In an open-source context, this serves as an "Example Configuration"
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