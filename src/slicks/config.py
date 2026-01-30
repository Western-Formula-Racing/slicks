import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# InfluxDB Configuration with Environment Overrides
# This makes the package "Open Source Ready" - anyone can swap these out via .env
INFLUX_URL: str = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN: str = os.getenv("INFLUX_TOKEN", "my-token") 
INFLUX_ORG: str = os.getenv("INFLUX_ORG", "Docs") 
INFLUX_DB: str = os.getenv("INFLUX_DB", "WFR25")

def connect_influxdb3(
    url: Optional[str] = None,
    token: Optional[str] = None,
    org: Optional[str] = None,
    db: Optional[str] = None,
) -> None:
    """
    Update the global configuration settings for InfluxDB connection.
    
    Call this before using any slicks functions to configure your database.
    
    Args:
        url: InfluxDB host URL (e.g., "https://your-instance.influxdb.cloud")
        token: Your InfluxDB API token
        org: Organization name (optional for InfluxDB 3.x)
        db: Database/bucket name (e.g., "WFR25")
    
    Example:
        >>> import slicks
        >>> slicks.connect_influxdb3(
        ...     url="https://us-east-1-1.aws.cloud2.influxdata.com",
        ...     token="your-api-token",
        ...     db="WFR25"
        ... )
    """
    global INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_DB
    if url: INFLUX_URL = url
    if token: INFLUX_TOKEN = token
    if org: INFLUX_ORG = org
    if db: INFLUX_DB = db

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