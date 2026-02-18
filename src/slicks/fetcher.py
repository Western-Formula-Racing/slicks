import os
from datetime import datetime, timedelta
import pandas as pd
from influxdb_client_3 import InfluxDBClient3
from . import config
from .query_utils import quote_table
from .movement_detector import filter_data_in_movement


def get_influx_client(url=None, token=None, org=None, db=None):
    """
    Returns an InfluxDB Client.
    Allows explicit overriding of credentials for library usage,
    otherwise falls back to config/env vars.
    """
    return InfluxDBClient3(
        host=url or config.INFLUX_URL,
        token=token or config.INFLUX_TOKEN,
        org=org or config.INFLUX_ORG, # Client3 uses 'org' param often, though strictly 'database' is key for IOx
        database=db or config.INFLUX_DB
    )


def list_target_sensors():
    """
    Returns the list of DEFAULT sensors configured in config.py.
    """
    return config.SIGNALS


def fetch_telemetry(start_time, end_time, signals=None, client=None, filter_movement=True, resample="1s"):
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
    
    # Construct query
    signal_list = "', '".join(signals)
    
    # Ensure safe defaults if config vars are missing or empty
    schema = config.INFLUX_SCHEMA or "iox"
    table = config.INFLUX_TABLE or config.INFLUX_DB
    table_ref = quote_table(schema, table)
    
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