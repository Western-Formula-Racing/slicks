import unittest
import sys
import os
from datetime import datetime
import pandas as pd

# Ensure src is in path so we can import slicks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from slicks.fetcher import fetch_telemetry, get_influx_client
from slicks.movement_detector import detect_movement_ratio
from slicks import config


def _influx_available() -> bool:
    """Return True if the configured InfluxDB instance is reachable and has data."""
    try:
        client = get_influx_client()
        client.query(
            query=f"SELECT 1 FROM \"{config.INFLUX_DB}\" LIMIT 1",
            mode="pandas",
        )
        return True
    except Exception:
        return False


_SKIP_REASON = "Live InfluxDB not available or configured database not found"


@unittest.skipUnless(_influx_available(), _SKIP_REASON)
class TestTelemetryPipeline(unittest.TestCase):
    def setUp(self):
        # Time range as specified: Sept 28-30
        # Assuming 2025 based on DB name WFR25
        self.start_time = datetime(2025, 9, 28)
        self.end_time = datetime(2025, 9, 30)

    def test_pipeline(self):
        print(f"\n[Test] Fetching telemetry from {self.start_time} to {self.end_time}...")
        
        # 1. Fetch Data
        df = fetch_telemetry(self.start_time, self.end_time)
        
        self.assertIsNotNone(df, "Fetcher returned None. Connection failed or no data.")
        self.assertFalse(df.empty, "Fetcher returned empty DataFrame.")
        
        print(f"[Test] Successfully fetched {len(df)} rows.")
        
        # 2. Verify Data Structure
        expected_columns = ["INV_Motor_Speed"] # We need this for movement detection
        for col in expected_columns:
            self.assertIn(col, df.columns, f"Missing expected column: {col}")
            
        # 3. Test Movement Detector
        # fetch_telemetry already calls filter_data_in_movement, so df is already filtered.
        # But let's run detect_movement_ratio on it just to ensure the function works and the data makes sense.
        # Since df is already filtered to only moving rows, movement_ratio should be 1.0 (or close to it/valid).
        
        stats = detect_movement_ratio(df)
        print(f"[Test] Movement Stats on filtered data: {stats}")
        
        self.assertGreater(stats['total_rows'], 0)
        # Since fetch_telemetry filters for movement, we expect mostly moving rows.
        # However, filter logic might be slightly different than pure threshold if we changed it, 
        # but here they share logic.
        self.assertEqual(stats['idle_rows'], 0, "Expected 0 idle rows in already filtered data.")

    def test_single_string_sensor(self):
        """Test passing a single string instead of a list."""
        print(f"\n[Test] Fetching single sensor 'INV_Motor_Speed'...")
        df = fetch_telemetry(self.start_time, self.end_time, signals="INV_Motor_Speed")
        
        self.assertIsNotNone(df)
        self.assertFalse(df.empty)
        self.assertIn("INV_Motor_Speed", df.columns)
        self.assertEqual(len(df.columns), 1)

    def test_optional_filtering(self):
        """Test disabling the movement filter."""
        print(f"\n[Test] Fetching with filter_movement=False...")
        # Fetch raw data (including idle times)
        df_raw = fetch_telemetry(
            self.start_time, 
            self.end_time, 
            signals="INV_Motor_Speed", 
            filter_movement=False
        )
        
        # Fetch filtered data (default)
        df_filtered = fetch_telemetry(
            self.start_time, 
            self.end_time, 
            signals="INV_Motor_Speed", 
            filter_movement=True
        )
        
        self.assertIsNotNone(df_raw)
        self.assertIsNotNone(df_filtered)
        
        # Raw data should have same or more rows than filtered data
        print(f"Raw rows: {len(df_raw)} | Filtered rows: {len(df_filtered)}")
        self.assertGreaterEqual(len(df_raw), len(df_filtered))

if __name__ == '__main__':
    unittest.main()
