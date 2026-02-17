"""
Tests for the scanner module.

Tests cover:
- _quote_table: SQL table name quoting
- TimeWindow: dataclass and to_dict()
- ScanResult: display, iteration, and export methods
- _compress_bins: merging consecutive time buckets
- scan_data_availability: main function with mocked InfluxDB
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import sys
import os

# Ensure src is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from slicks.scanner import (
    _quote_table,
    _compress_bins,
    TimeWindow,
    ScanResult,
    scan_data_availability,
    UTC,
)


class TestQuoteTable(unittest.TestCase):
    """Tests for the _quote_table helper function."""

    def test_simple_table_name(self):
        """Single table name should be quoted."""
        self.assertEqual(_quote_table("my_table"), '"my_table"')

    def test_schema_table_name(self):
        """Schema.table format should quote both parts."""
        self.assertEqual(_quote_table("iox.WFR25"), '"iox"."WFR25"')

    def test_table_with_special_chars(self):
        """Table names with special characters should be quoted."""
        self.assertEqual(_quote_table("my-table"), '"my-table"')


class TestTimeWindow(unittest.TestCase):
    """Tests for the TimeWindow dataclass."""

    def setUp(self):
        self.start_utc = datetime(2025, 1, 15, 10, 0, tzinfo=UTC)
        self.end_utc = datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
        self.window = TimeWindow(
            start_utc=self.start_utc,
            end_utc=self.end_utc,
            start_local=self.start_utc,
            end_local=self.end_utc,
            row_count=1000,
            bins=2,
        )

    def test_to_dict(self):
        """to_dict() should return proper dictionary."""
        result = self.window.to_dict()
        
        self.assertEqual(result["start_utc"], self.start_utc.isoformat())
        self.assertEqual(result["end_utc"], self.end_utc.isoformat())
        self.assertEqual(result["row_count"], 1000)
        self.assertEqual(result["bins"], 2)


class TestScanResult(unittest.TestCase):
    """Tests for the ScanResult class."""

    def setUp(self):
        """Create sample data for testing."""
        self.start1 = datetime(2025, 1, 15, 10, 0, tzinfo=UTC)
        self.end1 = datetime(2025, 1, 15, 12, 0, tzinfo=UTC)
        self.start2 = datetime(2025, 1, 15, 14, 0, tzinfo=UTC)
        self.end2 = datetime(2025, 1, 15, 16, 0, tzinfo=UTC)
        self.start3 = datetime(2025, 1, 16, 9, 0, tzinfo=UTC)
        self.end3 = datetime(2025, 1, 16, 11, 0, tzinfo=UTC)

        self.windows = {
            "2025-01-15": [
                TimeWindow(self.start1, self.end1, self.start1, self.end1, 1000, 2),
                TimeWindow(self.start2, self.end2, self.start2, self.end2, 500, 2),
            ],
            "2025-01-16": [
                TimeWindow(self.start3, self.end3, self.start3, self.end3, 750, 2),
            ],
        }
        self.result = ScanResult(self.windows, "UTC")

    def test_len(self):
        """__len__ should return number of days."""
        self.assertEqual(len(self.result), 2)

    def test_iter(self):
        """__iter__ should yield (day, windows) pairs in order."""
        items = list(self.result)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0][0], "2025-01-15")
        self.assertEqual(len(items[0][1]), 2)  # Two windows on Jan 15
        self.assertEqual(items[1][0], "2025-01-16")
        self.assertEqual(len(items[1][1]), 1)  # One window on Jan 16

    def test_days_property(self):
        """days property should return sorted list of dates."""
        days = self.result.days
        self.assertEqual(days, ["2025-01-15", "2025-01-16"])

    def test_total_rows_property(self):
        """total_rows should sum all row counts."""
        self.assertEqual(self.result.total_rows, 2250)  # 1000 + 500 + 750

    def test_to_dict(self):
        """to_dict() should return nested dictionary structure."""
        d = self.result.to_dict()
        
        self.assertIn("2025-01-15", d)
        self.assertIn("2025-01-16", d)
        self.assertEqual(len(d["2025-01-15"]), 2)
        self.assertEqual(d["2025-01-15"][0]["row_count"], 1000)

    def test_to_dataframe(self):
        """to_dataframe() should return DataFrame with correct structure."""
        df = self.result.to_dataframe()
        
        self.assertEqual(len(df), 3)  # 3 windows total
        self.assertIn("date", df.columns)
        self.assertIn("start_utc", df.columns)
        self.assertIn("row_count", df.columns)
        self.assertIn("duration_hours", df.columns)
        
        # Check duration calculation
        self.assertEqual(df.iloc[0]["duration_hours"], 2.0)

    def test_repr_not_empty(self):
        """__repr__ should return non-empty string."""
        repr_str = repr(self.result)
        self.assertIn("Data Availability", repr_str)
        self.assertIn("Day 15", repr_str)
        self.assertIn("2 days", repr_str)

    def test_repr_html_not_empty(self):
        """_repr_html_ should return valid HTML."""
        html = self.result._repr_html_()
        self.assertIn("<div", html)
        self.assertIn("Data Availability", html)

    def test_empty_result_repr(self):
        """Empty result should display appropriate message."""
        empty = ScanResult({}, "UTC")
        self.assertIn("No data found", repr(empty))
        self.assertIn("No data found", empty._repr_html_())


class TestCompressBins(unittest.TestCase):
    """Tests for the _compress_bins function."""

    def test_single_bin(self):
        """Single bin should create single window."""
        step = timedelta(hours=1)
        bins = [(datetime(2025, 1, 15, 10, 0, tzinfo=UTC), 100)]
        
        windows = _compress_bins(bins, step)
        
        self.assertEqual(len(windows), 1)
        start, end, bin_count, row_count = windows[0]
        self.assertEqual(bin_count, 1)
        self.assertEqual(row_count, 100)

    def test_consecutive_bins_merged(self):
        """Consecutive bins should be merged into one window."""
        step = timedelta(hours=1)
        bins = [
            (datetime(2025, 1, 15, 10, 0, tzinfo=UTC), 100),
            (datetime(2025, 1, 15, 11, 0, tzinfo=UTC), 150),
            (datetime(2025, 1, 15, 12, 0, tzinfo=UTC), 200),
        ]
        
        windows = _compress_bins(bins, step)
        
        self.assertEqual(len(windows), 1)
        start, end, bin_count, row_count = windows[0]
        self.assertEqual(start, datetime(2025, 1, 15, 10, 0, tzinfo=UTC))
        self.assertEqual(end, datetime(2025, 1, 15, 13, 0, tzinfo=UTC))
        self.assertEqual(bin_count, 3)
        self.assertEqual(row_count, 450)

    def test_gap_creates_separate_windows(self):
        """Gap between bins should create separate windows."""
        step = timedelta(hours=1)
        bins = [
            (datetime(2025, 1, 15, 10, 0, tzinfo=UTC), 100),
            (datetime(2025, 1, 15, 11, 0, tzinfo=UTC), 150),
            # Gap here (12:00 missing)
            (datetime(2025, 1, 15, 13, 0, tzinfo=UTC), 200),
            (datetime(2025, 1, 15, 14, 0, tzinfo=UTC), 250),
        ]
        
        windows = _compress_bins(bins, step)
        
        self.assertEqual(len(windows), 2)
        
        # First window: 10:00-12:00
        self.assertEqual(windows[0][0], datetime(2025, 1, 15, 10, 0, tzinfo=UTC))
        self.assertEqual(windows[0][1], datetime(2025, 1, 15, 12, 0, tzinfo=UTC))
        self.assertEqual(windows[0][2], 2)  # 2 bins
        self.assertEqual(windows[0][3], 250)  # 100 + 150
        
        # Second window: 13:00-15:00
        self.assertEqual(windows[1][0], datetime(2025, 1, 15, 13, 0, tzinfo=UTC))
        self.assertEqual(windows[1][1], datetime(2025, 1, 15, 15, 0, tzinfo=UTC))
        self.assertEqual(windows[1][2], 2)  # 2 bins
        self.assertEqual(windows[1][3], 450)  # 200 + 250

    def test_unsorted_input(self):
        """Function should handle unsorted input."""
        step = timedelta(hours=1)
        bins = [
            (datetime(2025, 1, 15, 12, 0, tzinfo=UTC), 200),
            (datetime(2025, 1, 15, 10, 0, tzinfo=UTC), 100),
            (datetime(2025, 1, 15, 11, 0, tzinfo=UTC), 150),
        ]
        
        windows = _compress_bins(bins, step)
        
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0][0], datetime(2025, 1, 15, 10, 0, tzinfo=UTC))
        self.assertEqual(windows[0][3], 450)

    def test_empty_input(self):
        """Empty input should return empty list."""
        windows = _compress_bins([], timedelta(hours=1))
        self.assertEqual(windows, [])

    def test_day_bin_size(self):
        """Should work with day-sized bins."""
        step = timedelta(days=1)
        bins = [
            (datetime(2025, 1, 15, 0, 0, tzinfo=UTC), 1000),
            (datetime(2025, 1, 16, 0, 0, tzinfo=UTC), 1500),
            # Gap
            (datetime(2025, 1, 18, 0, 0, tzinfo=UTC), 2000),
        ]
        
        windows = _compress_bins(bins, step)
        
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0][2], 2)  # 2 consecutive days
        self.assertEqual(windows[1][2], 1)  # 1 day after gap


class TestScanDataAvailabilityMocked(unittest.TestCase):
    """Tests for scan_data_availability with mocked InfluxDB."""

    @patch('slicks.scanner.InfluxDBClient3')
    @patch('slicks.scanner.config')
    def test_returns_empty_result_when_no_data(self, mock_config, mock_client_class):
        """Should return empty ScanResult when no data found."""
        # Setup mocks
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        # Mock client to return empty table (parallel mode: no context manager)
        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        result = scan_data_availability(
            start=datetime(2025, 1, 1),
            end=datetime(2025, 1, 2),
            show_progress=False,
        )

        self.assertEqual(len(result), 0)
        self.assertEqual(result.total_rows, 0)

    @patch('slicks.scanner.InfluxDBClient3')
    @patch('slicks.scanner.config')
    def test_handles_timezone_aware_inputs(self, mock_config, mock_client_class):
        """Should handle timezone-aware datetime inputs."""
        from zoneinfo import ZoneInfo

        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_client = MagicMock()
        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        eastern = ZoneInfo("America/Toronto")
        start = datetime(2025, 1, 1, tzinfo=eastern)
        end = datetime(2025, 1, 2, tzinfo=eastern)

        # Should not raise
        result = scan_data_availability(
            start=start,
            end=end,
            timezone="America/Toronto",
            show_progress=False,
        )

        self.assertIsInstance(result, ScanResult)

    @patch('slicks.scanner.InfluxDBClient3')
    @patch('slicks.scanner.config')
    def test_raises_on_permanent_error(self, mock_config, mock_client_class):
        """Should raise RuntimeError on permanent errors like auth failure."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "bad-token"
        mock_config.INFLUX_DB = "test-db"

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Unauthorized: invalid token")
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        with self.assertRaises(RuntimeError) as ctx:
            scan_data_availability(
                start=datetime(2025, 1, 1),
                end=datetime(2025, 1, 2),
                show_progress=False,
            )

        self.assertIn("non-recoverable", str(ctx.exception).lower())


class TestScanDataAvailabilityIntegration(unittest.TestCase):
    """Integration tests that use real database (skipped if no credentials)."""

    @classmethod
    def setUpClass(cls):
        """Check if database credentials are available."""
        import slicks
        from slicks import config
        
        # Skip if not configured
        if not config.INFLUX_URL or not config.INFLUX_TOKEN:
            raise unittest.SkipTest("InfluxDB credentials not configured")

    def test_scan_returns_scan_result(self):
        """Basic integration test with real database."""
        result = scan_data_availability(
            start=datetime(2025, 9, 28),
            end=datetime(2025, 9, 30),
            timezone="UTC",
            show_progress=False,
        )
        
        self.assertIsInstance(result, ScanResult)
        # Should have some days with data (based on Sept 28-30 test range)
        self.assertGreater(len(result), 0)


if __name__ == '__main__':
    unittest.main()
