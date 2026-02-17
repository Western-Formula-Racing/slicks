"""
Tests for the discovery module.

Tests cover:
- discover_sensors with mocked InfluxDB
- Arrow-direct extraction (no pandas)
- Deduplication and sorting
- Error classification and propagation
"""

import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from slicks.discovery import discover_sensors


class TestDiscoverSensorsMocked(unittest.TestCase):
    """Tests for discover_sensors with mocked InfluxDB."""

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_returns_sorted_unique_sensors(self, mock_config, mock_client_class):
        """Should return sorted deduplicated sensor list."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_col_values = [MagicMock(), MagicMock(), MagicMock()]
        mock_col_values[0].as_py.return_value = "SensorB"
        mock_col_values[1].as_py.return_value = "SensorA"
        mock_col_values[2].as_py.return_value = "SensorB"  # duplicate

        mock_table = MagicMock()
        mock_table.num_rows = 3
        mock_col = MagicMock()
        mock_col.__iter__ = MagicMock(return_value=iter(mock_col_values))
        mock_table.column.return_value = mock_col

        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 8),
            show_progress=False,
        )

        self.assertEqual(result, ["SensorA", "SensorB"])

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_returns_empty_list_when_no_data(self, mock_config, mock_client_class):
        """Should return empty list when no sensors found."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            show_progress=False,
        )

        self.assertEqual(result, [])

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_raises_on_permanent_error(self, mock_config, mock_client_class):
        """Should raise RuntimeError on auth failure."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "bad-token"
        mock_config.INFLUX_DB = "test-db"

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Unauthorized: invalid token")
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        with self.assertRaises(RuntimeError):
            discover_sensors(
                start_time=datetime(2025, 1, 1),
                end_time=datetime(2025, 1, 2),
                show_progress=False,
            )

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_filters_none_values(self, mock_config, mock_client_class):
        """Should skip None values in the signal name column."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_col_values = [MagicMock(), MagicMock()]
        mock_col_values[0].as_py.return_value = "SensorA"
        mock_col_values[1].as_py.return_value = None

        mock_table = MagicMock()
        mock_table.num_rows = 2
        mock_col = MagicMock()
        mock_col.__iter__ = MagicMock(return_value=iter(mock_col_values))
        mock_table.column.return_value = mock_col

        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            show_progress=False,
        )

        self.assertEqual(result, ["SensorA"])

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_default_chunk_size_is_7_days(self, mock_config, mock_client_class):
        """Default chunk_size_days=7 should produce 2 chunks for 10-day range."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 11),
            show_progress=False,
        )

        # 10-day range with 7-day chunks = 2 chunks, each triggers at least 1 query
        self.assertGreaterEqual(mock_client.query.call_count, 2)

    @patch('slicks.discovery.InfluxDBClient3')
    @patch('slicks.discovery.config')
    def test_backward_compatible_client_param(self, mock_config, mock_client_class):
        """Passing client= should not raise (backward compat), even though unused."""
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_DB = "test-db"

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client.close = MagicMock()
        mock_client_class.return_value = mock_client

        old_client = MagicMock()
        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            client=old_client,
            show_progress=False,
        )

        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
