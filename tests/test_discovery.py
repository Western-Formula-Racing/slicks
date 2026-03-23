"""
Tests for the discovery module.

Tests cover:
- discover_sensors with mocked InfluxDB
- Arrow-direct extraction (no pandas)
- Deduplication and sorting
- Error classification and propagation
- Wide schema (default) vs narrow schema (legacy, deprecated)
"""

import unittest
import warnings
from datetime import datetime
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from slicks.discovery import discover_sensors


def _make_wide_mock_config(mock_config):
    """Set config attributes expected by the wide schema path."""
    mock_config.INFLUX_SCHEMA = "iox"
    mock_config.INFLUX_TABLE = "WFR26"
    mock_config.INFLUX_DB = "WFR26"


class TestDiscoverSensorsWide(unittest.TestCase):
    """Tests for discover_sensors with wide schema (default)."""

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_returns_sorted_sensors(self, mock_config, mock_client_class):
        """Should return sorted sensor list from column metadata."""
        _make_wide_mock_config(mock_config)

        mock_col_values = [MagicMock(), MagicMock()]
        mock_col_values[0].as_py.return_value = "SensorB"
        mock_col_values[1].as_py.return_value = "SensorA"

        mock_table = MagicMock()
        mock_table.num_rows = 2
        mock_col = MagicMock()
        mock_col.__iter__ = MagicMock(return_value=iter(mock_col_values))
        mock_table.column.return_value = mock_col

        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 8),
            show_progress=False,
        )

        self.assertEqual(result, ["SensorA", "SensorB"])

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_returns_empty_list_when_no_data(self, mock_config, mock_client_class):
        """Should return empty list when no columns found."""
        _make_wide_mock_config(mock_config)

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            show_progress=False,
        )

        self.assertEqual(result, [])

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_filters_none_values(self, mock_config, mock_client_class):
        """Should skip None values in the column_name column."""
        _make_wide_mock_config(mock_config)

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
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            show_progress=False,
        )

        self.assertEqual(result, ["SensorA"])

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_single_query_regardless_of_time_range(self, mock_config, mock_client_class):
        """Wide schema should issue exactly one metadata query, ignoring time range."""
        _make_wide_mock_config(mock_config)

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 12, 31),
            show_progress=False,
        )

        self.assertEqual(mock_client.query.call_count, 1)

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_backward_compatible_client_param(self, mock_config, mock_client_class):
        """Passing client= should not raise (backward compat), even though unused."""
        _make_wide_mock_config(mock_config)

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        result = discover_sensors(
            start_time=datetime(2025, 1, 1),
            end_time=datetime(2025, 1, 2),
            client=MagicMock(),
            show_progress=False,
        )

        self.assertEqual(result, [])


class TestDiscoverSensorsNarrow(unittest.TestCase):
    """Tests for discover_sensors with narrow schema (legacy EAV, deprecated)."""

    def _make_narrow_config(self, mock_config):
        mock_config.INFLUX_URL = "http://localhost:8086"
        mock_config.INFLUX_TOKEN = "test-token"
        mock_config.INFLUX_SCHEMA = "iox"
        mock_config.INFLUX_TABLE = "WFR26"
        mock_config.INFLUX_DB = "WFR26"

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_emits_deprecation_warning(self, mock_config, mock_client_class):
        """schema='narrow' should emit a DeprecationWarning."""
        self._make_narrow_config(mock_config)

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            discover_sensors(
                start_time=datetime(2025, 1, 1),
                end_time=datetime(2025, 1, 2),
                schema="narrow",
                show_progress=False,
            )

        deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deprecation_warnings), 1)
        self.assertIn("narrow", str(deprecation_warnings[0].message))
        self.assertIn("wide", str(deprecation_warnings[0].message))

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_raises_on_permanent_error(self, mock_config, mock_client_class):
        """Should raise RuntimeError on auth failure."""
        self._make_narrow_config(mock_config)

        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Unauthorized: invalid token")
        mock_client_class.return_value = mock_client

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with self.assertRaises(RuntimeError):
                discover_sensors(
                    start_time=datetime(2025, 1, 1),
                    end_time=datetime(2025, 1, 2),
                    schema="narrow",
                    show_progress=False,
                )

    @patch('slicks.discovery.get_influx_client')
    @patch('slicks.discovery.config')
    def test_default_chunk_size_is_7_days(self, mock_config, mock_client_class):
        """Default chunk_size_days=7 should produce 2 chunks for 10-day range."""
        self._make_narrow_config(mock_config)

        mock_table = MagicMock()
        mock_table.num_rows = 0
        mock_client = MagicMock()
        mock_client.query.return_value = mock_table
        mock_client_class.return_value = mock_client

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            discover_sensors(
                start_time=datetime(2025, 1, 1),
                end_time=datetime(2025, 1, 11),
                schema="narrow",
                show_progress=False,
            )

        # 10-day range with 7-day chunks = 2 chunks, each triggers at least 1 query
        self.assertGreaterEqual(mock_client.query.call_count, 2)


if __name__ == '__main__':
    unittest.main()
