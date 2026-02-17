"""
Tests for the query_utils module.

Tests cover:
- is_permanent_error classification
- adaptive_query recursion and fallback
- run_chunks_parallel ordering and error propagation
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from slicks.query_utils import (
    is_permanent_error,
    adaptive_query,
    run_chunks_parallel,
    PermanentQueryError,
)

UTC = timezone.utc


class TestIsPermanentError(unittest.TestCase):
    """Tests for error classification."""

    def test_unauthorized_is_permanent(self):
        self.assertTrue(is_permanent_error(Exception("Unauthorized: invalid token")))

    def test_table_not_found_is_permanent(self):
        self.assertTrue(is_permanent_error(Exception("table not found: iox.WFR25")))

    def test_syntax_error_is_permanent(self):
        self.assertTrue(is_permanent_error(Exception("syntax error at position 42")))

    def test_permission_denied_is_permanent(self):
        self.assertTrue(is_permanent_error(Exception("permission denied for table")))

    def test_timeout_is_recoverable(self):
        self.assertFalse(is_permanent_error(Exception("query timeout exceeded")))

    def test_resource_limit_is_recoverable(self):
        self.assertFalse(is_permanent_error(Exception("resource limit exceeded")))

    def test_generic_error_is_recoverable(self):
        self.assertFalse(is_permanent_error(Exception("some transient failure")))


class TestAdaptiveQuery(unittest.TestCase):
    """Tests for adaptive_query recursion."""

    def test_returns_primary_result_on_success(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 1, 2, tzinfo=UTC)

        def primary(c, start, end):
            return [("data", 1)]

        result = adaptive_query(client, t0, t1, primary)
        self.assertEqual(result, [("data", 1)])

    def test_splits_on_recoverable_error(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 1, 3, tzinfo=UTC)

        call_count = [0]

        def primary(c, start, end):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("resource limit exceeded")
            return [("ok", 1)]

        result = adaptive_query(client, t0, t1, primary)
        self.assertEqual(len(result), 2)
        self.assertEqual(call_count[0], 3)  # 1 fail + 2 success

    def test_raises_on_permanent_error(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 1, 2, tzinfo=UTC)

        def primary(c, start, end):
            raise Exception("Unauthorized: invalid token")

        with self.assertRaises(PermanentQueryError):
            adaptive_query(client, t0, t1, primary)

    def test_uses_fallback_at_min_span(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 1, 1, 0, 5, tzinfo=UTC)  # 5 min

        def primary(c, start, end):
            raise Exception("timeout")

        def fallback(c, start, end):
            return [("fallback", 1)]

        result = adaptive_query(
            client, t0, t1, primary,
            fallback_fn=fallback,
            min_span=timedelta(hours=1),
        )
        self.assertEqual(result, [("fallback", 1)])

    def test_returns_empty_when_no_fallback_at_min_span(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 1, 1, 0, 5, tzinfo=UTC)

        def primary(c, start, end):
            raise Exception("timeout")

        result = adaptive_query(
            client, t0, t1, primary,
            min_span=timedelta(hours=1),
        )
        self.assertEqual(result, [])

    def test_respects_max_depth(self):
        client = MagicMock()
        t0 = datetime(2025, 1, 1, tzinfo=UTC)
        t1 = datetime(2025, 12, 31, tzinfo=UTC)

        call_count = [0]

        def primary(c, start, end):
            call_count[0] += 1
            raise Exception("resource limit")

        def fallback(c, start, end):
            return [("fb", 1)]

        result = adaptive_query(
            client, t0, t1, primary,
            fallback_fn=fallback,
            max_depth=2,
        )
        # Should stop recursing at depth 2 and use fallback
        self.assertTrue(len(result) > 0)
        self.assertTrue(all(r == ("fb", 1) for r in result))


class TestRunChunksParallel(unittest.TestCase):
    """Tests for parallel chunk execution."""

    def test_preserves_chunk_order(self):
        chunks = [
            (datetime(2025, 1, i, tzinfo=UTC),
             datetime(2025, 1, i + 1, tzinfo=UTC))
            for i in range(1, 5)
        ]

        def factory():
            m = MagicMock()
            m.close = MagicMock()
            return m

        def query_fn(client, t0, t1):
            return [(t0.day,)]

        results = run_chunks_parallel(factory, chunks, query_fn, max_workers=2)
        days = [r[0] for r in results]
        self.assertEqual(days, [1, 2, 3, 4])

    def test_propagates_permanent_error(self):
        chunks = [
            (datetime(2025, 1, 1, tzinfo=UTC),
             datetime(2025, 1, 2, tzinfo=UTC)),
        ]

        def factory():
            m = MagicMock()
            m.close = MagicMock()
            return m

        def query_fn(client, t0, t1):
            raise PermanentQueryError("table not found")

        with self.assertRaises(PermanentQueryError):
            run_chunks_parallel(factory, chunks, query_fn, max_workers=1)

    def test_empty_chunks_returns_empty(self):
        result = run_chunks_parallel(
            MagicMock, [], lambda c, t0, t1: [], max_workers=1,
        )
        self.assertEqual(result, [])

    def test_calls_on_chunk_done(self):
        chunks = [
            (datetime(2025, 1, 1, tzinfo=UTC),
             datetime(2025, 1, 2, tzinfo=UTC)),
            (datetime(2025, 1, 2, tzinfo=UTC),
             datetime(2025, 1, 3, tzinfo=UTC)),
        ]
        done_indices = []

        def factory():
            m = MagicMock()
            m.close = MagicMock()
            return m

        def query_fn(client, t0, t1):
            return [("ok",)]

        run_chunks_parallel(
            factory, chunks, query_fn,
            max_workers=1,
            on_chunk_done=lambda idx: done_indices.append(idx),
        )
        self.assertEqual(sorted(done_indices), [0, 1])

    def test_closes_clients(self):
        chunks = [
            (datetime(2025, 1, 1, tzinfo=UTC),
             datetime(2025, 1, 2, tzinfo=UTC)),
        ]
        clients_created = []

        def factory():
            m = MagicMock()
            m.close = MagicMock()
            clients_created.append(m)
            return m

        def query_fn(client, t0, t1):
            return []

        run_chunks_parallel(factory, chunks, query_fn, max_workers=1)
        self.assertEqual(len(clients_created), 1)
        clients_created[0].close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
