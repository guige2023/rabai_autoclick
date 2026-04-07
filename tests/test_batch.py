"""Tests for batch processing utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.batch import (
    BatchResult,
    WorkerStats,
    batch_process,
    parallel_map,
    chunk_process,
    WorkerPool,
    imap,
    starmap,
)


class TestBatchResult:
    """Tests for BatchResult."""

    def test_create(self) -> None:
        """Test creating batch result."""
        result = BatchResult(
            total=10,
            successful=8,
            failed=2,
            results=[1, 2, 3],
            errors=[Exception("e1"), Exception("e2")],
        )
        assert result.total == 10
        assert result.successful == 8
        assert result.failed == 2
        assert len(result.results) == 3
        assert len(result.errors) == 2


class TestBatchProcess:
    """Tests for batch_process."""

    def test_process_all_success(self) -> None:
        """Test batch processing all success."""
        items = [1, 2, 3, 4, 5]
        result = batch_process(items, lambda x: x * 2, batch_size=2, max_workers=2)
        assert result.total == 5
        assert result.successful == 5
        assert result.failed == 0
        assert result.results == [2, 4, 6, 8, 10]

    def test_process_with_failure(self) -> None:
        """Test batch processing with failures."""
        items = [1, 2, 3]
        def processor(x):
            if x == 2:
                raise ValueError("bad")
            return x * 2
        result = batch_process(items, processor, max_workers=2)
        assert result.total == 3
        assert result.successful == 2
        assert result.failed == 1
        assert len(result.errors) == 1

    def test_process_empty(self) -> None:
        """Test batch processing empty list."""
        result = batch_process([], lambda x: x)
        assert result.total == 0
        assert result.successful == 0
        assert result.failed == 0


class TestParallelMap:
    """Tests for parallel_map."""

    def test_parallel_map(self) -> None:
        """Test parallel map."""
        items = [1, 2, 3, 4, 5]
        result = parallel_map(lambda x: x * 2, items, max_workers=2)
        assert sorted(result) == [2, 4, 6, 8, 10]

    def test_parallel_map_single_worker(self) -> None:
        """Test parallel map with single worker."""
        items = [1, 2, 3]
        result = parallel_map(lambda x: x + 1, items, max_workers=1)
        assert result == [2, 3, 4]


class TestChunkProcess:
    """Tests for chunk_process."""

    def test_chunk_process(self) -> None:
        """Test chunk processing."""
        items = [1, 2, 3, 4, 5, 6, 7]
        chunks = list(chunk_process(items, sum, chunk_size=3))
        assert chunks == [[1, 2, 3], [4, 5, 6], [7]]

    def test_chunk_process_large_chunks(self) -> None:
        """Test chunk processing with chunk size larger than items."""
        items = [1, 2, 3]
        chunks = list(chunk_process(items, sum, chunk_size=10))
        assert chunks == [[1, 2, 3]]

    def test_chunk_process_exact_fit(self) -> None:
        """Test chunk processing when items fit exactly."""
        items = [1, 2, 3, 4]
        chunks = list(chunk_process(items, sum, chunk_size=2))
        assert chunks == [[1, 2], [3, 4]]


class TestWorkerStats:
    """Tests for WorkerStats."""

    def test_create(self) -> None:
        """Test creating worker stats."""
        stats = WorkerStats(worker_id=1, items_processed=100, errors=5)
        assert stats.worker_id == 1
        assert stats.items_processed == 100
        assert stats.errors == 5


class TestWorkerPool:
    """Tests for WorkerPool."""

    def test_create_default(self) -> None:
        """Test creating worker pool with defaults."""
        pool = WorkerPool()
        assert pool.num_workers >= 1

    def test_create_specified(self) -> None:
        """Test creating worker pool with specified workers."""
        pool = WorkerPool(num_workers=4)
        assert pool.num_workers == 4

    def test_map(self) -> None:
        """Test map using worker pool."""
        pool = WorkerPool(num_workers=2)
        items = [1, 2, 3, 4, 5]
        result = pool.map(lambda x: x * 2, items)
        assert sorted(result) == [2, 4, 6, 8, 10]

    def test_submit_batch(self) -> None:
        """Test submitting batch to worker pool."""
        pool = WorkerPool(num_workers=2)
        items = [1, 2, 3]
        result = pool.submit_batch(lambda x: x + 1, items, batch_size=2)
        assert result.total == 3
        assert result.successful == 3

    def test_stats_property(self) -> None:
        """Test stats property."""
        pool = WorkerPool(num_workers=2)
        assert pool.stats == []


class TestImap:
    """Tests for imap."""

    def test_imap(self) -> None:
        """Test imap yields results."""
        items = [1, 2, 3, 4, 5]
        result = list(imap(lambda x: x * 2, items, max_workers=2))
        assert sorted(result) == [2, 4, 6, 8, 10]

    def test_imap_with_exceptions(self) -> None:
        """Test imap handles exceptions."""
        items = [1, 2, 3]
        def func(x):
            if x == 2:
                raise ValueError("bad")
            return x
        result = list(imap(func, items, max_workers=2))
        assert result[0] == 1
        assert isinstance(result[1], ValueError)
        assert result[2] == 3

    def test_imap_preserves_order(self) -> None:
        """Test imap preserves item order."""
        items = [3, 1, 2]
        result = list(imap(lambda x: x, items, max_workers=4))
        assert result == [3, 1, 2]


class TestStarmap:
    """Tests for starmap."""

    def test_starmap(self) -> None:
        """Test starmap."""
        def add(a, b):
            return a + b
        args = [(1, 2), (3, 4), (5, 6)]
        result = starmap(add, args, max_workers=2)
        assert result == [3, 7, 11]

    def test_starmap_single_arg(self) -> None:
        """Test starmap with single argument tuple."""
        def increment(x):
            return x + 1
        args = [(1,), (2,), (3,)]
        result = starmap(increment, args, max_workers=2)
        assert result == [2, 3, 4]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])