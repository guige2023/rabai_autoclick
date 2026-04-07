"""Tests for queue utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.queue import (
    PriorityQueue,
    WorkQueue,
    BoundedQueue,
    QueueStats,
    create_queue,
)


class TestPriorityQueue:
    """Tests for PriorityQueue."""

    def test_create(self) -> None:
        """Test creating queue."""
        q = PriorityQueue()
        assert q.is_empty

    def test_put_get(self) -> None:
        """Test put and get."""
        q = PriorityQueue()
        q.put(1, priority=0)
        q.put(2, priority=1)
        item = q.get(blocking=False)
        assert item == 1

    def test_priority_order(self) -> None:
        """Test priority ordering."""
        q = PriorityQueue()
        q.put(3, priority=2)
        q.put(1, priority=0)
        q.put(2, priority=1)
        items = []
        while not q.is_empty:
            items.append(q.get(blocking=False))
        assert items == [1, 2, 3]

    def test_max_size(self) -> None:
        """Test max size."""
        q = PriorityQueue(max_size=2)
        q.put(1)
        q.put(2)
        result = q.put(3)
        assert result is False

    def test_remove(self) -> None:
        """Test removing item."""
        q = PriorityQueue()
        q.put(1)
        q.remove(1)
        assert q.is_empty


class TestWorkQueue:
    """Tests for WorkQueue."""

    def test_create(self) -> None:
        """Test creating queue."""
        q = WorkQueue(num_workers=1)
        assert q._num_workers == 1

    def test_submit(self) -> None:
        """Test submitting work."""
        q = WorkQueue(num_workers=0)
        results = []

        def processor(item):
            results.append(item)

        q.set_processor(processor)
        q.submit(1)
        q.submit(2)
        assert q.pending == 2

    def test_start_stop(self) -> None:
        """Test starting and stopping."""
        q = WorkQueue(num_workers=2)
        q.start()
        assert q._running is True
        q.stop()
        assert q._running is False


class TestBoundedQueue:
    """Tests for BoundedQueue."""

    def test_create(self) -> None:
        """Test creating queue."""
        q = BoundedQueue(capacity=2)
        assert q.is_empty
        assert not q.is_full

    def test_put_get(self) -> None:
        """Test put and get."""
        q = BoundedQueue(capacity=2)
        q.put(1)
        q.put(2)
        item = q.get()
        assert item == 1

    def test_full(self) -> None:
        """Test full state."""
        q = BoundedQueue(capacity=1)
        q.put(1)
        assert q.is_full


class TestQueueStats:
    """Tests for QueueStats."""

    def test_create(self) -> None:
        """Test creating stats."""
        stats = QueueStats()
        assert stats._total_put == 0

    def test_record_put(self) -> None:
        """Test recording put."""
        stats = QueueStats()
        stats.record_put()
        assert stats._total_put == 1

    def test_record_get(self) -> None:
        """Test recording get."""
        stats = QueueStats()
        stats.record_get()
        assert stats._total_get == 1

    def test_get_stats(self) -> None:
        """Test getting stats."""
        stats = QueueStats()
        stats.record_put()
        stats.record_put()
        stats.record_get()
        s = stats.get_stats()
        assert s["total_put"] == 2
        assert s["total_get"] == 1


class TestCreateQueue:
    """Tests for create_queue."""

    def test_create_queue(self) -> None:
        """Test creating queue."""
        q = create_queue(max_size=10)
        assert isinstance(q, PriorityQueue)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])