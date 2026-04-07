"""Tests for threading utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.threading_utils import (
    ThreadPool,
    RWLock,
    ThreadSafeDict,
    ThreadSafeCounter,
    FutureGroup,
    Once,
)


class TestThreadPool:
    """Tests for ThreadPool."""

    def test_submit(self) -> None:
        """Test submitting job to pool."""
        with ThreadPool(max_workers=2) as pool:
            future = pool.submit(lambda x: x * 2, 5)
            assert future.result() == 10

    def test_map(self) -> None:
        """Test mapping over pool."""
        with ThreadPool(max_workers=2) as pool:
            results = pool.map(lambda x: x * 2, [1, 2, 3])
            assert results == [2, 4, 6]


class TestRWLock:
    """Tests for RWLock."""

    def test_read_lock(self) -> None:
        """Test acquiring read lock."""
        lock = RWLock()
        with lock.read_lock():
            pass  # Should not raise

    def test_write_lock(self) -> None:
        """Test acquiring write lock."""
        lock = RWLock()
        with lock.write_lock():
            pass  # Should not raise

    def test_multiple_readers(self) -> None:
        """Test multiple readers can hold lock."""
        lock = RWLock()
        results = []

        def reader():
            with lock.read_lock():
                results.append(1)

        import threading
        t1 = threading.Thread(target=reader)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(results) == 2


class TestThreadSafeDict:
    """Tests for ThreadSafeDict."""

    def test_basic_operations(self) -> None:
        """Test basic dict operations."""
        d = ThreadSafeDict()
        d["key"] = "value"
        assert d["key"] == "value"
        assert len(d) == 1

    def test_atomic_update(self) -> None:
        """Test atomic update."""
        d = ThreadSafeDict({"counter": 0})
        new_val = d.atomic_update("counter", lambda x: x + 1)
        assert new_val == 1
        assert d["counter"] == 1

    def test_get_or_create(self) -> None:
        """Test get or create."""
        d = ThreadSafeDict()
        val = d.get_or_create("key", lambda: "created")
        assert val == "created"

        val2 = d.get_or_create("key", lambda: "ignored")
        assert val2 == "created"


class TestThreadSafeCounter:
    """Tests for ThreadSafeCounter."""

    def test_increment(self) -> None:
        """Test increment."""
        counter = ThreadSafeCounter(0)
        assert counter.inc() == 1
        assert counter.inc() == 2

    def test_decrement(self) -> None:
        """Test decrement."""
        counter = ThreadSafeCounter(5)
        assert counter.dec() == 4
        assert counter.dec() == 3


class TestOnce:
    """Tests for Once."""

    def test_runs_once(self) -> None:
        """Test code runs only once."""
        once = Once()
        count = [0]

        def increment():
            count[0] += 1

        once(increment)
        once(increment)
        assert count[0] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])