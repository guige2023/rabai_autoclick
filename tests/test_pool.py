"""Tests for object pooling utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.pool import ObjectPool, ConnectionPool, WorkerPool, PooledObject


class DummyObject:
    """Dummy object for testing."""
    def __init__(self):
        self.value = "test"


class TestObjectPool:
    """Tests for ObjectPool."""

    def test_acquire_returns_object(self) -> None:
        """Test acquiring object from pool."""
        pool = ObjectPool[DummyObject](DummyObject, max_size=3)
        obj = pool.acquire()
        assert isinstance(obj, DummyObject)

    def test_reuse_object(self) -> None:
        """Test that objects are reused."""
        pool = ObjectPool[DummyObject](DummyObject, max_size=2)
        obj1 = pool.acquire()
        pool.release(obj1)
        obj2 = pool.acquire()
        assert obj1 is obj2

    def test_max_size(self) -> None:
        """Test max size limit."""
        pool = ObjectPool[DummyObject](DummyObject, max_size=2)
        pool.acquire()
        pool.acquire()
        # Third acquire may or may not be from pool
        obj = pool.acquire()
        assert obj is not None

    def test_clear(self) -> None:
        """Test clearing pool."""
        pool = ObjectPool[DummyObject](DummyObject, max_size=2)
        pool.acquire()
        pool.acquire()
        pool.clear()
        assert pool.size == 0


class TestPooledObject:
    """Tests for PooledObject."""

    def test_context_manager(self) -> None:
        """Test context manager usage."""
        pool = ObjectPool[DummyObject](DummyObject, max_size=2)
        with PooledObject(pool, pool.acquire()) as obj:
            assert isinstance(obj, DummyObject)


class TestConnectionPool:
    """Tests for ConnectionPool."""

    def test_get_connection(self) -> None:
        """Test getting connection."""
        pool = ConnectionPool[DummyObject](
            DummyObject, max_connections=3
        )
        conn = pool.get_connection()
        assert conn is not None

    def test_return_connection(self) -> None:
        """Test returning connection."""
        pool = ConnectionPool[DummyObject](
            DummyObject, max_connections=3
        )
        conn = pool.get_connection()
        pool.return_connection(conn)

    def test_health_check(self) -> None:
        """Test health check on connection."""
        pool = ConnectionPool[DummyObject](
            DummyObject,
            max_connections=3,
            health_check=lambda x: hasattr(x, "value"),
        )
        conn = pool.get_connection()
        assert conn is not None


class TestWorkerPool:
    """Tests for WorkerPool."""

    def test_start_stop(self) -> None:
        """Test starting and stopping pool."""
        pool = WorkerPool(num_workers=2)
        pool.start()
        assert len(pool._workers) == 2
        pool.shutdown(wait=False)

    def test_submit_task(self) -> None:
        """Test submitting task."""
        results = []

        def task(x):
            results.append(x)

        pool = WorkerPool(num_workers=2)
        pool.start()
        pool.submit(task, 1)
        time.sleep(0.1)
        pool.shutdown(wait=True)
        assert 1 in results


if __name__ == "__main__":
    pytest.main([__file__, "-v"])