"""Tests for async/future utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.future import (
    FutureStatus,
    FutureResult,
    Future,
    ThreadFuture,
    Promise,
    FutureStub,
    AsyncBatch,
    async_call,
)


class TestFutureStatus:
    """Tests for FutureStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert FutureStatus.PENDING.value == "pending"
        assert FutureStatus.COMPLETED.value == "completed"


class TestFutureResult:
    """Tests for FutureResult."""

    def test_create(self) -> None:
        """Test creating result."""
        result = FutureResult(status=FutureStatus.COMPLETED, value=42)
        assert result.value == 42


class DummyFuture(Future):
    """Dummy future for testing."""

    def __init__(self):
        super().__init__()

    def get(self, timeout=None):
        return self._result


class TestFuture:
    """Tests for Future."""

    def test_status(self) -> None:
        """Test status tracking."""
        future = DummyFuture()
        assert future.status == FutureStatus.PENDING

    def test_is_done(self) -> None:
        """Test is_done check."""
        future = DummyFuture()
        assert future.is_done is False

    def test_on_complete(self) -> None:
        """Test completion callback."""
        future = DummyFuture()
        results = []
        future.on_complete(lambda r: results.append(r))
        future._result = 42
        future._status = FutureStatus.COMPLETED
        future._notify_complete()
        assert results == [42]


class TestThreadFuture:
    """Tests for ThreadFuture."""

    def test_create(self) -> None:
        """Test creating future."""
        future = ThreadFuture(lambda: 42)
        assert future.status == FutureStatus.PENDING

    def test_get(self) -> None:
        """Test getting result."""
        future = ThreadFuture(lambda: 42)
        result = future.get(timeout=5)
        assert result == 42
        assert future.is_success

    def test_get_with_args(self) -> None:
        """Test getting result with args."""
        def add(a, b):
            return a + b
        future = ThreadFuture(add, 1, 2)
        result = future.get(timeout=5)
        assert result == 3

    def test_get_error(self) -> None:
        """Test error propagation."""
        future = ThreadFuture(lambda: (_ for _ in ()).throw(ValueError("fail")))
        with pytest.raises(ValueError):
            future.get(timeout=5)


class TestPromise:
    """Tests for Promise."""

    def test_create(self) -> None:
        """Test creating promise."""
        promise = Promise()
        assert promise.future is not None

    def test_resolve(self) -> None:
        """Test resolving promise."""
        promise = Promise()
        promise.resolve(42)
        assert promise.future.get(timeout=1) == 42

    def test_reject(self) -> None:
        """Test rejecting promise."""
        promise = Promise()
        promise.reject(ValueError("fail"))
        with pytest.raises(ValueError):
            promise.future.get(timeout=1)


class TestFutureStub:
    """Tests for FutureStub."""

    def test_set_result(self) -> None:
        """Test setting result."""
        stub = FutureStub()
        stub._set_result(42)
        assert stub.get(timeout=1) == 42


class TestAsyncBatch:
    """Tests for AsyncBatch."""

    def test_create(self) -> None:
        """Test creating batch."""
        batch = AsyncBatch()
        assert len(batch.futures) == 0

    def test_add(self) -> None:
        """Test adding operation."""
        batch = AsyncBatch()
        batch.add(lambda: 1)
        assert len(batch.futures) == 1

    def test_wait_all(self) -> None:
        """Test waiting for all."""
        batch = AsyncBatch()
        batch.add(lambda: 1)
        batch.add(lambda: 2)
        batch.start_all()
        results = batch.wait_all(timeout=5)
        assert 1 in results
        assert 2 in results


class TestAsyncCall:
    """Tests for async_call."""

    def test_async_call(self) -> None:
        """Test async call."""
        future = async_call(lambda: 42)
        result = future.get(timeout=5)
        assert result == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])