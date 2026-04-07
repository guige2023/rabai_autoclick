"""Tests for locking utilities."""

import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.locker import (
    ReentrantLock,
    ReadWriteLock,
    Semaphore,
    CriticalSection,
    LockManager,
    InterProcessLock,
)


class TestReentrantLock:
    """Tests for ReentrantLock."""

    def test_acquire_release(self) -> None:
        """Test basic acquire/release."""
        lock = ReentrantLock()
        assert lock.acquire()
        lock.release()
        assert not lock._lock.locked()

    def test_reentrant(self) -> None:
        """Test reentrant acquire."""
        lock = ReentrantLock()
        lock.acquire()
        lock.acquire()
        lock.release()
        lock.release()
        assert not lock._lock.locked()

    def test_context_manager(self) -> None:
        """Test context manager."""
        lock = ReentrantLock()
        with lock:
            assert lock._lock.locked()
        assert not lock._lock.locked()


class TestReadWriteLock:
    """Tests for ReadWriteLock."""

    def test_acquire_read(self) -> None:
        """Test acquiring read lock."""
        lock = ReadWriteLock()
        assert lock.acquire_read()

    def test_acquire_write(self) -> None:
        """Test acquiring write lock."""
        lock = ReadWriteLock()
        assert lock.acquire_write()

    def test_read_write(self) -> None:
        """Test read-write interaction."""
        lock = ReadWriteLock()
        lock.acquire_read()
        lock.release_read()
        lock.acquire_write()
        lock.release_write()


class TestSemaphore:
    """Tests for Semaphore."""

    def test_acquire_release(self) -> None:
        """Test basic acquire/release."""
        sem = Semaphore(1)
        assert sem.acquire()
        sem.release()

    def test_context_manager(self) -> None:
        """Test context manager."""
        sem = Semaphore(1)
        with sem:
            pass


class TestCriticalSection:
    """Tests for CriticalSection."""

    def test_enter_leave(self) -> None:
        """Test basic enter/leave."""
        cs = CriticalSection()
        cs.enter()
        cs.leave()

    def test_context_manager(self) -> None:
        """Test context manager."""
        cs = CriticalSection()
        with cs:
            pass


class TestLockManager:
    """Tests for LockManager."""

    def test_get_lock(self) -> None:
        """Test getting named lock."""
        manager = LockManager()
        lock1 = manager.get_lock("test")
        lock2 = manager.get_lock("test")
        assert lock1 is lock2

    def test_get_different_locks(self) -> None:
        """Test getting different locks."""
        manager = LockManager()
        lock1 = manager.get_lock("a")
        lock2 = manager.get_lock("b")
        assert lock1 is not lock2

    def test_remove_lock(self) -> None:
        """Test removing lock."""
        manager = LockManager()
        manager.get_lock("test")
        assert manager.remove_lock("test")
        assert not manager.remove_lock("nonexistent")

    def test_clear(self) -> None:
        """Test clearing all locks."""
        manager = LockManager()
        manager.get_lock("a")
        manager.get_lock("b")
        manager.clear()
        assert len(manager._locks) == 0


class TestInterProcessLock:
    """Tests for InterProcessLock."""

    def test_acquire_release(self) -> None:
        """Test basic acquire/release."""
        lock = InterProcessLock("test")
        result = lock.acquire(blocking=False)
        if result:
            lock.release()

    def test_context_manager(self) -> None:
        """Test context manager."""
        lock = InterProcessLock("test2")
        result = lock.acquire(blocking=False)
        if result:
            with lock:
                pass
            lock.release()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])