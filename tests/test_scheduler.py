"""Tests for scheduler utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.scheduler import Scheduler, DelayedTask, RepeatingTask


class TestScheduler:
    """Tests for Scheduler."""

    def test_add_task(self) -> None:
        """Test adding a task."""
        scheduler = Scheduler()
        scheduler.add_task("test", lambda: None, interval=1)
        assert len(scheduler.tasks) == 1

    def test_remove_task(self) -> None:
        """Test removing a task."""
        scheduler = Scheduler()
        scheduler.add_task("test", lambda: None, interval=1)
        result = scheduler.remove_task("test")
        assert result is True
        assert len(scheduler.tasks) == 0

    def test_enable_disable_task(self) -> None:
        """Test enabling and disabling tasks."""
        scheduler = Scheduler()
        scheduler.add_task("test", lambda: None, interval=1)

        scheduler.disable_task("test")
        assert scheduler.get_task("test").enabled is False

        scheduler.enable_task("test")
        assert scheduler.get_task("test").enabled is True

    def test_get_task(self) -> None:
        """Test getting a task."""
        scheduler = Scheduler()
        scheduler.add_task("test", lambda: None, interval=1)
        task = scheduler.get_task("test")
        assert task is not None
        assert task.name == "test"


class TestDelayedTask:
    """Tests for DelayedTask."""

    def test_delayed_execution(self) -> None:
        """Test delayed task execution."""
        result = []

        def task():
            result.append(1)

        delayed = DelayedTask(task, delay=0.1)
        delayed.start()
        time.sleep(0.2)

        assert len(result) == 1


class TestRepeatingTask:
    """Tests for RepeatingTask."""

    def test_repeating_execution(self) -> None:
        """Test repeating task execution."""
        result = []

        def task():
            result.append(1)

        repeating = RepeatingTask(task, interval=0.05)
        repeating.start()
        time.sleep(0.15)
        repeating.stop()

        assert len(result) >= 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])