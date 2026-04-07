"""Tests for task management utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.task import (
    TaskStatus,
    TaskResult,
    Task,
    CallableTask,
    TaskGroup,
    TaskQueue,
    TaskTracker,
    TaskExecutor,
)


class TestTaskStatus:
    """Tests for TaskStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"


class TestTaskResult:
    """Tests for TaskResult."""

    def test_create(self) -> None:
        """Test creating result."""
        result = TaskResult(status=TaskStatus.COMPLETED, value=42)
        assert result.status == TaskStatus.COMPLETED
        assert result.value == 42

    def test_with_error(self) -> None:
        """Test result with error."""
        result = TaskResult(
            status=TaskStatus.FAILED,
            error=ValueError("test"),
        )
        assert result.error is not None


class DummyTask(Task):
    """Dummy task for testing."""

    def __init__(self, name: str = "test", result: Any = None, raise_error: bool = False):
        super().__init__(name)
        self._result = result
        self._raise_error = raise_error

    def execute(self) -> Any:
        if self._raise_error:
            raise ValueError("test error")
        return self._result


class TestTask:
    """Tests for Task."""

    def test_run_success(self) -> None:
        """Test successful task execution."""
        task = DummyTask(result=42)
        result = task.run()
        assert result.status == TaskStatus.COMPLETED
        assert result.value == 42

    def test_run_failure(self) -> None:
        """Test failed task execution."""
        task = DummyTask(raise_error=True)
        result = task.run()
        assert result.status == TaskStatus.FAILED

    def test_status(self) -> None:
        """Test status tracking."""
        task = DummyTask()
        assert task.status == TaskStatus.PENDING
        task.run()
        assert task.status == TaskStatus.COMPLETED


class TestCallableTask:
    """Tests for CallableTask."""

    def test_execute(self) -> None:
        """Test executing callable."""
        task = CallableTask("test", lambda: 42)
        result = task.run()
        assert result.value == 42

    def test_with_args(self) -> None:
        """Test executing with arguments."""
        def add(a, b):
            return a + b
        task = CallableTask("add", add, 1, 2)
        result = task.run()
        assert result.value == 3


class TestTaskGroup:
    """Tests for TaskGroup."""

    def test_add(self) -> None:
        """Test adding tasks."""
        group = TaskGroup("test")
        group.add(DummyTask("t1")).add(DummyTask("t2"))
        assert len(group._tasks) == 2

    def test_run_all(self) -> None:
        """Test running all tasks."""
        group = TaskGroup("test")
        group.add(DummyTask(result=1))
        group.add(DummyTask(result=2))
        results = group.run_all()
        assert len(results) == 2
        assert results[0].value == 1

    def test_run_parallel(self) -> None:
        """Test running tasks in parallel."""
        group = TaskGroup("test")
        group.add(DummyTask(result=1))
        group.add(DummyTask(result=2))
        results = group.run_parallel(num_workers=2)
        assert len(results) == 2


class TestTaskQueue:
    """Tests for TaskQueue."""

    def test_add(self) -> None:
        """Test adding to queue."""
        queue = TaskQueue()
        queue.add(DummyTask())
        assert queue.pending == 1

    def test_run(self) -> None:
        """Test running queue."""
        queue = TaskQueue()
        queue.add(DummyTask(result=1))
        queue.add(DummyTask(result=2))
        results = queue.run()
        assert len(results) == 2
        assert queue.pending == 0

    def test_cancel(self) -> None:
        """Test cancelling queue."""
        queue = TaskQueue()
        queue.add(DummyTask())
        queue.cancel()
        queue.run()
        assert queue.pending == 0


class TestTaskTracker:
    """Tests for TaskTracker."""

    def test_register(self) -> None:
        """Test registering task."""
        tracker = TaskTracker()
        tracker.register(DummyTask("test"))
        assert tracker.get_task("test") is not None

    def test_unregister(self) -> None:
        """Test unregistering task."""
        tracker = TaskTracker()
        tracker.register(DummyTask("test"))
        tracker.unregister("test")
        assert tracker.get_task("test") is None

    def test_track(self) -> None:
        """Test tracking results."""
        tracker = TaskTracker()
        result = TaskResult(status=TaskStatus.COMPLETED)
        tracker.track(result)
        history = tracker.get_history()
        assert len(history) == 1

    def test_get_stats(self) -> None:
        """Test getting statistics."""
        tracker = TaskTracker()
        tracker.track(TaskResult(status=TaskStatus.COMPLETED))
        tracker.track(TaskResult(status=TaskStatus.FAILED))
        stats = tracker.get_stats()
        assert stats[TaskStatus.COMPLETED] == 1
        assert stats[TaskStatus.FAILED] == 1


class TestTaskExecutor:
    """Tests for TaskExecutor."""

    def test_execute_success(self) -> None:
        """Test successful execution."""
        executor = TaskExecutor(timeout=5.0)
        result = executor.execute(lambda: 42)
        assert result.status == TaskStatus.COMPLETED
        assert result.value == 42

    def test_execute_timeout(self) -> None:
        """Test execution timeout."""
        executor = TaskExecutor(timeout=0.1)
        result = executor.execute(lambda: time.sleep(1))
        assert result.status == TaskStatus.TIMEOUT

    def test_execute_retry(self) -> None:
        """Test retry on failure."""
        attempts = []

        def failing():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return 42

        executor = TaskExecutor(timeout=5.0, max_retries=3)
        result = executor.execute(failing)
        assert result.status == TaskStatus.COMPLETED
        assert len(attempts) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])