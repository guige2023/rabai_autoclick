"""Tests for automation runner utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.runner import (
    RunnerStatus,
    StepResult,
    RunnerResult,
    Step,
    AutomationRunner,
    StepBuilder,
    RunnerContext,
)


class TestRunnerStatus:
    """Tests for RunnerStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert RunnerStatus.IDLE.value == "idle"
        assert RunnerStatus.RUNNING.value == "running"
        assert RunnerStatus.PAUSED.value == "paused"


class TestStepResult:
    """Tests for StepResult."""

    def test_create(self) -> None:
        """Test creating result."""
        result = StepResult(step_name="test", success=True)
        assert result.step_name == "test"
        assert result.success is True


class TestRunnerResult:
    """Tests for RunnerResult."""

    def test_create(self) -> None:
        """Test creating result."""
        result = RunnerResult(success=True, status=RunnerStatus.IDLE)
        assert result.success is True
        assert result.status == RunnerStatus.IDLE


class TestStep:
    """Tests for Step."""

    def test_create(self) -> None:
        """Test creating step."""
        step = Step(name="test", action=lambda: None)
        assert step.name == "test"
        assert step.retry == 0

    def test_with_condition(self) -> None:
        """Test step with condition."""
        step = Step(name="test", action=lambda: None, condition=lambda: False)
        assert step.condition is not None


class TestAutomationRunner:
    """Tests for AutomationRunner."""

    def test_create(self) -> None:
        """Test creating runner."""
        runner = AutomationRunner()
        assert runner.status == RunnerStatus.IDLE

    def test_add_step(self) -> None:
        """Test adding step."""
        runner = AutomationRunner()
        runner.add_step(Step(name="test", action=lambda: None))
        assert len(runner._steps) == 1

    def test_step_fluent(self) -> None:
        """Test fluent step addition."""
        runner = AutomationRunner()
        runner.step("test", lambda: None)
        assert len(runner._steps) == 1

    def test_run_empty(self) -> None:
        """Test running with no steps."""
        runner = AutomationRunner()
        result = runner.run()
        assert result.success is True

    def test_run_success(self) -> None:
        """Test successful run."""
        runner = AutomationRunner()
        runner.step("step1", lambda: None)
        result = runner.run()
        assert result.success is True
        assert len(result.step_results) == 1

    def test_run_failure(self) -> None:
        """Test failed run."""
        runner = AutomationRunner()
        runner.step("fail", lambda: (_ for _ in ()).throw(ValueError("fail")))
        result = runner.run()
        assert result.success is False

    def test_pause_resume(self) -> None:
        """Test pause and resume."""
        runner = AutomationRunner()
        runner.pause()
        assert runner.status == RunnerStatus.PAUSED
        runner.resume()
        assert runner.status == RunnerStatus.RUNNING

    def test_stop(self) -> None:
        """Test stop."""
        runner = AutomationRunner()
        runner.step("test", lambda: None)
        runner.stop()
        assert runner.status == RunnerStatus.STOPPED


class TestStepBuilder:
    """Tests for StepBuilder."""

    def test_create(self) -> None:
        """Test creating step."""
        step = StepBuilder.create("test", lambda: None)
        assert step.name == "test"

    def test_wait(self) -> None:
        """Test wait step."""
        step = StepBuilder.wait(0.01)
        assert step.name.startswith("Wait")


class TestRunnerContext:
    """Tests for RunnerContext."""

    def test_create(self) -> None:
        """Test creating context."""
        ctx = RunnerContext()
        assert len(ctx._data) == 0

    def test_set_get(self) -> None:
        """Test setting and getting values."""
        ctx = RunnerContext()
        ctx.set("key", "value")
        assert ctx.get("key") == "value"

    def test_get_default(self) -> None:
        """Test getting default value."""
        ctx = RunnerContext()
        assert ctx.get("nonexistent", "default") == "default"

    def test_clear(self) -> None:
        """Test clearing context."""
        ctx = RunnerContext()
        ctx.set("key", "value")
        ctx.clear()
        assert len(ctx._data) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])