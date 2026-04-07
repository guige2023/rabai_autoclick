"""Tests for workflow utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.workflow import (
    WorkflowStatus,
    WorkflowStep,
    WorkflowResult,
    Workflow,
    WorkflowBuilder,
    WorkflowValidator,
    WorkflowRunner,
    WorkflowRegistry,
)


class TestWorkflowStatus:
    """Tests for WorkflowStatus."""

    def test_values(self) -> None:
        """Test status values."""
        assert WorkflowStatus.PENDING.value == "pending"
        assert WorkflowStatus.RUNNING.value == "running"
        assert WorkflowStatus.COMPLETED.value == "completed"
        assert WorkflowStatus.FAILED.value == "failed"


class TestWorkflowStep:
    """Tests for WorkflowStep."""

    def test_create(self) -> None:
        """Test creating step."""
        step = WorkflowStep(name="test", action=lambda: None)
        assert step.name == "test"
        assert step.retry_count == 0


class TestWorkflowResult:
    """Tests for WorkflowResult."""

    def test_create(self) -> None:
        """Test creating result."""
        result = WorkflowResult(status=WorkflowStatus.COMPLETED)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.error is None


class TestWorkflow:
    """Tests for Workflow."""

    def test_create(self) -> None:
        """Test creating workflow."""
        wf = Workflow("test")
        assert wf.name == "test"
        assert len(wf._steps) == 0

    def test_add_step(self) -> None:
        """Test adding step."""
        wf = Workflow("test")
        step = WorkflowStep(name="step1", action=lambda: 1)
        wf.add_step(step)
        assert len(wf._steps) == 1

    def test_step_fluent(self) -> None:
        """Test fluent step addition."""
        wf = Workflow("test")
        wf.step("step1", lambda: 1).step("step2", lambda: 2)
        assert len(wf._steps) == 2

    def test_execute_success(self) -> None:
        """Test successful execution."""
        wf = Workflow("test")
        wf.step("step1", lambda: 1)
        result = wf.execute()
        assert result.status == WorkflowStatus.COMPLETED
        assert result.step_results["step1"] == 1

    def test_execute_failure(self) -> None:
        """Test failed execution."""
        wf = Workflow("test")
        wf.step("step1", lambda: (_ for _ in ()).throw(ValueError("fail")))
        result = wf.execute()
        assert result.status == WorkflowStatus.FAILED
        assert result.error is not None

    def test_execute_with_condition(self) -> None:
        """Test execution with condition."""
        wf = Workflow("test")
        wf.step("step1", lambda: 1, condition=lambda: False)
        result = wf.execute()
        assert result.status == WorkflowStatus.COMPLETED
        assert "step1" not in result.step_results

    def test_retry(self) -> None:
        """Test step retry."""
        attempts = []

        def failing_action():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return "success"

        wf = Workflow("test")
        wf.step("step1", failing_action, retry_count=3)
        result = wf.execute()
        assert result.status == WorkflowStatus.COMPLETED
        assert len(attempts) == 3


class TestWorkflowBuilder:
    """Tests for WorkflowBuilder."""

    def test_build(self) -> None:
        """Test building workflow."""
        builder = WorkflowBuilder("test")
        builder.add_step("step1", lambda: 1)
        wf = builder.build()
        assert wf.name == "test"
        assert len(wf._steps) == 1


class TestWorkflowValidator:
    """Tests for WorkflowValidator."""

    def test_validate_empty_workflow(self) -> None:
        """Test validating empty workflow."""
        wf = Workflow("")
        errors = WorkflowValidator.validate(wf)
        assert len(errors) > 0

    def test_validate_valid_workflow(self) -> None:
        """Test validating valid workflow."""
        wf = Workflow("test")
        wf.step("step1", lambda: 1)
        errors = WorkflowValidator.validate(wf)
        assert len(errors) == 0

    def test_is_valid(self) -> None:
        """Test is_valid check."""
        wf = Workflow("test")
        wf.step("step1", lambda: 1)
        assert WorkflowValidator.is_valid(wf) is True


class TestWorkflowRunner:
    """Tests for WorkflowRunner."""

    def test_run(self) -> None:
        """Test running workflow."""
        runner = WorkflowRunner()
        wf = Workflow("test")
        wf.step("step1", lambda: 1)
        result = runner.run(wf)
        assert result.status == WorkflowStatus.COMPLETED

    def test_cancel(self) -> None:
        """Test cancelling workflow."""
        runner = WorkflowRunner()
        wf = Workflow("test")
        wf.step("step1", lambda: 1)
        runner.run(wf)
        runner.cancel("test")
        assert runner.is_running("test") is False


class TestWorkflowRegistry:
    """Tests for WorkflowRegistry."""

    def test_register(self) -> None:
        """Test registering workflow."""
        registry = WorkflowRegistry()
        wf = Workflow("test")
        registry.register(wf)
        assert "test" in registry.list_workflows()

    def test_get(self) -> None:
        """Test getting workflow."""
        registry = WorkflowRegistry()
        wf = Workflow("test")
        registry.register(wf)
        retrieved = registry.get("test")
        assert retrieved is wf

    def test_get_not_found(self) -> None:
        """Test getting nonexistent workflow."""
        registry = WorkflowRegistry()
        assert registry.get("nonexistent") is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])