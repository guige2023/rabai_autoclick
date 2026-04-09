"""
Action Sequencer Action Module.

Sequences and choreographs complex multi-step automation
workflows with conditional branching, parallel execution
tracking, and rollback support.
"""

from enum import Enum
from typing import Any, Callable, Optional, Union


class StepStatus(Enum):
    """Status of a sequence step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


class SequenceStep:
    """A single step in an action sequence."""

    def __init__(
        self,
        step_id: str,
        action: Callable[[], Any],
        description: str = "",
        rollback: Optional[Callable[[], None]] = None,
        condition: Optional[Callable[[], bool]] = None,
        timeout: Optional[float] = None,
    ):
        """
        Initialize sequence step.

        Args:
            step_id: Unique step identifier.
            action: Function to execute for this step.
            description: Step description.
            rollback: Optional rollback function.
            condition: Optional condition to check before running.
            timeout: Optional step timeout in seconds.
        """
        self.step_id = step_id
        self.action = action
        self.description = description
        self.rollback = rollback
        self.condition = condition
        self.timeout = timeout
        self.status = StepStatus.PENDING
        self.result: Any = None
        self.error: Optional[Exception] = None


class ActionSequencer:
    """Sequences and executes multi-step automation workflows."""

    def __init__(self):
        """Initialize action sequencer."""
        self._steps: list[SequenceStep] = []
        self._current_index: int = -1
        self._completed_steps: list[SequenceStep] = []

    def add_step(
        self,
        step_id: str,
        action: Callable[[], Any],
        description: str = "",
        rollback: Optional[Callable[[], None]] = None,
        condition: Optional[Callable[[], bool]] = None,
        timeout: Optional[float] = None,
    ) -> "ActionSequencer":
        """
        Add a step to the sequence.

        Args:
            step_id: Unique step identifier.
            action: Function to execute.
            description: Step description.
            rollback: Optional rollback function.
            condition: Optional pre-condition.
            timeout: Optional timeout.

        Returns:
            Self for chaining.
        """
        step = SequenceStep(
            step_id=step_id,
            action=action,
            description=description,
            rollback=rollback,
            condition=condition,
            timeout=timeout,
        )
        self._steps.append(step)
        return self

    def execute(self) -> dict[str, Any]:
        """
        Execute the full sequence.

        Returns:
            Execution summary.
        """
        results = {
            "total": len(self._steps),
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "step_results": {},
            "rollback_performed": False,
        }

        for i, step in enumerate(self._steps):
            self._current_index = i

            if step.condition and not step.condition():
                step.status = StepStatus.SKIPPED
                results["skipped"] += 1
                results["step_results"][step.step_id] = {"status": "skipped"}
                continue

            step.status = StepStatus.RUNNING

            try:
                step.result = step.action()
                step.status = StepStatus.COMPLETED
                results["completed"] += 1
                results["step_results"][step.step_id] = {
                    "status": "completed",
                    "result": step.result,
                }
                self._completed_steps.append(step)
            except Exception as e:
                step.error = e
                step.status = StepStatus.FAILED
                results["failed"] += 1
                results["step_results"][step.step_id] = {
                    "status": "failed",
                    "error": str(e),
                }

                if step.rollback:
                    self._rollback_from(i)
                    results["rollback_performed"] = True

                break

        self._current_index = -1
        return results

    def _rollback_from(self, step_index: int) -> None:
        """Rollback steps from step_index - 1 down to 0."""
        for step in reversed(self._completed_steps[:step_index]):
            if step.rollback:
                try:
                    step.rollback()
                    step.status = StepStatus.ROLLED_BACK
                except Exception:
                    pass

    def get_step(self, step_id: str) -> Optional[SequenceStep]:
        """Get step by ID."""
        for step in self._steps:
            if step.step_id == step_id:
                return step
        return None

    def get_status(self) -> dict[str, str]:
        """Get status of all steps."""
        return {step.step_id: step.status.value for step in self._steps}
