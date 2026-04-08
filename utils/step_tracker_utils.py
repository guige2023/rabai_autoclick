"""
Step Tracker Utilities

Provides utilities for tracking workflow steps
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime


@dataclass
class WorkflowStep:
    """A step in a workflow."""
    step_id: str
    name: str
    status: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    result: Any = None
    error: str | None = None


class StepTracker:
    """
    Tracks workflow steps and execution.
    
    Records step status, timing, and
    results for debugging and reporting.
    """

    def __init__(self) -> None:
        self._steps: dict[str, WorkflowStep] = {}
        self._execution_order: list[str] = []

    def start_step(self, step_id: str, name: str) -> None:
        """Mark a step as started."""
        step = WorkflowStep(
            step_id=step_id,
            name=name,
            status="running",
            started_at=datetime.now(),
        )
        self._steps[step_id] = step
        self._execution_order.append(step_id)

    def complete_step(
        self,
        step_id: str,
        result: Any = None,
    ) -> None:
        """Mark a step as completed."""
        if step_id in self._steps:
            self._steps[step_id].status = "completed"
            self._steps[step_id].completed_at = datetime.now()
            self._steps[step_id].result = result

    def fail_step(self, step_id: str, error: str) -> None:
        """Mark a step as failed."""
        if step_id in self._steps:
            self._steps[step_id].status = "failed"
            self._steps[step_id].completed_at = datetime.now()
            self._steps[step_id].error = error

    def get_step(self, step_id: str) -> WorkflowStep | None:
        """Get step by ID."""
        return self._steps.get(step_id)

    def get_execution_summary(self) -> dict[str, Any]:
        """Get summary of step execution."""
        total = len(self._steps)
        completed = sum(1 for s in self._steps.values() if s.status == "completed")
        failed = sum(1 for s in self._steps.values() if s.status == "failed")
        return {
            "total_steps": total,
            "completed": completed,
            "failed": failed,
            "execution_order": self._execution_order,
        }
