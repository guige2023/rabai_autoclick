"""Automation Workflow Action Module.

Provides workflow orchestration with state machines,
step execution, and error recovery.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WorkflowState(Enum):
    """Workflow state."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class WorkflowStep:
    """Workflow step definition."""
    name: str
    action: Callable
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    retry_count: int = 0
    timeout: Optional[float] = None


@dataclass
class Workflow:
    """Workflow definition."""
    name: str
    steps: Dict[str, WorkflowStep]
    initial_step: str


class AutomationWorkflowAction:
    """Workflow orchestrator.

    Example:
        workflow = AutomationWorkflowAction()

        workflow.define("deploy", [
            WorkflowStep("checkout", checkout_code, on_success="build"),
            WorkflowStep("build", build_app, on_success="deploy"),
            WorkflowStep("deploy", deploy_app),
        ])

        result = await workflow.execute("deploy")
    """

    def __init__(self) -> None:
        self._workflows: Dict[str, Workflow] = {}
        self._current_workflow: Optional[str] = None
        self._current_step: Optional[str] = None
        self._state = WorkflowState.PENDING
        self._context: Dict[str, Any] = {}
        self._history: List[Dict] = []

    def define(
        self,
        name: str,
        steps: List[WorkflowStep],
    ) -> "AutomationWorkflowAction":
        """Define workflow.

        Returns self for chaining.
        """
        step_map = {s.name: s for s in steps}
        initial = steps[0].name if steps else ""

        self._workflows[name] = Workflow(
            name=name,
            steps=step_map,
            initial_step=initial,
        )

        return self

    async def execute(
        self,
        workflow_name: str,
        context: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Execute workflow.

        Args:
            workflow_name: Name of workflow to execute
            context: Initial context

        Returns:
            Execution result with history
        """
        if workflow_name not in self._workflows:
            raise ValueError(f"Unknown workflow: {workflow_name}")

        self._current_workflow = workflow_name
        self._context = context or {}
        self._history = []
        self._state = WorkflowState.RUNNING

        workflow = self._workflows[workflow_name]
        current_step_name = workflow.initial_step

        while current_step_name:
            self._current_step = current_step_name
            step = workflow.steps[current_step_name]

            self._history.append({
                "step": current_step_name,
                "state": "started",
            })

            try:
                result = await self._execute_step(step)

                self._history[-1]["state"] = "completed"
                self._history[-1]["result"] = result

                if result is not None:
                    self._context[f"{current_step_name}_result"] = result

                current_step_name = step.on_success

            except Exception as e:
                logger.error(f"Step {current_step_name} failed: {e}")
                self._history[-1]["state"] = "failed"
                self._history[-1]["error"] = str(e)

                if step.on_failure:
                    current_step_name = step.on_failure
                else:
                    self._state = WorkflowState.FAILED
                    break

        self._state = WorkflowState.COMPLETED

        return {
            "workflow": workflow_name,
            "state": self._state.value,
            "context": self._context,
            "history": self._history,
        }

    async def _execute_step(self, step: WorkflowStep) -> Any:
        """Execute single step."""
        if step.timeout:
            import asyncio
            return await asyncio.wait_for(
                step.action(self._context),
                timeout=step.timeout
            )

        return await step.action(self._context)

    def cancel(self) -> None:
        """Cancel current workflow."""
        self._state = WorkflowState.CANCELLED

    def pause(self) -> None:
        """Pause current workflow."""
        self._state = WorkflowState.PAUSED

    def resume(self) -> None:
        """Resume paused workflow."""
        self._state = WorkflowState.RUNNING

    def get_state(self) -> WorkflowState:
        """Get current workflow state."""
        return self._state

    def get_history(self) -> List[Dict]:
        """Get execution history."""
        return self._history.copy()
