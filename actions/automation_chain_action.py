"""Automation Chain Action Module.

Provides chaining mechanism for sequential automation
tasks with branching, error recovery, and state passing.

Author: RabAi Team
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ChainStatus(Enum):
    """Chain execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ChainStep:
    """A single step in automation chain."""
    step_id: str
    name: str
    action: str
    params: Dict[str, Any]
    enabled: bool = True
    continue_on_error: bool = False
    retry_count: int = 0
    timeout_seconds: float = 30.0


@dataclass
class StepResult:
    """Result of step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    retry_attempt: int = 0


@dataclass
class ChainExecution:
    """Execution state of a chain."""
    chain_id: str
    status: ChainStatus
    current_step_index: int = 0
    step_results: List[StepResult] = field(default_factory=list)
    shared_state: Dict[str, Any] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def duration_ms(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return (end - self.start_time) * 1000


class AutomationChain:
    """Chain of automation steps with state management."""

    def __init__(self, chain_id: str, name: str = ""):
        self.chain_id = chain_id
        self.name = name or chain_id
        self._steps: List[ChainStep] = []
        self._execution: Optional[ChainExecution] = None
        self._step_handlers: Dict[str, Callable] = {}
        self._error_handlers: Dict[str, Callable] = {}

    def add_step(
        self,
        step_id: str,
        name: str,
        action: str,
        params: Optional[Dict[str, Any]] = None,
        continue_on_error: bool = False,
        retry_count: int = 0,
        timeout_seconds: float = 30.0
    ) -> "AutomationChain":
        """Add step to chain."""
        step = ChainStep(
            step_id=step_id,
            name=name,
            action=action,
            params=params or {},
            continue_on_error=continue_on_error,
            retry_count=retry_count,
            timeout_seconds=timeout_seconds
        )

        self._steps.append(step)
        return self

    def register_handler(
        self,
        action: str,
        handler: Callable[[Dict[str, Any], Dict[str, Any]], Any]
    ) -> None:
        """Register handler for action."""
        self._step_handlers[action] = handler

    def register_error_handler(
        self,
        action: str,
        handler: Callable[[Dict[str, Any], Exception], Any]
    ) -> None:
        """Register error handler for action."""
        self._error_handlers[action] = handler

    def execute(self) -> ChainExecution:
        """Execute the chain."""
        self._execution = ChainExecution(
            chain_id=self.chain_id,
            status=ChainStatus.RUNNING,
            start_time=time.time()
        )

        for i, step in enumerate(self._steps):
            if not step.enabled:
                continue

            self._execution.current_step_index = i

            result = self._execute_step(step)

            self._execution.step_results.append(result)

            if result.status == StepStatus.FAILED:
                if not step.continue_on_error:
                    self._execution.status = ChainStatus.FAILED
                    self._execution.end_time = time.time()
                    return self._execution

        self._execution.status = ChainStatus.COMPLETED
        self._execution.end_time = time.time()
        return self._execution

    def _execute_step(self, step: ChainStep) -> StepResult:
        """Execute a single step."""
        start_time = time.time()

        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING
        )

        for attempt in range(max(1, step.retry_count + 1)):
            result.retry_attempt = attempt

            try:
                output = self._run_step(step)
                result.status = StepStatus.COMPLETED
                result.output = output

                if self._execution:
                    self._execution.shared_state[f"step_{step.step_id}"] = output

                break

            except Exception as e:
                result.error = str(e)

                if attempt >= step.retry_count:
                    result.status = StepStatus.FAILED
                else:
                    time.sleep(0.1 * (attempt + 1))

        result.duration_ms = (time.time() - start_time) * 1000
        return result

    def _run_step(self, step: ChainStep) -> Any:
        """Run step with handler."""
        if step.action in self._step_handlers:
            return self._step_handlers[step.action](
                step.params,
                self._execution.shared_state if self._execution else {}
            )

        return {"executed": step.action, "params": step.params}

    def pause(self) -> None:
        """Pause chain execution."""
        if self._execution:
            self._execution.status = ChainStatus.PAUSED

    def resume(self) -> ChainExecution:
        """Resume paused chain execution."""
        if not self._execution or self._execution.status != ChainStatus.PAUSED:
            return self._execution

        self._execution.status = ChainStatus.RUNNING
        return self.execute()

    def cancel(self) -> None:
        """Cancel chain execution."""
        if self._execution:
            self._execution.status = ChainStatus.CANCELLED
            self._execution.end_time = time.time()

    def get_execution(self) -> Optional[ChainExecution]:
        """Get current execution state."""
        return self._execution

    def get_step(self, step_id: str) -> Optional[ChainStep]:
        """Get step by ID."""
        for step in self._steps:
            if step.step_id == step_id:
                return step
        return None

    def get_execution_summary(self) -> Dict[str, Any]:
        """Get execution summary."""
        if not self._execution:
            return {"status": ChainStatus.PENDING.value}

        return {
            "chain_id": self.chain_id,
            "name": self.name,
            "status": self._execution.status.value,
            "current_step": self._execution.current_step_index,
            "total_steps": len(self._steps),
            "completed_steps": sum(
                1 for r in self._execution.step_results
                if r.status == StepStatus.COMPLETED
            ),
            "failed_steps": sum(
                1 for r in self._execution.step_results
                if r.status == StepStatus.FAILED
            ),
            "duration_ms": self._execution.duration_ms,
            "shared_state_keys": list(self._execution.shared_state.keys())
        }


class AutomationChainAction(BaseAction):
    """Action for automation chain operations."""

    def __init__(self):
        super().__init__("automation_chain")
        self._chains: Dict[str, AutomationChain] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute chain action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "add_step":
                return self._add_step(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "get_execution":
                return self._get_execution(params)
            elif operation == "summary":
                return self._get_summary(params)
            elif operation == "cancel":
                return self._cancel(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create new chain."""
        chain_id = params.get("chain_id", "")
        name = params.get("name", "")

        if not chain_id:
            return ActionResult(success=False, message="chain_id is required")

        self._chains[chain_id] = AutomationChain(chain_id, name)

        return ActionResult(
            success=True,
            message=f"Chain created: {chain_id}"
        )

    def _add_step(self, params: Dict[str, Any]) -> ActionResult:
        """Add step to chain."""
        chain_id = params.get("chain_id", "")
        step_id = params.get("step_id", "")
        name = params.get("name", "")
        action = params.get("action", "")
        step_params = params.get("params", {})
        continue_on_error = params.get("continue_on_error", False)
        retry_count = params.get("retry_count", 0)
        timeout = params.get("timeout_seconds", 30.0)

        if chain_id not in self._chains:
            return ActionResult(success=False, message=f"Chain not found: {chain_id}")

        self._chains[chain_id].add_step(
            step_id=step_id,
            name=name or step_id,
            action=action,
            params=step_params,
            continue_on_error=continue_on_error,
            retry_count=retry_count,
            timeout_seconds=timeout
        )

        return ActionResult(
            success=True,
            message=f"Step added to: {chain_id}"
        )

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute chain."""
        chain_id = params.get("chain_id", "")

        if chain_id not in self._chains:
            return ActionResult(success=False, message=f"Chain not found: {chain_id}")

        execution = self._chains[chain_id].execute()

        return ActionResult(
            success=execution.status == ChainStatus.COMPLETED,
            data={
                "chain_id": chain_id,
                "status": execution.status.value,
                "completed_steps": len([
                    r for r in execution.step_results
                    if r.status == StepStatus.COMPLETED
                ]),
                "failed_steps": len([
                    r for r in execution.step_results
                    if r.status == StepStatus.FAILED
                ]),
                "duration_ms": execution.duration_ms,
                "step_results": [
                    {
                        "step_id": r.step_id,
                        "status": r.status.value,
                        "duration_ms": r.duration_ms,
                        "error": r.error
                    }
                    for r in execution.step_results
                ]
            }
        )

    def _get_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Get chain execution state."""
        chain_id = params.get("chain_id", "")

        if chain_id not in self._chains:
            return ActionResult(success=False, message=f"Chain not found: {chain_id}")

        execution = self._chains[chain_id].get_execution()

        if not execution:
            return ActionResult(success=True, data={"status": "not_started"})

        return ActionResult(
            success=True,
            data={
                "chain_id": chain_id,
                "status": execution.status.value,
                "current_step": execution.current_step_index,
                "duration_ms": execution.duration_ms
            }
        )

    def _get_summary(self, params: Dict[str, Any]) -> ActionResult:
        """Get chain execution summary."""
        chain_id = params.get("chain_id", "")

        if chain_id not in self._chains:
            return ActionResult(success=False, message=f"Chain not found: {chain_id}")

        summary = self._chains[chain_id].get_execution_summary()
        return ActionResult(success=True, data=summary)

    def _cancel(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel chain execution."""
        chain_id = params.get("chain_id", "")

        if chain_id not in self._chains:
            return ActionResult(success=False, message=f"Chain not found: {chain_id}")

        self._chains[chain_id].cancel()

        return ActionResult(
            success=True,
            message=f"Chain cancelled: {chain_id}"
        )
