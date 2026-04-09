"""Automation Recovery V2 Action.

Advanced recovery system with checkpoint/resume, state migration,
compensating transactions, and saga pattern support.
"""
from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class RecoveryStrategy(Enum):
    """Recovery strategies for failed automations."""
    RETRY = "retry"
    CHECKPOINT_RESUME = "checkpoint_resume"
    COMPENSATE = "compensate"
    SAGA_ROLLBACK = "saga_rollback"
    FALLBACK = "fallback"
    SKIP = "skip"


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"
    SKIPPED = "skipped"


@dataclass
class Checkpoint:
    """A recovery checkpoint."""
    step_name: str
    step_index: int
    state: Dict[str, Any]
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompensatingAction:
    """A compensating action for saga rollback."""
    step_name: str
    compensate_fn: Callable[[Dict[str, Any]], Any]
    forward_completed: bool = False
    compensated: bool = False
    compensation_result: Any = None


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    step_name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    retry_count: int = 0
    checkpoint: Optional[Checkpoint] = None


class AutomationRecoveryV2Action:
    """Advanced recovery and rollback system for automation workflows."""

    def __init__(
        self,
        max_retries: int = 3,
        retry_delay_sec: float = 1.0,
        enable_checkpoints: bool = True,
        enable_saga: bool = False,
    ) -> None:
        self.max_retries = max_retries
        self.retry_delay = retry_delay_sec
        self.enable_checkpoints = enable_checkpoints
        self.enable_saga = enable_saga

        self._checkpoints: Dict[str, List[Checkpoint]] = {}
        self._compensations: Dict[str, List[CompensatingAction]] = {}
        self._fallback_handlers: Dict[str, Callable] = {}
        self._recovery_history: List[Tuple[str, str, str]] = []
        self._current_workflow: Optional[str] = None

    def register_compensation(
        self,
        workflow_id: str,
        step_name: str,
        compensate_fn: Callable[[Dict[str, Any]], Any],
    ) -> None:
        """Register a compensating action for saga rollback."""
        if workflow_id not in self._compensations:
            self._compensations[workflow_id] = []

        self._compensations[workflow_id].append(
            CompensatingAction(step_name=step_name, compensate_fn=compensate_fn)
        )

    def register_fallback(
        self,
        step_name: str,
        fallback_fn: Callable[[Dict[str, Any]], Any],
    ) -> None:
        """Register a fallback handler for a step."""
        self._fallback_handlers[step_name] = fallback_fn

    def save_checkpoint(
        self,
        workflow_id: str,
        step_name: str,
        step_index: int,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        """Save a recovery checkpoint."""
        if not self.enable_checkpoints:
            raise RuntimeError("Checkpoints are disabled")

        checkpoint = Checkpoint(
            step_name=step_name,
            step_index=step_index,
            state=dict(state),
            timestamp=time.time(),
            metadata=metadata or {},
        )

        if workflow_id not in self._checkpoints:
            self._checkpoints[workflow_id] = []
        self._checkpoints[workflow_id].append(checkpoint)

        return checkpoint

    def get_latest_checkpoint(
        self,
        workflow_id: str,
    ) -> Optional[Checkpoint]:
        """Get the latest checkpoint for a workflow."""
        checkpoints = self._checkpoints.get(workflow_id, [])
        if not checkpoints:
            return None
        return checkpoints[-1]

    def get_checkpoint_for_step(
        self,
        workflow_id: str,
        step_name: str,
    ) -> Optional[Checkpoint]:
        """Get the checkpoint for a specific step."""
        checkpoints = self._checkpoints.get(workflow_id, [])
        for cp in reversed(checkpoints):
            if cp.step_name == step_name:
                return cp
        return None

    def execute_with_recovery(
        self,
        workflow_id: str,
        step_name: str,
        fn: Callable[[Dict[str, Any]], Any],
        state: Dict[str, Any],
        retry_count: int = 0,
    ) -> StepResult:
        """Execute a step with automatic recovery."""
        start_time = time.time()

        try:
            output = fn(state)
            duration_ms = (time.time() - start_time) * 1000

            result = StepResult(
                step_name=step_name,
                status=StepStatus.COMPLETED,
                output=output,
                duration_ms=duration_ms,
                retry_count=retry_count,
            )

            if self.enable_checkpoints:
                result.checkpoint = self.save_checkpoint(
                    workflow_id, step_name, 0, state
                )

            self._record_recovery(workflow_id, step_name, "success")
            return result

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000

            if retry_count < self.max_retries:
                time.sleep(self.retry_delay * (2 ** retry_count))
                return self.execute_with_recovery(
                    workflow_id, step_name, fn, state, retry_count + 1
                )

            result = StepResult(
                step_name=step_name,
                status=StepStatus.FAILED,
                error=str(e),
                duration_ms=duration_ms,
                retry_count=retry_count,
            )

            self._record_recovery(workflow_id, step_name, f"failed:{e}")
            return result

    def execute_with_fallback(
        self,
        step_name: str,
        primary_fn: Callable[[Dict[str, Any]], Any],
        state: Dict[str, Any],
    ) -> StepResult:
        """Execute a step with fallback on failure."""
        start_time = time.time()

        try:
            output = primary_fn(state)
            return StepResult(
                step_name=step_name,
                status=StepStatus.COMPLETED,
                output=output,
                duration_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            if step_name in self._fallback_handlers:
                fallback_fn = self._fallback_handlers[step_name]
                try:
                    output = fallback_fn(state)
                    return StepResult(
                        step_name=step_name,
                        status=StepStatus.COMPLETED,
                        output=output,
                        duration_ms=(time.time() - start_time) * 1000,
                    )
                except Exception as fallback_error:
                    return StepResult(
                        step_name=step_name,
                        status=StepStatus.FAILED,
                        error=f"primary:{e}, fallback:{fallback_error}",
                        duration_ms=(time.time() - start_time) * 1000,
                    )

            return StepResult(
                step_name=step_name,
                status=StepStatus.FAILED,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
            )

    def rollback_saga(
        self,
        workflow_id: str,
        failed_step: str,
        state: Dict[str, Any],
    ) -> List[StepResult]:
        """Rollback a saga workflow using compensating transactions."""
        if not self.enable_saga or workflow_id not in self._compensations:
            return []

        results = []
        compensations = self._compensations[workflow_id]

        completed_steps = []
        for comp in compensations:
            if comp.forward_completed:
                completed_steps.append(comp)

        for comp in reversed(completed_steps):
            try:
                result = comp.compensate_fn(state)
                comp.compensated = True
                comp.compensation_result = result
                results.append(StepResult(
                    step_name=comp.step_name,
                    status=StepStatus.COMPENSATED,
                    output=result,
                ))
            except Exception as e:
                results.append(StepResult(
                    step_name=comp.step_name,
                    status=StepStatus.FAILED,
                    error=str(e),
                ))

        return results

    def mark_step_forward_complete(
        self,
        workflow_id: str,
        step_name: str,
    ) -> None:
        """Mark a saga step as forward-completed."""
        if workflow_id in self._compensations:
            for comp in self._compensations[workflow_id]:
                if comp.step_name == step_name:
                    comp.forward_completed = True

    def _record_recovery(
        self,
        workflow_id: str,
        step_name: str,
        outcome: str,
    ) -> None:
        """Record a recovery event."""
        self._recovery_history.append((
            workflow_id,
            step_name,
            outcome,
            datetime.now().isoformat(),
        ))
        if len(self._recovery_history) > 1000:
            self._recovery_history = self._recovery_history[-500:]

    def get_recovery_history(
        self,
        workflow_id: Optional[str] = None,
    ) -> List[Tuple[str, str, str, str]]:
        """Get recovery history."""
        if workflow_id:
            return [(wid, sn, out, ts) for wid, sn, out, ts in self._recovery_history if wid == workflow_id]
        return self._recovery_history

    def clear_checkpoints(self, workflow_id: Optional[str] = None) -> int:
        """Clear checkpoints. Returns count cleared."""
        if workflow_id:
            count = len(self._checkpoints.get(workflow_id, []))
            if workflow_id in self._checkpoints:
                del self._checkpoints[workflow_id]
            return count
        count = sum(len(v) for v in self._checkpoints.values())
        self._checkpoints.clear()
        return count
