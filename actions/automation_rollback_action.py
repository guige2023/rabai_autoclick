"""Automation rollback and recovery action."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class RollbackStatus(str, Enum):
    """Status of rollback operation."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Checkpoint:
    """A checkpoint for rollback."""

    checkpoint_id: str
    created_at: datetime
    state: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RollbackOperation:
    """Record of a rollback operation."""

    operation_id: str
    checkpoint_id: str
    status: RollbackStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    steps_completed: int = 0
    total_steps: int = 0
    error: Optional[str] = None


@dataclass
class RollbackStep:
    """A single step in a rollback sequence."""

    step_id: str
    name: str
    action: Callable[[], Coroutine[Any, Any, Any]]
    compensate: Optional[Callable[[], Coroutine[Any, Any, Any]]] = None
    order: int = 0


class AutomationRollbackAction:
    """Manages checkpoints and rollback operations for automations."""

    def __init__(
        self,
        max_checkpoints: int = 10,
        on_checkpoint_created: Optional[Callable[[str], None]] = None,
        on_rollback_started: Optional[Callable[[str], None]] = None,
        on_rollback_completed: Optional[Callable[[str], None]] = None,
    ):
        """Initialize rollback manager.

        Args:
            max_checkpoints: Maximum checkpoints to keep.
            on_checkpoint_created: Callback when checkpoint created.
            on_rollback_started: Callback when rollback starts.
            on_rollback_completed: Callback when rollback completes.
        """
        self._max_checkpoints = max_checkpoints
        self._checkpoints: list[Checkpoint] = []
        self._rollback_history: list[RollbackOperation] = []
        self._current_operation: Optional[RollbackOperation] = None
        self._on_checkpoint_created = on_checkpoint_created
        self._on_rollback_started = on_rollback_started
        self._on_rollback_completed = on_rollback_completed
        self._checkpoint_counter: int = 0

    def _next_checkpoint_id(self) -> str:
        """Generate next checkpoint ID."""
        self._checkpoint_counter += 1
        return f"cp_{self._checkpoint_counter}_{int(datetime.now().timestamp())}"

    async def create_checkpoint(
        self,
        state: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Create a checkpoint of current state.

        Args:
            state: State to save.
            metadata: Optional metadata.

        Returns:
            Checkpoint ID.
        """
        checkpoint_id = self._next_checkpoint_id()
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            created_at=datetime.now(),
            state=state.copy(),
            metadata=metadata or {},
        )

        self._checkpoints.append(checkpoint)

        while len(self._checkpoints) > self._max_checkpoints:
            self._checkpoints.pop(0)

        if self._on_checkpoint_created:
            self._on_checkpoint_created(checkpoint_id)

        return checkpoint_id

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get a checkpoint by ID."""
        for cp in self._checkpoints:
            if cp.checkpoint_id == checkpoint_id:
                return cp
        return None

    def get_latest_checkpoint(self) -> Optional[Checkpoint]:
        """Get the most recent checkpoint."""
        return self._checkpoints[-1] if self._checkpoints else None

    def list_checkpoints(self) -> list[str]:
        """List all checkpoint IDs."""
        return [cp.checkpoint_id for cp in self._checkpoints]

    async def rollback_to(
        self,
        checkpoint_id: str,
        restore_func: Callable[[dict[str, Any]], Coroutine[Any, Any, None]],
    ) -> RollbackOperation:
        """Rollback to a specific checkpoint.

        Args:
            checkpoint_id: Checkpoint to rollback to.
            restore_func: Async function to restore state.

        Returns:
            RollbackOperation with result.
        """
        checkpoint = self.get_checkpoint(checkpoint_id)
        if not checkpoint:
            operation = RollbackOperation(
                operation_id="failed_lookup",
                checkpoint_id=checkpoint_id,
                status=RollbackStatus.FAILED,
                started_at=datetime.now(),
                completed_at=datetime.now(),
                error=f"Checkpoint not found: {checkpoint_id}",
            )
            return operation

        operation_id = f"rollback_{len(self._rollback_history) + 1}"
        operation = RollbackOperation(
            operation_id=operation_id,
            checkpoint_id=checkpoint_id,
            status=RollbackStatus.IN_PROGRESS,
            started_at=datetime.now(),
            total_steps=1,
        )

        self._current_operation = operation

        if self._on_rollback_started:
            self._on_rollback_started(operation_id)

        try:
            await restore_func(checkpoint.state)
            operation.status = RollbackStatus.COMPLETED
            operation.steps_completed = 1
        except Exception as e:
            operation.status = RollbackStatus.FAILED
            operation.error = str(e)
        finally:
            operation.completed_at = datetime.now()
            self._rollback_history.append(operation)
            self._current_operation = None

            if self._on_rollback_completed:
                self._on_rollback_completed(operation_id)

        return operation

    async def rollback_with_steps(
        self,
        checkpoint_id: str,
        steps: list[RollbackStep],
    ) -> RollbackOperation:
        """Rollback using compensation steps.

        Args:
            checkpoint_id: Checkpoint to rollback to.
            steps: Ordered list of rollback steps.

        Returns:
            RollbackOperation with result.
        """
        checkpoint = self.get_checkpoint(checkpoint_id)
        if not checkpoint:
            return RollbackOperation(
                operation_id="failed",
                checkpoint_id=checkpoint_id,
                status=RollbackStatus.FAILED,
                started_at=datetime.now(),
                completed_at=datetime.now(),
                error="Checkpoint not found",
            )

        operation_id = f"rollback_{len(self._rollback_history) + 1}"
        operation = RollbackOperation(
            operation_id=operation_id,
            checkpoint_id=checkpoint_id,
            status=RollbackStatus.IN_PROGRESS,
            started_at=datetime.now(),
            total_steps=len(steps),
        )

        sorted_steps = sorted(steps, key=lambda s: s.order, reverse=True)

        self._current_operation = operation

        try:
            for i, step in enumerate(sorted_steps):
                if step.compensate:
                    await step.compensate()
                operation.steps_completed = i + 1

            operation.status = RollbackStatus.COMPLETED

        except Exception as e:
            operation.status = RollbackStatus.FAILED
            operation.error = str(e)

        finally:
            operation.completed_at = datetime.now()
            self._rollback_history.append(operation)
            self._current_operation = None

        return operation

    def delete_checkpoint(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint."""
        for i, cp in enumerate(self._checkpoints):
            if cp.checkpoint_id == checkpoint_id:
                self._checkpoints.pop(i)
                return True
        return False

    def clear_checkpoints(self) -> int:
        """Clear all checkpoints."""
        count = len(self._checkpoints)
        self._checkpoints.clear()
        return count

    def get_operation(self, operation_id: str) -> Optional[RollbackOperation]:
        """Get a rollback operation by ID."""
        for op in self._rollback_history:
            if op.operation_id == operation_id:
                return op
        return self._current_operation

    def get_history(self, limit: int = 100) -> list[RollbackOperation]:
        """Get rollback history."""
        return self._rollback_history[-limit:]

    def get_current_operation(self) -> Optional[RollbackOperation]:
        """Get currently running operation."""
        return self._current_operation
