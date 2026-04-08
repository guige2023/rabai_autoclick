"""
Automation Recovery Action Module.

Provides automated recovery mechanisms for failed operations
including checkpoint/resume, state rollback, and compensation logic.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import uuid
from collections import deque

logger = logging.getLogger(__name__)


class RecoveryStrategy(Enum):
    """Recovery strategies."""
    RETRY = "retry"
    ROLLBACK = "rollback"
    COMPENSATE = "compensate"
    CHECKPOINT = "checkpoint"
    FAILOVER = "failover"


class OperationState(Enum):
    """Operation execution state."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    COMPENSATED = "compensated"


@dataclass
class Checkpoint:
    """Checkpoint for recovery."""
    checkpoint_id: str
    operation_id: str
    step: int
    state: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompensationAction:
    """Compensation action for rollback."""
    action_id: str
    name: str
    action_type: str
    forward_func: Callable
    backward_func: Callable
    state: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationResult:
    """Result of an operation."""
    operation_id: str
    success: bool
    state: OperationState
    result: Any = None
    error: Optional[str] = None
    checkpoint_id: Optional[str] = None
    compensationApplied: bool = False
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None


class CheckpointManager:
    """Manages operation checkpoints."""

    def __init__(self, max_checkpoints: int = 100):
        self.max_checkpoints = max_checkpoints
        self.checkpoints: Dict[str, List[Checkpoint]] = {}

    def save(self, operation_id: str, step: int, state: Dict[str, Any]) -> Checkpoint:
        """Save a checkpoint."""
        checkpoint = Checkpoint(
            checkpoint_id=str(uuid.uuid4()),
            operation_id=operation_id,
            step=step,
            state=state.copy()
        )

        if operation_id not in self.checkpoints:
            self.checkpoints[operation_id] = []

        self.checkpoints[operation_id].append(checkpoint)

        if len(self.checkpoints[operation_id]) > self.max_checkpoints:
            self.checkpoints[operation_id] = self.checkpoints[operation_id][-self.max_checkpoints:]

        return checkpoint

    def get_latest(self, operation_id: str) -> Optional[Checkpoint]:
        """Get latest checkpoint."""
        checkpoints = self.checkpoints.get(operation_id, [])
        return checkpoints[-1] if checkpoints else None

    def restore(self, operation_id: str, checkpoint_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Restore from checkpoint."""
        if checkpoint_id:
            for cp in self.checkpoints.get(operation_id, []):
                if cp.checkpoint_id == checkpoint_id:
                    return cp.state.copy()
        else:
            latest = self.get_latest(operation_id)
            return latest.state.copy() if latest else None
        return None

    def clear(self, operation_id: str):
        """Clear checkpoints for operation."""
        if operation_id in self.checkpoints:
            del self.checkpoints[operation_id]


class CompensationLog:
    """Log of compensation actions."""

    def __init__(self):
        self.logs: Dict[str, List[CompensationAction]] = {}

    def record(self, operation_id: str, action: CompensationAction):
        """Record a compensation action."""
        if operation_id not in self.logs:
            self.logs[operation_id] = []
        self.logs[operation_id].append(action)

    def get_log(self, operation_id: str) -> List[CompensationAction]:
        """Get compensation log."""
        return self.logs.get(operation_id, [])

    def clear(self, operation_id: str):
        """Clear compensation log."""
        if operation_id in self.logs:
            del self.logs[operation_id]


class RecoveryManager:
    """Manages recovery operations."""

    def __init__(self):
        self.checkpoint_manager = CheckpointManager()
        self.compensation_log = CompensationLog()
        self._recovery_handlers: Dict[RecoveryStrategy, List[Callable]] = {}

    def register_handler(self, strategy: RecoveryStrategy, handler: Callable):
        """Register recovery handler."""
        if strategy not in self._recovery_handlers:
            self._recovery_handlers[strategy] = []
        self._recovery_handlers[strategy].append(handler)

    async def recover_with_checkpoint(
        self,
        operation_id: str,
        operation_func: Callable,
        state: Dict[str, Any]
    ) -> OperationResult:
        """Recover operation from checkpoint."""
        saved_state = self.checkpoint_manager.restore(operation_id)

        if saved_state:
            state.update(saved_state)

        try:
            if asyncio.iscoroutinefunction(operation_func):
                result = await operation_func(state)
            else:
                result = await asyncio.to_thread(operation_func, state)

            return OperationResult(
                operation_id=operation_id,
                success=True,
                state=OperationState.COMPLETED,
                result=result
            )

        except Exception as e:
            logger.error(f"Operation {operation_id} failed: {e}")
            return OperationResult(
                operation_id=operation_id,
                success=False,
                state=OperationState.FAILED,
                error=str(e)
            )

    async def rollback(
        self,
        operation_id: str,
        steps: List[CompensationAction]
    ) -> bool:
        """Rollback using compensation actions."""
        for step in reversed(steps):
            try:
                if asyncio.iscoroutinefunction(step.backward_func):
                    await step.backward_func(step.state)
                else:
                    await asyncio.to_thread(step.backward_func, step.state)
                logger.info(f"Rolled back: {step.name}")
            except Exception as e:
                logger.error(f"Rollback failed for {step.name}: {e}")
                return False

        self.compensation_log.clear(operation_id)
        return True


class SagaOrchestrator:
    """Orchestrates saga pattern for distributed transactions."""

    def __init__(self, recovery_manager: RecoveryManager):
        self.recovery_manager = recovery_manager
        self.sagas: Dict[str, Dict[str, Any]] = {}

    async def execute_saga(
        self,
        saga_id: str,
        steps: List[CompensationAction]
    ) -> OperationResult:
        """Execute saga with compensation."""
        completed_steps = []

        for step in steps:
            try:
                if asyncio.iscoroutinefunction(step.forward_func):
                    result = await step.forward_func(step.state)
                else:
                    result = await asyncio.to_thread(step.forward_func, step.state)

                step.state["result"] = result
                completed_steps.append(step)
                self.recovery_manager.compensation_log.record(saga_id, step)

            except Exception as e:
                logger.error(f"Saga step {step.name} failed: {e}")

                rollback_success = await self.recovery_manager.rollback(
                    saga_id,
                    completed_steps
                )

                return OperationResult(
                    operation_id=saga_id,
                    success=False,
                    state=OperationState.COMPENSATED if rollback_success else OperationState.FAILED,
                    error=str(e)
                )

        return OperationResult(
            operation_id=saga_id,
            success=True,
            state=OperationState.COMPLETED
        )


class StateRollback:
    """Manages state rollback."""

    def __init__(self):
        self.state_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

    def save_state(self, operation_id: str, state: Dict[str, Any]):
        """Save state for rollback."""
        self.state_history[operation_id].append(state.copy())

    def rollback_to_previous(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Rollback to previous state."""
        if operation_id in self.state_history:
            if len(self.state_history[operation_id]) > 1:
                self.state_history[operation_id].pop()
                return self.state_history[operation_id][-1]
        return None

    def get_current_state(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Get current state without rollback."""
        if operation_id in self.state_history:
            if self.state_history[operation_id]:
                return self.state_history[operation_id][-1]
        return None


async def forward_step(state: Dict[str, Any]) -> Dict[str, Any]:
    """Forward step for demo."""
    await asyncio.sleep(0.1)
    state["step_completed"] = state.get("step", 0)
    return state


async def backward_step(state: Dict[str, Any]) -> None:
    """Backward step for demo."""
    await asyncio.sleep(0.05)
    logger.info(f"Rolling back step {state.get('step', 0)}")


async def main():
    """Demonstrate recovery mechanisms."""
    recovery = RecoveryManager()
    saga = SagaOrchestrator(recovery)

    steps = [
        CompensationAction(
            action_id=str(uuid.uuid4()),
            name="step1",
            action_type="create",
            forward_func=forward_step,
            backward_func=backward_step,
            state={"step": 1}
        ),
        CompensationAction(
            action_id=str(uuid.uuid4()),
            name="step2",
            action_type="update",
            forward_func=forward_step,
            backward_func=backward_step,
            state={"step": 2}
        )
    ]

    result = await saga.execute_saga("saga-1", steps)
    print(f"Saga result: {result.success}, state: {result.state.value}")


if __name__ == "__main__":
    asyncio.run(main())
