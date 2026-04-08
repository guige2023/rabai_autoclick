"""Automation Recovery Action Module.

Provides automation recovery, checkpoint, and
failover capabilities for workflow automation.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import time
from datetime import datetime
import uuid


class RecoveryState(Enum):
    """State of recovery system."""
    IDLE = "idle"
    CHECKPOINTING = "checkpointing"
    RECOVERING = "recovering"
    FAILED = "failed"


@dataclass
class Checkpoint:
    """Represents a recovery checkpoint."""
    id: str
    workflow_id: str
    step_id: str
    state: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "step_id": self.step_id,
            "state": self.state,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class RecoveryAction:
    """Defines a recovery action."""
    action_type: str
    handler: Callable
    priority: int = 0
    condition: Optional[Callable[[Exception], bool]] = None


@dataclass
class FailoverConfig:
    """Configuration for failover behavior."""
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    exponential_backoff: bool = True
    max_backoff_seconds: float = 60.0
    fallback_handler: Optional[Callable] = None


class CheckpointManager:
    """Manages workflow checkpoints."""

    def __init__(self, storage_path: Optional[str] = None):
        self._checkpoints: Dict[str, List[Checkpoint]] = {}
        self._latest: Dict[str, Checkpoint] = {}
        self._storage_path = storage_path

    def save_checkpoint(
        self,
        workflow_id: str,
        step_id: str,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        """Save a checkpoint."""
        checkpoint = Checkpoint(
            id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            step_id=step_id,
            state=state.copy(),
            metadata=metadata or {},
        )

        if workflow_id not in self._checkpoints:
            self._checkpoints[workflow_id] = []

        self._checkpoints[workflow_id].append(checkpoint)
        self._latest[workflow_id] = checkpoint

        if self._storage_path:
            self._persist_checkpoint(checkpoint)

        return checkpoint

    def get_latest(self, workflow_id: str) -> Optional[Checkpoint]:
        """Get latest checkpoint for workflow."""
        return self._latest.get(workflow_id)

    def get_checkpoint(
        self,
        workflow_id: str,
        checkpoint_id: str,
    ) -> Optional[Checkpoint]:
        """Get specific checkpoint."""
        checkpoints = self._checkpoints.get(workflow_id, [])
        for cp in checkpoints:
            if cp.id == checkpoint_id:
                return cp
        return None

    def get_history(
        self,
        workflow_id: str,
        limit: int = 10,
    ) -> List[Checkpoint]:
        """Get checkpoint history."""
        checkpoints = self._checkpoints.get(workflow_id, [])
        return checkpoints[-limit:]

    def delete_checkpoints(self, workflow_id: str) -> int:
        """Delete all checkpoints for workflow."""
        if workflow_id in self._checkpoints:
            count = len(self._checkpoints[workflow_id])
            del self._checkpoints[workflow_id]
            if workflow_id in self._latest:
                del self._latest[workflow_id]
            return count
        return 0

    def _persist_checkpoint(self, checkpoint: Checkpoint):
        """Persist checkpoint to storage."""
        pass


class RetryManager:
    """Manages retry logic for failed operations."""

    def __init__(self, config: Optional[FailoverConfig] = None):
        self.config = config or FailoverConfig()
        self._retry_counts: Dict[str, int] = {}

    def should_retry(
        self,
        operation_id: str,
        error: Exception,
    ) -> bool:
        """Check if operation should be retried."""
        current_count = self._retry_counts.get(operation_id, 0)
        return current_count < self.config.max_retries

    def record_attempt(self, operation_id: str) -> int:
        """Record a retry attempt."""
        self._retry_counts[operation_id] = self._retry_counts.get(operation_id, 0) + 1
        return self._retry_counts[operation_id]

    def get_retry_delay(self, operation_id: str) -> float:
        """Calculate retry delay with backoff."""
        attempt = self._retry_counts.get(operation_id, 1)

        if self.config.exponential_backoff:
            delay = min(
                self.config.retry_delay_seconds * (2 ** (attempt - 1)),
                self.config.max_backoff_seconds,
            )
        else:
            delay = self.config.retry_delay_seconds

        return delay

    def reset(self, operation_id: str):
        """Reset retry count for operation."""
        if operation_id in self._retry_counts:
            del self._retry_counts[operation_id]

    def get_attempt_count(self, operation_id: str) -> int:
        """Get current attempt count."""
        return self._retry_counts.get(operation_id, 0)


class FailoverHandler:
    """Handles failover scenarios."""

    def __init__(
        self,
        recovery_actions: Optional[List[RecoveryAction]] = None,
        fallback_handler: Optional[Callable] = None,
    ):
        self._recovery_actions: Dict[str, RecoveryAction] = {}
        self._fallback_handler = fallback_handler

        if recovery_actions:
            for action in recovery_actions:
                self._recovery_actions[action.action_type] = action

    def register_action(self, action: RecoveryAction):
        """Register a recovery action."""
        self._recovery_actions[action.action_type] = action

    async def handle_failure(
        self,
        error: Exception,
        context: Dict[str, Any],
    ) -> Any:
        """Handle a failure and attempt recovery."""
        error_type = type(error).__name__

        if error_type in self._recovery_actions:
            action = self._recovery_actions[error_type]
            if action.condition is None or action.condition(error):
                try:
                    return await action.handler(context)
                except Exception as recovery_error:
                    context["recovery_error"] = str(recovery_error)

        if self._fallback_handler:
            return self._fallback_handler(context)

        raise error


class StateManager:
    """Manages workflow state for recovery."""

    def __init__(self):
        self._states: Dict[str, Dict[str, Any]] = {}
        self._snapshots: Dict[str, List[Dict[str, Any]]] = {}

    def set_state(
        self,
        workflow_id: str,
        state: Dict[str, Any],
    ):
        """Set workflow state."""
        self._states[workflow_id] = state.copy()

    def get_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow state."""
        return self._states.get(workflow_id)

    def create_snapshot(
        self,
        workflow_id: str,
        snapshot_name: Optional[str] = None,
    ) -> str:
        """Create a named snapshot of current state."""
        if workflow_id not in self._states:
            raise ValueError(f"No state found for workflow {workflow_id}")

        snapshot_id = snapshot_name or f"snapshot_{int(time.time())}"

        if workflow_id not in self._snapshots:
            self._snapshots[workflow_id] = []

        self._snapshots[workflow_id].append({
            "id": snapshot_id,
            "state": self._states[workflow_id].copy(),
            "timestamp": datetime.now().isoformat(),
        })

        return snapshot_id

    def restore_snapshot(
        self,
        workflow_id: str,
        snapshot_id: str,
    ) -> bool:
        """Restore state from snapshot."""
        snapshots = self._snapshots.get(workflow_id, [])
        for snapshot in snapshots:
            if snapshot["id"] == snapshot_id:
                self._states[workflow_id] = snapshot["state"].copy()
                return True
        return False

    def delete_state(self, workflow_id: str) -> bool:
        """Delete workflow state."""
        if workflow_id in self._states:
            del self._states[workflow_id]
            return True
        return False


class WorkflowRecovery:
    """Orchestrates workflow recovery."""

    def __init__(
        self,
        checkpoint_manager: Optional[CheckpointManager] = None,
        retry_manager: Optional[RetryManager] = None,
        failover_handler: Optional[FailoverHandler] = None,
        state_manager: Optional[StateManager] = None,
    ):
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.retry_manager = retry_manager or RetryManager()
        self.failover_handler = failover_handler or FailoverHandler()
        self.state_manager = state_manager or StateManager()
        self._recovery_handlers: Dict[str, Callable] = {}

    def register_recovery_handler(
        self,
        step_id: str,
        handler: Callable,
    ):
        """Register a recovery handler for a step."""
        self._recovery_handlers[step_id] = handler

    async def recover_workflow(
        self,
        workflow_id: str,
        from_checkpoint: bool = True,
    ) -> Dict[str, Any]:
        """Recover a workflow from checkpoint."""
        if from_checkpoint:
            checkpoint = self.checkpoint_manager.get_latest(workflow_id)
            if checkpoint:
                self.state_manager.set_state(workflow_id, checkpoint.state)
                return {
                    "recovered": True,
                    "checkpoint_id": checkpoint.id,
                    "step_id": checkpoint.step_id,
                    "state": checkpoint.state,
                }

        return {
            "recovered": False,
            "reason": "No checkpoint found",
        }

    async def execute_with_recovery(
        self,
        workflow_id: str,
        step_id: str,
        operation: Callable,
        context: Dict[str, Any],
    ) -> Any:
        """Execute operation with automatic recovery."""
        operation_id = f"{workflow_id}:{step_id}"

        try:
            self.checkpoint_manager.save_checkpoint(
                workflow_id, step_id, context
            )

            result = await operation(context)

            self.checkpoint_manager.save_checkpoint(
                workflow_id, step_id, {**context, "result": result}
            )

            self.retry_manager.reset(operation_id)

            return result

        except Exception as error:
            if self.retry_manager.should_retry(operation_id, error):
                attempt = self.retry_manager.record_attempt(operation_id)
                delay = self.retry_manager.get_retry_delay(operation_id)

                import asyncio
                await asyncio.sleep(delay)

                recovery_handler = self._recovery_handlers.get(step_id)
                if recovery_handler:
                    context = await recovery_handler(context, error)

                return await self.execute_with_recovery(
                    workflow_id, step_id, operation, context
                )

            return await self.failover_handler.handle_failure(error, context)


class AutomationRecoveryAction:
    """High-level automation recovery action."""

    def __init__(
        self,
        workflow_recovery: Optional[WorkflowRecovery] = None,
    ):
        self.workflow_recovery = workflow_recovery or WorkflowRecovery()

    def create_checkpoint(
        self,
        workflow_id: str,
        step_id: str,
        state: Dict[str, Any],
    ) -> Checkpoint:
        """Create a checkpoint."""
        return self.workflow_recovery.checkpoint_manager.save_checkpoint(
            workflow_id, step_id, state
        )

    def recover(
        self,
        workflow_id: str,
    ) -> Dict[str, Any]:
        """Recover workflow."""
        import asyncio
        return asyncio.run(self.workflow_recovery.recover_workflow(workflow_id))

    def register_recovery_handler(
        self,
        step_id: str,
        handler: Callable,
    ):
        """Register recovery handler for step."""
        self.workflow_recovery.register_recovery_handler(step_id, handler)


# Module exports
__all__ = [
    "AutomationRecoveryAction",
    "WorkflowRecovery",
    "CheckpointManager",
    "RetryManager",
    "FailoverHandler",
    "StateManager",
    "Checkpoint",
    "RecoveryAction",
    "FailoverConfig",
    "RecoveryState",
]
