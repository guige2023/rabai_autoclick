"""
Automation Rollback Action Module.

Provides rollback and recovery capabilities
for automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)


class RollbackStrategy(Enum):
    """Rollback strategies."""
    COMPENSATE = "compensate"
    RETRY = "retry"
    FAILOVER = "failover"
    RESTORE = "restore"


@dataclass
class RollbackAction:
    """Single rollback action."""
    action_id: str
    name: str
    execute_func: Callable
    compensate_func: Optional[Callable] = None
    order: int = 0


@dataclass
class Checkpoint:
    """Checkpoint for rollback."""
    checkpoint_id: str
    name: str
    timestamp: datetime
    state: Dict[str, Any]
    actions_completed: List[str] = field(default_factory=list)


@dataclass
class RollbackResult:
    """Result of rollback operation."""
    success: bool
    rolled_back_actions: int
    failed_actions: int
    errors: List[str] = field(default_factory=list)


class RollbackManager:
    """Manages rollback operations."""

    def __init__(self):
        self.checkpoints: List[Checkpoint] = []
        self.actions: List[RollbackAction] = []
        self.current_checkpoint: Optional[Checkpoint] = None

    def add_action(
        self,
        name: str,
        execute_func: Callable,
        compensate_func: Optional[Callable] = None
    ) -> str:
        """Add an action to the rollback chain."""
        action_id = str(uuid.uuid4())
        action = RollbackAction(
            action_id=action_id,
            name=name,
            execute_func=execute_func,
            compensate_func=compensate_func,
            order=len(self.actions)
        )
        self.actions.append(action)
        return action_id

    def create_checkpoint(self, name: str, state: Dict[str, Any]) -> str:
        """Create a checkpoint."""
        checkpoint_id = str(uuid.uuid4())
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            name=name,
            timestamp=datetime.now(),
            state=state.copy(),
            actions_completed=[a.action_id for a in self.actions]
        )
        self.checkpoints.append(checkpoint)
        self.current_checkpoint = checkpoint
        return checkpoint_id

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get checkpoint by ID."""
        for checkpoint in self.checkpoints:
            if checkpoint.checkpoint_id == checkpoint_id:
                return checkpoint
        return None

    async def rollback_to_checkpoint(
        self,
        checkpoint_id: str
    ) -> RollbackResult:
        """Rollback to a specific checkpoint."""
        checkpoint = self.get_checkpoint(checkpoint_id)
        if not checkpoint:
            return RollbackResult(
                success=False,
                rolled_back_actions=0,
                failed_actions=0,
                errors=[f"Checkpoint not found: {checkpoint_id}"]
            )

        actions_to_rollback = [
            a for a in self.actions
            if a.action_id not in checkpoint.actions_completed
        ]

        actions_to_rollback = sorted(
            actions_to_rollback,
            key=lambda a: a.order,
            reverse=True
        )

        rolled_back = 0
        failed = 0
        errors = []

        for action in actions_to_rollback:
            if action.compensate_func:
                try:
                    if asyncio.iscoroutinefunction(action.compensate_func):
                        await action.compensate_func(checkpoint.state)
                    else:
                        action.compensate_func(checkpoint.state)
                    rolled_back += 1
                except Exception as e:
                    failed += 1
                    errors.append(f"Rollback failed for {action.name}: {str(e)}")
                    logger.error(f"Rollback error: {e}")

        return RollbackResult(
            success=failed == 0,
            rolled_back_actions=rolled_back,
            failed_actions=failed,
            errors=errors
        )

    async def rollback_actions(
        self,
        action_ids: List[str]
    ) -> RollbackResult:
        """Rollback specific actions."""
        actions_to_rollback = [
            a for a in self.actions
            if a.action_id in action_ids
        ]

        actions_to_rollback = sorted(
            actions_to_rollback,
            key=lambda a: a.order,
            reverse=True
        )

        rolled_back = 0
        failed = 0
        errors = []

        for action in actions_to_rollback:
            if action.compensate_func:
                try:
                    if asyncio.iscoroutinefunction(action.compensate_func):
                        await action.compensate_func()
                    else:
                        action.compensate_func()
                    rolled_back += 1
                except Exception as e:
                    failed += 1
                    errors.append(f"Rollback failed for {action.name}: {str(e)}")

        return RollbackResult(
            success=failed == 0,
            rolled_back_actions=rolled_back,
            failed_actions=failed,
            errors=errors
        )


class StateManager:
    """Manages workflow state for recovery."""

    def __init__(self):
        self.states: Dict[str, Dict[str, Any]] = {}
        self.history: List[Dict[str, Any]] = []

    def save_state(self, workflow_id: str, state: Dict[str, Any]):
        """Save workflow state."""
        self.states[workflow_id] = state.copy()
        self.history.append({
            "workflow_id": workflow_id,
            "timestamp": datetime.now(),
            "state": state.copy()
        })

    def get_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get workflow state."""
        return self.states.get(workflow_id)

    def restore_state(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Restore workflow state."""
        state = self.states.get(workflow_id)
        if state:
            return state.copy()
        return None

    def delete_state(self, workflow_id: str) -> bool:
        """Delete workflow state."""
        if workflow_id in self.states:
            del self.states[workflow_id]
            return True
        return False


class RecoveryHandler:
    """Handles recovery operations."""

    def __init__(self, rollback_manager: RollbackManager):
        self.rollback_manager = rollback_manager
        self.recovery_strategies: Dict[str, RollbackStrategy] = {}

    def register_strategy(
        self,
        action_type: str,
        strategy: RollbackStrategy
    ):
        """Register recovery strategy."""
        self.recovery_strategies[action_type] = strategy

    async def recover(
        self,
        action_type: str,
        context: Dict[str, Any]
    ) -> bool:
        """Recover from failure using registered strategy."""
        strategy = self.recovery_strategies.get(action_type)

        if not strategy:
            logger.warning(f"No recovery strategy for {action_type}")
            return False

        if strategy == RollbackStrategy.COMPENSATE:
            return await self._compensate(context)
        elif strategy == RollbackStrategy.RETRY:
            return await self._retry(context)
        elif strategy == RollbackStrategy.FAILOVER:
            return await self._failover(context)
        elif strategy == RollbackStrategy.RESTORE:
            return await self._restore(context)

        return False

    async def _compensate(self, context: Dict[str, Any]) -> bool:
        """Compensate action."""
        action_id = context.get("action_id")
        if action_id:
            result = await self.rollback_manager.rollback_actions([action_id])
            return result.success
        return False

    async def _retry(self, context: Dict[str, Any]) -> bool:
        """Retry action."""
        func = context.get("retry_func")
        if func:
            try:
                if asyncio.iscoroutinefunction(func):
                    await func()
                else:
                    func()
                return True
            except Exception as e:
                logger.error(f"Retry failed: {e}")
                return False
        return False

    async def _failover(self, context: Dict[str, Any]) -> bool:
        """Failover to backup."""
        backup_func = context.get("backup_func")
        if backup_func:
            try:
                if asyncio.iscoroutinefunction(backup_func):
                    await backup_func()
                else:
                    backup_func()
                return True
            except Exception as e:
                logger.error(f"Failover failed: {e}")
                return False
        return False

    async def _restore(self, context: Dict[str, Any]) -> bool:
        """Restore from checkpoint."""
        checkpoint_id = context.get("checkpoint_id")
        if checkpoint_id:
            result = await self.rollback_manager.rollback_to_checkpoint(checkpoint_id)
            return result.success
        return False


async def main():
    """Demonstrate rollback."""
    manager = RollbackManager()

    def create_action():
        print("Creating resource")

    def delete_action(state):
        print("Deleting resource")

    manager.add_action("create_resource", create_action, delete_action)
    manager.create_checkpoint("before_action", {"resource_id": "123"})

    result = await manager.rollback_to_checkpoint(
        manager.checkpoints[0].checkpoint_id
    )
    print(f"Rollback success: {result.success}")


if __name__ == "__main__":
    asyncio.run(main())
