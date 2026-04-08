"""
Workflow Rollback Action Module.

Rolls back workflow execution to a previous checkpoint with
state restoration and compensating transactions.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class Checkpoint:
    """A workflow checkpoint."""
    id: str
    workflow_id: str
    timestamp: float
    state: dict[str, Any]
    completed_steps: list[str]


@dataclass
class RollbackResult:
    """Result of rollback operation."""
    success: bool
    restored_checkpoint: Optional[str]
    state_restored: dict[str, Any]
    error: Optional[str] = None


class WorkflowRollbackAction(BaseAction):
    """Rollback workflow to a previous checkpoint."""

    def __init__(self) -> None:
        super().__init__("workflow_rollback")
        self._checkpoints: dict[str, list[Checkpoint]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Create checkpoint or rollback.

        Args:
            context: Execution context
            params: Parameters:
                - action: checkpoint or rollback
                - workflow_id: Workflow identifier
                - state: Current state (for checkpoint)
                - completed_steps: Steps completed (for checkpoint)
                - checkpoint_id: Checkpoint to restore (for rollback)

        Returns:
            For checkpoint: confirmation
            For rollback: RollbackResult
        """
        import time

        action = params.get("action", "checkpoint")
        workflow_id = params.get("workflow_id", "default")

        if action == "checkpoint":
            state = params.get("state", {})
            completed_steps = params.get("completed_steps", [])

            checkpoint = Checkpoint(
                id=f"ckpt_{int(time.time() * 1000)}",
                workflow_id=workflow_id,
                timestamp=time.time(),
                state=state,
                completed_steps=completed_steps
            )

            if workflow_id not in self._checkpoints:
                self._checkpoints[workflow_id] = []
            self._checkpoints[workflow_id].append(checkpoint)

            return {"checkpoint_created": checkpoint.id, "timestamp": checkpoint.timestamp}

        elif action == "rollback":
            checkpoint_id = params.get("checkpoint_id")
            state = params.copy() if params.get("state") else {}

            if workflow_id not in self._checkpoints:
                return RollbackResult(False, None, {}, "No checkpoints found").__dict__

            checkpoints = self._checkpoints[workflow_id]

            if checkpoint_id:
                target = next((c for c in checkpoints if c.id == checkpoint_id), None)
            else:
                target = checkpoints[-2] if len(checkpoints) >= 2 else checkpoints[-1]

            if not target:
                return RollbackResult(False, None, {}, "Checkpoint not found").__dict__

            self._checkpoints[workflow_id] = [c for c in checkpoints if c.id != target.id]

            return RollbackResult(
                success=True,
                restored_checkpoint=target.id,
                state_restored=target.state
            ).__dict__

        return {"error": f"Unknown action: {action}"}

    def get_checkpoints(self, workflow_id: str) -> list[Checkpoint]:
        """Get all checkpoints for a workflow."""
        return self._checkpoints.get(workflow_id, [])
