"""
Automation Checkpoint Action Module.

Creates and manages checkpoints for long-running automation
 workflows with rollback and resume capabilities.
"""

from __future__ import annotations

import os
import json
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """A workflow checkpoint."""
    checkpoint_id: str
    workflow_id: str
    step: str
    state: dict[str, Any]
    created_at: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckpointResult:
    """Result of checkpoint operations."""
    success: bool
    checkpoint: Optional[Checkpoint] = None
    error: Optional[str] = None


class AutomationCheckpointAction:
    """
    Checkpoint management for workflow automation.

    Creates checkpoints during long-running workflows to enable
    resumption after failures and rollback capabilities.

    Example:
        checkpoint = AutomationCheckpointAction(checkpoint_dir="/tmp/checkpoints")
        checkpoint.save("workflow_001", "step_3", workflow_state)
        restored = checkpoint.load_latest("workflow_001")
    """

    def __init__(
        self,
        checkpoint_dir: str = "/tmp/automation_checkpoints",
        max_checkpoints: int = 10,
    ) -> None:
        self.checkpoint_dir = checkpoint_dir
        self.max_checkpoints = max_checkpoints
        self._checkpoints: dict[str, list[Checkpoint]] = {}
        os.makedirs(checkpoint_dir, exist_ok=True)

    def save(
        self,
        workflow_id: str,
        step: str,
        state: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> CheckpointResult:
        """Save a checkpoint for a workflow."""
        try:
            checkpoint_id = f"{workflow_id}_{step}_{int(time.time() * 1000)}"

            checkpoint = Checkpoint(
                checkpoint_id=checkpoint_id,
                workflow_id=workflow_id,
                step=step,
                state=state,
                created_at=time.time(),
                metadata=metadata or {},
            )

            if workflow_id not in self._checkpoints:
                self._checkpoints[workflow_id] = []

            self._checkpoints[workflow_id].append(checkpoint)

            self._trim_checkpoints(workflow_id)
            self._persist_checkpoint(checkpoint)

            return CheckpointResult(success=True, checkpoint=checkpoint)

        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
            return CheckpointResult(success=False, error=str(e))

    def load(
        self,
        checkpoint_id: str,
    ) -> Optional[Checkpoint]:
        """Load a specific checkpoint by ID."""
        filepath = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.json")

        if not os.path.exists(filepath):
            return None

        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            return Checkpoint(
                checkpoint_id=data["checkpoint_id"],
                workflow_id=data["workflow_id"],
                step=data["step"],
                state=data["state"],
                created_at=data["created_at"],
                metadata=data.get("metadata", {}),
            )

        except Exception as e:
            logger.error(f"Failed to load checkpoint {checkpoint_id}: {e}")
            return None

    def load_latest(
        self,
        workflow_id: str,
    ) -> Optional[Checkpoint]:
        """Load the latest checkpoint for a workflow."""
        checkpoints = self._checkpoints.get(workflow_id, [])

        if not checkpoints:
            checkpoint_files = [
                f for f in os.listdir(self.checkpoint_dir)
                if f.startswith(workflow_id) and f.endswith(".json")
            ]

            if not checkpoint_files:
                return None

            latest_file = sorted(checkpoint_files)[-1]
            return self.load(latest_file.replace(".json", ""))

        return checkpoints[-1]

    def list_checkpoints(
        self,
        workflow_id: str,
    ) -> list[Checkpoint]:
        """List all checkpoints for a workflow."""
        return self._checkpoints.get(workflow_id, [])

    def delete(
        self,
        checkpoint_id: str,
    ) -> bool:
        """Delete a specific checkpoint."""
        filepath = os.path.join(self.checkpoint_dir, f"{checkpoint_id}.json")

        try:
            if os.path.exists(filepath):
                os.remove(filepath)

            for wf_checkpoints in self._checkpoints.values():
                wf_checkpoints[:] = [
                    c for c in wf_checkpoints if c.checkpoint_id != checkpoint_id
                ]

            return True

        except Exception as e:
            logger.error(f"Failed to delete checkpoint {checkpoint_id}: {e}")
            return False

    def delete_workflow_checkpoints(
        self,
        workflow_id: str,
    ) -> int:
        """Delete all checkpoints for a workflow."""
        checkpoints = self._checkpoints.pop(workflow_id, [])
        deleted = 0

        for checkpoint in checkpoints:
            filepath = os.path.join(self.checkpoint_dir, f"{checkpoint.checkpoint_id}.json")
            if os.path.exists(filepath):
                os.remove(filepath)
                deleted += 1

        return deleted

    def _persist_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Write checkpoint to disk."""
        filepath = os.path.join(self.checkpoint_dir, f"{checkpoint.checkpoint_id}.json")

        data = {
            "checkpoint_id": checkpoint.checkpoint_id,
            "workflow_id": checkpoint.workflow_id,
            "step": checkpoint.step,
            "state": checkpoint.state,
            "created_at": checkpoint.created_at,
            "metadata": checkpoint.metadata,
        }

        with open(filepath, "w") as f:
            json.dump(data, f)

    def _trim_checkpoints(self, workflow_id: str) -> None:
        """Trim old checkpoints to keep only max_checkpoints."""
        checkpoints = self._checkpoints[workflow_id]

        while len(checkpoints) > self.max_checkpoints:
            oldest = checkpoints.pop(0)
            self.delete(oldest.checkpoint_id)
