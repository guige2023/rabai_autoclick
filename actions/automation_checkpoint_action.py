"""
Automation Checkpoint Action Module.

Checkpoint and resume for long-running automation,
persists state and allows recovery from failures.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import json
import os
import time

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Checkpoint data."""
    checkpoint_id: str
    task_name: str
    step: str
    state: dict[str, Any]
    timestamp: float
    completed_steps: list[str]


class AutomationCheckpointAction:
    """
    Checkpoint-based automation with state persistence.

    Saves state during execution for recovery
    and resumption after failures.

    Example:
        checkpoint = AutomationCheckpointAction(storage_path="./checkpoints")
        checkpoint.save("step_1", {"progress": 50})
        state = checkpoint.load("step_1")
    """

    def __init__(
        self,
        storage_path: str = "./checkpoints",
        task_name: str = "automation",
        max_checkpoints: int = 100,
    ) -> None:
        self.storage_path = storage_path
        self.task_name = task_name
        self.max_checkpoints = max_checkpoints
        self._current_checkpoint: Optional[Checkpoint] = None
        self._ensure_storage_dir()

    def save(
        self,
        step: str,
        state: dict[str, Any],
        completed_steps: Optional[list[str]] = None,
    ) -> str:
        """Save a checkpoint."""
        checkpoint_id = f"{self.task_name}_{step}_{int(time.time())}"

        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            task_name=self.task_name,
            step=step,
            state=state,
            timestamp=time.time(),
            completed_steps=completed_steps or [],
        )

        self._current_checkpoint = checkpoint
        self._write_checkpoint(checkpoint)
        self._cleanup_old_checkpoints()

        logger.info("Checkpoint saved: %s at step '%s'", checkpoint_id, step)

        return checkpoint_id

    def load(
        self,
        step: Optional[str] = None,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[Checkpoint]:
        """Load a checkpoint."""
        if checkpoint_id:
            return self._read_checkpoint(checkpoint_id)

        if step:
            checkpoint_id = self._find_latest_for_step(step)
            if checkpoint_id:
                return self._read_checkpoint(checkpoint_id)

        latest = self._find_latest_checkpoint()
        if latest:
            return self._read_checkpoint(latest)

        return None

    def resume(
        self,
        step: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Get state to resume from."""
        checkpoint = self.load(step=step)
        if checkpoint:
            return checkpoint.state
        return None

    def get_last_step(self) -> Optional[str]:
        """Get the last completed step."""
        latest = self._find_latest_checkpoint()
        if not latest:
            return None

        checkpoint = self._read_checkpoint(latest)
        return checkpoint.step if checkpoint else None

    def clear(self) -> None:
        """Clear all checkpoints."""
        if os.path.exists(self.storage_path):
            for filename in os.listdir(self.storage_path):
                if filename.endswith(".json"):
                    os.remove(os.path.join(self.storage_path, filename))

    def list_checkpoints(self) -> list[dict[str, Any]]:
        """List all available checkpoints."""
        if not os.path.exists(self.storage_path):
            return []

        checkpoints = []
        for filename in os.listdir(self.storage_path):
            if filename.endswith(".json"):
                path = os.path.join(self.storage_path, filename)
                stat = os.stat(path)
                checkpoints.append({
                    "filename": filename,
                    "size_bytes": stat.st_size,
                    "modified": stat.st_mtime,
                })

        checkpoints.sort(key=lambda x: x["modified"], reverse=True)
        return checkpoints

    def _ensure_storage_dir(self) -> None:
        """Ensure storage directory exists."""
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

    def _write_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Write checkpoint to disk."""
        path = os.path.join(
            self.storage_path,
            f"{checkpoint.checkpoint_id}.json"
        )

        with open(path, "w") as f:
            json.dump({
                "checkpoint_id": checkpoint.checkpoint_id,
                "task_name": checkpoint.task_name,
                "step": checkpoint.step,
                "state": checkpoint.state,
                "timestamp": checkpoint.timestamp,
                "completed_steps": checkpoint.completed_steps,
            }, f, indent=2, default=str)

    def _read_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Read checkpoint from disk."""
        path = os.path.join(self.storage_path, f"{checkpoint_id}.json")

        if not os.path.exists(path):
            return None

        try:
            with open(path) as f:
                data = json.load(f)

            return Checkpoint(
                checkpoint_id=data["checkpoint_id"],
                task_name=data["task_name"],
                step=data["step"],
                state=data["state"],
                timestamp=data["timestamp"],
                completed_steps=data.get("completed_steps", []),
            )
        except Exception as e:
            logger.error("Failed to read checkpoint %s: %s", checkpoint_id, e)
            return None

    def _find_latest_checkpoint(self) -> Optional[str]:
        """Find the most recent checkpoint."""
        if not os.path.exists(self.storage_path):
            return None

        checkpoints = [
            f[:-5] for f in os.listdir(self.storage_path)
            if f.endswith(".json")
        ]

        if not checkpoints:
            return None

        checkpoints_with_time = []
        for cp_id in checkpoints:
            checkpoint = self._read_checkpoint(cp_id)
            if checkpoint:
                checkpoints_with_time.append((cp_id, checkpoint.timestamp))

        if not checkpoints_with_time:
            return None

        checkpoints_with_time.sort(key=lambda x: x[1], reverse=True)
        return checkpoints_with_time[0][0]

    def _find_latest_for_step(self, step: str) -> Optional[str]:
        """Find latest checkpoint for a specific step."""
        if not os.path.exists(self.storage_path):
            return None

        checkpoints = [
            f[:-5] for f in os.listdir(self.storage_path)
            if f.endswith(".json") and step in f
        ]

        if not checkpoints:
            return None

        latest = None
        latest_time = 0

        for cp_id in checkpoints:
            checkpoint = self._read_checkpoint(cp_id)
            if checkpoint and checkpoint.step == step:
                if checkpoint.timestamp > latest_time:
                    latest_time = checkpoint.timestamp
                    latest = cp_id

        return latest

    def _cleanup_old_checkpoints(self) -> None:
        """Remove old checkpoints exceeding max."""
        if not os.path.exists(self.storage_path):
            return

        checkpoints = self.list_checkpoints()

        if len(checkpoints) > self.max_checkpoints:
            for cp in checkpoints[self.max_checkpoints:]:
                path = os.path.join(self.storage_path, cp["filename"])
                os.remove(path)
