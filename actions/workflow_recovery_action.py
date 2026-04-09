"""
Workflow Recovery and State Restoration Module.

Provides checkpoint-based recovery, state snapshots,
and fault tolerance for long-running automation workflows.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class CheckpointStatus(Enum):
    PENDING = auto()
    SAVED = auto()
    RESTORED = auto()
    FAILED = auto()


@dataclass
class Checkpoint:
    checkpoint_id: str
    workflow_id: str
    step_index: int
    step_name: str
    state: Dict[str, Any]
    data: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: CheckpointStatus = CheckpointStatus.PENDING
    file_path: Optional[str] = None


@dataclass
class RecoveryPoint:
    checkpoint_id: str
    workflow_id: str
    step_index: int
    step_name: str
    timestamp: float
    state_summary: Dict[str, Any]


class StateStore:
    """Persistent storage for workflow state."""

    def __init__(self, storage_dir: str = "/tmp/workflow_state"):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _get_path(self, workflow_id: str) -> str:
        return os.path.join(self.storage_dir, f"{workflow_id}.state")

    def save(self, workflow_id: str, state: Dict[str, Any]) -> None:
        path = self._get_path(workflow_id)
        with open(path, "wb") as f:
            pickle.dump({
                "workflow_id": workflow_id,
                "state": state,
                "saved_at": time.time(),
            }, f)
        logger.debug("Saved state for workflow %s", workflow_id)

    def load(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        path = self._get_path(workflow_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                data = pickle.load(f)
            return data.get("state")
        except Exception as exc:
            logger.error("Failed to load state for %s: %s", workflow_id, exc)
            return None

    def delete(self, workflow_id: str) -> None:
        path = self._get_path(workflow_id)
        if os.path.exists(path):
            os.remove(path)

    def list_workflows(self) -> List[str]:
        try:
            return [
                f.replace(".state", "") for f in os.listdir(self.storage_dir)
                if f.endswith(".state")
            ]
        except Exception:
            return []


class CheckpointManager:
    """
    Manages checkpoints for workflow recovery.
    """

    def __init__(self, storage_dir: str = "/tmp/workflow_checkpoints"):
        self.storage_dir = storage_dir
        self.state_store = StateStore(storage_dir)
        self._checkpoints: Dict[str, Checkpoint] = {}
        self._recovery_index: Dict[str, List[str]] = defaultdict(list)

    def create_checkpoint(
        self,
        workflow_id: str,
        step_index: int,
        step_name: str,
        state: Dict[str, Any],
        data: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        import uuid
        checkpoint_id = str(uuid.uuid4())[:8]

        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            workflow_id=workflow_id,
            step_index=step_index,
            step_name=step_name,
            state=state,
            data=data or {},
        )

        self._checkpoints[checkpoint_id] = checkpoint
        self._recovery_index[workflow_id].append(checkpoint_id)

        self.state_store.save(workflow_id, state)

        checkpoint.status = CheckpointStatus.SAVED
        logger.info(
            "Created checkpoint %s for workflow '%s' at step %d (%s)",
            checkpoint_id, workflow_id, step_index, step_name
        )

        return checkpoint

    def get_latest_checkpoint(
        self, workflow_id: str
    ) -> Optional[Checkpoint]:
        checkpoint_ids = self._recovery_index.get(workflow_id, [])
        if not checkpoint_ids:
            loaded = self.state_store.load(workflow_id)
            if loaded:
                checkpoint_id = checkpoint_ids[-1] if checkpoint_ids else "loaded"
                return Checkpoint(
                    checkpoint_id=checkpoint_id,
                    workflow_id=workflow_id,
                    step_index=0,
                    step_name="unknown",
                    state=loaded,
                )
            return None

        latest_id = checkpoint_ids[-1]
        return self._checkpoints.get(latest_id)

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        return self._checkpoints.get(checkpoint_id)

    def list_checkpoints(
        self, workflow_id: str
    ) -> List[RecoveryPoint]:
        checkpoint_ids = self._recovery_index.get(workflow_id, [])
        results = []
        for cid in checkpoint_ids:
            cp = self._checkpoints.get(cid)
            if cp:
                results.append(RecoveryPoint(
                    checkpoint_id=cid,
                    workflow_id=workflow_id,
                    step_index=cp.step_index,
                    step_name=cp.step_name,
                    timestamp=cp.created_at.timestamp(),
                    state_summary={"keys": list(cp.state.keys())},
                ))
        return sorted(results, key=lambda r: r.step_index, reverse=True)

    def delete_checkpoints(self, workflow_id: str) -> int:
        checkpoint_ids = self._recovery_index.pop(workflow_id, [])
        for cid in checkpoint_ids:
            self._checkpoints.pop(cid, None)
        self.state_store.delete(workflow_id)
        return len(checkpoint_ids)


class WorkflowRecoveryManager:
    """
    Manages workflow recovery with automatic checkpointing.
    """

    def __init__(self, checkpoint_interval: int = 5):
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_manager = CheckpointManager()
        self._recovery_handlers: Dict[str, Callable] = {}

    def register_recovery_handler(
        self, workflow_id: str, handler: Callable[[Dict[str, Any]], Any]
    ) -> None:
        self._recovery_handlers[workflow_id] = handler

    def should_checkpoint(self, step_index: int) -> bool:
        return step_index > 0 and step_index % self.checkpoint_interval == 0

    async def execute_with_recovery(
        self,
        workflow_id: str,
        steps: List[Callable],
        initial_state: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, Dict[str, Any]]:
        checkpoint = self.checkpoint_manager.get_latest_checkpoint(workflow_id)

        if checkpoint:
            logger.info(
                "Recovering workflow '%s' from checkpoint at step %d",
                workflow_id, checkpoint.step_index
            )
            current_state = checkpoint.state
            start_index = checkpoint.step_index + 1
        else:
            current_state = initial_state or {}
            start_index = 0

        logger.info(
            "Executing workflow '%s' from step %d (%d total steps)",
            workflow_id, start_index, len(steps)
        )

        success = True
        error: Optional[str] = None

        for i in range(start_index, len(steps)):
            step = steps[i]
            step_name = getattr(step, "__name__", f"step_{i}")

            try:
                if asyncio.iscoroutinefunction(step):
                    result = await step(current_state)
                else:
                    result = step(current_state)

                if isinstance(result, dict):
                    current_state.update(result)

                if self.should_checkpoint(i):
                    self.checkpoint_manager.create_checkpoint(
                        workflow_id=workflow_id,
                        step_index=i,
                        step_name=step_name,
                        state=current_state,
                    )

            except Exception as exc:
                logger.error("Workflow '%s' failed at step %d (%s): %s",
                             workflow_id, i, step_name, exc)
                success = False
                error = str(exc)

                self.checkpoint_manager.create_checkpoint(
                    workflow_id=workflow_id,
                    step_index=i,
                    step_name=step_name,
                    state=current_state,
                    data={"error": error},
                )

                recovery_handler = self._recovery_handlers.get(workflow_id)
                if recovery_handler:
                    try:
                        await recovery_handler(current_state)
                    except Exception as recovery_error:
                        logger.error("Recovery handler failed: %s", recovery_error)

                break

        if success:
            self.checkpoint_manager.delete_checkpoints(workflow_id)

        return success, current_state

    def get_recovery_info(self, workflow_id: str) -> Dict[str, Any]:
        checkpoints = self.checkpoint_manager.list_checkpoints(workflow_id)
        latest = self.checkpoint_manager.get_latest_checkpoint(workflow_id)
        return {
            "workflow_id": workflow_id,
            "can_recover": len(checkpoints) > 0,
            "latest_checkpoint": {
                "step_index": latest.step_index if latest else None,
                "step_name": latest.step_name if latest else None,
                "created_at": latest.created_at.isoformat() if latest else None,
            } if latest else None,
            "checkpoint_count": len(checkpoints),
        }
