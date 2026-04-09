"""Automation Checkpoint Action module.

Provides checkpoint and rollback capabilities for automation
workflows, supporting persistent state storage, versioning,
and recovery from failures.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

import aiofiles


class CheckpointStatus(Enum):
    """Checkpoint status."""

    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    EXPIRED = "expired"


@dataclass
class Checkpoint:
    """A workflow checkpoint."""

    checkpoint_id: str
    workflow_name: str
    step_name: str
    state: dict[str, Any]
    status: CheckpointStatus
    created_at: float = field(default_factory=time.time)
    committed_at: Optional[float] = None
    version: int = 0
    parent_id: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "workflow_name": self.workflow_name,
            "step_name": self.step_name,
            "state": self.state,
            "status": self.status.value,
            "created_at": self.created_at,
            "committed_at": self.committed_at,
            "version": self.version,
            "parent_id": self.parent_id,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Checkpoint":
        """Create from dictionary."""
        return cls(
            checkpoint_id=data["checkpoint_id"],
            workflow_name=data["workflow_name"],
            step_name=data["step_name"],
            state=data["state"],
            status=CheckpointStatus(data["status"]),
            created_at=data.get("created_at", time.time()),
            committed_at=data.get("committed_at"),
            version=data.get("version", 0),
            parent_id=data.get("parent_id"),
            metadata=data.get("metadata", {}),
        )


class CheckpointManager:
    """Manages workflow checkpoints."""

    def __init__(
        self,
        storage_path: Optional[str] = None,
        max_checkpoints: int = 100,
        ttl_seconds: Optional[float] = None,
    ):
        self.storage_path = storage_path or "/tmp/checkpoints"
        self.max_checkpoints = max_checkpoints
        self.ttl_seconds = ttl_seconds
        self._checkpoints: dict[str, Checkpoint] = {}
        self._lock = asyncio.Lock()
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize the checkpoint manager."""
        if self._initialized:
            return

        path = Path(self.storage_path)
        path.mkdir(parents=True, exist_ok=True)
        self._initialized = True

        await self._load_existing()

    async def _load_existing(self) -> None:
        """Load existing checkpoints from storage."""
        path = Path(self.storage_path)

        if not path.exists():
            return

        for file_path in path.glob("*.json"):
            try:
                async with aiofiles.open(file_path, "r") as f:
                    content = await f.read()
                    data = json.loads(content)
                    checkpoint = Checkpoint.from_dict(data)
                    self._checkpoints[checkpoint.checkpoint_id] = checkpoint
            except Exception:
                pass

    async def create(
        self,
        workflow_name: str,
        step_name: str,
        state: dict[str, Any],
        parent_id: Optional[str] = None,
        **metadata: Any,
    ) -> Checkpoint:
        """Create a new checkpoint.

        Args:
            workflow_name: Name of the workflow
            step_name: Current step name
            state: State to save
            parent_id: Optional parent checkpoint ID
            **metadata: Additional metadata

        Returns:
            Created Checkpoint
        """
        async with self._lock:
            await self.initialize()

            checkpoint_id = str(uuid.uuid4())

            version = 1
            if parent_id and parent_id in self._checkpoints:
                version = self._checkpoints[parent_id].version + 1

            checkpoint = Checkpoint(
                checkpoint_id=checkpoint_id,
                workflow_name=workflow_name,
                step_name=step_name,
                state=state,
                status=CheckpointStatus.ACTIVE,
                version=version,
                parent_id=parent_id,
                metadata=metadata,
            )

            self._checkpoints[checkpoint_id] = checkpoint

            await self._persist(checkpoint)
            await self._cleanup_old()

            return checkpoint

    async def commit(self, checkpoint_id: str) -> bool:
        """Commit a checkpoint, making it permanent.

        Args:
            checkpoint_id: Checkpoint to commit

        Returns:
            True if committed successfully
        """
        async with self._lock:
            checkpoint = self._checkpoints.get(checkpoint_id)
            if not checkpoint:
                return False

            checkpoint.status = CheckpointStatus.COMMITTED
            checkpoint.committed_at = time.time()

            await self._persist(checkpoint)
            return True

    async def rollback(self, checkpoint_id: str) -> Optional[dict[str, Any]]:
        """Rollback to a checkpoint.

        Args:
            checkpoint_id: Checkpoint to rollback to

        Returns:
            State at checkpoint or None
        """
        async with self._lock:
            checkpoint = self._checkpoints.get(checkpoint_id)
            if not checkpoint:
                return None

            checkpoint.status = CheckpointStatus.ROLLED_BACK

            rollback_checkpoint = Checkpoint(
                checkpoint_id=str(uuid.uuid4()),
                workflow_name=checkpoint.workflow_name,
                step_name=f"rollback_to_{checkpoint.step_name}",
                state=checkpoint.state.copy(),
                status=CheckpointStatus.ACTIVE,
                version=checkpoint.version + 1,
                parent_id=checkpoint_id,
                metadata={"rolled_back_from": checkpoint_id},
            )

            self._checkpoints[rollback_checkpoint.checkpoint_id] = rollback_checkpoint
            await self._persist(rollback_checkpoint)

            return checkpoint.state

    async def get(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get a checkpoint by ID.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            Checkpoint or None
        """
        async with self._lock:
            return self._checkpoints.get(checkpoint_id)

    async def get_latest(
        self,
        workflow_name: str,
        status: Optional[CheckpointStatus] = None,
    ) -> Optional[Checkpoint]:
        """Get latest checkpoint for a workflow.

        Args:
            workflow_name: Workflow name
            status: Optional status filter

        Returns:
            Latest Checkpoint or None
        """
        async with self._lock:
            checkpoints = [
                c for c in self._checkpoints.values()
                if c.workflow_name == workflow_name
                and (status is None or c.status == status)
            ]

            if not checkpoints:
                return None

            return max(checkpoints, key=lambda c: c.created_at)

    async def get_history(
        self,
        workflow_name: str,
        limit: int = 10,
    ) -> list[Checkpoint]:
        """Get checkpoint history for a workflow.

        Args:
            workflow_name: Workflow name
            limit: Maximum number to return

        Returns:
            List of checkpoints
        """
        async with self._lock:
            checkpoints = [
                c for c in self._checkpoints.values()
                if c.workflow_name == workflow_name
            ]

            checkpoints.sort(key=lambda c: c.created_at, reverse=True)
            return checkpoints[:limit]

    async def delete(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint.

        Args:
            checkpoint_id: Checkpoint to delete

        Returns:
            True if deleted
        """
        async with self._lock:
            if checkpoint_id not in self._checkpoints:
                return False

            checkpoint = self._checkpoints[checkpoint_id]
            del self._checkpoints[checkpoint_id]

            file_path = Path(self.storage_path) / f"{checkpoint_id}.json"
            if file_path.exists():
                file_path.unlink()

            return True

    async def _persist(self, checkpoint: Checkpoint) -> None:
        """Persist checkpoint to storage."""
        file_path = Path(self.storage_path) / f"{checkpoint.checkpoint_id}.json"

        async with aiofiles.open(file_path, "w") as f:
            await f.write(json.dumps(checkpoint.to_dict(), indent=2))

    async def _cleanup_old(self) -> None:
        """Cleanup old checkpoints exceeding max."""
        if len(self._checkpoints) <= self.max_checkpoints:
            return

        sorted_checkpoints = sorted(
            self._checkpoints.items(),
            key=lambda x: x[1].created_at,
        )

        to_remove = len(self._checkpoints) - self.max_checkpoints

        for checkpoint_id, checkpoint in sorted_checkpoints[:to_remove]:
            if checkpoint.status == CheckpointStatus.ACTIVE:
                continue

            await self.delete(checkpoint_id)

    async def expire_old(self, max_age_seconds: float) -> int:
        """Expire checkpoints older than max age.

        Args:
            max_age_seconds: Maximum age in seconds

        Returns:
            Number of checkpoints expired
        """
        async with self._lock:
            now = time.time()
            expired = []

            for checkpoint_id, checkpoint in self._checkpoints.items():
                if now - checkpoint.created_at > max_age_seconds:
                    if checkpoint.status != CheckpointStatus.ACTIVE:
                        expired.append(checkpoint_id)

            for checkpoint_id in expired:
                await self.delete(checkpoint_id)

            return len(expired)


class CheckpointedWorkflow:
    """Workflow with checkpoint support."""

    def __init__(
        self,
        name: str,
        manager: CheckpointManager,
        auto_checkpoint: bool = True,
    ):
        self.name = name
        self.manager = manager
        self.auto_checkpoint = auto_checkpoint
        self._current_checkpoint_id: Optional[str] = None

    async def run(
        self,
        steps: list[Callable],
        initial_state: dict[str, Any],
        rollback_on_failure: bool = True,
    ) -> tuple[bool, dict[str, Any]]:
        """Run workflow with checkpoint support.

        Args:
            steps: List of step functions
            initial_state: Initial workflow state
            rollback_on_failure: Whether to rollback on failure

        Returns:
            Tuple of (success, final_state)
        """
        state = initial_state.copy()
        self._current_checkpoint_id = None

        for step in steps:
            try:
                checkpoint = await self.manager.create(
                    workflow_name=self.name,
                    step_name=step.__name__,
                    state=state,
                    parent_id=self._current_checkpoint_id,
                )
                self._current_checkpoint_id = checkpoint.checkpoint_id

                if asyncio.iscoroutinefunction(step):
                    result = await step(state)
                else:
                    result = step(state)

                if isinstance(result, dict):
                    state.update(result)

            except Exception as e:
                if rollback_on_failure and self._current_checkpoint_id:
                    rolled_back_state = await self.manager.rollback(
                        self._current_checkpoint_id
                    )
                    if rolled_back_state:
                        state = rolled_back_state
                    return False, state
                raise e

        await self.manager.commit(self._current_checkpoint_id or "")
        return True, state
