"""Automation Recovery and Rollback System.

This module provides automation recovery capabilities:
- Automatic retry with backoff
- Checkpoint-based recovery
- State snapshots
- Rollback orchestration

Example:
    >>> from actions.automation_recovery_action import RecoveryManager
    >>> manager = RecoveryManager()
    >>> manager.save_checkpoint("deploy_v2", state)
    >>> recovered = manager.restore_checkpoint("deploy_v2")
"""

from __future__ import annotations

import json
import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class RecoveryStatus(Enum):
    """Recovery status."""
    IDLE = "idle"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class Checkpoint:
    """A saved state checkpoint."""
    checkpoint_id: str
    name: str
    state: dict[str, Any]
    created_at: float
    version: int
    description: str = ""


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    success: bool
    status: RecoveryStatus
    recovered_state: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    duration_ms: float = 0.0


class RecoveryManager:
    """Manages automation recovery and rollback."""

    def __init__(
        self,
        max_checkpoints: int = 100,
        checkpoint_ttl: float = 86400 * 7,
    ) -> None:
        """Initialize the recovery manager.

        Args:
            max_checkpoints: Maximum checkpoints to retain per name.
            checkpoint_ttl: Checkpoint TTL in seconds.
        """
        self._checkpoints: dict[str, list[Checkpoint]] = {}
        self._max_checkpoints = max_checkpoints
        self._checkpoint_ttl = checkpoint_ttl
        self._lock = threading.RLock()
        self._stats = {"checkpoints_saved": 0, "recoveries": 0, "rollbacks": 0}

    def save_checkpoint(
        self,
        name: str,
        state: dict[str, Any],
        description: str = "",
        checkpoint_id: Optional[str] = None,
    ) -> Checkpoint:
        """Save a state checkpoint.

        Args:
            name: Checkpoint name (e.g., workflow name).
            state: State dict to save.
            description: Checkpoint description.
            checkpoint_id: Custom ID. None = auto-generate.

        Returns:
            The created Checkpoint.
        """
        import uuid
        cid = checkpoint_id or str(uuid.uuid4())[:8]

        with self._lock:
            if name not in self._checkpoints:
                self._checkpoints[name] = []

            existing = self._checkpoints[name]
            version = (existing[-1].version + 1) if existing else 1

            checkpoint = Checkpoint(
                checkpoint_id=cid,
                name=name,
                state=state,
                created_at=time.time(),
                version=version,
                description=description,
            )

            self._checkpoints[name].append(checkpoint)
            self._stats["checkpoints_saved"] += 1

            if len(self._checkpoints[name]) > self._max_checkpoints:
                self._checkpoints[name] = self._checkpoints[name][-self._max_checkpoints:]

            logger.info("Saved checkpoint %s (v%d) for %s", cid, version, name)

        return checkpoint

    def restore_checkpoint(
        self,
        name: str,
        version: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Restore a checkpoint.

        Args:
            name: Checkpoint name.
            version: Specific version. None = latest.

        Returns:
            State dict if found, None otherwise.
        """
        with self._lock:
            checkpoints = self._checkpoints.get(name, [])
            if not checkpoints:
                return None

            if version is not None:
                for cp in reversed(checkpoints):
                    if cp.version == version:
                        self._stats["recoveries"] += 1
                        return cp.state
                return None
            else:
                self._stats["recoveries"] += 1
                return checkpoints[-1].state

    def list_checkpoints(
        self,
        name: Optional[str] = None,
    ) -> list[Checkpoint]:
        """List checkpoints.

        Args:
            name: Filter by name. None = all.

        Returns:
            List of Checkpoints.
        """
        with self._lock:
            if name:
                return list(self._checkpoints.get(name, []))
            all_cp = []
            for cps in self._checkpoints.values():
                all_cp.extend(cps)
            return sorted(all_cp, key=lambda c: c.created_at, reverse=True)

    def delete_checkpoint(
        self,
        name: str,
        version: Optional[int] = None,
    ) -> bool:
        """Delete checkpoints.

        Args:
            name: Checkpoint name.
            version: Specific version. None = all for name.

        Returns:
            True if deleted.
        """
        with self._lock:
            if name not in self._checkpoints:
                return False

            if version is None:
                del self._checkpoints[name]
                return True

            self._checkpoints[name] = [
                cp for cp in self._checkpoints[name] if cp.version != version
            ]
            return True

    def execute_with_recovery(
        self,
        operation: Callable[[], dict[str, Any]],
        name: str,
        rollback: Optional[Callable[[dict], None]] = None,
        max_retries: int = 3,
    ) -> RecoveryResult:
        """Execute an operation with automatic recovery.

        Args:
            operation: Operation to execute.
            name: Recovery checkpoint name.
            rollback: Rollback function if operation fails.
            max_retries: Maximum retry attempts.

        Returns:
            RecoveryResult.
        """
        start = time.time()
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                result = operation()
                duration_ms = (time.time() - start) * 1000
                self.save_checkpoint(name, result, description=f"attempt_{attempt}")
                return RecoveryResult(
                    success=True,
                    status=RecoveryStatus.SUCCESS,
                    recovered_state=result,
                    duration_ms=duration_ms,
                )
            except Exception as e:
                last_error = str(e)
                logger.error("Operation %s failed (attempt %d): %s", name, attempt + 1, e)
                if attempt < max_retries:
                    recovered_state = self.restore_checkpoint(name)
                    if recovered_state:
                        logger.info("Restored checkpoint for retry %d", attempt + 1)

        if rollback:
            state = self.restore_checkpoint(name)
            if state:
                try:
                    rollback(state)
                    self._stats["rollbacks"] += 1
                except Exception as e:
                    logger.error("Rollback failed: %s", e)

        duration_ms = (time.time() - start) * 1000
        return RecoveryResult(
            success=False,
            status=RecoveryStatus.FAILED,
            error=last_error,
            duration_ms=duration_ms,
        )

    def cleanup_expired(self) -> int:
        """Remove expired checkpoints.

        Returns:
            Number of checkpoints removed.
        """
        now = time.time()
        removed = 0

        with self._lock:
            for name in list(self._checkpoints.keys()):
                before = len(self._checkpoints[name])
                self._checkpoints[name] = [
                    cp for cp in self._checkpoints[name]
                    if (now - cp.created_at) < self._checkpoint_ttl
                ]
                removed += before - len(self._checkpoints[name])
                if not self._checkpoints[name]:
                    del self._checkpoints[name]

        if removed:
            logger.info("Cleaned up %d expired checkpoints", removed)
        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get recovery statistics."""
        with self._lock:
            return {
                **self._stats,
                "total_checkpoint_names": len(self._checkpoints),
                "total_checkpoints": sum(len(cps) for cps in self._checkpoints.values()),
            }
