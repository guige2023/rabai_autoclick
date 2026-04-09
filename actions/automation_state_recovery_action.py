"""Automation State Recovery Action.

Provides fine-grained state recovery with snapshots,
incremental checkpoints, state diffing, and version rollback.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class RecoveryMode(Enum):
    """State recovery modes."""
    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"
    CHECKPOINT = "checkpoint"


@dataclass
class StateSnapshot:
    """A complete state snapshot."""
    version: str
    state: Dict[str, Any]
    checksum: str
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateCheckpoint:
    """An incremental checkpoint."""
    version: str
    base_version: str
    diff: Dict[str, Any]
    created_at: float
    applied: bool = False


@dataclass
class StateVersion:
    """A versioned state entry."""
    version: str
    timestamp: float
    state: Dict[str, Any]
    snapshot: Optional[StateSnapshot] = None
    checkpoint: Optional[StateCheckpoint] = None


@dataclass
class StateDiff:
    """Difference between two state versions."""
    added: Dict[str, Any] = field(default_factory=dict)
    removed: Dict[str, Any] = field(default_factory=dict)
    modified: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)
    unchanged: List[str] = field(default_factory=list)


class AutomationStateRecoveryAction:
    """Manages state snapshots, checkpoints, and recovery."""

    def __init__(
        self,
        max_versions: int = 50,
        recovery_mode: RecoveryMode = RecoveryMode.SNAPSHOT,
    ) -> None:
        self.max_versions = max_versions
        self.recovery_mode = recovery_mode
        self._versions: deque = deque(maxlen=max_versions)
        self._snapshots: Dict[str, StateSnapshot] = {}
        self._current_version: Optional[str] = None
        self._current_state: Dict[str, Any] = {}
        self._version_counter = 0

    def save_snapshot(
        self,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StateSnapshot:
        """Save a complete state snapshot."""
        self._version_counter += 1
        version = f"v{self._version_counter}_{int(time.time())}"

        state_json = json.dumps(state, sort_keys=True, default=str)
        checksum = hashlib.sha256(state_json.encode()).hexdigest()

        snapshot = StateSnapshot(
            version=version,
            state=dict(state),
            checksum=checksum,
            created_at=time.time(),
            metadata=metadata or {},
        )

        self._snapshots[version] = snapshot

        versioned_state = StateVersion(
            version=version,
            timestamp=time.time(),
            state=dict(state),
            snapshot=snapshot,
        )

        self._versions.append(versioned_state)
        self._current_version = version
        self._current_state = dict(state)

        return snapshot

    def save_checkpoint(
        self,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StateCheckpoint:
        """Save an incremental checkpoint."""
        self._version_counter += 1
        version = f"v{self._version_counter}_{int(time.time())}"

        base = self._current_version or ""
        diff = self._compute_diff(self._current_state, state)

        checkpoint = StateCheckpoint(
            version=version,
            base_version=base,
            diff=diff,
            created_at=time.time(),
        )

        versioned_state = StateVersion(
            version=version,
            timestamp=time.time(),
            state=dict(state),
            checkpoint=checkpoint,
        )

        self._versions.append(versioned_state)
        self._current_version = version
        self._current_state = dict(state)

        return checkpoint

    def save(
        self,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Save state using configured recovery mode."""
        if self.recovery_mode == RecoveryMode.SNAPSHOT:
            return self.save_snapshot(state, metadata)
        else:
            return self.save_checkpoint(state, metadata)

    def recover_to_version(
        self,
        version: str,
        validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Recover to a specific version."""
        versioned_state = self._find_version(version)
        if versioned_state is None:
            return None

        if versioned_state.snapshot:
            recovered_state = dict(versioned_state.state)
        elif versioned_state.checkpoint:
            recovered_state = self._reconstruct_from_checkpoint(versioned_state.checkpoint)
        else:
            return None

        if validator and not validator(recovered_state):
            return None

        self._current_version = version
        self._current_state = recovered_state

        return recovered_state

    def recover_to_latest(
        self,
        validator: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Recover to the latest version."""
        if not self._versions:
            return None
        latest = self._versions[-1]
        return self.recover_to_version(latest.version, validator)

    def rollback(self, steps: int = 1) -> Optional[Dict[str, Any]]:
        """Rollback N versions from current."""
        if not self._versions:
            return None

        current_idx = None
        for i, v in enumerate(self._versions):
            if v.version == self._current_version:
                current_idx = i
                break

        if current_idx is None:
            return None

        target_idx = max(0, current_idx - steps)
        target_version = self._versions[target_idx]

        return self.recover_to_version(target_version.version)

    def diff_versions(
        self,
        version_a: str,
        version_b: str,
    ) -> Optional[StateDiff]:
        """Compute the diff between two versions."""
        versioned_a = self._find_version(version_a)
        versioned_b = self._find_version(version_b)

        if versioned_a is None or versioned_b is None:
            return None

        state_a = self._reconstruct_state(versioned_a)
        state_b = self._reconstruct_state(versioned_b)

        return self._compute_diff(state_a, state_b)

    def get_version_history(
        self,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Get version history summary."""
        history = []
        for v in list(self._versions)[-limit:]:
            entry = {
                "version": v.version,
                "timestamp": v.timestamp,
                "is_snapshot": v.snapshot is not None,
            }
            history.append(entry)
        return history

    def verify_checksum(self, version: str) -> bool:
        """Verify the checksum of a version."""
        versioned_state = self._find_version(version)
        if versioned_state is None or versioned_state.snapshot is None:
            return True

        snapshot = versioned_state.snapshot
        state_json = json.dumps(versioned_state.state, sort_keys=True, default=str)
        checksum = hashlib.sha256(state_json.encode()).hexdigest()

        return checksum == snapshot.checksum

    def _find_version(self, version: str) -> Optional[StateVersion]:
        """Find a version by version string."""
        for v in self._versions:
            if v.version == version:
                return v
        return None

    def _compute_diff(
        self,
        old_state: Dict[str, Any],
        new_state: Dict[str, Any],
    ) -> StateDiff:
        """Compute the difference between two states."""
        diff = StateDiff()

        all_keys = set(old_state.keys()) | set(new_state.keys())

        for key in all_keys:
            old_val = old_state.get(key)
            new_val = new_state.get(key)

            if key not in old_state:
                diff.added[key] = new_val
            elif key not in new_state:
                diff.removed[key] = old_val
            elif old_val != new_val:
                diff.modified[key] = (old_val, new_val)
            else:
                diff.unchanged.append(key)

        return diff

    def _reconstruct_state(self, versioned_state: StateVersion) -> Dict[str, Any]:
        """Reconstruct state from a versioned entry."""
        if versioned_state.snapshot:
            return dict(versioned_state.snapshot.state)
        elif versioned_state.checkpoint:
            return self._reconstruct_from_checkpoint(versioned_state.checkpoint)
        return dict(versioned_state.state)

    def _reconstruct_from_checkpoint(
        self,
        checkpoint: StateCheckpoint,
    ) -> Dict[str, Any]:
        """Reconstruct state from a checkpoint chain."""
        state: Dict[str, Any] = {}

        if checkpoint.base_version:
            base_versioned = self._find_version(checkpoint.base_version)
            if base_versioned:
                state = self._reconstruct_state(base_versioned)

        for key, value in checkpoint.diff.get("added", {}).items():
            state[key] = value

        for key in checkpoint.diff.get("removed", {}).keys():
            if key in state:
                del state[key]

        for key, (_, new_val) in checkpoint.diff.get("modified", {}).items():
            state[key] = new_val

        return state

    def get_current_state(self) -> Dict[str, Any]:
        """Get the current state."""
        return dict(self._current_state)

    def get_current_version(self) -> Optional[str]:
        """Get the current version string."""
        return self._current_version
