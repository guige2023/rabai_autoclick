"""
Automation Diff Action Module

Performs diff-based automation by comparing UI states
before and after actions and executing compensation on mismatch.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple

import logging

logger = logging.getLogger(__name__)


class DiffStrategy(Enum):
    """Strategy for handling UI state differences."""

    STRICT = auto()
    LENIENT = auto()
    STRUCTURE_ONLY = auto()
    CONTENT_ONLY = auto()


class DiffResult:
    """Result of a UI state diff."""

    def __init__(
        self,
        identical: bool,
        added_keys: Optional[List[str]] = None,
        removed_keys: Optional[List[str]] = None,
        changed_keys: Optional[List[str]] = None,
        diff_hash: Optional[str] = None,
    ) -> None:
        self.identical = identical
        self.added_keys = added_keys or []
        self.removed_keys = removed_keys or []
        self.changed_keys = changed_keys or []
        self.diff_hash = diff_hash


@dataclass
class UISnapshot:
    """Snapshot of a UI state for comparison."""

    state: Dict[str, Any]
    hash: str
    timestamp: float = field(default_factory=time.time)
    description: str = ""

    @classmethod
    def capture(
        cls,
        state: Dict[str, Any],
        description: str = "",
    ) -> UISnapshot:
        """Capture a new UI state snapshot."""
        serialized = cls._serialize(state)
        hash_val = hashlib.sha256(serialized.encode()).hexdigest()[:16]
        return cls(state=state, hash=hash_val, description=description)

    @staticmethod
    def _serialize(state: Dict[str, Any]) -> str:
        """Serialize state dict to deterministic string."""
        items = sorted(state.items())
        parts = []
        for k, v in items:
            if isinstance(v, dict):
                v = UISnapshot._serialize(v)
            elif isinstance(v, list):
                v = str(v)
            parts.append(f"{k}:{v}")
        return "|".join(parts)


@dataclass
class DiffConfig:
    """Configuration for diff automation."""

    strategy: DiffStrategy = DiffStrategy.STRICT
    ignored_keys: List[str] = field(default_factory=list)
    tolerance: float = 0.0
    max_drift_percent: float = 10.0
    retry_on_diff: bool = True
    max_retries: int = 3


class UISnapshotDiffer:
    """Compares UI snapshots and computes differences."""

    def __init__(self, config: Optional[DiffConfig] = None) -> None:
        self.config = config or DiffConfig()

    def compute_diff(
        self,
        before: UISnapshot,
        after: UISnapshot,
    ) -> DiffResult:
        """Compute diff between two UI snapshots."""
        if self.config.ignored_keys:
            before_state = self._filter_keys(before.state)
            after_state = self._filter_keys(after.state)
        else:
            before_state = before.state
            after_state = after.state

        added = []
        removed = []
        changed = []

        all_keys = set(before_state.keys()) | set(after_state.keys())

        for key in all_keys:
            if key not in before_state:
                added.append(key)
            elif key not in after_state:
                removed.append(key)
            elif before_state[key] != after_state[key]:
                changed.append(key)

        identical = not (added or removed or changed)

        if self.config.strategy == DiffStrategy.CONTENT_ONLY:
            identical = True
        elif self.config.strategy == DiffStrategy.STRUCTURE_ONLY:
            added = [k for k in added if not isinstance(after_state.get(k), dict)]
            removed = [k for k in removed if not isinstance(before_state.get(k), dict)]
            changed = []
            identical = not (added or removed)

        return DiffResult(
            identical=identical,
            added_keys=added,
            removed_keys=removed,
            changed_keys=changed,
            diff_hash=after.hash,
        )

    def _filter_keys(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out ignored keys from state."""
        return {k: v for k, v in state.items() if k not in self.config.ignored_keys}


class DiffAutomationAction:
    """Action class for diff-based automation."""

    def __init__(self, config: Optional[DiffConfig] = None) -> None:
        self.config = config or DiffConfig()
        self.differ = UISnapshotDiffer(self.config)
        self._snapshots: List[UISnapshot] = []

    def capture(self, state: Dict[str, Any], description: str = "") -> UISnapshot:
        """Capture a UI state snapshot."""
        snapshot = UISnapshot.capture(state, description)
        self._snapshots.append(snapshot)
        return snapshot

    def execute_with_diff(
        self,
        action: Callable[[], Any],
        expected_state: Dict[str, Any],
        pre_state: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Any, DiffResult]:
        """Execute an action and verify result against expected state."""
        if pre_state:
            self.capture(pre_state, "pre_action")

        result = action()

        after_state = expected_state
        after_snapshot = self.capture(after_state, "post_action")

        if len(self._snapshots) >= 2:
            before_snapshot = self._snapshots[-2]
            diff = self.differ.compute_diff(before_snapshot, after_snapshot)
        else:
            diff = DiffResult(identical=True)

        if not diff.identical and self.config.retry_on_diff:
            for attempt in range(self.config.max_retries):
                logger.info(f"Diff detected, retry {attempt + 1}/{self.config.max_retries}")
                result = action()
                after_snapshot = self.capture(expected_state, f"retry_{attempt}")
                if len(self._snapshots) >= 2:
                    diff = self.differ.compute_diff(self._snapshots[-2], after_snapshot)
                if diff.identical:
                    break

        return result, diff

    def get_snapshots(self) -> List[UISnapshot]:
        """Return all captured snapshots."""
        return self._snapshots.copy()
