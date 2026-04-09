"""UI Snapshot Comparator Utilities.

Compares UI snapshots to detect changes and regressions.

Example:
    >>> from ui_snapshot_comparator_utils import SnapshotComparator
    >>> comparator = SnapshotComparator()
    >>> diff = comparator.compare(snapshot_a, snapshot_b)
    >>> print(diff.changed_elements)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class ElementDiff:
    """Difference for a single element."""
    path: str
    old_value: Any
    new_value: Any
    diff_type: str


@dataclass
class SnapshotDiff:
    """Result of comparing two snapshots."""
    equal: bool
    changed_elements: List[ElementDiff] = field(default_factory=list)
    added_paths: List[str] = field(default_factory=list)
    removed_paths: List[str] = field(default_factory=list)
    similarity_score: float = 1.0

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return bool(self.added_paths or self.removed_paths or self.changed_elements)


class SnapshotComparator:
    """Compares UI snapshots for change detection."""

    def __init__(self, tolerance: float = 0.0):
        """Initialize comparator.

        Args:
            tolerance: Numeric tolerance for comparisons.
        """
        self.tolerance = tolerance

    def compare(
        self,
        old_snapshot: Dict[str, Any],
        new_snapshot: Dict[str, Any],
    ) -> SnapshotDiff:
        """Compare two snapshots.

        Args:
            old_snapshot: Previous snapshot.
            new_snapshot: Current snapshot.

        Returns:
            SnapshotDiff with change details.
        """
        added: Set[str] = set()
        removed: Set[str] = set()
        changed: List[ElementDiff] = []

        all_paths = set(old_snapshot.keys()) | set(new_snapshot.keys())

        for path in all_paths:
            in_old = path in old_snapshot
            in_new = path in new_snapshot

            if in_old and not in_new:
                removed.add(path)
            elif not in_old and in_new:
                added.add(path)
            else:
                old_val = old_snapshot[path]
                new_val = new_snapshot[path]
                if not self._values_equal(old_val, new_val):
                    changed.append(ElementDiff(path, old_val, new_val, "modified"))

        total = max(len(all_paths), 1)
        unchanged = total - len(added) - len(removed) - len(changed)
        similarity = unchanged / total

        return SnapshotDiff(
            equal=len(changed) == 0 and len(added) == 0 and len(removed) == 0,
            changed_elements=changed,
            added_paths=list(added),
            removed_paths=list(removed),
            similarity_score=similarity,
        )

    def _values_equal(self, a: Any, b: Any) -> bool:
        """Check if two values are equal within tolerance."""
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return abs(a - b) <= self.tolerance
        return a == b

    def compute_change_summary(self, diff: SnapshotDiff) -> str:
        """Generate a human-readable change summary.

        Args:
            diff: SnapshotDiff to summarize.

        Returns:
            Summary string.
        """
        parts = []
        if diff.added_paths:
            parts.append(f"Added {len(diff.added_paths)} element(s)")
        if diff.removed_paths:
            parts.append(f"Removed {len(diff.removed_paths)} element(s)")
        if diff.changed_elements:
            parts.append(f"Modified {len(diff.changed_elements)} element(s)")
        if not parts:
            return "No changes detected"
        return f"Changes: {', '.join(parts)}"
