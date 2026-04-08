"""
UI State Differ.

Compare two UI states (accessibility trees) and compute the differences
between them — useful for detecting what changed after an action.

Usage:
    from utils.ui_state_differ import StateDiffer, DiffResult

    before = bridge.build_accessibility_tree(app)
    # ... perform action ...
    after = bridge.build_accessibility_tree(app)

    differ = StateDiffer()
    result = differ.compare(before, after)
    print(f"Added: {result.added}, Removed: {result.removed}")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Set, Tuple, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto

if TYPE_CHECKING:
    pass


class DiffType(Enum):
    """Type of difference detected."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    MOVED = auto()
    REORDERED = auto()


@dataclass
class DiffEntry:
    """A single difference entry."""
    diff_type: DiffType
    path: str
    before: Optional[Any] = None
    after: Optional[Any] = None
    element_role: Optional[str] = None
    element_title: Optional[str] = None

    def __repr__(self) -> str:
        return f"DiffEntry({self.diff_type.name}, {self.path!r})"


@dataclass
class DiffResult:
    """Result of comparing two UI states."""
    added: List[DiffEntry] = field(default_factory=list)
    removed: List[DiffEntry] = field(default_factory=list)
    modified: List[DiffEntry] = field(default_factory=list)
    moved: List[DiffEntry] = field(default_factory=list)
    reordered: List[DiffEntry] = field(default_factory=list)

    @property
    def total_changes(self) -> int:
        """Total number of differences."""
        return (
            len(self.added) + len(self.removed) +
            len(self.modified) + len(self.moved) +
            len(self.reordered)
        )

    @property
    def has_changes(self) -> bool:
        """Return True if any differences were found."""
        return self.total_changes > 0

    def summary(self) -> str:
        """Return a human-readable summary."""
        parts = []
        if self.added:
            parts.append(f"+{len(self.added)} added")
        if self.removed:
            parts.append(f"-{len(self.removed)} removed")
        if self.modified:
            parts.append(f"~{len(self.modified)} modified")
        if self.moved:
            parts.append(f">{len(self.moved)} moved")
        if self.reordered:
            parts.append(f"#{len(self.reordered)} reordered")
        return f"DiffResult({', '.join(parts)}) if parts else 'No changes'"


class StateDiffer:
    """
    Compare two UI states and compute differences.

    Supports deep comparison of accessibility trees with awareness
    of added/removed/modified elements and reordering.

    Example:
        differ = StateDiffer()
        result = differ.compare(state_a, state_b)
        for diff in result.modified:
            print(f"Changed: {diff.path}")
    """

    def __init__(
        self,
        compare_values: bool = True,
        compare_order: bool = False,
        key_fields: Optional[List[str]] = None,
    ) -> None:
        """
        Initialize the differ.

        Args:
            compare_values: Whether to compare element values.
            compare_order: Whether to consider child order significant.
            key_fields: Fields to use as identity keys (default: role+title).
        """
        self._compare_values = compare_values
        self._compare_order = compare_order
        self._key_fields = key_fields or ["role", "title"]

    def compare(
        self,
        before: Dict[str, Any],
        after: Dict[str, Any],
    ) -> DiffResult:
        """
        Compare two UI states.

        Args:
            before: The "before" state dictionary.
            after: The "after" state dictionary.

        Returns:
            DiffResult with all detected differences.
        """
        result = DiffResult()

        before_index = self._build_index(before, "", set())
        after_index = self._build_index(after, "", set())

        all_paths = set(before_index.keys()) | set(after_index.keys())

        for path in all_paths:
            in_before = path in before_index
            in_after = path in after_index

            if in_before and not in_after:
                entry = DiffEntry(
                    diff_type=DiffType.REMOVED,
                    path=path,
                    before=before_index[path],
                    element_role=before_index[path].get("role") if before_index[path] else None,
                    element_title=before_index[path].get("title") if before_index[path] else None,
                )
                result.removed.append(entry)
            elif not in_before and in_after:
                entry = DiffEntry(
                    diff_type=DiffType.ADDED,
                    path=path,
                    after=after_index[path],
                    element_role=after_index[path].get("role") if after_index[path] else None,
                    element_title=after_index[path].get("title") if after_index[path] else None,
                )
                result.added.append(entry)
            elif in_before and in_after:
                diff = self._compare_elements(
                    before_index[path],
                    after_index[path],
                    path,
                )
                if diff:
                    result.modified.append(diff)

        if self._compare_order:
            self._detect_reorders(before, after, result)

        return result

    def _build_index(
        self,
        node: Dict[str, Any],
        path: str,
        visited: Set[int],
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Build a flat index of all elements by path."""
        index: Dict[str, Optional[Dict[str, Any]]] = {}

        node_id = id(node)
        if node_id in visited:
            return index
        visited.add(node_id)

        if path:
            index[path] = node

        for i, child in enumerate(node.get("children", [])):
            if isinstance(child, dict):
                child_path = f"{path}/{child.get('role', 'unknown')}[{i}]"
                index.update(self._build_index(child, child_path, visited))

        return index

    def _compare_elements(
        self,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        path: str,
    ) -> Optional[DiffEntry]:
        """Compare two element dictionaries for differences."""
        if before is None or after is None:
            return None

        important_fields = ["value", "title", "enabled", "selected", "focused"]
        if not self._compare_values:
            important_fields = ["enabled", "selected", "focused"]

        for field_name in important_fields:
            before_val = before.get(field_name)
            after_val = after.get(field_name)
            if before_val != after_val:
                return DiffEntry(
                    diff_type=DiffType.MODIFIED,
                    path=path,
                    before=before_val,
                    after=after_val,
                    element_role=after.get("role"),
                    element_title=after.get("title"),
                )

        return None

    def _detect_reorders(
        self,
        before: Dict[str, Any],
        after: Dict[str, Any],
        result: DiffResult,
    ) -> None:
        """Detect reordered children."""
        before_children = before.get("children", [])
        after_children = after.get("children", [])

        if len(before_children) != len(after_children):
            return

        for i, (bc, ac) in enumerate(zip(before_children, after_children)):
            if not isinstance(bc, dict) or not isinstance(ac, dict):
                continue

            bc_key = self._element_key(bc)
            ac_key = self._element_key(ac)

            if bc_key == ac_key and bc != ac:
                result.reordered.append(DiffEntry(
                    diff_type=DiffType.REORDERED,
                    path=f"{i}",
                    before=bc,
                    after=ac,
                    element_role=ac.get("role"),
                    element_title=ac.get("title"),
                ))

    def _element_key(self, element: Dict[str, Any]) -> str:
        """Generate a key for element identity matching."""
        parts = [element.get(field, "") or "" for field in self._key_fields]
        return "|".join(parts)


def quick_diff(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> DiffResult:
    """
    Quick diff between two states using default options.

    Args:
        before: Before state dictionary.
        after: After state dictionary.

    Returns:
        DiffResult with all differences.
    """
    differ = StateDiffer()
    return differ.compare(before, after)
