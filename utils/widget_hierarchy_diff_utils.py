"""
Widget hierarchy diff utilities.

This module provides utilities for comparing widget/accessibility hierarchies
and generating structured diffs between two UI states.
"""

from __future__ import annotations

from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class DiffOperation(Enum):
    """Type of hierarchy difference."""
    UNCHANGED = auto()
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    MOVED = auto()
    REORDERED = auto()


@dataclass
class WidgetNode:
    """Represents a widget/accessibility node."""
    ax_id: Optional[str] = None
    ax_type: Optional[str] = None
    ax_role: Optional[str] = None
    ax_label: Optional[str] = None
    ax_value: Optional[str] = None
    ax_enabled: bool = True
    ax_focused: bool = False
    children: List[WidgetNode] = field(default_factory=list)
    depth: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary."""
        return {
            "ax_id": self.ax_id,
            "ax_type": self.ax_type,
            "ax_role": self.ax_role,
            "ax_label": self.ax_label,
            "ax_value": self.ax_value,
            "ax_enabled": self.ax_enabled,
            "ax_focused": self.ax_focused,
            "children": [c.to_dict() for c in self.children],
            "depth": self.depth,
        }


@dataclass
class HierarchyDiff:
    """Result of comparing two widget hierarchies."""
    operation: DiffOperation = DiffOperation.UNCHANGED
    path: str = ""
    old_node: Optional[WidgetNode] = None
    new_node: Optional[WidgetNode] = None
    depth: int = 0
    children_diffs: List[HierarchyDiff] = field(default_factory=list)

    def is_changed(self) -> bool:
        """Return True if this diff represents a change."""
        return self.operation != DiffOperation.UNCHANGED


@dataclass
class HierarchyChangeSummary:
    """Summary of all changes between two hierarchies."""
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0
    moved_count: int = 0
    reordered_count: int = 0
    diffs: List[HierarchyDiff] = field(default_factory=list)


def node_key(node: WidgetNode) -> str:
    """Generate a stable key for a node for comparison."""
    parts = [
        node.ax_id or "",
        node.ax_role or "",
        node.ax_label or "",
    ]
    return "|".join(parts)


def diff_nodes(old: Optional[WidgetNode], new: Optional[WidgetNode], path: str = "") -> HierarchyDiff:
    """
    Recursively diff two widget nodes.

    Args:
        old: Old hierarchy node (or None if added).
        new: New hierarchy node (or None if removed).
        path: Current path in hierarchy for reporting.

    Returns:
        HierarchyDiff describing the differences.
    """
    if old is None and new is None:
        return HierarchyDiff(operation=DiffOperation.UNCHANGED, path=path)

    if old is None and new is not None:
        return HierarchyDiff(
            operation=DiffOperation.ADDED,
            path=path,
            new_node=new,
            depth=new.depth,
        )

    if old is not None and new is None:
        return HierarchyDiff(
            operation=DiffOperation.REMOVED,
            path=path,
            old_node=old,
            depth=old.depth,
        )

    # Both exist - check for modifications
    old = old!
    new = new!

    # Check if properties changed
    changed = (
        old.ax_id != new.ax_id
        or old.ax_role != new.ax_role
        or old.ax_label != new.ax_label
        or old.ax_value != new.ax_value
        or old.ax_enabled != new.ax_enabled
        or old.ax_focused != new.ax_focused
    )

    # Build child maps
    old_children_by_key: Dict[str, WidgetNode] = {node_key(c): c for c in old.children}
    new_children_by_key: Dict[str, WidgetNode] = {node_key(c): c for c in new.children}

    old_keys = set(old_children_by_key.keys())
    new_keys = set(new_children_by_key.keys())

    added_keys = new_keys - old_keys
    removed_keys = old_keys - new_keys
    common_keys = old_keys & new_keys

    child_diffs: List[HierarchyDiff] = []

    # Process added children
    for key in added_keys:
        child_diffs.append(HierarchyDiff(
            operation=DiffOperation.ADDED,
            path=f"{path}/{key}",
            new_node=new_children_by_key[key],
            depth=new_children_by_key[key].depth,
        ))

    # Process removed children
    for key in removed_keys:
        child_diffs.append(HierarchyDiff(
            operation=DiffOperation.REMOVED,
            path=f"{path}/{key}",
            old_node=old_children_by_key[key],
            depth=old_children_by_key[key].depth,
        ))

    # Process common children
    for key in common_keys:
        child_diff = diff_nodes(
            old_children_by_key[key],
            new_children_by_key[key],
            f"{path}/{key}",
        )
        child_diffs.append(child_diff)

    # Detect reordering among common children
    old_order = [c for c in old.children if node_key(c) in common_keys]
    new_order = [c for c in new.children if node_key(c) in common_keys]
    if [node_key(c) for c in old_order] != [node_key(c) for c in new_order]:
        child_diffs.append(HierarchyDiff(
            operation=DiffOperation.REORDERED,
            path=path,
            old_node=old,
            new_node=new,
            depth=old.depth,
        ))

    if changed:
        return HierarchyDiff(
            operation=DiffOperation.MODIFIED,
            path=path,
            old_node=old,
            new_node=new,
            depth=old.depth,
            children_diffs=child_diffs,
        )

    return HierarchyDiff(
        operation=DiffOperation.UNCHANGED if not child_diffs else DiffOperation.MODIFIED,
        path=path,
        old_node=old,
        new_node=new,
        depth=old.depth,
        children_diffs=child_diffs,
    )


def summarize_diff(diff: HierarchyDiff) -> HierarchyChangeSummary:
    """
    Summarize all changes from a hierarchy diff.

    Args:
        diff: Root of the diff tree.

    Returns:
        HierarchyChangeSummary with counts.
    """
    summary = HierarchyChangeSummary()

    def walk(d: HierarchyDiff):
        if d.operation == DiffOperation.ADDED:
            summary.added_count += 1
        elif d.operation == DiffOperation.REMOVED:
            summary.removed_count += 1
        elif d.operation == DiffOperation.MODIFIED:
            summary.modified_count += 1
        elif d.operation == DiffOperation.MOVED:
            summary.moved_count += 1
        elif d.operation == DiffOperation.REORDERED:
            summary.reordered_count += 1

        if d.is_changed():
            summary.diffs.append(d)

        for child in d.children_diffs:
            walk(child)

    walk(diff)
    return summary


def get_changed_paths(diff: HierarchyDiff, max_depth: int = 10) -> List[Tuple[str, DiffOperation]]:
    """
    Extract all changed paths from a diff tree.

    Args:
        diff: Root of the diff tree.
        max_depth: Maximum depth to traverse.

    Returns:
        List of (path, operation) tuples.
    """
    result: List[Tuple[str, DiffOperation]] = []
    if diff.depth > max_depth:
        return result

    if diff.is_changed():
        result.append((diff.path, diff.operation))

    for child in diff.children_diffs:
        result.extend(get_changed_paths(child, max_depth))

    return result
