"""
Accessibility snapshot diff utilities.

This module provides utilities for computing and analyzing
differences between two accessibility snapshots.
"""

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum, auto


class DiffType(Enum):
    """Type of accessibility tree difference."""
    NONE = auto()
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    REORDERED = auto()


@dataclass
class AXNode:
    """An accessibility tree node."""
    role: str
    identifier: str
    label: str = ""
    value: str = ""
    enabled: bool = True
    focused: bool = False
    children: List["AXNode"] = field(default_factory=list)
    depth: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "role": self.role,
            "identifier": self.identifier,
            "label": self.label,
            "value": self.value,
            "enabled": self.enabled,
            "focused": self.focused,
            "children": [c.to_dict() for c in self.children],
            "depth": self.depth,
        }


@dataclass
class AXDiff:
    """Difference between two accessibility nodes."""
    diff_type: DiffType
    path: str
    old_node: Optional[AXNode] = None
    new_node: Optional[AXNode] = None
    property_changes: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)


@dataclass
class SnapshotDiffSummary:
    """Summary of differences between two snapshots."""
    total_changes: int = 0
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0
    reordered_count: int = 0
    changed_paths: List[str] = field(default_factory=list)
    focus_changed: bool = False
    hierarchy_changed: bool = False


def compute_node_hash(node: AXNode) -> str:
    """
    Compute a stable hash for an accessibility node.

    Args:
        node: Accessibility node.

    Returns:
        Hash string.
    """
    parts = [
        node.role,
        node.identifier,
        str(node.enabled),
    ]
    return "|".join(parts)


def compute_snapshot_fingerprint(snapshot: List[AXNode]) -> Set[str]:
    """
    Compute fingerprints for all nodes in a snapshot.

    Args:
        snapshot: List of accessibility nodes.

    Returns:
        Set of node hashes.
    """
    return {compute_node_hash(node) for node in snapshot}


def diff_accessibility_snapshots(
    old_snapshot: List[AXNode],
    new_snapshot: List[AXNode],
) -> Tuple[List[AXDiff], SnapshotDiffSummary]:
    """
    Diff two accessibility snapshots.

    Args:
        old_snapshot: Previous accessibility tree.
        new_snapshot: Current accessibility tree.

    Returns:
        Tuple of (diffs, summary).
    """
    old_hashes = compute_snapshot_fingerprint(old_snapshot)
    new_hashes = compute_snapshot_fingerprint(new_snapshot)

    added = new_hashes - old_hashes
    removed = old_hashes - new_hashes
    common = old_hashes & new_hashes

    # Build hash -> node maps
    old_map: Dict[str, AXNode] = {}
    new_map: Dict[str, AXNode] = {}

    def index_nodes(nodes: List[AXNode], map_dict: Dict[str, AXNode]) -> None:
        for node in nodes:
            map_dict[compute_node_hash(node)] = node
            index_nodes(node.children, map_dict)

    index_nodes(old_snapshot, old_map)
    index_nodes(new_snapshot, new_map)

    diffs: List[AXDiff] = []
    focus_changed = False
    hierarchy_changed = False

    # Check removed nodes
    for h in removed:
        node = old_map.get(h)
        if node:
            diffs.append(AXDiff(diff_type=DiffType.REMOVED, path=f"/{node.role}"))

    # Check added nodes
    for h in added:
        node = new_map.get(h)
        if node:
            diffs.append(AXDiff(diff_type=DiffType.ADDED, path=f"/{node.role}", new_node=node))

    # Check common nodes for modifications
    for h in common:
        old_node = old_map[h]
        new_node = new_map[h]
        changes: Dict[str, Tuple[Any, Any]] = {}

        if old_node.label != new_node.label:
            changes["label"] = (old_node.label, new_node.label)
        if old_node.value != new_node.value:
            changes["value"] = (old_node.value, new_node.value)
        if old_node.enabled != new_node.enabled:
            changes["enabled"] = (old_node.enabled, new_node.enabled)
        if old_node.focused != new_node.focused:
            changes["focused"] = (old_node.focused, new_node.focused)
            focus_changed = True

        if old_node.children != new_node.children:
            hierarchy_changed = True

        if changes:
            diffs.append(AXDiff(
                diff_type=DiffType.MODIFIED,
                path=f"/{old_node.role}",
                old_node=old_node,
                new_node=new_node,
                property_changes=changes,
            ))

    summary = SnapshotDiffSummary(
        total_changes=len(diffs),
        added_count=len(added),
        removed_count=len(removed),
        modified_count=len([d for d in diffs if d.diff_type == DiffType.MODIFIED]),
        changed_paths=[d.path for d in diffs],
        focus_changed=focus_changed,
        hierarchy_changed=hierarchy_changed,
    )

    return diffs, summary


def get_changed_properties(diffs: List[AXDiff]) -> Dict[str, int]:
    """
    Get counts of changed properties.

    Args:
        diffs: List of accessibility diffs.

    Returns:
        Dictionary of property_name -> change_count.
    """
    counts: Dict[str, int] = {}
    for diff in diffs:
        for prop in diff.property_changes.keys():
            counts[prop] = counts.get(prop, 0) + 1
    return counts
