"""
Accessibility Tree Diff Utilities

Compute and represent differences between two accessibility trees,
useful for change detection and debugging.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List, Callable, Tuple


@dataclass
class TreeDiff:
    """Represents a change in an accessibility tree."""
    diff_type: str  # 'added', 'removed', 'modified'
    element_id: str
    role: str
    label: str
    path: str  # breadcrumb path to the element
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class TreeDiffSummary:
    """Summary of tree differences."""
    total_changes: int
    added_count: int
    removed_count: int
    modified_count: int
    unchanged_count: int
    change_ratio: float  # fraction of elements that changed


def diff_element_values(
    old_label: str,
    new_label: str,
    old_value: str,
    new_value: str,
) -> List[Tuple[str, str]]:
    """Compare old and new element properties, returning changed pairs."""
    changes = []
    if old_label != new_label:
        changes.append(("label", f"{old_label} -> {new_label}"))
    if old_value != new_value:
        changes.append(("value", f"{old_value} -> {new_value}"))
    return changes


def build_element_path(node: dict, path: str = "") -> str:
    """Build a human-readable path string for an element in the tree."""
    role = node.get("role", "unknown")
    label = node.get("label", "")
    current = f"{role}"
    if label:
        current += f"[{label}]"
    if path:
        return f"{path} > {current}"
    return current


def diff_accessibility_trees(
    old_tree: dict,
    new_tree: dict,
    get_id: Callable[[dict], str],
    get_role: Callable[[dict], str],
    get_label: Callable[[dict], str] = lambda n: n.get("label", ""),
    get_value: Callable[[dict], str] = lambda n: n.get("value", ""),
    get_children: Callable[[dict], List[dict]] = lambda n: n.get("children", []),
) -> Tuple[List[TreeDiff], TreeDiffSummary]:
    """
    Diff two accessibility trees and return the changes.

    Args:
        old_tree: The previous tree state.
        new_tree: The current tree state.
        get_id: Function to get element ID from a node dict.
        get_role: Function to get element role from a node dict.
        get_label: Function to get element label (default uses 'label' key).
        get_value: Function to get element value (default uses 'value' key).
        get_children: Function to get children (default uses 'children' key).

    Returns:
        Tuple of (list of TreeDiffs, TreeDiffSummary).
    """
    diffs: List[TreeDiff] = []
    old_elements = {get_id(n): n for n in _flatten_tree(old_tree, get_children)}
    new_elements = {get_id(n): n for n in _flatten_tree(new_tree, get_children)}

    old_ids = set(old_elements.keys())
    new_ids = set(new_elements.keys())

    # Detect removed elements
    for elem_id in old_ids - new_ids:
        node = old_elements[elem_id]
        diffs.append(TreeDiff(
            diff_type="removed",
            element_id=elem_id,
            role=get_role(node),
            label=get_label(node),
            path="",
        ))

    # Detect added elements
    for elem_id in new_ids - old_ids:
        node = new_elements[elem_id]
        diffs.append(TreeDiff(
            diff_type="added",
            element_id=elem_id,
            role=get_role(node),
            label=get_label(node),
            path="",
        ))

    # Detect modified elements
    for elem_id in old_ids & new_ids:
        old_node = old_elements[elem_id]
        new_node = new_elements[elem_id]
        old_val = get_value(old_node)
        new_val = get_value(new_node)
        old_lbl = get_label(old_node)
        new_lbl = get_label(new_node)
        if old_val != new_val or old_lbl != new_lbl:
            diffs.append(TreeDiff(
                diff_type="modified",
                element_id=elem_id,
                role=get_role(new_node),
                label=new_lbl,
                path="",
                old_value=old_val,
                new_value=new_val,
            ))

    total_elements = max(len(old_ids), len(new_ids))
    unchanged = total_elements - len(diffs)

    summary = TreeDiffSummary(
        total_changes=len(diffs),
        added_count=len(new_ids - old_ids),
        removed_count=len(old_ids - new_ids),
        modified_count=len(old_ids & new_ids),
        unchanged_count=unchanged,
        change_ratio=len(diffs) / total_elements if total_elements > 0 else 0.0,
    )

    return diffs, summary


def _flatten_tree(
    node: dict,
    get_children: Callable[[dict], List[dict]],
) -> List[dict]:
    """Flatten a tree into a list of nodes."""
    result = [node]
    for child in get_children(node):
        result.extend(_flatten_tree(child, get_children))
    return result
