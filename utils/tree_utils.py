"""Tree utilities for RabAI AutoClick.

Provides:
- Tree data structure helpers
- Tree traversal
- Tree operations
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Generic,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class TreeNode(Generic[T]):
    """A node in a tree."""

    def __init__(
        self,
        value: T,
        children: Optional[List["TreeNode[T]"]] = None,
    ) -> None:
        self.value = value
        self.children = children or []


def bfs(root: TreeNode[T], visit: Callable[[TreeNode[T]], None]) -> None:
    """Breadth-first traversal.

    Args:
        root: Root node.
        visit: Function to call on each node.
    """
    queue: List[TreeNode[T]] = [root]
    while queue:
        node = queue.pop(0)
        visit(node)
        queue.extend(node.children)


def dfs(root: TreeNode[T], visit: Callable[[TreeNode[T]], None]) -> None:
    """Depth-first traversal.

    Args:
        root: Root node.
        visit: Function to call on each node.
    """
    visit(root)
    for child in root.children:
        dfs(child, visit)


def preorder(root: TreeNode[T], visit: Callable[[TreeNode[T]], None]) -> None:
    """Pre-order traversal (visit before children)."""
    visit(root)
    for child in root.children:
        preorder(child, visit)


def postorder(root: TreeNode[T], visit: Callable[[TreeNode[T]], None]) -> None:
    """Post-order traversal (visit after children)."""
    for child in root.children:
        postorder(child, visit)
    visit(root)


def collect_values(root: TreeNode[T]) -> List[T]:
    """Collect all values from tree.

    Args:
        root: Root node.

    Returns:
        List of all values.
    """
    result: List[T] = []

    def _collect(node: TreeNode[T]) -> None:
        result.append(node.value)
        for child in node.children:
            _collect(child)

    _collect(root)
    return result


def tree_height(root: TreeNode[T]) -> int:
    """Get height of tree.

    Args:
        root: Root node.

    Returns:
        Height (1 for single node).
    """
    if not root.children:
        return 1
    return 1 + max(tree_height(c) for c in root.children)


def tree_size(root: TreeNode[T]) -> int:
    """Get number of nodes in tree.

    Args:
        root: Root node.

    Returns:
        Node count.
    """
    count = 1
    for child in root.children:
        count += tree_size(child)
    return count


__all__ = [
    "TreeNode",
    "bfs",
    "dfs",
    "preorder",
    "postorder",
    "collect_values",
    "tree_height",
    "tree_size",
]
