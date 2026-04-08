"""Visitor utilities for RabAI AutoClick.

Provides:
- Tree visitor pattern
- Node traversal
- Visitor callback management
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
    """A node in a tree structure."""

    def __init__(
        self,
        value: T,
        children: Optional[List["TreeNode[T]"]] = None,
    ) -> None:
        self.value = value
        self.children = children or []


class TreeVisitor(Generic[T]):
    """Visitor for traversing tree structures.

    Args:
        on_enter: Callback when entering a node.
        on_exit: Callback when exiting a node.
    """

    def __init__(
        self,
        on_enter: Optional[Callable[[TreeNode[T]], None]] = None,
        on_exit: Optional[Callable[[TreeNode[T]], None]] = None,
    ) -> None:
        self._on_enter = on_enter
        self._on_exit = on_exit

    def visit(self, node: TreeNode[T]) -> None:
        """Visit a node and its children.

        Args:
            node: Root node to visit.
        """
        if self._on_enter:
            self._on_enter(node)
        for child in node.children:
            self.visit(child)
        if self._on_exit:
            self._on_exit(node)

    def visit_iterative(self, node: TreeNode[T]) -> None:
        """Visit nodes iteratively (non-recursive).

        Args:
            node: Root node to visit.
        """
        stack: List[TreeNode[T]] = [node]
        while stack:
            current = stack.pop()
            if self._on_enter:
                self._on_enter(current)
            for child in reversed(current.children):
                stack.append(child)


class NodeCollector(Generic[T]):
    """Collect nodes matching a predicate during traversal."""

    def __init__(self, predicate: Callable[[T], bool]) -> None:
        self._predicate = predicate
        self._matches: List[TreeNode[T]] = []

    def collect(self, root: TreeNode[T]) -> List[TreeNode[T]]:
        """Collect matching nodes from tree.

        Args:
            root: Root node.

        Returns:
            List of matching nodes.
        """
        self._matches = []
        self._dfs(root)
        return self._matches

    def _dfs(self, node: TreeNode[T]) -> None:
        if self._predicate(node.value):
            self._matches.append(node)
        for child in node.children:
            self._dfs(child)


def flatten_tree(root: TreeNode[T]) -> List[T]:
    """Flatten a tree to a list of values.

    Args:
        root: Root node.

    Returns:
        List of values in traversal order.
    """
    result: List[T] = []

    def _walk(node: TreeNode[T]) -> None:
        result.append(node.value)
        for child in node.children:
            _walk(child)

    _walk(root)
    return result


def tree_depth(node: TreeNode[T]) -> int:
    """Get the depth of a tree.

    Args:
        node: Root node.

    Returns:
        Maximum depth (1 for single node).
    """
    if not node.children:
        return 1
    return 1 + max(tree_depth(c) for c in node.children)


__all__ = [
    "TreeNode",
    "TreeVisitor",
    "NodeCollector",
    "flatten_tree",
    "tree_depth",
]
