"""
Visitor pattern implementation for tree and graph traversal.

Provides generic visitor infrastructure for traversing
nested data structures with pre/post order hooks.

Example:
    >>> from utils.visitor_utils_v2 import Visitor, visit_tree
    >>> visitor = Visitor(visit_fn=lambda node: print(node))
    >>> visit_tree(data, visitor)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic

T = TypeVar("T")


class Visitor(ABC, Generic[T]):
    """
    Abstract visitor base class.

    Subclass this to define behavior for different node types.
    """

    @abstractmethod
    def visit(self, node: T) -> Any:
        """Visit a node. Return value controls traversal."""
        pass


class TreeNode:
    """
    Generic tree node for use with visitor pattern.
    """

    def __init__(
        self,
        value: Any,
        children: Optional[List["TreeNode"]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Initialize a tree node.

        Args:
            value: Node value.
            children: Child nodes.
            metadata: Additional metadata.
        """
        self.value = value
        self.children = children or []
        self.metadata = metadata or {}

    def add_child(self, child: "TreeNode") -> None:
        """Add a child node."""
        self.children.append(child)

    def is_leaf(self) -> bool:
        """Check if node is a leaf."""
        return len(self.children) == 0

    def __repr__(self) -> str:
        return f"TreeNode({self.value!r}, children={len(self.children)})"


def visit_tree(
    root: Any,
    visitor: Visitor,
    get_children: Optional[Callable[[Any], List[Any]]] = None,
    get_value: Optional[Callable[[Any], Any]] = None,
    order: str = "pre",
) -> List[Any]:
    """
    Traverse a tree structure with a visitor.

    Args:
        root: Root node of the tree.
        visitor: Visitor instance.
        get_children: Function to get children of a node.
        get_value: Function to get value from a node.
        order: Traversal order ('pre', 'post', 'both').

    Returns:
        List of visitor return values.
    """
    results: List[Any] = []

    def get_node_children(node: Any) -> List[Any]:
        if get_children:
            return get_children(node)
        if isinstance(node, TreeNode):
            return node.children
        if hasattr(node, "children"):
            return node.children
        if isinstance(node, dict) and "children" in node:
            return node["children"]
        if isinstance(node, (list, tuple)):
            return list(node)
        return []

    def get_node_value(node: Any) -> Any:
        if get_value:
            return get_value(node)
        if isinstance(node, TreeNode):
            return node.value
        if hasattr(node, "value"):
            return node.value
        if isinstance(node, dict) and "value" in node:
            return node["value"]
        return node

    def pre_order(node: Any) -> None:
        result = visitor.visit(node)
        results.append(result)
        for child in get_node_children(node):
            pre_order(child)

    def post_order(node: Any) -> None:
        for child in get_node_children(node):
            post_order(child)
        result = visitor.visit(node)
        results.append(result)

    if order == "pre":
        pre_order(root)
    elif order == "post":
        post_order(root)
    else:
        pre_order(root)

    return results


class RecursiveVisitor(Visitor[T]):
    """
    Visitor that recursively traverses all children.
    """

    def __init__(
        self,
        visit_fn: Optional[Callable[[T], Any]] = None,
        children_fn: Optional[Callable[[T], List[T]]] = None,
        process_result: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        """
        Initialize the recursive visitor.

        Args:
            visit_fn: Function to call on each node.
            children_fn: Function to get children of a node.
            process_result: Function to process return values.
        """
        self.visit_fn = visit_fn
        self.children_fn = children_fn
        self.process_result = process_result

    def visit(self, node: T) -> Any:
        """Visit a node and recursively visit children."""
        result = None

        if self.visit_fn:
            result = self.visit_fn(node)

        if self.process_result and result is not None:
            result = self.process_result(result)

        if self.children_fn:
            for child in self.children_fn(node):
                child_result = self.visit(child)

        return result


class CollectingVisitor(Visitor[T]):
    """
    Visitor that collects values from nodes.
    """

    def __init__(
        self,
        filter_fn: Optional[Callable[[T], bool]] = None,
        transform_fn: Optional[Callable[[T], Any]] = None,
    ) -> None:
        """
        Initialize the collecting visitor.

        Args:
            filter_fn: Function to filter which nodes to collect.
            transform_fn: Function to transform collected values.
        """
        self.filter_fn = filter_fn
        self.transform_fn = transform_fn
        self.collected: List[Any] = []

    def visit(self, node: T) -> Any:
        """Visit a node and collect if matching filter."""
        if self.filter_fn and not self.filter_fn(node):
            return None

        value = node
        if self.transform_fn:
            value = self.transform_fn(node)

        self.collected.append(value)
        return value


class DepthTrackingVisitor(Visitor[T]):
    """
    Visitor that tracks the depth of each node.
    """

    def __init__(
        self,
        visit_fn: Callable[[T, int], Any],
        children_fn: Optional[Callable[[T], List[T]]] = None,
    ) -> None:
        """
        Initialize the depth tracking visitor.

        Args:
            visit_fn: Function to call on each node, receives (node, depth).
            children_fn: Function to get children.
        """
        self.visit_fn = visit_fn
        self.children_fn = children_fn

    def visit(self, node: T) -> Any:
        """Visit a node (depth must be managed by caller)."""
        pass

    def visit_at_depth(
        self,
        node: T,
        depth: int,
    ) -> Any:
        """Visit a node at a specific depth."""
        result = self.visit_fn(node, depth)

        if self.children_fn:
            for child in self.children_fn(node):
                self.visit_at_depth(child, depth + 1)

        return result


class GraphVisitor(Visitor[T]):
    """
    Visitor for graph structures with cycle detection.
    """

    def __init__(
        self,
        visit_fn: Optional[Callable[[T], Any]] = None,
        get_neighbors: Optional[Callable[[T], List[T]]] = None,
        max_depth: Optional[int] = None,
    ) -> None:
        """
        Initialize the graph visitor.

        Args:
            visit_fn: Function to call on each node.
            get_neighbors: Function to get neighbors of a node.
            max_depth: Maximum traversal depth.
        """
        self.visit_fn = visit_fn
        self.get_neighbors = get_neighbors
        self.max_depth = max_depth
        self._visited: set = set()

    def visit(self, node: T) -> Any:
        """Visit a node with cycle detection."""
        if id(node) in self._visited:
            return None

        self._visited.add(id(node))

        result = None
        if self.visit_fn:
            result = self.visit_fn(node)

        if self.get_neighbors:
            for neighbor in self.get_neighbors(node):
                self.visit(neighbor)

        return result

    def reset(self) -> None:
        """Reset visited set for new traversal."""
        self._visited.clear()


def flatten_tree(
    root: Any,
    children_fn: Optional[Callable[[Any], List[Any]]] = None,
    order: str = "pre",
) -> List[Any]:
    """
    Flatten a tree structure into a list.

    Args:
        root: Root node.
        children_fn: Function to get children.
        order: Traversal order.

    Returns:
        Flattened list of nodes.
    """

    class FlatVisitor(Visitor):
        def __init__(self) -> None:
            self.items: List[Any] = []

        def visit(self, node: Any) -> Any:
            self.items.append(node)
            return None

    visitor = FlatVisitor()
    visit_tree(root, visitor, get_children=children_fn, order=order)
    return visitor.items


def count_nodes(
    root: Any,
    children_fn: Optional[Callable[[Any], List[Any]]] = None,
) -> int:
    """
    Count total number of nodes in a tree.

    Args:
        root: Root node.
        children_fn: Function to get children.

    Returns:
        Total node count.
    """
    return len(flatten_tree(root, children_fn=children_fn))


def find_nodes(
    root: Any,
    predicate: Callable[[Any], bool],
    children_fn: Optional[Callable[[Any], List[Any]]] = None,
) -> List[Any]:
    """
    Find all nodes matching a predicate.

    Args:
        root: Root node.
        predicate: Function to test each node.
        children_fn: Function to get children.

    Returns:
        List of matching nodes.
    """

    class FindVisitor(Visitor):
        def __init__(self, pred: Callable[[Any], bool]) -> None:
            self.predicate = pred
            self.matches: List[Any] = []

        def visit(self, node: Any) -> Any:
            if self.predicate(node):
                self.matches.append(node)
            return None

    visitor = FindVisitor(predicate)
    visit_tree(root, visitor, get_children=children_fn)
    return visitor.matches
