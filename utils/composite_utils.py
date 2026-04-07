"""
Composite Pattern Implementation

Provides hierarchical tree structures for representing part-whole
relationships with uniform interfaces for individual and composite objects.
"""

from __future__ import annotations

import copy
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class ComponentNode(ABC, Generic[T]):
    """
    Abstract base class for composite pattern.

    Type Parameters:
        T: The type of value stored in this node.
    """
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @abstractmethod
    def is_leaf(self) -> bool:
        """Return True if this is a leaf node (has no children)."""
        pass

    @abstractmethod
    def is_composite(self) -> bool:
        """Return True if this node can have children."""
        pass

    @abstractmethod
    def get_children(self) -> list[ComponentNode[T]]:
        """Return all child nodes."""
        pass

    @abstractmethod
    def add(self, child: ComponentNode[T]) -> ComponentNode[T]:
        """Add a child node."""
        pass

    @abstractmethod
    def remove(self, child: ComponentNode[T]) -> bool:
        """Remove a child node."""
        pass

    @abstractmethod
    def traverse(self, visitor: Callable[[ComponentNode[T]], None]) -> None:
        """Visit all nodes in this subtree."""
        pass

    @abstractmethod
    def find(self, predicate: Callable[[ComponentNode[T]], bool]) -> ComponentNode[T] | None:
        """Find first node matching predicate."""
        pass

    @abstractmethod
    def find_all(self, predicate: Callable[[ComponentNode[T]], bool]) -> list[ComponentNode[T]]:
        """Find all nodes matching predicate."""
        pass

    @abstractmethod
    def count(self) -> int:
        """Return total number of nodes in this subtree."""
        pass

    def __iter__(self) -> Iterator[ComponentNode[T]]:
        """Iterate over this node and all descendants (pre-order)."""
        yield self
        for child in self.get_children():
            yield from child


@dataclass
class LeafNode(ComponentNode[T]):
    """
    Leaf node in the composite structure.
    Cannot have children.
    """
    value: T | None = None

    def is_leaf(self) -> bool:
        return True

    def is_composite(self) -> bool:
        return False

    def get_children(self) -> list[ComponentNode[T]]:
        return []

    def add(self, child: ComponentNode[T]) -> ComponentNode[T]:
        raise NotImplementedError("Leaf nodes cannot have children")

    def remove(self, child: ComponentNode[T]) -> bool:
        return False

    def traverse(self, visitor: Callable[[ComponentNode[T]], None]) -> None:
        visitor(self)

    def find(self, predicate: Callable[[ComponentNode[T]], bool]) -> ComponentNode[T] | None:
        return self if predicate(self) else None

    def find_all(self, predicate: Callable[[ComponentNode[T]], bool]) -> list[ComponentNode[T]]:
        return [self] if predicate(self) else []

    def count(self) -> int:
        return 1


@dataclass
class CompositeNode(ComponentNode[T]):
    """
    Composite node that can contain child nodes.
    """
    children: list[ComponentNode[T]] = field(default_factory=list)
    _name: str = ""

    def __post_init__(self):
        object.__setattr__(self, "_name", self.name)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        object.__setattr__(self, "_name", value)

    def is_leaf(self) -> bool:
        return False

    def is_composite(self) -> bool:
        return True

    def get_children(self) -> list[ComponentNode[T]]:
        return list(self.children)

    def add(self, child: ComponentNode[T]) -> ComponentNode[T]:
        """Add a child node."""
        if child in self.children:
            return child
        self.children.append(child)
        return child

    def remove(self, child: ComponentNode[T]) -> bool:
        """Remove a child node."""
        try:
            self.children.remove(child)
            return True
        except ValueError:
            return False

    def clear(self) -> None:
        """Remove all children."""
        self.children.clear()

    def traverse(self, visitor: Callable[[ComponentNode[T]], None]) -> None:
        """Visit this node then all descendants."""
        visitor(self)
        for child in self.children:
            child.traverse(visitor)

    def find(self, predicate: Callable[[ComponentNode[T]], bool]) -> ComponentNode[T] | None:
        """Find first node matching predicate (depth-first)."""
        if predicate(self):
            return self
        for child in self.children:
            result = child.find(predicate)
            if result is not None:
                return result
        return None

    def find_all(self, predicate: Callable[[ComponentNode[T]], bool]) -> list[ComponentNode[T]]:
        """Find all nodes matching predicate."""
        results: list[ComponentNode[T]] = []
        if predicate(self):
            results.append(self)
        for child in self.children:
            results.extend(child.find_all(predicate))
        return results

    def count(self) -> int:
        """Return total number of nodes in this subtree."""
        return 1 + sum(child.count() for child in self.children)

    def get_depth(self) -> int:
        """Return maximum depth of this subtree."""
        if not self.children:
            return 1
        return 1 + max(child.get_depth() if isinstance(child, CompositeNode) else 1 for child in self.children)

    def get_leaves(self) -> list[LeafNode[T]]:
        """Return all leaf descendants."""
        if not self.children:
            return []
        leaves: list[LeafNode[T]] = []
        for child in self.children:
            if child.is_leaf():
                leaves.append(child)  # type: ignore
            elif isinstance(child, CompositeNode):
                leaves.extend(child.get_leaves())
        return leaves

    def get_path(self, target: ComponentNode[T]) -> list[ComponentNode[T]] | None:
        """Get path from this node to target (inclusive)."""
        if self is target:
            return [self]

        for child in self.children:
            if isinstance(child, CompositeNode):
                path = child.get_path(target)
                if path is not None:
                    return [self] + path
            elif child is target:
                return [self, child]

        return None


class TreeVisitor(ABC):
    """Abstract visitor for tree traversal operations."""

    @abstractmethod
    def visit(self, node: ComponentNode) -> Any:
        """Visit a node and return result."""
        pass


class DepthFirstVisitor(TreeVisitor[T]):
    """Performs depth-first traversal with a visitor function."""

    def __init__(self, visitor: Callable[[ComponentNode[T]], Any]):
        self.visitor = visitor

    def visit(self, node: ComponentNode[T]) -> list[Any]:
        results: list[Any] = []

        def _traverse(n: ComponentNode[T]) -> None:
            results.append(self.visitor(n))
            for child in n.get_children():
                if isinstance(child, CompositeNode):
                    _traverse(child)
                else:
                    results.append(self.visitor(child))

        _traverse(node)
        return results


class BreadthFirstVisitor(TreeVisitor[T]):
    """Performs breadth-first traversal with a visitor function."""

    def __init__(self, visitor: Callable[[ComponentNode[T]], Any]):
        self.visitor = visitor

    def visit(self, node: ComponentNode[T]) -> list[Any]:
        results: list[Any] = []
        queue: list[ComponentNode[T]] = [node]

        while queue:
            current = queue.pop(0)
            results.append(self.visitor(current))
            for child in current.get_children():
                queue.append(child)

        return results


@dataclass
class TreeStatistics:
    """Statistics about a tree structure."""
    total_nodes: int
    leaf_nodes: int
    composite_nodes: int
    max_depth: int
    avg_branching_factor: float


def compute_statistics(root: ComponentNode) -> TreeStatistics:
    """Compute statistics for a tree structure."""
    total = 0
    leaves = 0
    composites = 0
    max_depth = 0

    def _compute(node: ComponentNode, depth: int) -> int:
        nonlocal total, leaves, composites, max_depth
        total += 1
        max_depth = max(max_depth, depth)

        if node.is_leaf():
            leaves += 1
        else:
            composites += 1
            if isinstance(node, CompositeNode):
                child_depths = [_compute(child, depth + 1) for child in node.children]
                return max(child_depths) if child_depths else depth

        return depth

    _compute(root, 1)
    avg_branching = (
        sum(len(c.children) for c in root if isinstance(c, CompositeNode)) / composites
        if composites > 0
        else 0.0
    )

    return TreeStatistics(
        total_nodes=total,
        leaf_nodes=leaves,
        composite_nodes=composites,
        max_depth=max_depth,
        avg_branching_factor=avg_branching,
    )


def clone_tree(root: ComponentNode[T]) -> ComponentNode[T]:
    """Create a deep clone of a tree structure."""
    if root.is_leaf():
        leaf = root  # type: ignore
        return LeafNode(
            id=uuid.uuid4().hex[:8],
            name=leaf.name,
            metadata=copy.deepcopy(leaf.metadata),
            value=copy.deepcopy(leaf.value),
        )

    composite = root  # type: ignore
    cloned_children = [clone_tree(child) for child in composite.children]
    return CompositeNode(
        id=uuid.uuid4().hex[:8],
        name=composite.name,
        metadata=copy.deepcopy(composite.metadata),
        children=cloned_children,
    )
