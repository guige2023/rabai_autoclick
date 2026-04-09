"""
Tree data structure module.

Provides generic tree and binary tree implementations with
traversal, search, and manipulation operations.

Author: Aito Auto Agent
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar('T')


class TraversalOrder(Enum):
    """Tree traversal order types."""
    PRE_ORDER = auto()
    IN_ORDER = auto()
    POST_ORDER = auto()
    LEVEL_ORDER = auto()
    REVERSE_LEVEL_ORDER = auto()


@dataclass
class TreeNode(Generic[T]):
    """
    Node in a tree structure.

    Contains data and references to child nodes.
    """
    data: T
    children: list[TreeNode[T]] = field(default_factory=list)
    parent: Optional[TreeNode[T]] = None
    metadata: dict = field(default_factory=dict)

    def add_child(self, data: T) -> TreeNode[T]:
        """Add a child node with the given data."""
        child = TreeNode(data=data, parent=self)
        self.children.append(child)
        return child

    def remove_child(self, child: TreeNode[T]) -> bool:
        """Remove a child node."""
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            return True
        return False

    def is_leaf(self) -> bool:
        """Return True if this node has no children."""
        return len(self.children) == 0

    def is_root(self) -> bool:
        """Return True if this node has no parent."""
        return self.parent is None

    def depth(self) -> int:
        """Calculate depth of this node from root."""
        depth = 0
        node = self
        while node.parent is not None:
            depth += 1
            node = node.parent
        return depth

    def height(self) -> int:
        """Calculate height of subtree rooted at this node."""
        if self.is_leaf():
            return 1
        return 1 + max((child.height() for child in self.children), default=0)

    def size(self) -> int:
        """Calculate number of nodes in subtree."""
        return 1 + sum(child.size() for child in self.children)

    def ancestors(self) -> list[TreeNode[T]]:
        """Get list of ancestor nodes (excluding self)."""
        result = []
        node = self.parent
        while node is not None:
            result.append(node)
            node = node.parent
        return result

    def descendants(self) -> list[TreeNode[T]]:
        """Get list of all descendant nodes."""
        result = []
        queue = deque(self.children)
        while queue:
            node = queue.popleft()
            result.append(node)
            queue.extend(node.children)
        return result

    def siblings(self) -> list[TreeNode[T]]:
        """Get sibling nodes (excluding self)."""
        if self.parent is None:
            return []
        return [c for c in self.parent.children if c is not self]


@dataclass
class BinaryTreeNode(Generic[T]):
    """
    Node in a binary tree.

    Contains data and references to left and right children.
    """
    data: T
    left: Optional[BinaryTreeNode[T]] = None
    right: Optional[BinaryTreeNode[T]] = None
    parent: Optional[BinaryTreeNode[T]] = None
    metadata: dict = field(default_factory=dict)

    def is_leaf(self) -> bool:
        """Return True if node has no children."""
        return self.left is None and self.right is None

    def is_full(self) -> bool:
        """Return True if node has both children."""
        return self.left is not None and self.right is not None

    def is_complete(self) -> bool:
        """Return True if node has children on all levels except possibly last."""
        pass

    def size(self) -> int:
        """Calculate number of nodes in subtree."""
        left_size = self.left.size() if self.left else 0
        right_size = self.right.size() if self.right else 0
        return 1 + left_size + right_size

    def height(self) -> int:
        """Calculate height of subtree."""
        left_height = self.left.height() if self.left else 0
        right_height = self.right.height() if self.right else 0
        return 1 + max(left_height, right_height)

    def depth(self) -> int:
        """Calculate depth of this node from root."""
        depth = 0
        node = self
        while node.parent is not None:
            depth += 1
            node = node.parent
        return depth

    def in_order_successor(self) -> Optional[BinaryTreeNode[T]]:
        """Find in-order successor (next in sorted order)."""
        if self.right is not None:
            node = self.right
            while node.left is not None:
                node = node.left
            return node

        node = self
        while node.parent is not None and node.parent.right is node:
            node = node.parent
        return node.parent


class Tree(Generic[T]):
    """
    Generic tree data structure.

    Provides various traversal methods and tree operations.

    Example:
        tree = Tree[int]()
        root = tree.add_root(1)
        child_a = root.add_child(2)
        child_b = root.add_child(3)
        child_a.add_child(4)

        for node in tree.traverse(TraversalOrder.PRE_ORDER):
            print(node.data)
    """

    def __init__(self, thread_safe: bool = False):
        self._root: Optional[TreeNode[T]] = None
        self._lock = threading.RLock() if thread_safe else None
        self._size = 0

    def _lock_acquire(self):
        if self._lock:
            self._lock.acquire()

    def _lock_release(self):
        if self._lock:
            self._lock.release()

    def add_root(self, data: T) -> TreeNode[T]:
        """
        Add root node to empty tree.

        Args:
            data: Data for root node

        Returns:
            The created root node

        Raises:
            ValueError: If tree already has a root
        """
        with self._lock:
            if self._root is not None:
                raise ValueError("Tree already has a root")
            self._root = TreeNode(data=data)
            self._size = 1
            return self._root

    def get_root(self) -> Optional[TreeNode[T]]:
        """Get the root node of the tree."""
        return self._root

    def is_empty(self) -> bool:
        """Return True if tree has no nodes."""
        return self._root is None

    def size(self) -> int:
        """Return number of nodes in tree."""
        return self._size

    def clear(self) -> None:
        """Remove all nodes from tree."""
        with self._lock:
            self._root = None
            self._size = 0

    def traverse(
        self,
        order: TraversalOrder = TraversalOrder.PRE_ORDER
    ) -> Iterator[TreeNode[T]]:
        """
        Traverse tree in specified order.

        Args:
            order: Traversal order

        Yields:
            Tree nodes in traversal order
        """
        if self._root is None:
            return

        if order == TraversalOrder.PRE_ORDER:
            yield from self._traverse_preorder(self._root)
        elif order == TraversalOrder.POST_ORDER:
            yield from self._traverse_postorder(self._root)
        elif order == TraversalOrder.LEVEL_ORDER:
            yield from self._traverse_level_order(self._root)
        elif order == TraversalOrder.REVERSE_LEVEL_ORDER:
            yield from self._traverse_reverse_level_order(self._root)
        else:
            yield from self._traverse_preorder(self._root)

    def _traverse_preorder(self, node: TreeNode[T]) -> Iterator[TreeNode[T]]:
        """Pre-order traversal: root, then children."""
        yield node
        for child in node.children:
            yield from self._traverse_preorder(child)

    def _traverse_postorder(self, node: TreeNode[T]) -> Iterator[TreeNode[T]]:
        """Post-order traversal: children first, then root."""
        for child in node.children:
            yield from self._traverse_postorder(child)
        yield node

    def _traverse_level_order(self, node: TreeNode[T]) -> Iterator[TreeNode[T]]:
        """Level-order traversal using BFS."""
        queue = deque([node])
        while queue:
            current = queue.popleft()
            yield current
            queue.extend(current.children)

    def _traverse_reverse_level_order(self, node: TreeNode[T]) -> Iterator[TreeNode[T]]:
        """Reverse level-order traversal."""
        levels = []
        queue = deque([(node, 0)])

        while queue:
            current, level = queue.popleft()
            if len(levels) <= level:
                levels.append([])
            levels[level].append(current)
            queue.extend((c, level + 1) for c in current.children)

        for level in reversed(levels):
            yield from level

    def find(
        self,
        predicate: Callable[[T], bool]
    ) -> Optional[TreeNode[T]]:
        """
        Find first node matching predicate.

        Args:
            predicate: Function that returns True for desired nodes

        Returns:
            First matching node or None
        """
        for node in self.traverse(TraversalOrder.LEVEL_ORDER):
            if predicate(node.data):
                return node
        return None

    def find_all(
        self,
        predicate: Callable[[T], bool]
    ) -> list[TreeNode[T]]:
        """
        Find all nodes matching predicate.

        Args:
            predicate: Function that returns True for desired nodes

        Returns:
            List of all matching nodes
        """
        return [
            node for node in self.traverse(TraversalOrder.LEVEL_ORDER)
            if predicate(node.data)
        ]

    def filter_nodes(
        self,
        predicate: Callable[[TreeNode[T]], bool]
    ) -> list[TreeNode[T]]:
        """Filter nodes using node-level predicate."""
        return [
            node for node in self.traverse(TraversalOrder.LEVEL_ORDER)
            if predicate(node)
        ]

    def map_nodes(self, func: Callable[[T], T]) -> None:
        """Apply function to all node data in place."""
        for node in self.traverse(TraversalOrder.PRE_ORDER):
            node.data = func(node.data)


class BinaryTree(Generic[T]):
    """
    Binary tree data structure.

    Specialized tree where each node has at most two children.

    Example:
        tree = BinaryTree[int]()
        root = tree.add_root(1)
        root.left = tree.add_node(2)
        root.right = tree.add_node(3)

        for node in tree.traverse(TraversalOrder.IN_ORDER):
            print(node.data)
    """

    def __init__(self, thread_safe: bool = False):
        self._root: Optional[BinaryTreeNode[T]] = None
        self._lock = threading.RLock() if thread_safe else None
        self._size = 0

    def add_root(self, data: T) -> BinaryTreeNode[T]:
        """Add root node to empty tree."""
        with self._lock:
            if self._root is not None:
                raise ValueError("Tree already has a root")
            self._root = BinaryTreeNode(data=data)
            self._size = 1
            return self._root

    def add_left(self, parent: BinaryTreeNode[T], data: T) -> BinaryTreeNode[T]:
        """Add left child to a node."""
        with self._lock:
            if parent.left is not None:
                raise ValueError("Node already has left child")
            parent.left = BinaryTreeNode(data=data, parent=parent)
            self._size += 1
            return parent.left

    def add_right(self, parent: BinaryTreeNode[T], data: T) -> BinaryTreeNode[T]:
        """Add right child to a node."""
        with self._lock:
            if parent.right is not None:
                raise ValueError("Node already has right child")
            parent.right = BinaryTreeNode(data=data, parent=parent)
            self._size += 1
            return parent.right

    def get_root(self) -> Optional[BinaryTreeNode[T]]:
        """Get the root node."""
        return self._root

    def is_empty(self) -> bool:
        """Return True if tree is empty."""
        return self._root is None

    def size(self) -> int:
        """Return number of nodes."""
        return self._size

    def traverse(
        self,
        order: TraversalOrder = TraversalOrder.PRE_ORDER
    ) -> Iterator[BinaryTreeNode[T]]:
        """
        Traverse binary tree in specified order.

        Note: IN_ORDER gives sorted order for BSTs.
        """
        if self._root is None:
            return

        if order == TraversalOrder.PRE_ORDER:
            yield from self._traverse_preorder(self._root)
        elif order == TraversalOrder.IN_ORDER:
            yield from self._traverse_inorder(self._root)
        elif order == TraversalOrder.POST_ORDER:
            yield from self._traverse_postorder(self._root)
        elif order == TraversalOrder.LEVEL_ORDER:
            yield from self._traverse_level_order(self._root)
        else:
            yield from self._traverse_preorder(self._root)

    def _traverse_preorder(self, node: BinaryTreeNode[T]) -> Iterator[BinaryTreeNode[T]]:
        """Pre-order: root, left, right."""
        yield node
        if node.left:
            yield from self._traverse_preorder(node.left)
        if node.right:
            yield from self._traverse_preorder(node.right)

    def _traverse_inorder(self, node: BinaryTreeNode[T]) -> Iterator[BinaryTreeNode[T]]:
        """In-order: left, root, right."""
        if node.left:
            yield from self._traverse_inorder(node.left)
        yield node
        if node.right:
            yield from self._traverse_inorder(node.right)

    def _traverse_postorder(self, node: BinaryTreeNode[T]) -> Iterator[BinaryTreeNode[T]]:
        """Post-order: left, right, root."""
        if node.left:
            yield from self._traverse_postorder(node.left)
        if node.right:
            yield from self._traverse_postorder(node.right)
        yield node

    def _traverse_level_order(self, node: BinaryTreeNode[T]) -> Iterator[BinaryTreeNode[T]]:
        """Level-order BFS."""
        queue = deque([node])
        while queue:
            current = queue.popleft()
            yield current
            if current.left:
                queue.append(current.left)
            if current.right:
                queue.append(current.right)


def create_tree(thread_safe: bool = False) -> Tree:
    """Factory to create a Tree."""
    return Tree(thread_safe=thread_safe)


def create_binary_tree(thread_safe: bool = False) -> BinaryTree:
    """Factory to create a BinaryTree."""
    return BinaryTree(thread_safe=thread_safe)
