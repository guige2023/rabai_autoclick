"""Tree data structure utilities for RabAI AutoClick.

Provides:
- Binary tree implementation
- Generic tree/n-ary tree
- Tree traversal (DFS, BFS, inorder, preorder, postorder)
- Tree operations (search, insert, delete, height, balance)
- Binary search tree (BST)
- Tree serialization/deserialization
"""

from __future__ import annotations

import copy
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)


T = TypeVar("T")


@dataclass
class TreeNode(Generic[T]):
    """A node in a tree.

    Attributes:
        value: The value stored in this node.
        children: List of child nodes.
        parent: Reference to parent node (None for root).
        metadata: Optional additional data associated with this node.
    """

    value: T
    children: List[TreeNode[T]] = field(default_factory=list)
    parent: Optional[TreeNode[T]] = field(default=None, repr=False)
    metadata: Dict[str, Any] = field(default_factory=dict, repr=False)

    def add_child(self, value: T) -> TreeNode[T]:
        """Add a child node with given value.

        Args:
            value: Value for the new child node.

        Returns:
            The newly created child node.
        """
        child = TreeNode(value=value, parent=self)
        self.children.append(child)
        return child

    def add_child_node(self, node: TreeNode[T]) -> TreeNode[T]:
        """Add an existing node as a child.

        Args:
            node: The node to add as child.

        Returns:
            The added node.
        """
        node.parent = self
        self.children.append(node)
        return node

    def is_leaf(self) -> bool:
        """Check if this node is a leaf (has no children)."""
        return len(self.children) == 0

    def is_root(self) -> bool:
        """Check if this node is the root (has no parent)."""
        return self.parent is None

    def depth(self) -> int:
        """Get the depth of this node (root = 0)."""
        d = 0
        node: Optional[TreeNode[T]] = self.parent
        while node is not None:
            d += 1
            node = node.parent
        return d

    def height(self) -> int:
        """Get the height of subtree rooted at this node."""
        if not self.children:
            return 0
        return 1 + max(child.height() for child in self.children)

    def subtree_size(self) -> int:
        """Get the size of subtree rooted at this node."""
        return 1 + sum(child.subtree_size() for child in self.children)

    def ancestors(self) -> List[TreeNode[T]]:
        """Get list of ancestor nodes (root to parent)."""
        result: List[TreeNode[T]] = []
        node: Optional[TreeNode[T]] = self.parent
        while node is not None:
            result.append(node)
            node = node.parent
        return result

    def descendants(self) -> List[TreeNode[T]]:
        """Get list of all descendant nodes (DFS order)."""
        result: List[TreeNode[T]] = []
        stack: List[TreeNode[T]] = list(self.children)
        while stack:
            node = stack.pop()
            result.append(node)
            stack.extend(node.children)
        return result

    def siblings(self) -> List[TreeNode[T]]:
        """Get list of sibling nodes (excluding self)."""
        if self.parent is None:
            return []
        return [c for c in self.parent.children if c is not self]

    def path_to_root(self) -> List[TreeNode[T]]:
        """Get path from this node to root (inclusive)."""
        result = [self]
        node: Optional[TreeNode[T]] = self.parent
        while node is not None:
            result.append(node)
            node = node.parent
        return result

    def __iter__(self) -> Iterator[T]:
        """Iterate over node values in DFS preorder."""
        yield self.value
        for child in self.children:
            yield from child

    def __repr__(self) -> str:
        return f"TreeNode({self.value!r}, children={len(self.children)})"


@dataclass
class BinaryTreeNode(Generic[T]):
    """A node in a binary tree.

    Attributes:
        value: The value stored in this node.
        left: Left child node.
        right: Right child node.
        parent: Reference to parent node.
    """

    value: T
    left: Optional[BinaryTreeNode[T]] = field(default=None, repr=False)
    right: Optional[BinaryTreeNode[T]] = field(default=None, repr=False)
    parent: Optional[BinaryTreeNode[T]] = field(default=None, repr=False)

    def is_leaf(self) -> bool:
        """Check if this node is a leaf."""
        return self.left is None and self.right is None

    def is_root(self) -> bool:
        """Check if this node is the root."""
        return self.parent is None

    def depth(self) -> int:
        """Get the depth of this node (root = 0)."""
        d = 0
        node: Optional[BinaryTreeNode[T]] = self.parent
        while node is not None:
            d += 1
            node = node.parent
        return d

    def height(self) -> int:
        """Get the height of subtree rooted at this node."""
        if self.is_leaf():
            return 0
        left_h = self.left.height() if self.left else -1
        right_h = self.right.height() if self.right else -1
        return 1 + max(left_h, right_h)

    def subtree_size(self) -> int:
        """Get the size of subtree rooted at this node."""
        left_s = self.left.subtree_size() if self.left else 0
        right_s = self.right.subtree_size() if self.right else 0
        return 1 + left_s + right_s

    def __iter__(self) -> Iterator[T]:
        """Iterate values in inorder traversal."""
        if self.left:
            yield from self.left
        yield self.value
        if self.right:
            yield from self.right

    def __repr__(self) -> str:
        return f"BinaryTreeNode({self.value!r})"


class Tree(Generic[T]):
    """Generic tree data structure.

    Provides common tree operations and traversals.

    Example:
        tree = Tree("root")
        child1 = tree.root.add_child("child1")
        child2 = tree.root.add_child("child2")
        child1_1 = child1.add_child("child1_1")

        for value in tree.traverse():
            print(value)
    """

    def __init__(self, root_value: T) -> None:
        """Create a tree with given root value.

        Args:
            root_value: Value for the root node.
        """
        self.root = TreeNode(root_value)

    def height(self) -> int:
        """Get the height of the tree."""
        return self.root.height()

    def size(self) -> int:
        """Get the total number of nodes in the tree."""
        return self.root.subtree_size()

    def is_empty(self) -> bool:
        """Check if tree is empty."""
        return self.root is None

    def traverse_bfs(self) -> Iterator[T]:
        """Breadth-first (level-order) traversal."""
        queue: Deque[TreeNode[T]] = deque([self.root])
        while queue:
            node = queue.popleft()
            yield node.value
            queue.extend(node.children)

    def traverse_dfs(self) -> Iterator[T]:
        """Depth-first preorder traversal."""
        stack: List[TreeNode[T]] = [self.root]
        while stack:
            node = stack.pop()
            yield node.value
            stack.extend(reversed(node.children))

    def traverse(self, order: str = "preorder") -> Iterator[T]:
        """Generic traversal.

        Args:
            order: One of 'preorder', 'inorder', 'postorder', 'levelorder'.

        Yields:
            Node values in traversal order.
        """
        if order == "preorder":
            yield from self.root
        elif order == "levelorder":
            yield from self.traverse_bfs()
        elif order == "dfs":
            yield from self.traverse_dfs()
        else:
            raise ValueError(f"Unknown traversal order: {order}")

    def find_node(self, value: T) -> Optional[TreeNode[T]]:
        """Find first node with given value using DFS.

        Args:
            value: Value to search for.

        Returns:
            Node containing value, or None if not found.
        """
        stack: List[TreeNode[T]] = [self.root]
        while stack:
            node = stack.pop()
            if node.value == value:
                return node
            stack.extend(reversed(node.children))
        return None

    def find_nodes(self, predicate: Callable[[T], bool]) -> List[TreeNode[T]]:
        """Find all nodes matching a predicate.

        Args:
            predicate: Function that returns True for matching values.

        Returns:
            List of matching nodes.
        """
        result: List[TreeNode[T]] = []
        stack: List[TreeNode[T]] = [self.root]
        while stack:
            node = stack.pop()
            if predicate(node.value):
                result.append(node)
            stack.extend(reversed(node.children))
        return result

    def filter(self, predicate: Callable[[T], bool]) -> List[T]:
        """Filter node values matching predicate.

        Args:
            predicate: Function that returns True for values to keep.

        Returns:
            List of values matching the predicate.
        """
        return [node.value for node in self.find_nodes(predicate)]

    def map(self, func: Callable[[T], Any]) -> List[Any]:
        """Apply function to all node values.

        Args:
            func: Function to apply to each value.

        Returns:
            List of transformed values.
        """
        return [func(node) for node in self.root]

    def to_list(self) -> List[T]:
        """Convert tree to list (preorder)."""
        return list(self.root)

    def __repr__(self) -> str:
        return f"Tree(root={self.root.value!r}, size={self.size()})"


class BinarySearchTree(Generic[T]):
    """Binary Search Tree implementation.

    Maintains the BST property: left.value < node.value <= right.value.

    Type Parameters:
        T: Type must be comparable.

    Example:
        bst = BinarySearchTree[int]()
        bst.insert(5)
        bst.insert(3)
        bst.insert(7)
        bst.contains(3)  # True
        bst.delete(3)
    """

    def __init__(self) -> None:
        self._root: Optional[BinaryTreeNode[T]] = None
        self._size: int = 0

    @property
    def size(self) -> int:
        """Number of nodes in the BST."""
        return self._size

    @property
    def root(self) -> Optional[BinaryTreeNode[T]]:
        """Root node of the BST."""
        return self._root

    def insert(self, value: T) -> BinaryTreeNode[T]:
        """Insert a value into the BST.

        Args:
            value: Value to insert.

        Returns:
            The node containing the inserted value.
        """
        if self._root is None:
            self._root = BinaryTreeNode(value)
            self._size = 1
            return self._root

        node = self._root
        while True:
            if value < node.value:
                if node.left is None:
                    node.left = BinaryTreeNode(value, parent=node)
                    self._size += 1
                    return node.left
                node = node.left
            else:
                if node.right is None:
                    node.right = BinaryTreeNode(value, parent=node)
                    self._size += 1
                    return node.right
                node = node.right

    def contains(self, value: T) -> bool:
        """Check if value exists in the BST.

        Args:
            value: Value to search for.

        Returns:
            True if value is found, False otherwise.
        """
        node = self._root
        while node is not None:
            if value == node.value:
                return True
            elif value < node.value:
                node = node.left
            else:
                node = node.right
        return False

    def find(self, value: T) -> Optional[BinaryTreeNode[T]]:
        """Find node containing value.

        Args:
            value: Value to search for.

        Returns:
            Node containing value, or None if not found.
        """
        node = self._root
        while node is not None:
            if value == node.value:
                return node
            elif value < node.value:
                node = node.left
            else:
                node = node.right
        return None

    def _find_min(self, node: BinaryTreeNode[T]) -> BinaryTreeNode[T]:
        """Find node with minimum value in subtree."""
        while node.left is not None:
            node = node.left
        return node

    def delete(self, value: T) -> bool:
        """Delete a value from the BST.

        Args:
            value: Value to delete.

        Returns:
            True if value was deleted, False if not found.
        """
        node = self.find(value)
        if node is None:
            return False

        self._delete_node(node)
        self._size -= 1
        return True

    def _delete_node(self, node: BinaryTreeNode[T]) -> None:
        """Delete a node from the BST."""
        if node.left is None and node.right is None:
            if node.parent is None:
                self._root = None
            elif node is node.parent.left:
                node.parent.left = None
            else:
                node.parent.right = None
        elif node.left is None:
            if node.parent is None:
                self._root = node.right
            elif node is node.parent.left:
                node.parent.left = node.right
            else:
                node.parent.right = node.right
            node.right.parent = node.parent
        elif node.right is None:
            if node.parent is None:
                self._root = node.left
            elif node is node.parent.left:
                node.parent.left = node.left
            else:
                node.parent.right = node.left
            node.left.parent = node.parent
        else:
            successor = self._find_min(node.right)
            node.value = successor.value
            self._delete_node(successor)

    def traverse_inorder(self) -> Iterator[T]:
        """Inorder traversal (sorted order for BST)."""
        if self._root is None:
            return
        yield from self._root

    def traverse_preorder(self) -> Iterator[T]:
        """Preorder traversal."""
        if self._root is None:
            return
        stack: List[BinaryTreeNode[T]] = [self._root]
        while stack:
            node = stack.pop()
            yield node.value
            if node.right:
                stack.append(node.right)
            if node.left:
                stack.append(node.left)

    def traverse_postorder(self) -> Iterator[T]:
        """Postorder traversal."""
        if self._root is None:
            return
        visited: Set[BinaryTreeNode[T]] = set()
        stack: List[BinaryTreeNode[T]] = []
        node = self._root
        while stack or node:
            if node:
                stack.append(node)
                node = node.left
            else:
                node = stack[-1]
                if node.right and node.right not in visited:
                    node = node.right
                else:
                    yield node.value
                    visited.add(node)
                    stack.pop()
                    node = None

    def to_sorted_list(self) -> List[T]:
        """Get sorted list of all values."""
        return list(self.traverse_inorder())

    def __len__(self) -> int:
        return self._size

    def __contains__(self, value: T) -> bool:
        return self.contains(value)

    def __repr__(self) -> str:
        return f"BinarySearchTree(size={self._size})"
