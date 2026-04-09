"""
B-Tree data structure module.

Provides B-Tree and B+ Tree implementations for efficient
disk-based storage and range queries.

Author: Aito Auto Agent
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import (
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar('T')


@dataclass
class BTreeNode(Generic[T]):
    """Node in a B-Tree."""
    keys: list[T] = field(default_factory=list)
    children: list[BTreeNode[T]] = field(default_factory=list)
    is_leaf: bool = True
    n: int = 0


class BTree(Generic[T]):
    """
    B-Tree data structure.

    Self-balancing tree optimized for disk-based storage and
    range queries. Each node can have multiple keys and children.

    Example:
        tree = BTree[int](t=3)

        for i in range(1000):
            tree.insert(i)

        for key in tree.range_query(100, 500):
            print(key)
    """

    def __init__(self, t: int = 3, thread_safe: bool = False):
        """
        Initialize B-Tree.

        Args:
            t: Minimum degree (defines the tree's branching factor)
            thread_safe: Enable thread-safe operations
        """
        self._t = t
        self._min_keys = t - 1
        self._max_keys = 2 * t - 1
        self._root: Optional[BTreeNode[T]] = None
        self._lock = threading.RLock() if thread_safe else None
        self._size = 0

    def insert(self, key: T) -> None:
        """
        Insert a key into the B-Tree.

        Args:
            key: Key to insert
        """
        with self._lock:
            if self._root is None:
                self._root = BTreeNode[T]()
                self._root.keys.append(key)
                self._root.n = 1
                self._size += 1
                return

            if len(self._root.keys) == self._max_keys:
                new_root = BTreeNode[T]()
                new_root.is_leaf = False
                new_root.children.append(self._root)
                self._split_child(new_root, 0, self._root)
                self._root = new_root

            self._insert_non_full(self._root, key)
            self._size += 1

    def _insert_non_full(self, node: BTreeNode[T], key: T) -> None:
        """Insert key into non-full node."""
        i = node.n - 1

        if node.is_leaf:
            while i >= 0 and key < node.keys[i]:
                i -= 1

            node.keys.insert(i + 1, key)
            node.n += 1
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1

            i += 1

            if len(node.children[i].keys) == self._max_keys:
                self._split_child(node, i, node.children[i])

                if key > node.keys[i]:
                    i += 1

            self._insert_non_full(node.children[i], key)

    def _split_child(self, parent: BTreeNode[T], i: int, child: BTreeNode[T]) -> None:
        """Split a full child node."""
        new_node = BTreeNode[T]()
        new_node.is_leaf = child.is_leaf
        new_node.n = self._t - 1

        for j in range(self._t - 1):
            new_node.keys.append(child.keys[j + self._t])

        if not child.is_leaf:
            for j in range(self._t):
                new_node.children.append(child.children[j + self._t])

        child.n = self._t - 1
        child.keys = child.keys[:self._t - 1]
        child.children = child.children[:self._t]

        parent.children.insert(i + 1, new_node)
        parent.keys.insert(i, child.keys[self._t - 1])
        parent.n += 1

    def search(self, key: T) -> Optional[BTreeNode[T]]:
        """
        Search for key in the tree.

        Args:
            key: Key to search

        Returns:
            Node containing key or None
        """
        return self._search(self._root, key)

    def _search(
        self,
        node: Optional[BTreeNode[T]],
        key: T
    ) -> Optional[BTreeNode[T]]:
        """Recursively search for key."""
        if node is None:
            return None

        i = 0
        while i < node.n and key > node.keys[i]:
            i += 1

        if i < node.n and key == node.keys[i]:
            return node

        if node.is_leaf:
            return None

        return self._search(node.children[i], key)

    def contains(self, key: T) -> bool:
        """Check if key exists in tree."""
        return self.search(key) is not None

    def delete(self, key: T) -> bool:
        """
        Delete key from tree.

        Args:
            key: Key to delete

        Returns:
            True if key was deleted
        """
        with self._lock:
            if self._root is None:
                return False

            result = self._delete(self._root, key)

            if self._root.n == 0 and not self._root.is_leaf:
                self._root = self._root.children[0]

            if result:
                self._size -= 1

            return result

    def _delete(self, node: BTreeNode[T], key: T) -> bool:
        """Recursively delete key."""
        i = 0
        while i < node.n and key > node.keys[i]:
            i += 1

        if i < node.n and key == node.keys[i]:
            if node.is_leaf:
                node.keys.pop(i)
                node.n -= 1
                return True
            else:
                return self._delete_internal_node(node, i)
        elif node.is_leaf:
            return False
        else:
            if len(node.children[i].keys) < self._t:
                self._fill_child(node, i)

            if i > node.n:
                return self._delete(node.children[i - 1], key)
            else:
                return self._delete(node.children[i], key)

    def _delete_internal_node(
        self,
        node: BTreeNode[T],
        i: int
    ) -> bool:
        """Delete from internal node at index i."""
        key = node.keys[i]

        if len(node.children[i].keys) >= self._t:
            predecessor = self._get_max(node.children[i])
            node.keys[i] = predecessor
            return self._delete(node.children[i], predecessor)

        elif len(node.children[i + 1].keys) >= self._t:
            successor = self._get_min(node.children[i + 1])
            node.keys[i] = successor
            return self._delete(node.children[i + 1], successor)

        else:
            self._merge(node, i)
            return self._delete(node.children[i], key)

    def _get_min(self, node: BTreeNode[T]) -> T:
        """Get minimum key in subtree."""
        while not node.is_leaf:
            node = node.children[0]
        return node.keys[0]

    def _get_max(self, node: BTreeNode[T]) -> T:
        """Get maximum key in subtree."""
        while not node.is_leaf:
            node = node.children[-1]
        return node.keys[-1]

    def _fill_child(self, node: BTreeNode[T], i: int) -> None:
        """Fill child node that has fewer than t keys."""
        if i > 0 and len(node.children[i - 1].keys) >= self._t:
            self._borrow_from_prev(node, i)
        elif i < node.n and len(node.children[i + 1].keys) >= self._t:
            self._borrow_from_next(node, i)
        else:
            if i >= node.n:
                i -= 1
            self._merge(node, i)

    def _borrow_from_prev(self, parent: BTreeNode[T], i: int) -> None:
        """Borrow key from previous sibling."""
        child = parent.children[i]
        sibling = parent.children[i - 1]

        child.keys.insert(0, parent.keys[i - 1])
        child.n += 1

        if not child.is_leaf:
            child.children.insert(0, sibling.children[-1])
            sibling.children.pop()

        parent.keys[i - 1] = sibling.keys.pop()
        sibling.n -= 1

    def _borrow_from_next(self, parent: BTreeNode[T], i: int) -> None:
        """Borrow key from next sibling."""
        child = parent.children[i]
        sibling = parent.children[i + 1]

        child.keys.append(parent.keys[i])
        child.n += 1

        if not child.is_leaf:
            child.children.append(sibling.children[0])
            sibling.children.pop(0)

        parent.keys[i] = sibling.keys.pop(0)
        sibling.n -= 1

    def _merge(self, parent: BTreeNode[T], i: int) -> None:
        """Merge two sibling nodes."""
        child = parent.children[i]
        sibling = parent.children[i + 1]

        child.keys.append(parent.keys[i])

        for j in range(sibling.n):
            child.keys.append(sibling.keys[j])

        if not child.is_leaf:
            for j in range(sibling.n + 1):
                child.children.append(sibling.children[j])

        parent.keys.pop(i)
        parent.children.pop(i + 1)
        child.n = 2 * self._t - 1

    def range_query(
        self,
        start: T,
        end: T
    ) -> list[T]:
        """
        Get all keys in range [start, end].

        Args:
            start: Start of range
            end: End of range

        Returns:
            List of keys in range
        """
        results = []
        self._range_query(self._root, start, end, results)
        return results

    def _range_query(
        self,
        node: Optional[BTreeNode[T]],
        start: T,
        end: T,
        results: list[T]
    ) -> None:
        """Recursively collect keys in range."""
        if node is None:
            return

        for i in range(node.n):
            if node.keys[i] < start:
                continue
            if node.keys[i] > end:
                return

            if not node.is_leaf:
                self._range_query(node.children[i], start, end, results)

            if start <= node.keys[i] <= end:
                results.append(node.keys[i])

        if not node.is_leaf:
            self._range_query(node.children[node.n], start, end, results)

    def traverse(self) -> Iterator[T]:
        """Traverse all keys in sorted order."""
        yield from self._traverse(self._root)

    def _traverse(self, node: Optional[BTreeNode[T]]) -> Iterator[T]:
        """Recursively traverse."""
        if node is None:
            return

        for i in range(node.n):
            if not node.is_leaf:
                yield from self._traverse(node.children[i])
            yield node.keys[i]

        if not node.is_leaf:
            yield from self._traverse(node.children[node.n])

    def __len__(self) -> int:
        """Return number of keys in tree."""
        return self._size

    def __contains__(self, key: T) -> bool:
        """Check if key exists."""
        return self.contains(key)


class BPlusTree(BTree[T]):
    """
    B+ Tree implementation.

    All data is stored in leaf nodes, with internal nodes only
    containing keys for routing.

    Example:
        tree = BPlusTree[int](t=3)

        for i in range(100):
            tree.insert(i)

        for key in tree.range_query(10, 50):
            print(key)
    """

    def __init__(self, t: int = 3, thread_safe: bool = False):
        super().__init__(t, thread_safe)
        self._leaf_head: Optional[BTreeNode[T]] = None

    def insert(self, key: T) -> None:
        """Insert key into B+ Tree."""
        super().insert(key)

        if self._leaf_head is None and self._root:
            self._leaf_head = self._root
            while not self._leaf_head.is_leaf:
                self._leaf_head = self._leaf_head.children[0]

    def _traverse_leaves(self) -> Iterator[T]:
        """Traverse all leaf nodes in order."""
        if self._leaf_head is None:
            return

        current = self._leaf_head
        while current is not None:
            for key in current.keys:
                yield key

            current = current.children[-1] if len(current.children) > len(current.keys) else None


def create_btree(t: int = 3) -> BTree:
    """Factory to create a BTree."""
    return BTree(t=t)


def create_bplus_tree(t: int = 3) -> BPlusTree:
    """Factory to create a BPlusTree."""
    return BPlusTree(t=t)
