"""Tree traversal utilities.

Provides tree data structure and traversal algorithms
for hierarchical data processing in automation workflows.
"""

from collections import deque
from typing import Any, Callable, Generic, Iterator, List, Optional, TypeVar


T = TypeVar("T")


class TreeNode(Generic[T]):
    """A node in a tree structure.

    Example:
        root = TreeNode("root")
        child1 = root.add_child("child1")
        child2 = root.add_child("child2")
        child1.add_child("grandchild")
    """

    def __init__(
        self,
        data: T,
        parent: Optional["TreeNode[T]"] = None,
    ) -> None:
        self.data = data
        self.parent = parent
        self.children: List["TreeNode[T]"] = []

    def add_child(self, data: T) -> "TreeNode[T]":
        """Add a child node.

        Args:
            data: Data for child node.

        Returns:
            New child node.
        """
        child = TreeNode(data, self)
        self.children.append(child)
        return child

    def remove_child(self, child: "TreeNode[T]") -> bool:
        """Remove a child node.

        Args:
            child: Child to remove.

        Returns:
            True if removed.
        """
        if child in self.children:
            self.children.remove(child)
            child.parent = None
            return True
        return False

    @property
    def is_root(self) -> bool:
        """Check if node is root."""
        return self.parent is None

    @property
    def is_leaf(self) -> bool:
        """Check if node is leaf."""
        return len(self.children) == 0

    @property
    def depth(self) -> int:
        """Get depth of node (root = 0)."""
        depth = 0
        node: Optional[TreeNode] = self.parent
        while node:
            depth += 1
            node = node.parent
        return depth

    @property
    def height(self) -> int:
        """Get height of subtree (leaf = 1)."""
        if self.is_leaf:
            return 1
        return 1 + max(child.height for child in self.children)

    def ancestors(self) -> List["TreeNode[T]"]:
        """Get list of ancestors (parent, grandparent, ...)."""
        result: List[TreeNode[T]] = []
        node = self.parent
        while node:
            result.append(node)
            node = node.parent
        return result

    def siblings(self) -> List["TreeNode[T]"]:
        """Get sibling nodes (excluding self)."""
        if self.parent is None:
            return []
        return [c for c in self.parent.children if c is not self]

    def root(self) -> "TreeNode[T]":
        """Get root node of tree."""
        node: TreeNode[T] = self
        while node.parent:
            node = node.parent
        return node

    def traverse_bfs(self) -> Iterator["TreeNode[T]"]:
        """Breadth-first traversal."""
        queue = deque([self])
        while queue:
            node = queue.popleft()
            yield node
            queue.extend(node.children)

    def traverse_dfs_preorder(self) -> Iterator["TreeNode[T]"]:
        """Depth-first pre-order traversal."""
        yield self
        for child in self.children:
            yield from child.traverse_dfs_preorder()

    def traverse_dfs_postorder(self) -> Iterator["TreeNode[T]"]:
        """Depth-first post-order traversal."""
        for child in self.children:
            yield from child.traverse_dfs_postorder()
        yield self

    def find(self, predicate: Callable[[T], bool]) -> Optional["TreeNode[T]"]:
        """Find first node matching predicate.

        Args:
            predicate: Condition function.

        Returns:
            First matching node or None.
        """
        if predicate(self.data):
            return self
        for child in self.children:
            found = child.find(predicate)
            if found:
                return found
        return None

    def find_all(self, predicate: Callable[[T], bool]) -> List["TreeNode[T]"]:
        """Find all nodes matching predicate.

        Args:
            predicate: Condition function.

        Returns:
            List of matching nodes.
        """
        results: List[TreeNode[T]] = []
        if predicate(self.data):
            results.append(self)
        for child in self.children:
            results.extend(child.find_all(predicate))
        return results

    def to_dict(self) -> dict:
        """Convert subtree to nested dictionary."""
        return {
            "data": self.data,
            "children": [child.to_dict() for child in self.children],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TreeNode[Any]":
        """Create tree from nested dictionary."""
        node = cls(d["data"])
        for child_d in d.get("children", []):
            node.add_child_from_dict(child_d)
        return node

    def add_child_from_dict(self, d: dict) -> "TreeNode[Any]":
        """Add child from dictionary."""
        child = self.add_child(d["data"])
        for child_d in d.get("children", []):
            child.add_child_from_dict(child_d)
        return child


class Tree(Generic[T]):
    """Tree container with a root node.

    Example:
        tree = Tree("root")
        tree.root.add_child("child1")
        for node in tree.traverse_bfs():
            print(node.data)
    """

    def __init__(self, root_data: T) -> None:
        self.root = TreeNode(root_data)

    def traverse_bfs(self) -> Iterator[TreeNode[T]]:
        """Breadth-first traversal."""
        return self.root.traverse_bfs()

    def traverse_dfs_preorder(self) -> Iterator[TreeNode[T]]:
        """Depth-first pre-order traversal."""
        return self.root.traverse_dfs_preorder()

    def traverse_dfs_postorder(self) -> Iterator[TreeNode[T]]:
        """Depth-first post-order traversal."""
        return self.root.traverse_dfs_postorder()

    def find(self, predicate: Callable[[T], bool]) -> Optional[TreeNode[T]]:
        """Find first node matching predicate."""
        return self.root.find(predicate)

    def find_all(self, predicate: Callable[[T], bool]) -> List[TreeNode[T]]:
        """Find all nodes matching predicate."""
        return self.root.find_all(predicate)

    @property
    def height(self) -> int:
        """Get height of tree."""
        return self.root.height

    @property
    def size(self) -> int:
        """Get total number of nodes."""
        return sum(1 for _ in self.traverse_bfs())
