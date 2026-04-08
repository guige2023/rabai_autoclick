"""Iterator action module for RabAI AutoClick.

Provides iterator pattern implementation:
- Iterator: Abstract iterator interface
- ConcreteIterator: Specific iterator implementation
- Iterable: Collection that can be iterated
- TreeIterator: Iterate tree structures
- FilteredIterator: Iterator with filtering
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Generic
from abc import ABC, abstractmethod
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")


class Iterator(ABC, Generic[T]):
    """Abstract iterator interface."""

    @abstractmethod
    def has_next(self) -> bool:
        """Check if there's a next element."""
        pass

    @abstractmethod
    def next(self) -> Optional[T]:
        """Get next element."""
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the iterator."""
        pass


class ConcreteIterator(Iterator[T]):
    """Concrete iterator implementation."""

    def __init__(self, items: List[T]):
        self._items = items
        self._index = 0

    def has_next(self) -> bool:
        """Check if there's a next element."""
        return self._index < len(self._items)

    def next(self) -> Optional[T]:
        """Get next element."""
        if self.has_next():
            item = self._items[self._index]
            self._index += 1
            return item
        return None

    def reset(self) -> None:
        """Reset the iterator."""
        self._index = 0

    def get_current(self) -> Optional[T]:
        """Get current element without advancing."""
        if 0 <= self._index < len(self._items):
            return self._items[self._index]
        return None

    def get_position(self) -> int:
        """Get current position."""
        return self._index


class ReverseIterator(Iterator[T]):
    """Reverse iterator."""

    def __init__(self, items: List[T]):
        self._items = items
        self._index = len(items) - 1

    def has_next(self) -> bool:
        """Check if there's a next element."""
        return self._index >= 0

    def next(self) -> Optional[T]:
        """Get next element."""
        if self.has_next():
            item = self._items[self._index]
            self._index -= 1
            return item
        return None

    def reset(self) -> None:
        """Reset the iterator."""
        self._index = len(self._items) - 1


class FilteredIterator(Iterator[T]):
    """Iterator with filtering."""

    def __init__(self, items: List[T], predicate: Callable[[T], bool]):
        self._items = items
        self._predicate = predicate
        self._index = 0
        self._filtered: Optional[List[T]] = None

    def _build_filtered(self) -> List[T]:
        """Build filtered list."""
        if self._filtered is None:
            self._filtered = [item for item in self._items if self._predicate(item)]
        return self._filtered

    def has_next(self) -> bool:
        """Check if there's a next element."""
        filtered = self._build_filtered()
        return self._index < len(filtered)

    def next(self) -> Optional[T]:
        """Get next element."""
        filtered = self._build_filtered()
        if self._index < len(filtered):
            item = filtered[self._index]
            self._index += 1
            return item
        return None

    def reset(self) -> None:
        """Reset the iterator."""
        self._index = 0


class TransformIterator(Iterator[Any]):
    """Iterator that transforms items."""

    def __init__(self, items: List[T], transformer: Callable[[T], Any]):
        self._items = items
        self._transformer = transformer
        self._index = 0

    def has_next(self) -> bool:
        """Check if there's a next element."""
        return self._index < len(self._items)

    def next(self) -> Any:
        """Get next transformed element."""
        if self.has_next():
            item = self._transformer(self._items[self._index])
            self._index += 1
            return item
        return None

    def reset(self) -> None:
        """Reset the iterator."""
        self._index = 0


class BatchIterator(Iterator[List[T]]):
    """Iterator that yields batches."""

    def __init__(self, items: List[T], batch_size: int):
        self._items = items
        self._batch_size = batch_size
        self._index = 0

    def has_next(self) -> bool:
        """Check if there's a next batch."""
        return self._index < len(self._items)

    def next(self) -> List[T]:
        """Get next batch."""
        if self.has_next():
            batch = self._items[self._index:self._index + self._batch_size]
            self._index += self._batch_size
            return batch
        return []

    def reset(self) -> None:
        """Reset the iterator."""
        self._index = 0


class TreeNode:
    """Tree node for iteration."""

    def __init__(self, node_id: str, value: Any):
        self.node_id = node_id
        self.value = value
        self.children: List["TreeNode"] = []

    def add_child(self, child: "TreeNode") -> None:
        """Add a child node."""
        self.children.append(child)


class TreeIterator(Iterator[Any]):
    """Iterator for tree structures."""

    def __init__(self, root: TreeNode, traversal: str = "dfs"):
        self._root = root
        self._traversal = traversal
        self._stack: List[TreeNode] = []
        self._queue: List[TreeNode] = []
        self._visited: set = set()
        self._init_iterator()

    def _init_iterator(self) -> None:
        """Initialize iterator."""
        if self._traversal == "dfs":
            if self._root:
                self._stack.append(self._root)
        elif self._traversal == "bfs":
            if self._root:
                self._queue.append(self._root)

    def has_next(self) -> bool:
        """Check if there's a next element."""
        if self._traversal == "dfs":
            return len(self._stack) > 0
        elif self._traversal == "bfs":
            return len(self._queue) > 0
        return False

    def next(self) -> Any:
        """Get next element."""
        if self._traversal == "dfs":
            return self._next_dfs()
        elif self._traversal == "bfs":
            return self._next_bfs()
        return None

    def _next_dfs(self) -> Any:
        """Get next using DFS."""
        while self._stack:
            node = self._stack.pop()
            if node.node_id in self._visited:
                continue
            self._visited.add(node.node_id)
            for child in reversed(node.children):
                if child.node_id not in self._visited:
                    self._stack.append(child)
            return node.value
        return None

    def _next_bfs(self) -> Any:
        """Get next using BFS."""
        while self._queue:
            node = self._queue.pop(0)
            if node.node_id in self._visited:
                continue
            self._visited.add(node.node_id)
            for child in node.children:
                if child.node_id not in self._visited:
                    self._queue.append(child)
            return node.value
        return None

    def reset(self) -> None:
        """Reset the iterator."""
        self._visited.clear()
        self._stack.clear()
        self._queue.clear()
        self._init_iterator()


class Enumerator:
    """Enumerable collection."""

    def __init__(self, items: List[T]):
        self._items = items

    def __iter__(self) -> Iterator[T]:
        """Return iterator."""
        return ConcreteIterator(self._items)

    def iterate(self) -> Iterator[T]:
        """Return iterator."""
        return ConcreteIterator(self._items)

    def reverse(self) -> Iterator[T]:
        """Return reverse iterator."""
        return ReverseIterator(self._items)

    def filter(self, predicate: Callable[[T], bool]) -> FilteredIterator[T]:
        """Return filtered iterator."""
        return FilteredIterator(self._items, predicate)

    def transform(self, transformer: Callable[[T], Any]) -> TransformIterator[T]:
        """Return transforming iterator."""
        return TransformIterator(self._items, transformer)

    def batch(self, size: int) -> BatchIterator[T]:
        """Return batch iterator."""
        return BatchIterator(self._items, size)

    def tree(self, root_id: str, traversal: str = "dfs") -> Optional[TreeIterator]:
        """Return tree iterator if items are tree nodes."""
        for item in self._items:
            if isinstance(item, TreeNode) and item.node_id == root_id:
                return TreeIterator(item, traversal)
        return None


class IteratorAction(BaseAction):
    """Iterator pattern action."""
    action_type = "iterator"
    display_name = "迭代器模式"
    description = "数据迭代遍历"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "iterate")

            if operation == "iterate":
                return self._iterate(params)
            elif operation == "filter":
                return self._filter(params)
            elif operation == "transform":
                return self._transform(params)
            elif operation == "batch":
                return self._batch(params)
            elif operation == "tree":
                return self._tree(params)
            elif operation == "collect":
                return self._collect(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Iterator error: {str(e)}")

    def _iterate(self, params: Dict[str, Any]) -> ActionResult:
        """Iterate over items."""
        items = params.get("items", [])
        limit = params.get("limit", 100)
        reverse = params.get("reverse", False)

        iterator = ReverseIterator(items) if reverse else ConcreteIterator(items)

        results = []
        count = 0
        while iterator.has_next() and count < limit:
            item = iterator.next()
            if item is not None:
                results.append(item)
                count += 1

        return ActionResult(success=True, message=f"Iterated {len(results)} items", data={"items": results, "count": len(results)})

    def _filter(self, params: Dict[str, Any]) -> ActionResult:
        """Filter and iterate."""
        items = params.get("items", [])
        predicate_fn = params.get("predicate")

        if not predicate_fn:
            def default_predicate(x): return bool(x)
            predicate_fn = default_predicate

        iterator = FilteredIterator(items, predicate_fn)

        results = []
        while iterator.has_next():
            item = iterator.next()
            if item is not None:
                results.append(item)

        return ActionResult(success=True, message=f"Filtered to {len(results)} items", data={"items": results, "count": len(results)})

    def _transform(self, params: Dict[str, Any]) -> ActionResult:
        """Transform and iterate."""
        items = params.get("items", [])
        transformer_fn = params.get("transformer")

        if not transformer_fn:
            return ActionResult(success=False, message="transformer is required")

        iterator = TransformIterator(items, transformer_fn)

        results = []
        while iterator.has_next():
            item = iterator.next()
            if item is not None:
                results.append(item)

        return ActionResult(success=True, message=f"Transformed {len(results)} items", data={"items": results, "count": len(results)})

    def _batch(self, params: Dict[str, Any]) -> ActionResult:
        """Batch iterate."""
        items = params.get("items", [])
        batch_size = params.get("batch_size", 10)

        iterator = BatchIterator(items, batch_size)

        batches = []
        while iterator.has_next():
            batch = iterator.next()
            if batch:
                batches.append(batch)

        return ActionResult(success=True, message=f"{len(batches)} batches", data={"batches": batches, "batch_count": len(batches)})

    def _tree(self, params: Dict[str, Any]) -> ActionResult:
        """Tree traverse."""
        nodes = params.get("nodes", [])
        root_id = params.get("root_id")
        traversal = params.get("traversal", "dfs")

        node_map = {n.node_id: n for n in nodes if isinstance(n, TreeNode)}
        root = node_map.get(root_id)

        if not root:
            return ActionResult(success=False, message=f"Root not found: {root_id}")

        iterator = TreeIterator(root, traversal)

        results = []
        while iterator.has_next():
            item = iterator.next()
            if item is not None:
                results.append(item)

        return ActionResult(success=True, message=f"Traversed {len(results)} nodes", data={"items": results, "count": len(results)})

    def _collect(self, params: Dict[str, Any]) -> ActionResult:
        """Collect with multiple operations."""
        items = params.get("items", [])
        operations = params.get("operations", [])

        enumerator = Enumerator(items)

        for op in operations:
            op_type = op.get("type")

            if op_type == "filter":
                predicate = op.get("predicate", lambda x: bool(x))
                enumerator = enumerator.filter(predicate)
            elif op_type == "transform":
                transformer = op.get("transformer", lambda x: x)
                enumerator = enumerator.transform(transformer)
            elif op_type == "reverse":
                enumerator = enumerator.reverse()

        results = list(enumerator.iterate())

        return ActionResult(success=True, message=f"Collected {len(results)} items", data={"items": results, "count": len(results)})
