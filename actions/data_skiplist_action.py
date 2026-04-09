"""Data Skip List Action Module.

Provides a Skip List data structure implementation for
efficient sorted data storage and fast lookup.
"""

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class SkipListNode:
    """A node in the skip list."""
    key: Any
    value: Any
    forward: List["SkipListNode"] = field(default_factory=list)  # Forward pointers at each level


class SkipList:
    """Skip list implementation."""

    MAX_LEVEL = 16

    def __init__(self) -> None:
        self.level = 0
        self.header = SkipListNode(key=None, value=None, forward=[None] * (self.MAX_LEVEL + 1))
        self.size = 0

    def random_level(self) -> int:
        """Generate a random level for a new node."""
        level = 0
        while random.random() < 0.5 and level < self.MAX_LEVEL:
            level += 1
        return level

    def insert(self, key: Any, value: Any) -> bool:
        """Insert a key-value pair."""
        update = [None] * (self.MAX_LEVEL + 1)
        current = self.header

        # Find position to insert at each level
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            # Update existing key
            current.value = value
            return False
        else:
            # Insert new node
            new_level = self.random_level()
            if new_level > self.level:
                for i in range(self.level + 1, new_level + 1):
                    update[i] = self.header
                self.level = new_level

            new_node = SkipListNode(key=key, value=value, forward=[None] * (new_level + 1))
            for i in range(new_level + 1):
                new_node.forward[i] = update[i].forward[i]
                update[i].forward[i] = new_node

            self.size += 1
            return True

    def search(self, key: Any) -> Optional[Any]:
        """Search for a key and return its value."""
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
        current = current.forward[0]
        if current and current.key == key:
            return current.value
        return None

    def delete(self, key: Any) -> bool:
        """Delete a key from the skip list."""
        update = [None] * (self.MAX_LEVEL + 1)
        current = self.header

        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.key == key:
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]

            while self.level > 0 and self.header.forward[self.level] is None:
                self.level -= 1
            self.size -= 1
            return True
        return False

    def range_query(self, min_key: Any, max_key: Any) -> List[Tuple[Any, Any]]:
        """Get all key-value pairs in a range."""
        results = []
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < min_key:
                current = current.forward[i]
        current = current.forward[0]
        while current and current.key <= max_key:
            results.append((current.key, current.value))
            current = current.forward[0]
        return results

    def get_all(self) -> List[Tuple[Any, Any]]:
        """Get all items in sorted order."""
        results = []
        current = self.header.forward[0]
        while current:
            results.append((current.key, current.value))
            current = current.forward[0]
        return results

    def rank(self, key: Any) -> int:
        """Get the rank (1-based) of a key."""
        rank = 0
        current = self.header
        for i in range(self.level, -1, -1):
            while current.forward[i] and current.forward[i].key < key:
                rank += 1
                current = current.forward[i]
        current = current.forward[0]
        if current and current.key == key:
            return rank + 1
        return -1

    def kth(self, k: int) -> Optional[Tuple[Any, Any]]:
        """Get the k-th smallest element (1-based)."""
        current = self.header.forward[0]
        count = 0
        while current:
            count += 1
            if count == k:
                return (current.key, current.value)
            current = current.forward[0]
        return None


class DataSkipListAction(BaseAction):
    """Skip List data structure action.

    Provides efficient sorted data storage with O(log n) operations
    for insert, delete, search, and range queries.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (insert, search, delete, range, rank, kth, get_all, clear, status)
            - key: Key for single operations
            - value: Value for insert operations
            - items: List of (key, value) tuples for batch insert
            - min_key: Minimum key for range queries
            - max_key: Maximum key for range queries
            - k: Rank or k-th position
            - dataset_id: Identifier for the skip list
    """
    action_type = "data_skiplist"
    display_name = "数据跳跃表"
    description = "跳跃表数据结构用于高效有序存储"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "key": None,
            "value": None,
            "items": [],
            "min_key": None,
            "max_key": None,
            "k": None,
            "dataset_id": "default",
        }

    def __init__(self) -> None:
        super().__init__()
        self._lists: Dict[str, SkipList] = {}

    def _get_list(self, dataset_id: str) -> SkipList:
        """Get or create a skip list."""
        if dataset_id not in self._lists:
            self._lists[dataset_id] = SkipList()
        return self._lists[dataset_id]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute skip list operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        key = params.get("key")
        value = params.get("value")
        items = params.get("items", [])
        min_key = params.get("min_key")
        max_key = params.get("max_key")
        k = params.get("k")
        dataset_id = params.get("dataset_id", "default")

        sl = self._get_list(dataset_id)

        if operation == "insert":
            return self._insert(sl, key, value, dataset_id, start_time)
        elif operation == "batch_insert":
            return self._batch_insert(sl, items, dataset_id, start_time)
        elif operation == "search":
            return self._search(sl, key, dataset_id, start_time)
        elif operation == "delete":
            return self._delete(sl, key, dataset_id, start_time)
        elif operation == "range":
            return self._range_query(sl, min_key, max_key, dataset_id, start_time)
        elif operation == "rank":
            return self._get_rank(sl, key, dataset_id, start_time)
        elif operation == "kth":
            return self._get_kth(sl, k, dataset_id, start_time)
        elif operation == "get_all":
            return self._get_all(sl, dataset_id, start_time)
        elif operation == "clear":
            return self._clear(sl, dataset_id, start_time)
        elif operation == "status":
            return self._status(sl, dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _insert(
        self,
        sl: SkipList,
        key: Any,
        value: Any,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Insert a key-value pair."""
        if key is None or value is None:
            return ActionResult(success=False, message="key and value required for insert", duration=time.time() - start_time)

        inserted = sl.insert(key, value)
        return ActionResult(
            success=True,
            message=f"{'Inserted' if inserted else 'Updated'} key '{key}' in '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "key": key,
                "inserted": inserted,
                "size": sl.size,
            },
            duration=time.time() - start_time
        )

    def _batch_insert(
        self,
        sl: SkipList,
        items: List[Tuple],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Batch insert items."""
        inserted_count = 0
        updated_count = 0
        for item in items:
            if len(item) >= 2:
                key, value = item[0], item[1]
                if sl.insert(key, value):
                    inserted_count += 1
                else:
                    updated_count += 1

        return ActionResult(
            success=True,
            message=f"Batch insert: {inserted_count} new, {updated_count} updated",
            data={
                "dataset_id": dataset_id,
                "inserted": inserted_count,
                "updated": updated_count,
                "size": sl.size,
            },
            duration=time.time() - start_time
        )

    def _search(
        self,
        sl: SkipList,
        key: Any,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Search for a key."""
        if key is None:
            return ActionResult(success=False, message="key required for search", duration=time.time() - start_time)

        result = sl.search(key)
        return ActionResult(
            success=result is not None,
            message=f"{'Found' if result is not None else 'Not found'} key '{key}' in '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "key": key,
                "found": result is not None,
                "value": result,
            },
            duration=time.time() - start_time
        )

    def _delete(
        self,
        sl: SkipList,
        key: Any,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Delete a key."""
        if key is None:
            return ActionResult(success=False, message="key required for delete", duration=time.time() - start_time)

        deleted = sl.delete(key)
        return ActionResult(
            success=deleted,
            message=f"{'Deleted' if deleted else 'Not found'} key '{key}' from '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "key": key,
                "deleted": deleted,
                "size": sl.size,
            },
            duration=time.time() - start_time
        )

    def _range_query(
        self,
        sl: SkipList,
        min_key: Any,
        max_key: Any,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Query a range of keys."""
        if min_key is None or max_key is None:
            return ActionResult(success=False, message="min_key and max_key required for range query", duration=time.time() - start_time)

        results = sl.range_query(min_key, max_key)
        return ActionResult(
            success=True,
            message=f"Range query [{min_key}, {max_key}] returned {len(results)} items",
            data={
                "dataset_id": dataset_id,
                "min_key": min_key,
                "max_key": max_key,
                "count": len(results),
                "items": results,
            },
            duration=time.time() - start_time
        )

    def _get_rank(
        self,
        sl: SkipList,
        key: Any,
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get rank of a key."""
        if key is None:
            return ActionResult(success=False, message="key required for rank", duration=time.time() - start_time)

        rank = sl.rank(key)
        return ActionResult(
            success=rank > 0,
            message=f"Rank of '{key}': {rank}" if rank > 0 else f"Key '{key}' not found",
            data={
                "dataset_id": dataset_id,
                "key": key,
                "rank": rank,
            },
            duration=time.time() - start_time
        )

    def _get_kth(
        self,
        sl: SkipList,
        k: Optional[int],
        dataset_id: str,
        start_time: float
    ) -> ActionResult:
        """Get k-th smallest element."""
        if k is None:
            return ActionResult(success=False, message="k required for kth", duration=time.time() - start_time)

        result = sl.kth(k)
        return ActionResult(
            success=result is not None,
            message=f"{k}-th smallest: {result}" if result else f"No {k}-th element exists",
            data={
                "dataset_id": dataset_id,
                "k": k,
                "result": result,
            },
            duration=time.time() - start_time
        )

    def _get_all(self, sl: SkipList, dataset_id: str, start_time: float) -> ActionResult:
        """Get all items."""
        items = sl.get_all()
        return ActionResult(
            success=True,
            message=f"Retrieved all {len(items)} items from '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "size": len(items),
                "items": items,
            },
            duration=time.time() - start_time
        )

    def _clear(self, sl: SkipList, dataset_id: str, start_time: float) -> ActionResult:
        """Clear the skip list."""
        size = sl.size
        sl.size = 0
        sl.level = 0
        sl.header.forward = [None] * (sl.MAX_LEVEL + 1)
        return ActionResult(
            success=True,
            message=f"Cleared {size} items from '{dataset_id}'",
            data={"dataset_id": dataset_id, "cleared": size},
            duration=time.time() - start_time
        )

    def _status(self, sl: SkipList, dataset_id: str, start_time: float) -> ActionResult:
        """Get status of skip list."""
        return ActionResult(
            success=True,
            message=f"Skip list '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "size": sl.size,
                "level": sl.level,
                "max_level": sl.MAX_LEVEL,
            },
            duration=time.time() - start_time
        )
