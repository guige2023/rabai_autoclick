"""Data LSM Tree Action Module.

Provides a Log-Structured Merge Tree implementation for
high-throughput write workloads with tiered storage.
"""

import logging
import time
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class LSMLevel:
    """A level in the LSM tree."""
    level_id: int
    max_size: int
    current_size: int = 0
    tables: OrderedDict = field(default_factory=OrderedDict)


class DataLSMTreeAction(BaseAction):
    """LSM Tree data structure action.

    Provides high-throughput key-value storage optimized for
    write-heavy workloads using tiered compaction.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (create, write, read, delete, range, clear, status)
            - key: Key for single operations
            - value: Value for write operations
            - items: List of (key, value) tuples for batch write
            - dataset_id: Identifier for the LSM tree
            - num_levels: Number of levels in the tree
    """
    action_type = "data_lsm_tree"
    display_name = "数据LSM树"
    description = "日志结构合并树用于高速写入"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "key": None,
            "value": None,
            "items": [],
            "dataset_id": "default",
            "num_levels": 4,
            "base_size": 1000,
            "size_ratio": 10,
        }

    def __init__(self) -> None:
        super().__init__()
        self._trees: Dict[str, Dict[str, Any]] = {}
        self._memtable: Dict[str, Any] = {}

    def _get_or_create_tree(self, dataset_id: str, num_levels: int, base_size: int, size_ratio: int) -> Dict[str, Any]:
        """Get or create an LSM tree."""
        if dataset_id not in self._trees:
            levels = []
            for i in range(num_levels):
                max_size = base_size * (size_ratio ** i)
                levels.append(LSMLevel(level_id=i, max_size=max_size, current_size=0, tables=OrderedDict()))
            self._trees[dataset_id] = {
                "levels": levels,
                "memtable": {},
                "size_ratio": size_ratio,
                "base_size": base_size,
                "write_count": 0,
                "read_count": 0,
            }
        return self._trees[dataset_id]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute LSM tree operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        key = params.get("key")
        value = params.get("value")
        items = params.get("items", [])
        dataset_id = params.get("dataset_id", "default")
        num_levels = params.get("num_levels", 4)
        base_size = params.get("base_size", 1000)
        size_ratio = params.get("size_ratio", 10)

        tree = self._get_or_create_tree(dataset_id, num_levels, base_size, size_ratio)

        if operation == "write":
            return self._write(tree, key, value, dataset_id, start_time)
        elif operation == "batch_write":
            return self._batch_write(tree, items, dataset_id, start_time)
        elif operation == "read":
            return self._read(tree, key, dataset_id, start_time)
        elif operation == "delete":
            return self._delete(tree, key, dataset_id, start_time)
        elif operation == "range":
            return self._range_query(tree, params.get("min_key"), params.get("max_key"), dataset_id, start_time)
        elif operation == "compact":
            return self._trigger_compaction(tree, dataset_id, start_time)
        elif operation == "clear":
            return self._clear_tree(tree, dataset_id, start_time)
        elif operation == "status":
            return self._get_status(tree, dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _write(self, tree: Dict, key: Any, value: Any, dataset_id: str, start_time: float) -> ActionResult:
        """Write a key-value pair to memtable."""
        if key is None:
            return ActionResult(success=False, message="key required", duration=time.time() - start_time)

        tree["memtable"][str(key)] = value
        tree["write_count"] += 1

        # Check if memtable needs flushing
        if len(tree["memtable"]) >= tree["base_size"]:
            self._flush_memtable(tree)

        return ActionResult(
            success=True,
            message=f"Wrote key '{key}' to '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "key": str(key),
                "memtable_size": len(tree["memtable"]),
                "write_count": tree["write_count"],
            },
            duration=time.time() - start_time
        )

    def _batch_write(self, tree: Dict, items: List[Tuple], dataset_id: str, start_time: float) -> ActionResult:
        """Batch write key-value pairs."""
        written = 0
        for item in items:
            if len(item) >= 2:
                k, v = str(item[0]), item[1]
                tree["memtable"][k] = v
                tree["write_count"] += 1
                written += 1

        if len(tree["memtable"]) >= tree["base_size"]:
            self._flush_memtable(tree)

        return ActionResult(
            success=True,
            message=f"Batch wrote {written} items to '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "written": written,
                "memtable_size": len(tree["memtable"]),
                "write_count": tree["write_count"],
            },
            duration=time.time() - start_time
        )

    def _flush_memtable(self, tree: Dict) -> None:
        """Flush memtable to first level."""
        if not tree["memtable"]:
            return

        level0 = tree["levels"][0]
        table_id = f"L0_{int(time.time() * 1000000)}"
        level0.tables[table_id] = dict(tree["memtable"])
        level0.current_size = len(tree["memtable"])
        tree["memtable"].clear()

        # Trigger compaction if needed
        self._maybe_compact(tree)

    def _maybe_compact(self, tree: Dict) -> None:
        """Check if compaction is needed and perform it."""
        for i, level in enumerate(tree["levels"][:-1]):
            if level.current_size >= level.max_size:
                self._compact_level(tree, i)
                break

    def _compact_level(self, tree: Dict, level_id: int) -> None:
        """Compact a level into the next level."""
        levels = tree["levels"]
        if level_id >= len(levels) - 1:
            return

        current = levels[level_id]
        next_level = levels[level_id + 1]

        # Merge all tables from current level
        merged = {}
        for table in current.tables.values():
            merged.update(table)

        # Add to next level
        table_id = f"L{level_id + 1}_{int(time.time() * 1000000)}"
        next_level.tables[table_id] = merged
        next_level.current_size += len(merged)

        # Clear current level
        current.tables.clear()
        current.current_size = 0

    def _read(self, tree: Dict, key: Any, dataset_id: str, start_time: float) -> ActionResult:
        """Read a key, checking memtable first then levels."""
        if key is None:
            return ActionResult(success=False, message="key required", duration=time.time() - start_time)

        k = str(key)
        tree["read_count"] += 1

        # Check memtable first
        if k in tree["memtable"]:
            return ActionResult(
                success=True,
                message=f"Found key '{key}' in memtable",
                data={"dataset_id": dataset_id, "key": k, "value": tree["memtable"][k], "found": True, "source": "memtable"},
                duration=time.time() - start_time
            )

        # Search levels (newest first)
        for level in tree["levels"]:
            for table in reversed(list(level.tables.values())):
                if k in table:
                    return ActionResult(
                        success=True,
                        message=f"Found key '{key}' in level {level.level_id}",
                        data={"dataset_id": dataset_id, "key": k, "value": table[k], "found": True, "source": f"level_{level.level_id}"},
                        duration=time.time() - start_time
                    )

        # Check if deleted (tombstone)
        for level in tree["levels"]:
            for table in level.tables.values():
                if k in table and table[k] is None:
                    return ActionResult(
                        success=True,
                        message=f"Key '{key}' was deleted (tombstone)",
                        data={"dataset_id": dataset_id, "key": k, "found": False, "deleted": True},
                        duration=time.time() - start_time
                    )

        return ActionResult(
            success=True,
            message=f"Key '{key}' not found",
            data={"dataset_id": dataset_id, "key": k, "found": False},
            duration=time.time() - start_time
        )

    def _delete(self, tree: Dict, key: Any, dataset_id: str, start_time: float) -> ActionResult:
        """Delete a key (write tombstone)."""
        if key is None:
            return ActionResult(success=False, message="key required", duration=time.time() - start_time)

        k = str(key)
        tree["memtable"][k] = None  # Tombstone
        tree["write_count"] += 1

        return ActionResult(
            success=True,
            message=f"Deleted key '{key}' from '{dataset_id}'",
            data={"dataset_id": dataset_id, "key": k, "deleted": True},
            duration=time.time() - start_time
        )

    def _range_query(self, tree: Dict, min_key: Any, max_key: Any, dataset_id: str, start_time: float) -> ActionResult:
        """Get all key-value pairs in a range."""
        if min_key is None or max_key is None:
            return ActionResult(success=False, message="min_key and max_key required", duration=time.time() - start_time)

        results = []
        all_data = {}

        # Collect from all levels (newer overwrites older)
        for level in reversed(tree["levels"]):
            for table in reversed(list(level.tables.values())):
                all_data.update(table)

        # Apply tombstones filter
        for k, v in all_data.items():
            if v is not None and min_key <= k <= max_key:
                results.append((k, v))

        results.sort(key=lambda x: x[0])

        return ActionResult(
            success=True,
            message=f"Range query [{min_key}, {max_key}] returned {len(results)} items",
            data={
                "dataset_id": dataset_id,
                "min_key": str(min_key),
                "max_key": str(max_key),
                "count": len(results),
                "items": results[:100],  # Limit results
            },
            duration=time.time() - start_time
        )

    def _trigger_compaction(self, tree: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Manually trigger compaction."""
        self._maybe_compact(tree)
        return ActionResult(
            success=True,
            message=f"Compaction triggered for '{dataset_id}'",
            data={"dataset_id": dataset_id},
            duration=time.time() - start_time
        )

    def _clear_tree(self, tree: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Clear the LSM tree."""
        write_count = tree["write_count"]
        tree["memtable"].clear()
        for level in tree["levels"]:
            level.tables.clear()
            level.current_size = 0
        tree["write_count"] = 0
        tree["read_count"] = 0

        return ActionResult(
            success=True,
            message=f"Cleared LSM tree '{dataset_id}'",
            data={"dataset_id": dataset_id, "cleared_writes": write_count},
            duration=time.time() - start_time
        )

    def _get_status(self, tree: Dict, dataset_id: str, start_time: float) -> ActionResult:
        """Get LSM tree status."""
        level_info = []
        for level in tree["levels"]:
            level_info.append({
                "level_id": level.level_id,
                "tables_count": len(level.tables),
                "current_size": level.current_size,
                "max_size": level.max_size,
            })

        return ActionResult(
            success=True,
            message=f"LSM tree '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "memtable_size": len(tree["memtable"]),
                "write_count": tree["write_count"],
                "read_count": tree["read_count"],
                "levels": level_info,
            },
            duration=time.time() - start_time
        )
