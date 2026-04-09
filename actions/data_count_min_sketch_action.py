"""Data Count-Min Sketch Action Module.

Provides Count-Min Sketch data structure for probabilistic
frequency estimation with configurable accuracy and space.
"""

import hashlib
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class CMSConfig:
    """Count-Min Sketch configuration."""
    width: int
    depth: int
    epsilon: float  # Relative error
    delta: float    # Probability of error
    table: List[List[int]] = field(default_factory=list)
    count: int = 0


class DataCountMinSketchAction(BaseAction):
    """Count-Min Sketch probabilistic frequency estimation action.

    Estimates frequencies of elements with guaranteed error bounds
    using minimal memory. O(1) operations for add and estimate.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (create, add, estimate, merge, clear, status)
            - item: Single item to add
            - items: List of items for batch add
            - dataset_id: Identifier for the sketch
            - width: CMS width parameter
            - depth: CMS depth parameter
            - epsilon: Relative error parameter
            - delta: Probability parameter
    """
    action_type = "data_count_min_sketch"
    display_name = "数据Count-Min草图"
    description = "Count-Min草图用于概率频率估计"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "item": None,
            "items": [],
            "dataset_id": "default",
            "width": None,
            "depth": None,
            "epsilon": 0.01,
            "delta": 0.001,
        }

    def __init__(self) -> None:
        super().__init__()
        self._sketches: Dict[str, CMSConfig] = {}

    def _hash_item(self, item: Any, seed: int) -> int:
        """Generate hash for an item with seed."""
        data = f"{seed}:{item}".encode()
        return int(hashlib.sha256(data).hexdigest(), 16)

    def _create_sketch(self, width: int, depth: int) -> CMSConfig:
        """Create a new CMS table."""
        table = [[0] * width for _ in range(depth)]
        return CMSConfig(width=width, depth=depth, epsilon=0, delta=0, table=table)

    def _create_from_params(self, epsilon: float, delta: float) -> Tuple[int, int]:
        """Calculate width and depth from epsilon and delta."""
        width = int(math.ceil(math.e / epsilon))
        depth = int(math.ceil(math.log(1.0 / delta)))
        return width, depth

    def _get_sketch(self, dataset_id: str, width: Optional[int], depth: Optional[int], epsilon: float, delta: float) -> CMSConfig:
        """Get or create a sketch."""
        if dataset_id not in self._sketches:
            if width and depth:
                w, d = width, depth
            else:
                w, d = self._create_from_params(epsilon, delta)
            self._sketches[dataset_id] = self._create_sketch(w, d)
        return self._sketches[dataset_id]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute Count-Min Sketch operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        item = params.get("item")
        items = params.get("items", [])
        dataset_id = params.get("dataset_id", "default")
        width = params.get("width")
        depth = params.get("depth")
        epsilon = params.get("epsilon", 0.01)
        delta = params.get("delta", 0.001)

        if operation == "create":
            return self._create_new_sketch(dataset_id, width, depth, epsilon, delta, start_time)
        elif operation == "add":
            return self._add_item(dataset_id, item, width, depth, epsilon, delta, start_time)
        elif operation == "batch_add":
            return self._batch_add_items(dataset_id, items, width, depth, epsilon, delta, start_time)
        elif operation == "estimate":
            return self._estimate(dataset_id, item, start_time)
        elif operation == "merge":
            return self._merge_sketches(dataset_id, params.get("other_dataset_id"), width, depth, epsilon, delta, start_time)
        elif operation == "clear":
            return self._clear_sketch(dataset_id, start_time)
        elif operation == "status":
            return self._get_status(dataset_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _create_new_sketch(
        self,
        dataset_id: str,
        width: Optional[int],
        depth: Optional[int],
        epsilon: float,
        delta: float,
        start_time: float
    ) -> ActionResult:
        """Create a new Count-Min Sketch."""
        if width and depth:
            w, d = int(width), int(depth)
        else:
            w, d = self._create_from_params(epsilon, delta)

        cms = self._create_sketch(w, d)
        cms.epsilon = epsilon
        cms.delta = delta
        self._sketches[dataset_id] = cms

        return ActionResult(
            success=True,
            message=f"Count-Min Sketch '{dataset_id}' created",
            data={
                "dataset_id": dataset_id,
                "width": w,
                "depth": d,
                "epsilon": epsilon,
                "delta": delta,
                "memory_bytes": w * d * 8,
            },
            duration=time.time() - start_time
        )

    def _add_item(
        self,
        dataset_id: str,
        item: Any,
        width: Optional[int],
        depth: Optional[int],
        epsilon: float,
        delta: float,
        start_time: float
    ) -> ActionResult:
        """Add an item to the sketch."""
        if item is None:
            return ActionResult(success=False, message="item required", duration=time.time() - start_time)

        cms = self._get_sketch(dataset_id, width, depth, epsilon, delta)
        item_str = str(item)

        for i in range(cms.depth):
            hash_val = self._hash_item(item_str, i)
            col = hash_val % cms.width
            cms.table[i][col] += 1

        cms.count += 1

        return ActionResult(
            success=True,
            message=f"Added '{item_str}' to sketch '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "item": item_str,
                "total_items": cms.count,
                "estimated_count": self._estimate_value(cms, item_str),
            },
            duration=time.time() - start_time
        )

    def _batch_add_items(
        self,
        dataset_id: str,
        items: List[Any],
        width: Optional[int],
        depth: Optional[int],
        epsilon: float,
        delta: float,
        start_time: float
    ) -> ActionResult:
        """Batch add items to the sketch."""
        cms = self._get_sketch(dataset_id, width, depth, epsilon, delta)
        added = 0
        for item in items:
            item_str = str(item)
            for i in range(cms.depth):
                hash_val = self._hash_item(item_str, i)
                col = hash_val % cms.width
                cms.table[i][col] += 1
            added += 1

        cms.count += added

        return ActionResult(
            success=True,
            message=f"Added {added} items to sketch '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "added": added,
                "total_items": cms.count,
            },
            duration=time.time() - start_time
        )

    def _estimate_value(self, cms: CMSConfig, item_str: str) -> int:
        """Get minimum count estimate for an item."""
        min_count = float('inf')
        for i in range(cms.depth):
            hash_val = self._hash_item(item_str, i)
            col = hash_val % cms.width
            min_count = min(min_count, cms.table[i][col])
        return int(min_count)

    def _estimate(
        self,
        dataset_id: str,
        item: Any,
        start_time: float
    ) -> ActionResult:
        """Estimate frequency of an item."""
        if dataset_id not in self._sketches:
            return ActionResult(success=False, message=f"Sketch '{dataset_id}' not found", duration=time.time() - start_time)
        if item is None:
            return ActionResult(success=False, message="item required", duration=time.time() - start_time)

        cms = self._sketches[dataset_id]
        item_str = str(item)
        estimate = self._estimate_value(cms, item_str)

        # Upper bound (worst case)
        upper_bound = max(row[hash(str(item)) % cms.width] for row in cms.table)

        return ActionResult(
            success=True,
            message=f"Estimate for '{item_str}': {estimate}",
            data={
                "dataset_id": dataset_id,
                "item": item_str,
                "estimated_count": estimate,
                "upper_bound": int(upper_bound),
                "total_items": cms.count,
            },
            duration=time.time() - start_time
        )

    def _merge_sketches(
        self,
        dataset_id: str,
        other_dataset_id: Optional[str],
        width: Optional[int],
        depth: Optional[int],
        epsilon: float,
        delta: float,
        start_time: float
    ) -> ActionResult:
        """Merge two sketches."""
        if not other_dataset_id:
            return ActionResult(success=False, message="other_dataset_id required for merge", duration=time.time() - start_time)
        if other_dataset_id not in self._sketches:
            return ActionResult(success=False, message=f"Other sketch '{other_dataset_id}' not found", duration=time.time() - start_time)

        other = self._sketches[other_dataset_id]
        cms = self._get_sketch(dataset_id, width, depth, epsilon, delta)

        if cms.width != other.width or cms.depth != other.depth:
            return ActionResult(success=False, message="Sketches must have same dimensions for merge", duration=time.time() - start_time)

        total_items = 0
        for i in range(cms.depth):
            for j in range(cms.width):
                cms.table[i][j] += other.table[i][j]
                total_items = max(total_items, cms.table[i][j])

        cms.count += other.count

        return ActionResult(
            success=True,
            message=f"Merged '{other_dataset_id}' into '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "other_dataset_id": other_dataset_id,
                "total_items": cms.count,
            },
            duration=time.time() - start_time
        )

    def _clear_sketch(self, dataset_id: str, start_time: float) -> ActionResult:
        """Clear a sketch."""
        if dataset_id in self._sketches:
            cms = self._sketches[dataset_id]
            count = cms.count
            for i in range(cms.depth):
                for j in range(cms.width):
                    cms.table[i][j] = 0
            cms.count = 0
            return ActionResult(success=True, message=f"Cleared sketch '{dataset_id}'", data={"cleared_items": count}, duration=time.time() - start_time)
        return ActionResult(success=False, message=f"Sketch '{dataset_id}' not found", duration=time.time() - start_time)

    def _get_status(self, dataset_id: str, start_time: float) -> ActionResult:
        """Get sketch status."""
        if dataset_id not in self._sketches:
            return ActionResult(success=False, message=f"Sketch '{dataset_id}' not found", duration=time.time() - start_time)

        cms = self._sketches[dataset_id]
        # Find max value in table
        max_val = max(max(row) for row in cms.table)

        return ActionResult(
            success=True,
            message=f"Count-Min Sketch '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "width": cms.width,
                "depth": cms.depth,
                "epsilon": cms.epsilon,
                "delta": cms.delta,
                "total_items_added": cms.count,
                "max_count": max_val,
                "memory_bytes": cms.width * cms.depth * 8,
            },
            duration=time.time() - start_time
        )
