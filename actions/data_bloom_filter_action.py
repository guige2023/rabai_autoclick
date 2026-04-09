"""Data Bloom Filter Action Module.

Provides Bloom Filter data structure for probabilistic set
membership testing with zero false negatives.
"""

import hashlib
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class BloomFilterConfig:
    """Bloom filter configuration."""
    size: int  # Number of bits
    hash_count: int  # Number of hash functions
    expected_elements: int
    false_positive_rate: float
    bits: Set[int] = field(default_factory=set)
    count: int = 0


class DataBloomFilterAction(BaseAction):
    """Bloom Filter probabilistic set membership action.

    Tests set membership with zero false negatives and
    configurable false positive rate.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (create, add, contains, batch_add, clear, status)
            - item: Single item to add/check
            - items: List of items for batch operations
            - dataset_id: Identifier for the filter
            - expected_elements: Expected number of elements
            - false_positive_rate: Desired false positive rate (0-1)
    """
    action_type = "data_bloom_filter"
    display_name = "数据布隆过滤器"
    description = "布隆过滤器用于概率集合成员检测"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "item": None,
            "items": [],
            "dataset_id": "default",
            "expected_elements": 10000,
            "false_positive_rate": 0.01,
        }

    def __init__(self) -> None:
        super().__init__()
        self._filters: Dict[str, BloomFilterConfig] = {}

    def _optimal_size(self, n: int, p: float) -> int:
        """Calculate optimal size in bits."""
        m = - (n * math.log(p)) / (math.log(2) ** 2)
        return int(math.ceil(m))

    def _optimal_hash_count(self, m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * math.log(2)
        return max(1, int(math.ceil(k)))

    def _hash_item(self, item: Any, seed: int, size: int) -> int:
        """Generate hash for item with seed."""
        data = f"{seed}:{item}".encode()
        h = hashlib.sha256(data).hexdigest()
        return int(h, 16) % size

    def _get_filter(self, dataset_id: str, expected_elements: int, false_positive_rate: float) -> BloomFilterConfig:
        """Get or create a bloom filter."""
        if dataset_id not in self._filters:
            size = self._optimal_size(expected_elements, false_positive_rate)
            hash_count = self._optimal_hash_count(size, expected_elements)
            self._filters[dataset_id] = BloomFilterConfig(
                size=size,
                hash_count=hash_count,
                expected_elements=expected_elements,
                false_positive_rate=false_positive_rate,
                bits=set(),
                count=0,
            )
        return self._filters[dataset_id]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bloom filter operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        item = params.get("item")
        items = params.get("items", [])
        dataset_id = params.get("dataset_id", "default")
        expected_elements = params.get("expected_elements", 10000)
        false_positive_rate = params.get("false_positive_rate", 0.01)

        if operation == "create":
            return self._create_filter(dataset_id, expected_elements, false_positive_rate, start_time)
        elif operation == "add":
            return self._add_item(dataset_id, item, expected_elements, false_positive_rate, start_time)
        elif operation == "batch_add":
            return self._batch_add_items(dataset_id, items, expected_elements, false_positive_rate, start_time)
        elif operation == "contains":
            return self._check_contains(dataset_id, item, start_time)
        elif operation == "clear":
            return self._clear_filter(dataset_id, start_time)
        elif operation == "status":
            return self._get_status(dataset_id, start_time)
        elif operation == "union":
            return self._union_filters(dataset_id, params.get("other_dataset_id"), start_time)
        elif operation == "intersection":
            return self._intersect_filters(dataset_id, params.get("other_dataset_id"), start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _create_filter(
        self,
        dataset_id: str,
        expected_elements: int,
        false_positive_rate: float,
        start_time: float
    ) -> ActionResult:
        """Create a new bloom filter."""
        size = self._optimal_size(expected_elements, false_positive_rate)
        hash_count = self._optimal_hash_count(size, expected_elements)

        bf = BloomFilterConfig(
            size=size,
            hash_count=hash_count,
            expected_elements=expected_elements,
            false_positive_rate=false_positive_rate,
            bits=set(),
            count=0,
        )
        self._filters[dataset_id] = bf

        return ActionResult(
            success=True,
            message=f"Bloom filter '{dataset_id}' created",
            data={
                "dataset_id": dataset_id,
                "size_bits": size,
                "hash_count": hash_count,
                "expected_elements": expected_elements,
                "false_positive_rate": false_positive_rate,
                "memory_bytes": math.ceil(size / 8),
            },
            duration=time.time() - start_time
        )

    def _add_item(
        self,
        dataset_id: str,
        item: Any,
        expected_elements: int,
        false_positive_rate: float,
        start_time: float
    ) -> ActionResult:
        """Add an item to the filter."""
        if item is None:
            return ActionResult(success=False, message="item required", duration=time.time() - start_time)

        bf = self._get_filter(dataset_id, expected_elements, false_positive_rate)
        item_str = str(item)

        for i in range(bf.hash_count):
            bit_pos = self._hash_item(item_str, i, bf.size)
            bf.bits.add(bit_pos)

        bf.count += 1

        return ActionResult(
            success=True,
            message=f"Added '{item_str}' to bloom filter '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "item": item_str,
                "total_items": bf.count,
                "bits_set": len(bf.bits),
            },
            duration=time.time() - start_time
        )

    def _batch_add_items(
        self,
        dataset_id: str,
        items: List[Any],
        expected_elements: int,
        false_positive_rate: float,
        start_time: float
    ) -> ActionResult:
        """Batch add items to the filter."""
        bf = self._get_filter(dataset_id, expected_elements, false_positive_rate)
        added = 0

        for item in items:
            item_str = str(item)
            for i in range(bf.hash_count):
                bit_pos = self._hash_item(item_str, i, bf.size)
                bf.bits.add(bit_pos)
            added += 1

        bf.count += added

        return ActionResult(
            success=True,
            message=f"Added {added} items to bloom filter '{dataset_id}'",
            data={
                "dataset_id": dataset_id,
                "added": added,
                "total_items": bf.count,
                "bits_set": len(bf.bits),
            },
            duration=time.time() - start_time
        )

    def _check_contains(self, dataset_id: str, item: Any, start_time: float) -> ActionResult:
        """Check if item might be in the filter."""
        if dataset_id not in self._filters:
            return ActionResult(success=False, message=f"Filter '{dataset_id}' not found", duration=time.time() - start_time)
        if item is None:
            return ActionResult(success=False, message="item required", duration=time.time() - start_time)

        bf = self._filters[dataset_id]
        item_str = str(item)

        # Check all hash positions
        for i in range(bf.hash_count):
            bit_pos = self._hash_item(item_str, i, bf.size)
            if bit_pos not in bf.bits:
                return ActionResult(
                    success=True,
                    message=f"'{item_str}' is DEFINITELY NOT in bloom filter '{dataset_id}'",
                    data={
                        "dataset_id": dataset_id,
                        "item": item_str,
                        "contains": False,
                        "definitely_not": True,
                    },
                    duration=time.time() - start_time
                )

        return ActionResult(
            success=True,
            message=f"'{item_str}' MAYBE in bloom filter '{dataset_id}' (false positive possible)",
            data={
                "dataset_id": dataset_id,
                "item": item_str,
                "contains": True,
                "definitely_not": False,
                "false_positive_rate": bf.false_positive_rate,
                "bits_set": len(bf.bits),
            },
            duration=time.time() - start_time
        )

    def _clear_filter(self, dataset_id: str, start_time: float) -> ActionResult:
        """Clear a bloom filter."""
        if dataset_id not in self._filters:
            return ActionResult(success=False, message=f"Filter '{dataset_id}' not found", duration=time.time() - start_time)

        bf = self._filters[dataset_id]
        count = bf.count
        bf.bits.clear()
        bf.count = 0

        return ActionResult(
            success=True,
            message=f"Bloom filter '{dataset_id}' cleared",
            data={"dataset_id": dataset_id, "cleared_items": count},
            duration=time.time() - start_time
        )

    def _get_status(self, dataset_id: str, start_time: float) -> ActionResult:
        """Get bloom filter status."""
        if dataset_id not in self._filters:
            return ActionResult(success=False, message=f"Filter '{dataset_id}' not found", duration=time.time() - start_time)

        bf = self._filters[dataset_id]
        fill_ratio = len(bf.bits) / bf.size if bf.size > 0 else 0
        actual_fpr = self._estimate_fpr(bf)

        return ActionResult(
            success=True,
            message=f"Bloom filter '{dataset_id}' status",
            data={
                "dataset_id": dataset_id,
                "size_bits": bf.size,
                "hash_count": bf.hash_count,
                "total_items": bf.count,
                "bits_set": len(bf.bits),
                "fill_ratio": fill_ratio,
                "expected_fpr": bf.false_positive_rate,
                "estimated_fpr": actual_fpr,
            },
            duration=time.time() - start_time
        )

    def _estimate_fpr(self, bf: BloomFilterConfig) -> float:
        """Estimate actual false positive rate."""
        n = bf.count
        m = bf.size
        k = bf.hash_count
        if m == 0 or n == 0:
            return 0.0
        # (1 - e^(-kn/m))^k
        try:
            exponent = - (k * n) / m
            return (1 - math.exp(exponent)) ** k
        except OverflowError:
            return 1.0

    def _union_filters(self, dataset_id: str, other_dataset_id: Optional[str], start_time: float) -> ActionResult:
        """Union two bloom filters."""
        if not other_dataset_id:
            return ActionResult(success=False, message="other_dataset_id required", duration=time.time() - start_time)
        if dataset_id not in self._filters or other_dataset_id not in self._filters:
            return ActionResult(success=False, message="Both filters must exist", duration=time.time() - start_time)

        bf1 = self._filters[dataset_id]
        bf2 = self._filters[other_dataset_id]

        if bf1.size != bf2.size or bf1.hash_count != bf2.hash_count:
            return ActionResult(success=False, message="Filters must have same size and hash count", duration=time.time() - start_time)

        union_bits = bf1.bits | bf2.bits
        bf1.bits = union_bits
        bf1.count = max(bf1.count, bf2.count)

        return ActionResult(
            success=True,
            message=f"Union of '{dataset_id}' and '{other_dataset_id}' stored in '{dataset_id}'",
            data={"dataset_id": dataset_id, "union_bits": len(union_bits)},
            duration=time.time() - start_time
        )

    def _intersect_filters(self, dataset_id: str, other_dataset_id: Optional[str], start_time: float) -> ActionResult:
        """Intersect two bloom filters."""
        if not other_dataset_id:
            return ActionResult(success=False, message="other_dataset_id required", duration=time.time() - start_time)
        if dataset_id not in self._filters or other_dataset_id not in self._filters:
            return ActionResult(success=False, message="Both filters must exist", duration=time.time() - start_time)

        bf1 = self._filters[dataset_id]
        bf2 = self._filters[other_dataset_id]

        if bf1.size != bf2.size or bf1.hash_count != bf2.hash_count:
            return ActionResult(success=False, message="Filters must have same size and hash count", duration=time.time() - start_time)

        intersection_bits = bf1.bits & bf2.bits
        bf1.bits = intersection_bits
        bf1.count = min(bf1.count, bf2.count)

        return ActionResult(
            success=True,
            message=f"Intersection of '{dataset_id}' and '{other_dataset_id}' stored in '{dataset_id}'",
            data={"dataset_id": dataset_id, "intersection_bits": len(intersection_bits)},
            duration=time.time() - start_time
        )
