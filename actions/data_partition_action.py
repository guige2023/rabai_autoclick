"""Data Partition Action Module.

Provides data partitioning with hash-based, range-based, list-based,
and round-robin strategies for distributing data into partitions.
"""

from __future__ import annotations

import hashlib
import logging
import math
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Partitioning strategies."""
    HASH = "hash"
    RANGE = "range"
    LIST = "list"
    ROUND_ROBIN = "round_robin"
    CONSISTENT_HASH = "consistent_hash"
    MULTI维 = "multi_column"


@dataclass
class RangePartition:
    """A range-based partition definition."""
    name: str
    min_value: Any
    max_value: Any
    inclusive_min: bool = True
    inclusive_max: bool = False


@dataclass
class ListPartition:
    """A list-based partition definition."""
    name: str
    values: Set[Any]


@dataclass
class Partition:
    """A data partition container."""
    name: str
    data: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add(self, item: Dict[str, Any]) -> None:
        """Add an item to this partition."""
        self.data.append(item)

    def add_batch(self, items: List[Dict[str, Any]]) -> None:
        """Add multiple items to this partition."""
        self.data.extend(items)

    @property
    def size(self) -> int:
        """Get number of items in partition."""
        return len(self.data)


@dataclass
class PartitionStats:
    """Statistics for partitioning operation."""
    total_records: int = 0
    num_partitions: int = 0
    partition_sizes: Dict[str, int] = field(default_factory=dict)
    empty_partitions: List[str] = field(default_factory=list)
    partition_time_ms: float = 0.0
    max_partition_size: int = 0
    min_partition_size: int = 0
    avg_partition_size: float = 0.0


def _get_nested_value(item: Dict[str, Any], field: str) -> Any:
    """Get nested field value using dot notation."""
    parts = field.split(".")
    value = item
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, (list, tuple)):
            try:
                value = value[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if value is None:
            return None
    return value


def _hash_value(value: Any) -> int:
    """Generate hash for a value."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value) % 1000000
    try:
        return int(hashlib.md5(str(value).encode()).hexdigest(), 16) % 1000000
    except Exception:
        return 0


class DataPartitionAction(BaseAction):
    """Data Partition Action for distributing data.

    Supports multiple partitioning strategies including hash, range,
    list, round-robin, and consistent hashing.

    Examples:
        >>> action = DataPartitionAction()
        >>> result = action.execute(ctx, {
        ...     "data": [{"id": 1}, {"id": 2}, {"id": 3}],
        ...     "strategy": "hash",
        ...     "partition_field": "id",
        ...     "num_partitions": 4
        ... })
    """

    action_type = "data_partition"
    display_name = "数据分区"
    description = "支持Hash/Range/List/Round-Robin多种分区策略"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data partitioning.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to partition
                - strategy: Partition strategy ('hash', 'range', 'list', 'round_robin')
                - partition_field: Field to partition on
                - num_partitions: Number of partitions (for hash/round_robin)
                - range_partitions: List of RangePartition definitions
                - list_partitions: List of ListPartition definitions
                - partition_names: Custom names for partitions
                - shuffle: Shuffle data before partitioning

        Returns:
            ActionResult with partitioned data and statistics.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        strategy_str = params.get("strategy", "hash")
        partition_field = params.get("partition_field")
        num_partitions = params.get("num_partitions", 4)
        range_partitions = params.get("range_partitions", [])
        list_partitions = params.get("list_partitions", [])
        partition_names = params.get("partition_names")
        shuffle = params.get("shuffle", False)

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' parameter must be a list"
            )

        try:
            strategy = PartitionStrategy(strategy_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid strategy: {strategy_str}"
            )

        # Prepare data
        work_data = list(data)
        if shuffle:
            random.shuffle(work_data)

        # Create partitions
        if strategy == PartitionStrategy.HASH:
            partitions = self._hash_partition(
                work_data, partition_field, num_partitions, partition_names
            )
        elif strategy == PartitionStrategy.ROUND_ROBIN:
            partitions = self._round_robin_partition(
                work_data, num_partitions, partition_names
            )
        elif strategy == PartitionStrategy.RANGE:
            partitions = self._range_partition(
                work_data, partition_field, range_partitions
            )
        elif strategy == PartitionStrategy.LIST:
            partitions = self._list_partition(
                work_data, partition_field, list_partitions
            )
        elif strategy == PartitionStrategy.CONSISTENT_HASH:
            partitions = self._consistent_hash_partition(
                work_data, partition_field, num_partitions, partition_names
            )
        else:
            return ActionResult(
                success=False,
                message=f"Strategy not implemented: {strategy_str}"
            )

        duration_ms = (time.time() - start_time) * 1000

        # Compute stats
        sizes = {p.name: p.size for p in partitions}
        non_empty = [n for n, s in sizes.items() if s > 0]
        stats = PartitionStats(
            total_records=len(data),
            num_partitions=len(partitions),
            partition_sizes=sizes,
            empty_partitions=[n for n, s in sizes.items() if s == 0],
            partition_time_ms=duration_ms,
            max_partition_size=max(sizes.values()) if sizes else 0,
            min_partition_size=min([s for s in sizes.values() if s > 0] or [0]),
            avg_partition_size=sum(sizes.values()) / len(partitions) if partitions else 0,
        )

        return ActionResult(
            success=True,
            message=f"Partitioned {len(data)} records into {len(partitions)} partitions",
            data={
                "partitions": [
                    {"name": p.name, "data": p.data, "size": p.size, "metadata": p.metadata}
                    for p in partitions
                ],
                "stats": {
                    "total_records": stats.total_records,
                    "num_partitions": stats.num_partitions,
                    "partition_sizes": stats.partition_sizes,
                    "empty_partitions": stats.empty_partitions,
                    "partition_time_ms": stats.partition_time_ms,
                    "max_partition_size": stats.max_partition_size,
                    "min_partition_size": stats.min_partition_size,
                    "avg_partition_size": stats.avg_partition_size,
                }
            }
        )

    def _hash_partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        num_partitions: int,
        names: Optional[List[str]],
    ) -> List[Partition]:
        """Perform hash-based partitioning."""
        partitions = self._create_partitions(num_partitions, names)

        for item in data:
            if not isinstance(item, dict):
                continue
            value = _get_nested_value(item, field) if field else item
            hash_val = _hash_value(value)
            idx = hash_val % num_partitions
            partitions[idx].add(item)

        return partitions

    def _round_robin_partition(
        self,
        data: List[Dict[str, Any]],
        num_partitions: int,
        names: Optional[List[str]],
    ) -> List[Partition]:
        """Perform round-robin partitioning."""
        partitions = self._create_partitions(num_partitions, names)

        for i, item in enumerate(data):
            if isinstance(item, dict):
                partitions[i % num_partitions].add(item)

        return partitions

    def _range_partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        range_specs: List[Dict[str, Any]],
    ) -> List[Partition]:
        """Perform range-based partitioning."""
        partitions = []
        for spec in range_specs:
            name = spec.get("name", f"partition_{len(partitions)}")
            partitions.append(Partition(name=name))

        # Add default partition for out-of-range values
        partitions.append(Partition(name="other"))

        for item in data:
            if not isinstance(item, dict):
                continue
            value = _get_nested_value(item, field)

            placed = False
            for spec in range_specs:
                min_val = spec.get("min_value")
                max_val = spec.get("max_value")
                inclusive_min = spec.get("inclusive_min", True)
                inclusive_max = spec.get("inclusive_max", False)

                try:
                    if min_val is not None and max_val is not None:
                        above_min = value > min_val if not inclusive_min else value >= min_val
                        below_max = value < max_val if not inclusive_max else value <= max_val
                        if above_min and below_max:
                            partitions[range_specs.index(spec)].add(item)
                            placed = True
                            break
                except TypeError:
                    continue

            if not placed:
                partitions[-1].add(item)

        return partitions

    def _list_partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        list_specs: List[Dict[str, Any]],
    ) -> List[Partition]:
        """Perform list-based partitioning."""
        partitions = []
        value_to_partition: Dict[Any, int] = {}

        for i, spec in enumerate(list_specs):
            name = spec.get("name", f"partition_{i}")
            partitions.append(Partition(name=name))
            for val in spec.get("values", []):
                value_to_partition[val] = i

        # Default partition
        partitions.append(Partition(name="other"))

        for item in data:
            if not isinstance(item, dict):
                continue
            value = _get_nested_value(item, field)

            if value in value_to_partition:
                partitions[value_to_partition[value]].add(item)
            else:
                partitions[-1].add(item)

        return partitions

    def _consistent_hash_partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        num_partitions: int,
        names: Optional[List[str]],
    ) -> List[Partition]:
        """Perform consistent hash partitioning with virtual nodes."""
        # Simple consistent hash: map to points on a circle
        partitions = self._create_partitions(num_partitions, names)

        # Build hash ring
        ring_size = num_partitions * 100  # Virtual nodes
        ring: Dict[int, int] = {}  # hash -> partition index

        for i in range(ring_size):
            h = int(hashlib.md5(f"vn_{i}".encode()).hexdigest(), 16) % ring_size
            ring[h] = i % num_partitions

        sorted_points = sorted(ring.keys())

        for item in data:
            if not isinstance(item, dict):
                continue
            value = _get_nested_value(item, field) if field else item
            hash_val = _hash_value(value) % ring_size

            # Find first point on ring >= hash_val
            for point in sorted_points:
                if point >= hash_val:
                    partitions[ring[point]].add(item)
                    break
            else:
                # Wrap around to first point
                partitions[ring[sorted_points[0]]].add(item)

        return partitions

    def _create_partitions(
        self,
        num: int,
        names: Optional[List[str]],
    ) -> List[Partition]:
        """Create empty partitions."""
        if names and len(names) == num:
            return [Partition(name=n) for n in names]
        return [Partition(name=f"partition_{i}") for i in range(num)]

    def rebalance(
        self,
        partitions: List[Partition],
        strategy: str = "size",
    ) -> List[Partition]:
        """Rebalance data across partitions."""
        all_data = []
        for p in partitions:
            all_data.extend(p.data)

        return self.execute(
            None,
            {
                "data": all_data,
                "strategy": "round_robin",
                "num_partitions": len(partitions),
            }
        ).data.get("partitions", partitions)

    def get_required_params(self) -> List[str]:
        return ["data", "strategy"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "partition_field": None,
            "num_partitions": 4,
            "range_partitions": [],
            "list_partitions": [],
            "partition_names": None,
            "shuffle": False,
        }
