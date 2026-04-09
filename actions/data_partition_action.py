"""
Data Partition Action Module

Data partitioning strategies for efficient processing of large datasets.
Supports hash, range, round-robin, and composite partitioning.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Partitioning strategies."""

    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    LIST = "list"
    COMPOSITE = "composite"


@dataclass
class Partition:
    """A single data partition."""

    partition_id: int
    name: str
    data: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionConfig:
    """Configuration for partitioning."""

    strategy: PartitionStrategy = PartitionStrategy.HASH
    num_partitions: int = 4
    partition_key: str = "id"
    custom_hash_fn: Optional[Callable[[Any], int]] = None
    range_boundaries: Optional[List[Any]] = None


class HashPartitioner:
    """Hash-based partitioner."""

    def __init__(self, num_partitions: int, partition_key: str):
        self.num_partitions = num_partitions
        self.partition_key = partition_key

    def partition(self, item: Dict[str, Any]) -> int:
        """Get partition number for an item."""
        key_value = item.get(self.partition_key, 0)
        if isinstance(key_value, str):
            hash_value = int(hashlib.md5(key_value.encode()).hexdigest(), 16)
        else:
            hash_value = hash(key_value)
        return hash_value % self.num_partitions


class RangePartitioner:
    """Range-based partitioner."""

    def __init__(self, boundaries: List[Any]):
        self.boundaries = sorted(boundaries)

    def partition(self, item: Dict[str, Any]) -> int:
        """Get partition number for an item."""
        for i, boundary in enumerate(self.boundaries):
            if item.get("key", 0) < boundary:
                return i
        return len(self.boundaries)


class RoundRobinPartitioner:
    """Round-robin partitioner."""

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
        self._current = 0

    def partition(self, item: Dict[str, Any]) -> int:
        """Get next partition number."""
        partition = self._current
        self._current = (self._current + 1) % self.num_partitions
        return partition


class ListPartitioner:
    """List-based partitioner."""

    def __init__(self, partition_key: str, partition_values: Dict[int, List[Any]]):
        self.partition_key = partition_key
        self.partition_values = partition_values

    def partition(self, item: Dict[str, Any]) -> int:
        """Get partition number for an item."""
        value = item.get(self.partition_key)
        for partition_id, values in self.partition_values.items():
            if value in values:
                return partition_id
        return -1


class DataPartitionAction:
    """
    Main action class for data partitioning.

    Features:
    - Hash partitioning for uniform distribution
    - Range partitioning for ordered data
    - Round-robin for simple load balancing
    - List partitioning for categorical data
    - Custom partition functions

    Usage:
        action = DataPartitionAction(config)
        partitions = action.partition(data)
    """

    def __init__(self, config: Optional[PartitionConfig] = None):
        self.config = config or PartitionConfig()
        self._partitioner: Optional[Any] = None
        self._initialize_partitioner()

    def _initialize_partitioner(self) -> None:
        """Initialize the appropriate partitioner."""
        if self.config.strategy == PartitionStrategy.HASH:
            self._partitioner = HashPartitioner(
                self.config.num_partitions,
                self.config.partition_key,
            )
        elif self.config.strategy == PartitionStrategy.ROUND_ROBIN:
            self._partitioner = RoundRobinPartitioner(self.config.num_partitions)
        elif self.config.strategy == PartitionStrategy.RANGE:
            self._partitioner = RangePartitioner(
                self.config.range_boundaries or [1, 2, 3],
            )
        elif self.config.strategy == PartitionStrategy.LIST:
            self._partitioner = ListPartitioner(
                self.config.partition_key,
                {i: [] for i in range(self.config.num_partitions)},
            )

    def partition(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Partition]:
        """Partition data into multiple partitions."""
        partitions = [
            Partition(partition_id=i, name=f"partition_{i}")
            for i in range(self.config.num_partitions)
        ]

        for item in data:
            if self._partitioner:
                partition_id = self._partitioner.partition(item)
                if partition_id >= 0 and partition_id < len(partitions):
                    partitions[partition_id].data.append(item)
                else:
                    partitions[0].data.append(item)
            else:
                partitions[0].data.append(item)

        # Update metadata
        for partition in partitions:
            partition.metadata = {
                "count": len(partition.data),
                "strategy": self.config.strategy.value,
            }

        return partitions

    def partition_with_key(
        self,
        data: List[Dict[str, Any]],
        key_fn: Callable[[Dict[str, Any]], Any],
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Partition data using a custom key function."""
        result: Dict[Any, List[Dict[str, Any]]] = {}

        for item in data:
            key = key_fn(item)
            if key not in result:
                result[key] = []
            result[key].append(item)

        return result

    def merge_partitions(
        self,
        partitions: List[Partition],
    ) -> List[Dict[str, Any]]:
        """Merge partitions back into a single list."""
        result = []
        for partition in partitions:
            result.extend(partition.data)
        return result


def demo_partition():
    """Demonstrate partitioning."""
    data = [
        {"id": 1, "name": "Alice", "category": "A"},
        {"id": 2, "name": "Bob", "category": "B"},
        {"id": 3, "name": "Charlie", "category": "A"},
        {"id": 4, "name": "Diana", "category": "C"},
        {"id": 5, "name": "Eve", "category": "B"},
        {"id": 6, "name": "Frank", "category": "A"},
    ]

    # Hash partition
    config = PartitionConfig(
        strategy=PartitionStrategy.HASH,
        num_partitions=3,
        partition_key="id",
    )
    action = DataPartitionAction(config)
    partitions = action.partition(data)

    for p in partitions:
        print(f"{p.name}: {len(p.data)} items")


if __name__ == "__main__":
    demo_partition()
