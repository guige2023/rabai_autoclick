"""
Data Partitioning Action Module.

Provides data partitioning strategies including
range, list, hash, and round-robin partitioning.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import hashlib
import logging

logger = logging.getLogger(__name__)


class PartitionStrategy(Enum):
    """Partition strategies."""
    RANGE = "range"
    LIST = "list"
    HASH = "hash"
    ROUND_ROBIN = "round_robin"
    COMPOSITE = "composite"


@dataclass
class Partition:
    """Data partition."""
    partition_id: str
    name: str
    strategy: PartitionStrategy
    records: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionConfig:
    """Partition configuration."""
    partition_by: List[str]
    strategy: PartitionStrategy
    num_partitions: int = 4
    ranges: Optional[List[Tuple[Any, Any]]] = None
    lists: Optional[Dict[Any, List[Any]]] = None


class RangePartitioner:
    """Range-based partitioner."""

    def __init__(self, field: str, ranges: List[Tuple[Any, Any]]):
        self.field = field
        self.ranges = ranges

    def get_partition(self, record: Dict[str, Any]) -> int:
        """Get partition index for record."""
        value = record.get(self.field)

        for i, (low, high) in enumerate(self.ranges):
            if low <= value <= high:
                return i

        return len(self.ranges)


class ListPartitioner:
    """List-based partitioner."""

    def __init__(self, field: str, lists: Dict[Any, List[Any]]):
        self.field = field
        self.lists = lists
        self._value_to_partition: Dict[Any, int] = {}

        for partition_id, values in enumerate(lists):
            for value in lists[values]:
                self._value_to_partition[value] = partition_id

    def get_partition(self, record: Dict[str, Any]) -> int:
        """Get partition index for record."""
        value = record.get(self.field)
        return self._value_to_partition.get(value, -1)


class HashPartitioner:
    """Hash-based partitioner."""

    def __init__(self, field: str, num_partitions: int):
        self.field = field
        self.num_partitions = num_partitions

    def _hash(self, value: Any) -> int:
        """Hash a value."""
        if isinstance(value, str):
            return int(hashlib.md5(value.encode()).hexdigest(), 16)
        return hash(value)

    def get_partition(self, record: Dict[str, Any]) -> int:
        """Get partition index for record."""
        value = record.get(self.field)
        if value is None:
            return 0
        return self._hash(value) % self.num_partitions


class RoundRobinPartitioner:
    """Round-robin partitioner."""

    def __init__(self, num_partitions: int):
        self.num_partitions = num_partitions
        self._current = 0

    def get_partition(self, record: Dict[str, Any]) -> int:
        """Get partition index for record."""
        partition = self._current
        self._current = (self._current + 1) % self.num_partitions
        return partition


class DataPartitioner:
    """Partitions data based on configuration."""

    def __init__(self, config: PartitionConfig):
        self.config = config
        self.partitioner = self._create_partitioner()

    def _create_partitioner(self):
        """Create partitioner based on strategy."""
        field = self.config.partition_by[0] if self.config.partition_by else "id"

        if self.config.strategy == PartitionStrategy.RANGE:
            return RangePartitioner(field, self.config.ranges or [])

        elif self.config.strategy == PartitionStrategy.LIST:
            return ListPartitioner(field, self.config.lists or {})

        elif self.config.strategy == PartitionStrategy.HASH:
            return HashPartitioner(field, self.config.num_partitions)

        elif self.config.strategy == PartitionStrategy.ROUND_ROBIN:
            return RoundRobinPartitioner(self.config.num_partitions)

        else:
            return HashPartitioner(field, self.config.num_partitions)

    def partition(self, records: List[Dict[str, Any]]) -> List[Partition]:
        """Partition records into partitions."""
        partitions = [
            Partition(
                partition_id=str(i),
                name=f"partition_{i}",
                strategy=self.config.strategy
            )
            for i in range(self.config.num_partitions)
        ]

        for record in records:
            partition_id = self.partitioner.get_partition(record)
            if 0 <= partition_id < len(partitions):
                partitions[partition_id].records.append(record)

        for i, partition in enumerate(partitions):
            partition.metadata = {
                "count": len(partition.records),
                "partition_id": i
            }

        return partitions

    def get_partition_for_record(self, record: Dict[str, Any]) -> int:
        """Get partition ID for a single record."""
        return self.partitioner.get_partition(record)


class CompositePartitioner:
    """Composite partitioner using multiple strategies."""

    def __init__(self, partitioners: List[DataPartitioner]):
        self.partitioners = partitioners

    def partition(self, records: List[Dict[str, Any]]) -> List[Partition]:
        """Partition using multiple partitioners."""
        if not self.partitioners:
            return []

        current_partitions = self.partitioners[0].partition(records)

        for partitioner in self.partitioners[1:]:
            new_partitions = []
            for part in current_partitions:
                sub_partitions = partitioner.partition(part.records)
                for i, sub_part in enumerate(sub_partitions):
                    sub_part.name = f"{part.name}_{sub_part.name}"
                    sub_part.partition_id = f"{part.partition_id}_{i}"
                    new_partitions.append(sub_part)

            current_partitions = new_partitions

        return current_partitions


def main():
    """Demonstrate data partitioning."""
    config = PartitionConfig(
        partition_by=["status"],
        strategy=PartitionStrategy.HASH,
        num_partitions=3
    )

    partitioner = DataPartitioner(config)

    records = [
        {"id": 1, "status": "active", "value": 100},
        {"id": 2, "status": "inactive", "value": 200},
        {"id": 3, "status": "active", "value": 150},
        {"id": 4, "status": "pending", "value": 50},
    ]

    partitions = partitioner.partition(records)

    for part in partitions:
        print(f"{part.name}: {len(part.records)} records")


if __name__ == "__main__":
    main()
