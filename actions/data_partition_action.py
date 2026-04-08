"""
Data Partition Action Module.

Provides data partitioning strategies for distributed
processing including range, hash, and list partitioning.
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
    HASH = "hash"
    LIST = "list"
    ROUND_ROBIN = "round_robin"
    COMPOSITE = "composite"


@dataclass
class Partition:
    """Data partition."""
    partition_id: str
    strategy: PartitionStrategy
    name: str
    data: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.data)


@dataclass
class PartitionConfig:
    """Configuration for partitioning."""
    strategy: PartitionStrategy
    num_partitions: int = 4
    partition_keys: List[str] = field(default_factory=list)
    custom_ranges: Optional[List[Any]] = None
    hash_modulo: int = 256


class DataPartitioner:
    """Partitions data using various strategies."""

    def __init__(self, config: PartitionConfig):
        self.config = config
        self.partitions: Dict[str, Partition] = {}
        self._round_robin_index = 0

    def partition(self, data: List[Any], key_extractor: Callable[[Any], Any]) -> Dict[str, Partition]:
        """Partition data according to strategy."""
        if self.config.strategy == PartitionStrategy.RANGE:
            return self._partition_by_range(data, key_extractor)
        elif self.config.strategy == PartitionStrategy.HASH:
            return self._partition_by_hash(data, key_extractor)
        elif self.config.strategy == PartitionStrategy.LIST:
            return self._partition_by_list(data, key_extractor)
        elif self.config.strategy == PartitionStrategy.ROUND_ROBIN:
            return self._partition_by_round_robin(data)
        else:
            return self._partition_by_hash(data, key_extractor)

    def _partition_by_range(
        self,
        data: List[Any],
        key_extractor: Callable[[Any], Any]
    ) -> Dict[str, Partition]:
        """Partition by range of keys."""
        keys = [key_extractor(item) for item in data]
        sorted_keys = sorted(keys)

        if not sorted_keys:
            return {}

        min_val, max_val = sorted_keys[0], sorted_keys[-1]
        range_size = (max_val - min_val) / self.config.num_partitions

        partitions = {}
        for i in range(self.config.num_partitions):
            pid = f"range_{i}"
            partitions[pid] = Partition(
                partition_id=pid,
                strategy=PartitionStrategy.RANGE,
                name=f"Range Partition {i}",
                metadata={
                    "start": min_val + i * range_size,
                    "end": min_val + (i + 1) * range_size
                }
            )

        for item in data:
            key = key_extractor(item)
            partition_idx = min(
                int((key - min_val) / range_size),
                self.config.num_partitions - 1
            ) if range_size > 0 else 0
            pid = f"range_{partition_idx}"
            partitions[pid].data.append(item)

        self.partitions = partitions
        return partitions

    def _partition_by_hash(
        self,
        data: List[Any],
        key_extractor: Callable[[Any], Any]
    ) -> Dict[str, Partition]:
        """Partition by hash of keys."""
        partitions = {}

        for i in range(self.config.num_partitions):
            pid = f"hash_{i}"
            partitions[pid] = Partition(
                partition_id=pid,
                strategy=PartitionStrategy.HASH,
                name=f"Hash Partition {i}"
            )

        for item in data:
            key = key_extractor(item)
            key_str = str(key)
            hash_val = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
            partition_idx = hash_val % self.config.num_partitions

            pid = f"hash_{partition_idx}"
            partitions[pid].data.append(item)

        self.partitions = partitions
        return partitions

    def _partition_by_list(
        self,
        data: List[Any],
        key_extractor: Callable[[Any], Any]
    ) -> Dict[str, Partition]:
        """Partition by list of values."""
        partitions = {}
        ranges = self.config.custom_ranges or []

        for i, value_range in enumerate(ranges):
            pid = f"list_{i}"
            partitions[pid] = Partition(
                partition_id=pid,
                strategy=PartitionStrategy.LIST,
                name=f"List Partition {i}",
                metadata={"range": value_range}
            )

        for item in data:
            key = key_extractor(item)
            matched = False

            for i, value_range in enumerate(ranges):
                if key in value_range:
                    pid = f"list_{i}"
                    partitions[pid].data.append(item)
                    matched = True
                    break

            if not matched:
                pid = f"list_unmatched"
                if pid not in partitions:
                    partitions[pid] = Partition(
                        partition_id=pid,
                        strategy=PartitionStrategy.LIST,
                        name="Unmatched"
                    )
                partitions[pid].data.append(item)

        self.partitions = partitions
        return partitions

    def _partition_by_round_robin(self, data: List[Any]) -> Dict[str, Partition]:
        """Partition using round-robin."""
        partitions = {}

        for i in range(self.config.num_partitions):
            pid = f"rr_{i}"
            partitions[pid] = Partition(
                partition_id=pid,
                strategy=PartitionStrategy.ROUND_ROBIN,
                name=f"Round Robin Partition {i}"
            )

        for item in data:
            pid = f"rr_{self._round_robin_index}"
            partitions[pid].data.append(item)
            self._round_robin_index = (self._round_robin_index + 1) % self.config.num_partitions

        self.partitions = partitions
        return partitions

    def get_partition(self, partition_id: str) -> Optional[Partition]:
        """Get partition by ID."""
        return self.partitions.get(partition_id)

    def get_all_partitions(self) -> List[Partition]:
        """Get all partitions."""
        return list(self.partitions.values())


class PartitionProcessor:
    """Processes data in partitions."""

    def __init__(self, partitioner: DataPartitioner):
        self.partitioner = partitioner

    async def process_partitions(
        self,
        processor: Callable[[Partition], Any],
        parallel: bool = True
    ) -> Dict[str, Any]:
        """Process all partitions."""
        results = {}

        if parallel:
            tasks = [
                self._process_single(processor, pid, partition)
                for pid, partition in self.partitioner.partitions.items()
            ]
            completed = await asyncio.gather(*tasks)
            for pid, result in zip(self.partitioner.partitions.keys(), completed):
                results[pid] = result
        else:
            for pid, partition in self.partitioner.partitions.items():
                result = await self._process_single(processor, pid, partition)
                results[pid] = result

        return results

    async def _process_single(
        self,
        processor: Callable[[Partition], Any],
        partition_id: str,
        partition: Partition
    ) -> Any:
        """Process single partition."""
        try:
            if asyncio.iscoroutinefunction(processor):
                return await processor(partition)
            else:
                return processor(partition)
        except Exception as e:
            logger.error(f"Partition {partition_id} processing error: {e}")
            return None


def main():
    """Demonstrate partitioning."""
    data = [{"id": i, "value": i * 10} for i in range(100)]

    config = PartitionConfig(
        strategy=PartitionStrategy.HASH,
        num_partitions=4
    )

    partitioner = DataPartitioner(config)
    partitions = partitioner.partition(data, lambda x: x["id"])

    for pid, partition in partitions.items():
        print(f"{pid}: {len(partition)} items")


if __name__ == "__main__":
    main()
