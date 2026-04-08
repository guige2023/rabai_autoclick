"""
Data Partitioner Action Module.

Partitions data into segments based on keys,
supports hash-based and range-based partitioning.
"""

from __future__ import annotations

from typing import Any, Callable, Hashable, Optional
from dataclasses import dataclass
import logging
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class Partition:
    """Data partition."""
    index: int
    key: Any
    data: list = None


class DataPartitionerAction:
    """
    Data partitioning across multiple segments.

    Supports hash-based and key-based partitioning
    for parallel processing.

    Example:
        partitioner = DataPartitionerAction(num_partitions=4)
        partitions = partitioner.partition(data, key_func=lambda x: x["id"])
    """

    def __init__(
        self,
        num_partitions: int = 4,
        partition_func: Optional[Callable[[Any, int], int]] = None,
    ) -> None:
        self.num_partitions = num_partitions
        self.partition_func = partition_func or self._default_hash_partition

    def partition(
        self,
        data: list,
        key_func: Callable[[Any], Hashable],
    ) -> list[list]:
        """Partition data based on key function."""
        partitions: list[list] = [[] for _ in range(self.num_partitions)]

        for item in data:
            key = key_func(item)
            partition_idx = self.partition_func(key, self.num_partitions)
            partitions[partition_idx].append(item)

        return partitions

    def partition_by_hash(
        self,
        data: list,
        key_field: str,
    ) -> list[list]:
        """Partition data using hash of key field."""
        def hash_key(item):
            return hash(item.get(key_field, ""))

        return self.partition(data, hash_key)

    def partition_by_range(
        self,
        data: list,
        key_func: Callable[[Any], float],
    ) -> list[list]:
        """Partition data using range-based bucketing."""
        values = [key_func(item) for item in data]

        if not values:
            return [[] for _ in range(self.num_partitions)]

        min_val = min(values)
        max_val = max(values)

        if min_val == max_val:
            return [[item for item in data]]

        range_size = (max_val - min_val) / self.num_partitions
        partitions: list[list] = [[] for _ in range(self.num_partitions)]

        for item in data:
            value = key_func(item)
            idx = min(int((value - min_val) / range_size), self.num_partitions - 1)
            partitions[idx].append(item)

        return partitions

    def get_partition_stats(
        self,
        partitions: list[list],
    ) -> list[dict[str, Any]]:
        """Get statistics for each partition."""
        return [
            {
                "index": i,
                "size": len(p),
                "percent": (len(p) / sum(len(x) for x in partitions) * 100)
                           if partitions else 0,
            }
            for i, p in enumerate(partitions)
        ]

    @staticmethod
    def _default_hash_partition(key: Hashable, num_partitions: int) -> int:
        """Default hash-based partitioning."""
        if isinstance(key, str):
            hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        else:
            hash_val = hash(key)

        return abs(hash_val) % num_partitions
