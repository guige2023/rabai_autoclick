"""Data Partitioning Action module.

Provides data partitioning strategies including range,
hash, list, and round-robin partitioning for distributed
processing and parallel execution.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import numpy as np


@dataclass
class Partition:
    """A data partition."""

    partition_id: int
    data: list[Any]
    size: int = field(init=False)

    def __post_init__(self):
        self.size = len(self.data)

    def is_empty(self) -> bool:
        """Check if partition is empty."""
        return len(self.data) == 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "partition_id": self.partition_id,
            "size": self.size,
        }


@dataclass
class PartitioningConfig:
    """Configuration for partitioning."""

    num_partitions: int = 4
    partition_func: Optional[Callable[[Any], int]] = None
    hash_modulo: Optional[int] = None


def range_partition(
    data: list[Any],
    num_partitions: int,
    key_func: Optional[Callable[[Any], Any]] = None,
) -> list[Partition]:
    """Partition data by range on a key.

    Args:
        data: Data to partition
        num_partitions: Number of partitions
        key_func: Function to extract sort key

    Returns:
        List of Partitions
    """
    if not data:
        return [Partition(partition_id=i, data=[]) for i in range(num_partitions)]

    sorted_data = sorted(data, key=key_func or (lambda x: x))
    partition_size = len(sorted_data) / num_partitions

    partitions = []
    for i in range(num_partitions):
        start_idx = int(i * partition_size)
        end_idx = int((i + 1) * partition_size) if i < num_partitions - 1 else len(sorted_data)
        partitions.append(
            Partition(
                partition_id=i,
                data=sorted_data[start_idx:end_idx],
            )
        )

    return partitions


def hash_partition(
    data: list[Any],
    num_partitions: int,
    key_func: Optional[Callable[[Any], Any]] = None,
) -> list[Partition]:
    """Partition data using consistent hash.

    Args:
        data: Data to partition
        num_partitions: Number of partitions
        key_func: Function to extract partition key

    Returns:
        List of Partitions
    """
    partitions = [Partition(partition_id=i, data=[]) for i in range(num_partitions)]

    for item in data:
        key = key_func(item) if key_func else item
        key_str = str(key).encode()
        hash_val = int(hashlib.md5(key_str).hexdigest(), 16)
        partition_id = hash_val % num_partitions
        partitions[partition_id].data.append(item)

    return partitions


def list_partition(
    data: list[Any],
    partition_func: Callable[[Any], int],
) -> list[Partition]:
    """Partition data using custom function.

    Args:
        data: Data to partition
        partition_func: Function returning partition ID

    Returns:
        List of Partitions
    """
    partition_map: dict[int, list[Any]] = defaultdict(list)

    for item in data:
        partition_id = partition_func(item)
        partition_map[partition_id].append(item)

    partition_ids = sorted(partition_map.keys())
    return [
        Partition(partition_id=pid, data=partition_map[pid])
        for pid in partition_ids
    ]


def round_robin_partition(
    data: list[Any],
    num_partitions: int,
) -> list[Partition]:
    """Partition data using round-robin.

    Args:
        data: Data to partition
        num_partitions: Number of partitions

    Returns:
        List of Partitions
    """
    partitions = [Partition(partition_id=i, data=[]) for i in range(num_partitions)]

    for i, item in enumerate(data):
        partition_id = i % num_partitions
        partitions[partition_id].data.append(item)

    return partitions


def stratified_partition(
    data: list[dict[str, Any]],
    num_partitions: int,
    stratify_key: str,
) -> list[Partition]:
    """Stratified partition preserving distribution.

    Args:
        data: Data to partition
        num_partitions: Number of partitions
        stratify_key: Key to stratify by

    Returns:
        List of Partitions
    """
    strata: dict[Any, list[dict[str, Any]]] = defaultdict(list)

    for record in data:
        if stratify_key in record:
            strata[record[stratify_key]].append(record)

    for stratum_data in strata.values():
        stratum_data.sort(key=lambda x: hash(str(x)))

    partitions = [Partition(partition_id=i, data=[]) for i in range(num_partitions)]

    for stratum_key, stratum_data in strata.items():
        for i, item in enumerate(stratum_data):
            partition_id = i % num_partitions
            partitions[partition_id].data.append(item)

    return partitions


@dataclass
class RangePartitionBoundary:
    """Boundary for range partition."""

    partition_id: int
    min_value: Any
    max_value: Any
    count: int = 0


def compute_range_boundaries(
    data: list[Any],
    num_partitions: int,
    key_func: Optional[Callable[[Any], Any]] = None,
) -> list[RangePartitionBoundary]:
    """Compute boundaries for range partitioning.

    Args:
        data: Data to partition
        num_partitions: Number of partitions
        key_func: Key extraction function

    Returns:
        List of boundaries
    """
    if not data:
        return []

    if key_func is None:
        key_func = lambda x: x

    sorted_data = sorted(data, key=key_func)
    values = [key_func(item) for item in sorted_data]

    boundaries = []
    partition_size = len(values) / num_partitions

    for i in range(num_partitions):
        start_idx = int(i * partition_size)
        end_idx = int((i + 1) * partition_size) if i < num_partitions - 1 else len(values)

        boundaries.append(
            RangePartitionBoundary(
                partition_id=i,
                min_value=values[start_idx],
                max_value=values[end_idx - 1] if end_idx > start_idx else values[start_idx],
                count=end_idx - start_idx,
            )
        )

    return boundaries


@dataclass
class PartitionStats:
    """Statistics for partitions."""

    total_records: int
    num_partitions: int
    min_partition_size: int
    max_partition_size: int
    avg_partition_size: float
    empty_partitions: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_records": self.total_records,
            "num_partitions": self.num_partitions,
            "min_partition_size": self.min_partition_size,
            "max_partition_size": self.max_partition_size,
            "avg_partition_size": self.avg_partition_size,
            "empty_partitions": self.empty_partitions,
        }


def compute_partition_stats(partitions: list[Partition]) -> PartitionStats:
    """Compute statistics for partitions.

    Args:
        partitions: List of partitions

    Returns:
        PartitionStats
    """
    sizes = [p.size for p in partitions]
    empty_count = sum(1 for p in partitions if p.is_empty())

    return PartitionStats(
        total_records=sum(sizes),
        num_partitions=len(partitions),
        min_partition_size=min(sizes) if sizes else 0,
        max_partition_size=max(sizes) if sizes else 0,
        avg_partition_size=sum(sizes) / len(sizes) if sizes else 0,
        empty_partitions=empty_count,
    )


class DataPartitioner:
    """Main partitioning interface."""

    def __init__(self, config: Optional[PartitioningConfig] = None):
        self.config = config or PartitioningConfig()

    def partition(
        self,
        data: list[Any],
        strategy: str = "hash",
        **kwargs: Any,
    ) -> list[Partition]:
        """Partition data using specified strategy.

        Args:
            data: Data to partition
            strategy: Partitioning strategy ('range', 'hash', 'roundrobin', 'stratified')
            **kwargs: Strategy-specific arguments

        Returns:
            List of Partitions
        """
        num_partitions = kwargs.get("num_partitions", self.config.num_partitions)

        if strategy == "range":
            return range_partition(
                data,
                num_partitions,
                key_func=kwargs.get("key_func"),
            )
        elif strategy == "hash":
            return hash_partition(
                data,
                num_partitions,
                key_func=kwargs.get("key_func"),
            )
        elif strategy == "roundrobin":
            return round_robin_partition(data, num_partitions)
        elif strategy == "stratified":
            return stratified_partition(
                data,
                num_partitions,
                stratify_key=kwargs.get("stratify_key", ""),
            )
        elif self.config.partition_func:
            return list_partition(data, self.config.partition_func)
        else:
            return hash_partition(data, num_partitions)

    def rebalance(
        self,
        partitions: list[Partition],
        target_partitions: int,
    ) -> list[Partition]:
        """Rebalance partitions to new count.

        Args:
            partitions: Existing partitions
            target_partitions: Target number of partitions

        Returns:
            New list of partitions
        """
        all_data = []
        for p in partitions:
            all_data.extend(p.data)

        if not all_data:
            return [Partition(partition_id=i, data=[]) for i in range(target_partitions)]

        return hash_partition(all_data, target_partitions)
