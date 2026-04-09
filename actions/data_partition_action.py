"""Data Partition and Sharding Utility.

This module provides data partitioning:
- Range-based partitioning
- Hash-based partitioning
- List-based partitioning
- Partition routing

Example:
    >>> from actions.data_partition_action import DataPartitioner
    >>> partitioner = DataPartitioner(strategy="hash")
    >>> partition_id = partitioner.get_partition(record, num_partitions=10)
"""

from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DataPartitioner:
    """Partitions data for distributed processing."""

    def __init__(
        self,
        strategy: str = "hash",
        num_partitions: int = 10,
    ) -> None:
        """Initialize the data partitioner.

        Args:
            strategy: Partitioning strategy (hash, range, list, round_robin).
            num_partitions: Number of partitions.
        """
        self._strategy = strategy
        self._num_partitions = num_partitions
        self._lock = threading.Lock()
        self._stats = {"partitions_assigned": 0}

    def get_partition(
        self,
        key: Any,
        num_partitions: Optional[int] = None,
    ) -> int:
        """Get partition number for a key.

        Args:
            key: Partition key.
            num_partitions: Override number of partitions.

        Returns:
            Partition number (0 to num_partitions-1).
        """
        n = num_partitions or self._num_partitions

        with self._lock:
            self._stats["partitions_assigned"] += 1

        if self._strategy == "hash":
            return self._hash_partition(key, n)
        elif self._strategy == "range":
            return self._range_partition(key, n)
        elif self._strategy == "round_robin":
            return self._round_robin_partition(n)
        else:
            return self._hash_partition(key, n)

    def _hash_partition(self, key: Any, n: int) -> int:
        """Hash-based partitioning."""
        if key is None:
            return 0
        key_str = str(key).encode("utf-8")
        hash_val = int(hashlib.md5(key_str).hexdigest(), 16)
        return hash_val % n

    def _range_partition(self, key: Any, n: int) -> int:
        """Range-based partitioning."""
        if isinstance(key, int):
            if n == 1:
                return 0
            return min(int(key / (100 / n)), n - 1)
        return self._hash_partition(key, n)

    def _round_robin_partition(self, n: int) -> int:
        """Round-robin partitioning."""
        with self._lock:
            idx = self._stats["partitions_assigned"] % n
        return idx

    def partition_records(
        self,
        records: list[dict[str, Any]],
        key_field: str,
        num_partitions: Optional[int] = None,
    ) -> list[list[dict[str, Any]]]:
        """Partition records into buckets.

        Args:
            records: List of records.
            key_field: Field to use as partition key.
            num_partitions: Number of partitions.

        Returns:
            List of partition buckets.
        """
        n = num_partitions or self._num_partitions
        partitions: list[list] = [[] for _ in range(n)]

        for record in records:
            key = record.get(key_field)
            part_idx = self.get_partition(key, n)
            partitions[part_idx].append(record)

        return partitions

    def partition_by_range(
        self,
        records: list[dict[str, Any]],
        key_field: str,
        ranges: list[tuple[Any, Any]],
    ) -> list[list[dict[str, Any]]]:
        """Partition records by value ranges.

        Args:
            records: List of records.
            key_field: Field to partition on.
            ranges: List of (min, max) tuples.

        Returns:
            List of partitioned buckets.
        """
        partitions: list[list] = [[] for _ in range(len(ranges) + 1)]

        for record in records:
            key = record.get(key_field)
            placed = False

            for i, (min_val, max_val) in enumerate(ranges):
                if min_val <= key <= max_val:
                    partitions[i].append(record)
                    placed = True
                    break

            if not placed:
                partitions[-1].append(record)

        return partitions

    def partition_by_list(
        self,
        records: list[dict[str, Any]],
        key_field: str,
        partition_values: list[list[Any]],
    ) -> list[list[dict[str, Any]]]:
        """Partition records by value lists.

        Args:
            records: List of records.
            key_field: Field to partition on.
            partition_values: List of value lists for each partition.

        Returns:
            List of partitioned buckets.
        """
        partitions: list[list] = [[] for _ in range(len(partition_values))]
        value_to_partition = {}
        for i, values in enumerate(partition_values):
            for v in values:
                value_to_partition[v] = i

        for record in records:
            key = record.get(key_field)
            part_idx = value_to_partition.get(key, -1)
            if 0 <= part_idx < len(partitions):
                partitions[part_idx].append(record)
            else:
                partitions[-1].append(record)

        return partitions

    def get_partition_stats(
        self,
        records: list[dict[str, Any]],
        key_field: str,
        num_partitions: Optional[int] = None,
    ) -> dict[int, int]:
        """Get partition statistics.

        Args:
            records: List of records.
            key_field: Partition key field.
            num_partitions: Number of partitions.

        Returns:
            Dict mapping partition ID to record count.
        """
        n = num_partitions or self._num_partitions
        counts = {i: 0 for i in range(n)}

        for record in records:
            key = record.get(key_field)
            part_idx = self.get_partition(key, n)
            counts[part_idx] += 1

        return counts

    def get_stats(self) -> dict[str, int]:
        """Get partitioner statistics."""
        with self._lock:
            return dict(self._stats)
