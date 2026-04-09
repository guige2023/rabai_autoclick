"""API partitioning and sharding utilities.

This module provides API partitioning:
- Consistent hashing
- Round-robin distribution
- Weighted routing
- Partition management

Example:
    >>> from actions.api_partition_action import PartitionRouter
    >>> router = PartitionRouter(partitions=10)
    >>> partition = router.route(api_key)
"""

from __future__ import annotations

import hashlib
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = __import__('logging').getLogger(__name__)


@dataclass
class Partition:
    """A partition for routing."""
    id: int
    weight: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)


class PartitionRouter:
    """Route requests to partitions using various strategies.

    Example:
        >>> router = PartitionRouter(partitions=10, strategy="consistent_hash")
        >>> partition = router.route("user-123")
    """

    STRATEGIES = ["consistent_hash", "round_robin", "random", "weighted"]

    def __init__(
        self,
        partitions: int = 10,
        strategy: str = "consistent_hash",
        weights: Optional[list[int]] = None,
    ) -> None:
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}")
        self.partitions = partitions
        self.strategy = strategy
        self._partition_weights = weights or [1] * partitions
        self._round_robin_index = 0
        self._round_robin_lock = threading.Lock()
        self._partition_metadata: dict[int, dict[str, Any]] = {}

    def route(self, key: str) -> int:
        """Route a key to a partition.

        Args:
            key: Key to route.

        Returns:
            Partition index.
        """
        if self.strategy == "consistent_hash":
            return self._consistent_hash(key)
        elif self.strategy == "round_robin":
            return self._round_robin()
        elif self.strategy == "random":
            import random
            return random.randint(0, self.partitions - 1)
        elif self.strategy == "weighted":
            return self._weighted_route(key)
        return self._consistent_hash(key)

    def _consistent_hash(self, key: str) -> int:
        """Consistent hash routing."""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % self.partitions

    def _round_robin(self) -> int:
        """Round-robin routing."""
        with self._round_robin_lock:
            partition = self._round_robin_index
            self._round_robin_index = (self._round_robin_index + 1) % self.partitions
            return partition

    def _weighted_route(self, key: str) -> int:
        """Weighted routing based on partition weights."""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        total_weight = sum(self._partition_weights)
        normalized = (hash_value % total_weight) + 1
        cumulative = 0
        for i, weight in enumerate(self._partition_weights):
            cumulative += weight
            if normalized <= cumulative:
                return i
        return self.partitions - 1

    def set_partition_metadata(
        self,
        partition_id: int,
        metadata: dict[str, Any],
    ) -> None:
        """Set metadata for a partition."""
        self._partition_metadata[partition_id] = metadata

    def get_partition_metadata(self, partition_id: int) -> dict[str, Any]:
        """Get metadata for a partition."""
        return self._partition_metadata.get(partition_id, {})


class ShardManager:
    """Manage sharded data storage.

    Example:
        >>> manager = ShardManager(num_shards=4, key_func=lambda x: x["user_id"])
        >>> shard = manager.get_shard(user_data)
    """

    def __init__(
        self,
        num_shards: int = 10,
        key_func: Optional[Callable[[Any], str]] = None,
    ) -> None:
        self.num_shards = num_shards
        self.key_func = key_func or (lambda x: str(x) if not isinstance(x, str) else x)
        self._shards: dict[int, list[Any]] = defaultdict(list)
        self._shard_locks: dict[int, threading.Lock] = {
            i: threading.Lock() for i in range(num_shards)
        }

    def get_shard_id(self, key: str) -> int:
        """Get shard ID for a key."""
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_value % self.num_shards

    def get_shard(self, key: str) -> list[Any]:
        """Get all items in a shard."""
        shard_id = self.get_shard_id(key)
        return self._shards[shard_id]

    def add(self, item: Any, key: Optional[str] = None) -> int:
        """Add an item to its shard.

        Returns:
            Shard ID where item was added.
        """
        key = key or self.key_func(item)
        shard_id = self.get_shard_id(key)
        with self._shard_locks[shard_id]:
            self._shards[shard_id].append(item)
        return shard_id

    def remove(self, item: Any, key: Optional[str] = None) -> bool:
        """Remove an item from its shard."""
        key = key or self.key_func(item)
        shard_id = self.get_shard_id(key)
        with self._shard_locks[shard_id]:
            if item in self._shards[shard_id]:
                self._shards[shard_id].remove(item)
                return True
        return False

    def get_all_shard_ids(self) -> list[int]:
        """Get all shard IDs."""
        return list(range(self.num_shards))

    def get_shard_size(self, shard_id: int) -> int:
        """Get size of a shard."""
        return len(self._shards[shard_id])

    def rebalance(
        self,
        new_key_func: Optional[Callable[[Any], str]] = None,
    ) -> dict[int, int]:
        """Rebalance items across shards with new key function.

        Returns:
            Dictionary of moved item counts per shard.
        """
        if new_key_func:
            self.key_func = new_key_func
        moved: dict[int, int] = {i: 0 for i in range(self.num_shards)}
        for shard_id in list(self._shards.keys()):
            items = list(self._shards[shard_id])
            self._shards[shard_id].clear()
            for item in items:
                new_shard_id = self.get_shard_id(self.key_func(item))
                self._shards[new_shard_id].append(item)
                if new_shard_id != shard_id:
                    moved[new_shard_id] += 1
        return moved


def consistent_hash(key: str, num_slots: int = 1024) -> int:
    """Simple consistent hash function.

    Args:
        key: Key to hash.
        num_slots: Number of hash slots.

    Returns:
        Hash slot index.
    """
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_slots


def hash_ring_get_node(
    key: str,
    nodes: list[str],
    num_replicas: int = 100,
) -> list[str]:
    """Get nodes for a key using consistent hashing ring.

    Args:
        key: Key to route.
        nodes: List of node identifiers.
        num_replicas: Virtual node replicas.

    Returns:
        Ordered list of nodes for the key.
    """
    if not nodes:
        return []
    positions = []
    for node in nodes:
        for i in range(num_replicas):
            position_key = f"{node}:{i}"
            position = int(hashlib.md5(position_key.encode()).hexdigest(), 16)
            positions.append((position, node))
    key_position = int(hashlib.md5(key.encode()).hexdigest(), 16)
    sorted_positions = sorted(positions)
    for pos, node in sorted_positions:
        if pos >= key_position:
            idx = nodes.index(node)
            result = [nodes[(idx + i) % len(nodes)] for i in range(len(nodes))]
            return result
    return nodes
