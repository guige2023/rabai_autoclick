"""
API Partition Action Module.

Provides consistent hashing routing and data partitioning
for distributed API calls.
"""

import hashlib
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class PartitionStrategy(Enum):
    """Partition strategies."""
    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"


@dataclass
class PartitionNode:
    """Partition node."""
    id: str
    address: str
    weight: int = 1
    healthy: bool = True
    metadata: dict = field(default_factory=dict)


@dataclass
class PartitionConfig:
    """Partition configuration."""
    strategy: PartitionStrategy = PartitionStrategy.HASH
    virtual_nodes: int = 100
    replication_factor: int = 1
    node_timeout: float = 5.0


class ConsistentHash:
    """Consistent hashing implementation."""

    def __init__(self, virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._lock = asyncio.Lock()

    def _hash(self, key: str) -> int:
        """Hash function."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    async def add_node(self, node_id: str) -> None:
        """Add node to hash ring."""
        async with self._lock:
            for i in range(self.virtual_nodes):
                key = self._hash(f"{node_id}:{i}")
                self._ring[key] = node_id
            self._sorted_keys = sorted(self._ring.keys())
            self._rebuild_ring()

    async def remove_node(self, node_id: str) -> None:
        """Remove node from hash ring."""
        async with self._lock:
            keys_to_remove = [
                k for k, v in self._ring.items() if v == node_id
            ]
            for key in keys_to_remove:
                del self._ring[key]
            self._sorted_keys = sorted(self._ring.keys())

    def _rebuild_ring(self) -> None:
        """Rebuild sorted ring."""
        self._sorted_keys = sorted(self._ring.keys())

    async def get_node(self, key: str) -> Optional[str]:
        """Get node for key."""
        if not self._sorted_keys:
            return None

        hash_value = self._hash(key)
        pos = 0
        for i, ring_key in enumerate(self._sorted_keys):
            if ring_key >= hash_value:
                pos = i
                break
        else:
            pos = 0

        return self._ring.get(self._sorted_keys[pos])

    async def get_nodes(self, key: str, count: int) -> list[str]:
        """Get multiple nodes for key (replication)."""
        if not self._sorted_keys:
            return []

        hash_value = self._hash(key)
        nodes = []
        seen = set()

        pos = 0
        for i, ring_key in enumerate(self._sorted_keys):
            if ring_key >= hash_value:
                pos = i
                break
        else:
            pos = 0

        while len(nodes) < count and len(nodes) < len(self._sorted_keys):
            node = self._ring.get(self._sorted_keys[pos])
            if node and node not in seen:
                nodes.append(node)
                seen.add(node)
            pos = (pos + 1) % len(self._sorted_keys)

        return nodes


class RangePartitioner:
    """Range-based partitioner."""

    def __init__(self):
        self._ranges: list[tuple[Any, Any, str]] = []
        self._lock = asyncio.Lock()

    async def add_range(
        self,
        start: Any,
        end: Any,
        node_id: str
    ) -> None:
        """Add range mapping."""
        async with self._lock:
            self._ranges.append((start, end, node_id))
            self._ranges.sort(key=lambda x: x[0])

    async def remove_range(
        self,
        start: Any,
        end: Any,
        node_id: str
    ) -> None:
        """Remove range mapping."""
        async with self._lock:
            self._ranges = [
                r for r in self._ranges
                if not (r[0] == start and r[1] == end and r[2] == node_id)
            ]

    async def get_node(self, key: Any) -> Optional[str]:
        """Get node for key."""
        for start, end, node_id in self._ranges:
            if start <= key <= end:
                return node_id
        return None


class RoundRobinPartitioner:
    """Round-robin partitioner."""

    def __init__(self):
        self._nodes: list[str] = []
        self._current: int = 0
        self._lock = asyncio.Lock()

    async def add_node(self, node_id: str) -> None:
        """Add node."""
        async with self._lock:
            if node_id not in self._nodes:
                self._nodes.append(node_id)

    async def remove_node(self, node_id: str) -> None:
        """Remove node."""
        async with self._lock:
            if node_id in self._nodes:
                self._nodes.remove(node_id)

    async def get_node(self) -> Optional[str]:
        """Get next node."""
        async with self._lock:
            if not self._nodes:
                return None
            node = self._nodes[self._current]
            self._current = (self._current + 1) % len(self._nodes)
            return node


class APIPartitionAction:
    """
    Data partitioning for distributed API routing.

    Example:
        partitioner = APIPartitionAction(
            strategy=PartitionStrategy.HASH,
            virtual_nodes=150
        )

        await partitioner.add_node("node1", "http://api1.example.com")
        await partitioner.add_node("node2", "http://api2.example.com")

        node = await partitioner.get_node("user_123")
    """

    def __init__(
        self,
        strategy: PartitionStrategy = PartitionStrategy.HASH,
        virtual_nodes: int = 100
    ):
        self.config = PartitionConfig(
            strategy=strategy,
            virtual_nodes=virtual_nodes
        )
        self._nodes: dict[str, PartitionNode] = {}
        self._lock = asyncio.Lock()

        if strategy == PartitionStrategy.HASH:
            self._hasher = ConsistentHash(virtual_nodes)
        elif strategy == PartitionStrategy.RANGE:
            self._hasher = RangePartitioner()
        elif strategy == PartitionStrategy.ROUND_ROBIN:
            self._hasher = RoundRobinPartitioner()
        else:
            self._hasher = ConsistentHash(virtual_nodes)

    async def add_node(
        self,
        node_id: str,
        address: str,
        weight: int = 1,
        metadata: Optional[dict] = None
    ) -> None:
        """Add partition node."""
        async with self._lock:
            node = PartitionNode(
                id=node_id,
                address=address,
                weight=weight,
                metadata=metadata or {}
            )
            self._nodes[node_id] = node

            if self.config.strategy == PartitionStrategy.HASH:
                await self._hasher.add_node(node_id)
            elif self.config.strategy == PartitionStrategy.ROUND_ROBIN:
                await self._hasher.add_node(node_id)

    async def remove_node(self, node_id: str) -> None:
        """Remove partition node."""
        async with self._lock:
            if node_id in self._nodes:
                del self._nodes[node_id]

            if self.config.strategy == PartitionStrategy.HASH:
                await self._hasher.remove_node(node_id)
            elif self.config.strategy == PartitionStrategy.ROUND_ROBIN:
                await self._hasher.remove_node(node_id)

    async def get_node(self, key: str) -> Optional[str]:
        """Get node for key."""
        node_id = await self._hasher.get_node(key)
        if node_id and node_id in self._nodes:
            return self._nodes[node_id].address
        return None

    async def get_nodes(
        self,
        key: str,
        count: int = 1
    ) -> list[str]:
        """Get multiple nodes for key."""
        if self.config.strategy == PartitionStrategy.HASH:
            node_ids = await self._hasher.get_nodes(key, count)
            return [
                self._nodes[nid].address
                for nid in node_ids
                if nid in self._nodes
            ]
        else:
            node = await self._hasher.get_node()
            if node and node in self._nodes:
                return [self._nodes[node].address]
            return []

    async def get_all_nodes(self) -> list[PartitionNode]:
        """Get all nodes."""
        return list(self._nodes.values())

    async def mark_healthy(self, node_id: str, healthy: bool) -> None:
        """Mark node health status."""
        async with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].healthy = healthy

    async def route_request(
        self,
        key: str,
        func: Callable[[str], T],
        fallback: Optional[Callable[[str], T]] = None
    ) -> T:
        """Route request to appropriate node."""
        node_address = await self.get_node(key)

        if not node_address:
            if fallback:
                return fallback(key)
            raise ValueError("No available nodes")

        return func(node_address)
