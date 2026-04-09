"""
API partitioning and consistent hashing for distributed routing.

This module provides consistent hashing implementation for distributing
API requests across multiple backend nodes with minimal reshuffling.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from collections import defaultdict
import bisect
import threading

logger = logging.getLogger(__name__)


@dataclass
class Node:
    """Represents a node in the hash ring."""
    id: str
    address: str
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    healthy: bool = True
    current_load: int = 0
    max_load: int = 1000

    @property
    def is_available(self) -> bool:
        """Check if node can accept requests."""
        return self.healthy and self.current_load < self.max_load


class HashRing:
    """
    Consistent hashing ring for distributed node routing.

    Features:
    - Consistent hashing with virtual nodes
    - Configurable replication factor
    - Node health tracking
    - Weighted distribution
    - Minimal key remapping on node changes

    Example:
        >>> ring = HashRing(replication=150)
        >>> ring.add_node("node1", "10.0.0.1:8000", weight=2.0)
        >>> ring.add_node("node2", "10.0.0.2:8000", weight=1.0)
        >>> node = ring.get_node("user:123")
        >>> node = ring.get_nodes("session:abc", count=2)  # For replication
    """

    def __init__(
        self,
        replication: int = 150,
        hash_function: Optional[Callable[[bytes], int]] = None,
    ):
        """
        Initialize the hash ring.

        Args:
            replication: Number of virtual nodes per physical node
            hash_function: Custom hash function (default: md5)
        """
        self.replication = replication
        self.hash_function = hash_function or self._default_hash
        self._nodes: Dict[str, Node] = {}
        self._ring: List[int] = []
        self._ring_nodes: List[Node] = []
        self._node_positions: Dict[str, List[int]] = defaultdict(list)
        self._lock = threading.RLock()
        logger.info(f"HashRing initialized (replication={replication})")

    @staticmethod
    def _default_hash(key: bytes) -> int:
        """Default hash using MD5."""
        return int(hashlib.md5(key).hexdigest(), 16)

    def add_node(
        self,
        node_id: str,
        address: str,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Node:
        """
        Add a node to the ring.

        Args:
            node_id: Unique node identifier
            address: Node address/endpoint
            weight: Relative weight for distribution
            metadata: Optional node metadata

        Returns:
            Created Node
        """
        with self._lock:
            if node_id in self._nodes:
                raise ValueError(f"Node {node_id} already exists")

            node = Node(
                id=node_id,
                address=address,
                weight=weight,
                metadata=metadata or {},
            )
            self._nodes[node_id] = node
            self._rebuild_ring()
            logger.info(f"Node added: {node_id} ({address}) weight={weight}")
            return node

    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the ring."""
        with self._lock:
            if node_id not in self._nodes:
                return False

            del self._nodes[node_id]
            self._rebuild_ring()
            logger.info(f"Node removed: {node_id}")
            return True

    def update_node_weight(self, node_id: str, weight: float) -> None:
        """Update a node's weight."""
        with self._lock:
            if node_id not in self._nodes:
                raise ValueError(f"Node {node_id} not found")
            self._nodes[node_id].weight = weight
            self._rebuild_ring()
            logger.info(f"Node {node_id} weight updated to {weight}")

    def set_node_healthy(self, node_id: str, healthy: bool) -> None:
        """Set node health status."""
        with self._lock:
            if node_id in self._nodes:
                self._nodes[node_id].healthy = healthy
                logger.info(f"Node {node_id} health set to {healthy}")

    def get_node(self, key: str) -> Optional[Node]:
        """
        Get the primary node for a key.

        Args:
            key: The key to hash

        Returns:
            Node responsible for this key, or None if ring is empty
        """
        nodes = self.get_nodes(key, count=1)
        return nodes[0] if nodes else None

    def get_nodes(self, key: str, count: int = 1) -> List[Node]:
        """
        Get multiple nodes for a key (for replication).

        Args:
            key: The key to hash
            count: Number of nodes to return

        Returns:
            List of nodes responsible for this key
        """
        with self._lock:
            if not self._ring:
                return []

            key_hash = self.hash_function(key.encode("utf-8"))
            idx = self._find_position(key_hash)

            result = []
            seen_ids = set()
            attempts = 0
            max_attempts = len(self._ring) * 2

            while len(result) < count and attempts < max_attempts:
                node = self._ring_nodes[idx % len(self._ring_nodes)]
                idx += 1
                attempts += 1

                if node.id in seen_ids:
                    continue
                if not node.is_available:
                    continue

                seen_ids.add(node.id)
                result.append(node)

            return result

    def _find_position(self, hash_value: int) -> int:
        """Find the position in the ring for a hash value."""
        if not self._ring:
            return 0
        return bisect.bisect_left(self._ring, hash_value)

    def _rebuild_ring(self) -> None:
        """Rebuild the hash ring with virtual nodes."""
        self._ring = []
        self._ring_nodes = []
        self._node_positions.clear()

        for node in self._nodes.values():
            for i in range(self.replication):
                virtual_key = f"{node.id}:{i}"
                hash_value = self.hash_function(virtual_key.encode("utf-8"))

                self._ring.append(hash_value)
                self._ring_nodes.append(node)
                self._node_positions[node.id].append(hash_value)

        sorted_indices = sorted(range(len(self._ring)), key=lambda i: self._ring[i])
        self._ring = [self._ring[i] for i in sorted_indices]
        self._ring_nodes = [self._ring_nodes[i] for i in sorted_indices]

        logger.debug(
            f"Ring rebuilt: {len(self._ring)} virtual nodes, "
            f"{len(self._nodes)} physical nodes"
        )

    def get_ring_info(self) -> Dict[str, Any]:
        """Get information about the ring."""
        with self._lock:
            return {
                "total_nodes": len(self._nodes),
                "virtual_nodes": len(self._ring),
                "replication_factor": self.replication,
                "nodes": {
                    node_id: {
                        "address": node.address,
                        "weight": node.weight,
                        "healthy": node.healthy,
                        "virtual_positions": len(self._node_positions.get(node_id, [])),
                    }
                    for node_id, node in self._nodes.items()
                },
            }

    def __len__(self) -> int:
        """Get number of physical nodes."""
        return len(self._nodes)


class ConsistentHashRouter:
    """
    Router using consistent hashing for distributed API routing.

    Example:
        >>> router = ConsistentHashRouter()
        >>> router.add_backend("api1", "http://10.0.0.1:8000")
        >>> router.add_backend("api2", "http://10.0.0.2:8000")
        >>>
        >>> # Returns (node, address) tuple
        >>> node, addr = router.route("user:123")
        >>> response = requests.get(f"{addr}/users/123")
    """

    def __init__(self, replication: int = 150):
        """Initialize the router."""
        self.ring = HashRing(replication=replication)
        self._route_count = 0
        self._route_errors = 0
        logger.info("ConsistentHashRouter initialized")

    def add_backend(
        self,
        backend_id: str,
        address: str,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a backend server."""
        self.ring.add_node(backend_id, address, weight, metadata)

    def remove_backend(self, backend_id: str) -> bool:
        """Remove a backend server."""
        return self.ring.remove_node(backend_id)

    def route(self, key: str) -> Optional[Tuple[Node, str]]:
        """
        Route a request to the appropriate backend.

        Args:
            key: Routing key (e.g., user_id, session_id)

        Returns:
            Tuple of (Node, address) or None
        """
        node = self.ring.get_node(key)
        if node:
            self._route_count += 1
            return (node, node.address)
        self._route_count += 1
        self._route_errors += 1
        return None

    def route_replicated(self, key: str, count: int = 2) -> List[Tuple[Node, str]]:
        """
        Route to multiple backends for replication.

        Args:
            key: Routing key
            count: Number of backends

        Returns:
            List of (Node, address) tuples
        """
        nodes = self.ring.get_nodes(key, count=count)
        return [(n, n.address) for n in nodes]

    def get_stats(self) -> Dict[str, Any]:
        """Get router statistics."""
        return {
            "total_backends": len(self.ring),
            "route_count": self._route_count,
            "route_errors": self._route_errors,
            "error_rate": (
                self._route_errors / self._route_count
                if self._route_count > 0
                else 0.0
            ),
            "ring": self.ring.get_ring_info(),
        }


class PartitionManager:
    """
    Manages data partitioning across multiple shards.

    Example:
        >>> manager = PartitionManager(num_partitions=16)
        >>> manager.add_shard("shard0", "10.0.0.1:27017")
        >>> manager.add_shard("shard1", "10.0.0.2:27017")
        >>>
        >>> partition = manager.get_partition("user:123")
        >>> shard_info = manager.get_shard_info(partition)
    """

    def __init__(self, num_partitions: int = 256):
        """
        Initialize partition manager.

        Args:
            num_partitions: Total number of partitions
        """
        self.num_partitions = num_partitions
        self._shards: List[Dict[str, Any]] = []
        self._partition_map: List[int] = []
        self._lock = threading.Lock()
        logger.info(f"PartitionManager initialized (partitions={num_partitions})")

    def add_shard(
        self,
        shard_id: str,
        address: str,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Add a shard and return its first partition index.

        Args:
            shard_id: Unique shard identifier
            address: Shard address
            weight: Relative weight
            metadata: Optional metadata

        Returns:
            Starting partition index
        """
        with self._lock:
            partitions_needed = max(1, int(weight * self.num_partitions / sum(s.get("weight", 1) for s in self._shards + [{"weight": 1}])))

            shard = {
                "id": shard_id,
                "address": address,
                "weight": weight,
                "metadata": metadata or {},
                "partitions": [],
            }

            start_idx = len(self._partition_map)
            for i in range(partitions_needed):
                shard["partitions"].append(start_idx + i)
                self._partition_map.append(len(self._shards))

            self._shards.append(shard)
            logger.info(
                f"Shard added: {shard_id} ({address}) - "
                f"{partitions_needed} partitions starting at {start_idx}"
            )
            return start_idx

    def get_partition(self, key: str) -> int:
        """Get partition number for a key."""
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return hash_val % self.num_partitions

    def get_shard(self, key: str) -> Optional[Dict[str, Any]]:
        """Get shard info for a key."""
        partition = self.get_partition(key)
        return self.get_shard_by_partition(partition)

    def get_shard_by_partition(self, partition: int) -> Optional[Dict[str, Any]]:
        """Get shard info by partition number."""
        with self._lock:
            if partition >= len(self._partition_map):
                return None
            shard_idx = self._partition_map[partition]
            return self._shards[shard_idx] if shard_idx < len(self._shards) else None

    def get_stats(self) -> Dict[str, Any]:
        """Get partition statistics."""
        with self._lock:
            return {
                "total_partitions": self.num_partitions,
                "total_shards": len(self._shards),
                "shards": [
                    {
                        "id": s["id"],
                        "address": s["address"],
                        "weight": s["weight"],
                        "num_partitions": len(s["partitions"]),
                        "partitions": s["partitions"],
                    }
                    for s in self._shards
                ],
            }
