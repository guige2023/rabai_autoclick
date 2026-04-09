"""
Data partition action for distributed data processing.

Provides consistent hashing, range partitioning, and key-based sharding.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import hashlib
import bisect


class DataPartitionAction:
    """Data partitioning with multiple strategies."""

    def __init__(
        self,
        num_partitions: int = 16,
        strategy: str = "hash",
        replication_factor: int = 1,
    ) -> None:
        """
        Initialize data partitioner.

        Args:
            num_partitions: Number of partitions
            strategy: Partitioning strategy ('hash', 'range', 'list')
            replication_factor: Number of replicas per partition
        """
        self.num_partitions = num_partitions
        self.strategy = strategy
        self.replication_factor = replication_factor

        self._partition_map: Dict[int, str] = {}
        self._range_boundaries: List[Any] = []
        self._list_partitions: Dict[str, int] = {}
        self._replica_map: Dict[int, List[int]] = {}

        self._initialize_partitions()

    def _initialize_partitions(self) -> None:
        """Initialize partition mapping."""
        for i in range(self.num_partitions):
            self._partition_map[i] = f"partition_{i}"
            self._replica_map[i] = self._get_replicas(i)

    def _get_replicas(self, partition_id: int) -> List[int]:
        """Get replica partition IDs for given partition."""
        replicas = [partition_id]
        for _ in range(self.replication_factor - 1):
            next_replica = (partition_id + len(replicas)) % self.num_partitions
            if next_replica not in replicas:
                replicas.append(next_replica)
        return replicas

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute partition operation.

        Args:
            params: Dictionary containing:
                - operation: 'partition', 'get', 'add_node', 'remove_node'
                - key: Partition key
                - value: Value to partition
                - node_id: Node identifier

        Returns:
            Dictionary with partition assignment
        """
        operation = params.get("operation", "partition")

        if operation == "partition":
            return self._get_partition(params)
        elif operation == "get":
            return self._get_partition_for_key(params)
        elif operation == "add_node":
            return self._add_partition(params)
        elif operation == "remove_node":
            return self._remove_partition(params)
        elif operation == "rebalance":
            return self._rebalance_partitions(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _get_partition(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get partition assignment for key."""
        key = params.get("key", "")
        include_replicas = params.get("include_replicas", False)

        if not key:
            return {"success": False, "error": "Key is required"}

        if self.strategy == "hash":
            partition_id = self._hash_partition(key)
        elif self.strategy == "range":
            partition_id = self._range_partition(key)
        elif self.strategy == "list":
            partition_id = self._list_partition(key)
        else:
            partition_id = self._hash_partition(key)

        result = {
            "success": True,
            "key": key,
            "partition_id": partition_id,
            "partition_name": self._partition_map[partition_id],
            "strategy": self.strategy,
        }

        if include_replicas or self.replication_factor > 1:
            result["replicas"] = self._replica_map[partition_id]

        return result

    def _get_partition_for_key(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get partition info for existing key."""
        return self._get_partition(params)

    def _hash_partition(self, key: str) -> int:
        """Hash-based partitioning."""
        hash_value = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
        return hash_value % self.num_partitions

    def _range_partition(self, key: str) -> int:
        """Range-based partitioning."""
        if not self._range_boundaries:
            return 0

        key_numeric = self._key_to_numeric(key)
        idx = bisect.bisect_right(self._range_boundaries, key_numeric)
        return min(idx, self.num_partitions - 1)

    def _list_partition(self, key: str) -> int:
        """List-based partitioning."""
        if key in self._list_partitions:
            return self._list_partitions[key]

        hash_value = self._hash_partition(key)
        self._list_partitions[key] = hash_value
        return hash_value

    def _key_to_numeric(self, key: str) -> int:
        """Convert key to numeric value for range partitioning."""
        try:
            return int(key)
        except (ValueError, TypeError):
            return int(hashlib.md5(str(key).encode()).hexdigest(), 16) % (10**9)

    def _add_partition(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add new partition."""
        node_id = params.get("node_id", "")

        if len(self._partition_map) >= self.num_partitions * 2:
            return {"success": False, "error": "Cannot add more partitions"}

        new_partition_id = max(self._partition_map.keys()) + 1
        partition_name = node_id or f"partition_{new_partition_id}"

        self._partition_map[new_partition_id] = partition_name
        self._replica_map[new_partition_id] = self._get_replicas(new_partition_id)

        return {
            "success": True,
            "partition_id": new_partition_id,
            "partition_name": partition_name,
        }

    def _remove_partition(self, params: dict[str, Any]) -> dict[str, Any]:
        """Remove partition."""
        partition_id = params.get("partition_id")

        if partition_id not in self._partition_map:
            return {"success": False, "error": "Partition not found"}

        partition_name = self._partition_map[partition_id]
        del self._partition_map[partition_id]
        if partition_id in self._replica_map:
            del self._replica_map[partition_id]

        return {"success": True, "removed_partition_id": partition_id, "name": partition_name}

    def _rebalance_partitions(self, params: dict[str, Any]) -> dict[str, Any]:
        """Rebalance partitions across nodes."""
        nodes = params.get("nodes", [])

        if not nodes:
            return {"success": False, "error": "Nodes list is required"}

        partition_size = len(self._partition_map) // len(nodes)
        remainder = len(self._partition_map) % len(nodes)

        assignments = {}
        partition_idx = 0

        for i, node in enumerate(nodes):
            node_partitions = []
            for _ in range(partition_size + (1 if i < remainder else 0)):
                if partition_idx in self._partition_map:
                    node_partitions.append(partition_idx)
                partition_idx += 1
            assignments[node] = node_partitions

        return {"success": True, "assignments": assignments}

    def set_range_boundaries(self, boundaries: List[Any]) -> None:
        """Set range boundaries for range partitioning."""
        self._range_boundaries = sorted(boundaries)

    def get_partition_stats(self) -> Dict[str, Any]:
        """Get partition statistics."""
        return {
            "num_partitions": len(self._partition_map),
            "strategy": self.strategy,
            "replication_factor": self.replication_factor,
            "partitions": {
                pid: {"name": name, "replicas": self._replica_map.get(pid, [])}
                for pid, name in self._partition_map.items()
            },
        }
