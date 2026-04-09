"""
Data Partition Module.

Provides data partitioning strategies: hash, range, round-robin,
and consistent hashing for distributed data processing.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PartitionStrategy(Enum) if False else object:
    """Partition strategy types."""
    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    CONSISTENT_HASH = "consistent_hash"
    LIST = "list"


@dataclass
class PartitionConfig:
    """Configuration for data partitioning."""
    num_partitions: int = 4
    strategy: str = "hash"  # hash, range, round_robin, consistent_hash
    hash_field: str = "id"
    range_bounds: Optional[List[Any]] = None
    virtual_nodes: int = 100  # For consistent hashing
    replication_factor: int = 1


@dataclass
class Partition:
    """Container for a data partition."""
    partition_id: int
    data: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __iter__(self) -> Iterator[Any]:
        return iter(self.data)
        
    def __len__(self) -> int:
        return len(self.data)


class DataPartitioner:
    """
    Data partitioning for distributed processing.
    
    Example:
        partitioner = DataPartitioner(PartitionConfig(
            num_partitions=4,
            strategy="hash",
            hash_field="user_id"
        ))
        
        partitions = partitioner.partition(data, key_func=lambda x: x["user_id"])
        
        for pid, partition in enumerate(partitions):
            process_partition(partition)
    """
    
    def __init__(self, config: Optional[PartitionConfig] = None) -> None:
        """
        Initialize the partitioner.
        
        Args:
            config: Partition configuration.
        """
        self.config = config or PartitionConfig()
        self._round_robin_index = 0
        self._consistent_hash_ring: OrderedDict = OrderedDict()
        
        if self.config.strategy == "consistent_hash":
            self._build_hash_ring()
            
    def partition(
        self,
        data: List[T],
        key_func: Optional[Callable[[T], Any]] = None,
    ) -> List[Partition]:
        """
        Partition data according to configured strategy.
        
        Args:
            data: List of items to partition.
            key_func: Function to extract partition key from item.
            
        Returns:
            List of Partition objects.
        """
        if self.config.strategy == "hash":
            return self._partition_hash(data, key_func)
        elif self.config.strategy == "range":
            return self._partition_range(data, key_func)
        elif self.config.strategy == "round_robin":
            return self._partition_round_robin(data)
        elif self.config.strategy == "consistent_hash":
            return self._partition_consistent_hash(data, key_func)
        else:
            return self._partition_list(data)
            
    def _partition_hash(
        self,
        data: List[T],
        key_func: Optional[Callable[[T], Any]] = None,
    ) -> List[Partition]:
        """Hash-based partitioning."""
        partitions = [Partition(partition_id=i) for i in range(self.config.num_partitions)]
        
        for item in data:
            if key_func:
                key = key_func(item)
            else:
                key = item
                
            partition_id = self._hash_key(key) % self.config.num_partitions
            partitions[partition_id].data.append(item)
            
        return partitions
        
    def _partition_range(
        self,
        data: List[T],
        key_func: Optional[Callable[[T], Any]] = None,
    ) -> List[Partition]:
        """Range-based partitioning."""
        bounds = self.config.range_bounds or self._compute_bounds(data, key_func)
        partitions = [Partition(partition_id=i) for i in range(len(bounds) + 1)]
        
        for item in data:
            if key_func:
                key = key_func(item)
            else:
                key = item
                
            partition_id = self._find_range_partition(key, bounds)
            partitions[partition_id].data.append(item)
            
        return partitions
        
    def _partition_round_robin(self, data: List[T]) -> List[Partition]:
        """Round-robin partitioning."""
        partitions = [Partition(partition_id=i) for i in range(self.config.num_partitions)]
        
        for item in data:
            partition_id = self._round_robin_index % self.config.num_partitions
            partitions[partition_id].data.append(item)
            self._round_robin_index += 1
            
        return partitions
        
    def _partition_consistent_hash(
        self,
        data: List[T],
        key_func: Optional[Callable[[T], Any]] = None,
    ) -> List[Partition]:
        """Consistent hash-based partitioning."""
        partitions = [Partition(partition_id=i) for i in range(self.config.num_partitions)]
        
        for item in data:
            if key_func:
                key = key_func(item)
            else:
                key = item
                
            partition_id = self._find_on_hash_ring(key)
            partitions[partition_id].data.append(item)
            
        return partitions
        
    def _partition_list(self, data: List[T]) -> List[Partition]:
        """List partitioning (divide equally)."""
        partitions = [Partition(partition_id=i) for i in range(self.config.num_partitions)]
        
        chunk_size = max(1, len(data) // self.config.num_partitions)
        
        for i, item in enumerate(data):
            partition_id = min(i // chunk_size, self.config.num_partitions - 1)
            partitions[partition_id].data.append(item)
            
        return partitions
        
    def _hash_key(self, key: Any) -> int:
        """Hash a key to an integer."""
        if isinstance(key, str):
            return int(hashlib.md5(key.encode()).hexdigest(), 16)
        elif isinstance(key, (int, float)):
            return int(key) if isinstance(key, int) else int(key * 1000000)
        else:
            return int(hashlib.md5(str(key).encode()).hexdigest(), 16)
            
    def _compute_bounds(
        self,
        data: List[T],
        key_func: Optional[Callable[[T], Any]],
    ) -> List[Any]:
        """Compute range bounds for data."""
        if not data:
            return []
            
        # Extract keys
        keys = [key_func(item) if key_func else item for item in data]
        
        # Sort numeric keys
        try:
            sorted_keys = sorted(keys)
            min_val, max_val = sorted_keys[0], sorted_keys[-1]
            
            # Create equal-width bins
            bounds = []
            step = (max_val - min_val) / self.config.num_partitions
            for i in range(1, self.config.num_partitions):
                bounds.append(min_val + step * i)
                
            return bounds
        except TypeError:
            return []
            
    def _find_range_partition(self, key: Any, bounds: List[Any]) -> int:
        """Find partition for range-based key."""
        for i, bound in enumerate(bounds):
            if key < bound:
                return i
        return len(bounds)
        
    def _build_hash_ring(self) -> None:
        """Build consistent hash ring with virtual nodes."""
        self._consistent_hash_ring.clear()
        
        for node_id in range(self.config.num_partitions):
            for vn in range(self.config.virtual_nodes):
                virtual_key = f"node_{node_id}_vn_{vn}"
                hash_val = self._hash_key(virtual_key)
                self._consistent_hash_ring[hash_val] = node_id
                
        # Sort by hash
        self._consistent_hash_ring = OrderedDict(
            sorted(self._consistent_hash_ring.items())
        )
        
    def _find_on_hash_ring(self, key: Any) -> int:
        """Find node on consistent hash ring."""
        if not self._consistent_hash_ring:
            return self._hash_key(key) % self.config.num_partitions
            
        hash_val = self._hash_key(key)
        
        # Find first node with hash >= key hash
        for hash_point, node_id in self._consistent_hash_ring.items():
            if hash_point >= hash_val:
                return node_id
                
        # Wrap around to first node
        return next(iter(self._consistent_hash_ring.values()))
        
    def get_partition_for_key(self, key: Any) -> int:
        """
        Get partition ID for a specific key.
        
        Args:
            key: The key to find partition for.
            
        Returns:
            Partition ID.
        """
        if self.config.strategy == "hash":
            return self._hash_key(key) % self.config.num_partitions
        elif self.config.strategy == "consistent_hash":
            return self._find_on_hash_ring(key)
        elif self.config.strategy == "range":
            bounds = self.config.range_bounds or []
            return self._find_range_partition(key, bounds)
        else:
            return 0
            
    def rebalance(
        self,
        current_data: Dict[int, List[T]],
        key_func: Optional[Callable[[T], Any]] = None,
    ) -> Dict[int, Tuple[List[T], List[T]]]:
        """
        Calculate data movement for rebalancing.
        
        Args:
            current_data: Current partition data.
            key_func: Key extraction function.
            
        Returns:
            Dict of partition_id -> (data_to_keep, data_to_move).
        """
        all_data: List[T] = []
        for partition_data in current_data.values():
            all_data.extend(partition_data)
            
        new_partitions = self.partition(all_data, key_func)
        new_data = {p.partition_id: list(p.data) for p in new_partitions}
        
        # Calculate movement
        movements: Dict[int, Tuple[List[T], List[T]]] = {}
        
        for partition_id in range(self.config.num_partitions):
            old_data = set(str(item) for item in current_data.get(partition_id, []))
            new_data_set = set(str(item) for item in new_data.get(partition_id, []))
            
            keep = [item for item in new_data.get(partition_id, []) if str(item) in old_data]
            move = [item for item in new_data.get(partition_id, []) if str(item) not in old_data]
            
            movements[partition_id] = (keep, move)
            
        return movements


class ConsistentHash:
    """
    Standalone consistent hashing implementation with replication.
    
    Example:
        ch = ConsistentHash(virtual_nodes=100)
        
        # Add nodes
        ch.add_node("node1")
        ch.add_node("node2")
        ch.add_node("node3")
        
        # Find node for key
        node = ch.get_node("user_123")
    """
    
    def __init__(self, virtual_nodes: int = 100) -> None:
        """
        Initialize consistent hash.
        
        Args:
            virtual_nodes: Number of virtual nodes per physical node.
        """
        self.virtual_nodes = virtual_nodes
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self._nodes: Dict[str, int] = {}  # node -> number of virtual nodes
        
    def add_node(self, node_id: str) -> None:
        """
        Add a node to the hash ring.
        
        Args:
            node_id: Unique node identifier.
        """
        if node_id in self._nodes:
            logger.warning(f"Node {node_id} already exists")
            return
            
        for vn in range(self.virtual_nodes):
            virtual_key = f"{node_id}_vn_{vn}"
            hash_val = self._hash(virtual_key)
            self._ring[hash_val] = node_id
            
        self._sorted_keys = sorted(self._ring.keys())
        self._nodes[node_id] = self.virtual_nodes
        
        logger.info(f"Added node {node_id} with {self.virtual_nodes} virtual nodes")
        
    def remove_node(self, node_id: str) -> None:
        """
        Remove a node from the hash ring.
        
        Args:
            node_id: Node identifier to remove.
        """
        if node_id not in self._nodes:
            return
            
        for vn in range(self._nodes[node_id]):
            virtual_key = f"{node_id}_vn_{vn}"
            hash_val = self._hash(virtual_key)
            if hash_val in self._ring:
                del self._ring[hash_val]
                
        del self._nodes[node_id]
        self._sorted_keys = sorted(self._ring.keys())
        
        logger.info(f"Removed node {node_id}")
        
    def get_node(self, key: str, replicas: int = 1) -> List[str]:
        """
        Get node(s) for a key.
        
        Args:
            key: Key to lookup.
            replicas: Number of replica nodes to return.
            
        Returns:
            List of node IDs (for replication).
        """
        if not self._sorted_keys:
            return []
            
        hash_val = self._hash(key)
        
        # Find first node
        nodes = []
        for sorted_key in self._sorted_keys:
            if sorted_key >= hash_val:
                nodes.append(self._ring[sorted_key])
                if len(nodes) >= replicas:
                    break
                    
        # Wrap around if needed
        while len(nodes) < replicas and len(self._nodes) > 0:
            for sorted_key in self._sorted_keys:
                node = self._ring[sorted_key]
                if node not in nodes:
                    nodes.append(node)
                    break
            else:
                break
                
        return nodes
        
    def _hash(self, key: str) -> int:
        """Hash function."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
        
    def get_stats(self) -> Dict[str, Any]:
        """Get ring statistics."""
        return {
            "total_nodes": len(self._nodes),
            "virtual_nodes": sum(self._nodes.values()),
            "ring_size": len(self._ring),
        }
