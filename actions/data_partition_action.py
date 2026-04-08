"""
Data Partition Action Module

Provides data partitioning, sharding, and distribution strategies.
"""
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import hashlib
import bisect


T = TypeVar('T')


@dataclass
class PartitionConfig:
    """Configuration for data partitioning."""
    num_partitions: int = 4
    strategy: str = "hash"  # hash, range, round_robin, composite
    key_extractor: Optional[Callable[[Any], Any]] = None
    salt: str = ""


@dataclass
class PartitionStats:
    """Statistics for a partition."""
    partition_id: int
    item_count: int
    byte_size: int
    last_updated: datetime
    min_key: Optional[Any] = None
    max_key: Optional[Any] = None


@dataclass
class PartitionResult:
    """Result of a partition operation."""
    partitions: dict[int, list]
    stats: dict[int, PartitionStats]
    duration_ms: float


class ConsistentHashRing:
    """Consistent hashing ring for distributed data."""
    
    def __init__(self, nodes: Optional[list[str]] = None, virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []
        
        if nodes:
            for node in nodes:
                self.add_node(node)
    
    def add_node(self, node: str):
        """Add a node to the ring."""
        for i in range(self.virtual_nodes):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)
    
    def remove_node(self, node: str):
        """Remove a node from the ring."""
        keys_to_remove = [
            key for key, n in self.ring.items() if n == node
        ]
        for key in keys_to_remove:
            del self.ring[key]
            self.sorted_keys.remove(key)
    
    def get_node(self, key: Any) -> str:
        """Get the node responsible for a key."""
        if not self.ring:
            raise ValueError("No nodes in ring")
        
        hash_key = self._hash(str(key))
        
        # Find the first node with key >= hash_key
        idx = bisect.bisect(self.sorted_keys, hash_key)
        if idx >= len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def _hash(self, key: str) -> int:
        """Generate a hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


class DataPartitionAction:
    """Main data partitioning action handler."""
    
    def __init__(self, config: Optional[PartitionConfig] = None):
        self.config = config or PartitionConfig()
        self._hash_ring: Optional[ConsistentHashRing] = None
        self._range_boundaries: list[Any] = []
        self._partition_stats: dict[int, PartitionStats] = defaultdict(
            lambda: PartitionStats(
                partition_id=0,
                item_count=0,
                byte_size=0,
                last_updated=datetime.now()
            )
        )
        self._round_robin_index = 0
    
    def _extract_key(self, item: Any) -> Any:
        """Extract partition key from an item."""
        if self.config.key_extractor:
            return self.config.key_extractor(item)
        if isinstance(item, dict):
            return item.get("id") or item.get("key") or item.get("name")
        if hasattr(item, "id"):
            return item.id
        return str(item)
    
    def _hash_key(self, key: Any) -> int:
        """Hash a key to a partition index."""
        key_str = str(key) + self.config.salt
        hash_val = int(hashlib.sha256(key_str.encode()).hexdigest(), 16)
        return hash_val % self.config.num_partitions
    
    async def partition_data(
        self,
        data: list[Any],
        keys: Optional[list[str]] = None
    ) -> PartitionResult:
        """
        Partition data according to configured strategy.
        
        Args:
            data: List of items to partition
            keys: Optional list of keys (for range partitioning)
            
        Returns:
            PartitionResult with partitioned data and stats
        """
        start_time = datetime.now()
        partitions: dict[int, list] = {i: [] for i in range(self.config.num_partitions)}
        
        for item in data:
            partition_id = self._get_partition_id(item, keys)
            partitions[partition_id].append(item)
            
            # Update stats
            self._update_stats(partition_id, item)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return PartitionResult(
            partitions=partitions,
            stats=self._partition_stats.copy(),
            duration_ms=duration_ms
        )
    
    def _get_partition_id(self, item: Any, keys: Optional[list[str]] = None) -> int:
        """Get partition ID for an item based on strategy."""
        if self.config.strategy == "hash":
            key = self._extract_key(item)
            return self._hash_key(key)
        
        elif self.config.strategy == "range":
            return self._get_range_partition(item, keys)
        
        elif self.config.strategy == "round_robin":
            return self._get_round_robin_partition()
        
        elif self.config.strategy == "composite":
            return self._get_composite_partition(item)
        
        else:
            return self._hash_key(self._extract_key(item))
    
    def _get_range_partition(self, item: Any, keys: Optional[list[str]]) -> int:
        """Range-based partitioning."""
        if not keys:
            return 0
        
        key = self._extract_key(item)
        if key is None:
            return 0
        
        for i, boundary in enumerate(self._range_boundaries):
            if key < boundary:
                return i
        
        return len(self._range_boundaries)
    
    def _get_round_robin_partition(self) -> int:
        """Round-robin partitioning."""
        partition_id = self._round_robin_index
        self._round_robin_index = (self._round_robin_index + 1) % self.config.num_partitions
        return partition_id
    
    def _get_composite_partition(self, item: Any) -> int:
        """Composite partitioning using multiple keys."""
        key = self._extract_key(item)
        hash1 = self._hash_key(key)
        
        # Use a secondary hash for composite
        if isinstance(item, dict):
            secondary = item.get("type") or item.get("category") or "default"
        else:
            secondary = "default"
        
        hash2 = int(hashlib.md5(str(secondary).encode()).hexdigest(), 16) % 1000
        
        return (hash1 * 7 + hash2) % self.config.num_partitions
    
    def _update_stats(self, partition_id: int, item: Any):
        """Update partition statistics."""
        stats = self._partition_stats[partition_id]
        stats.partition_id = partition_id
        stats.item_count += 1
        stats.byte_size += len(str(item))
        stats.last_updated = datetime.now()
        
        key = self._extract_key(item)
        if key is not None:
            if stats.min_key is None or key < stats.min_key:
                stats.min_key = key
            if stats.max_key is None or key > stats.max_key:
                stats.max_key = key
    
    async def set_hash_ring_nodes(self, nodes: list[str]):
        """Initialize consistent hash ring with nodes."""
        self._hash_ring = ConsistentHashRing(nodes)
    
    async def get_partition_for_key(self, key: Any) -> int:
        """Get partition ID for a specific key."""
        if self._hash_ring:
            node = self._hash_ring.get_node(key)
            # Map node to partition (simplified)
            return hash(node) % self.config.num_partitions
        
        return self._hash_key(key)
    
    async def rebalance_partitions(
        self,
        new_num_partitions: int
    ) -> dict[int, list[int]]:
        """
        Calculate partition reassignments for rebalancing.
        
        Returns:
            Mapping of old partition -> new partition IDs
        """
        self.config.num_partitions = new_num_partitions
        reassignments: dict[int, list[int]] = defaultdict(list)
        
        for old_id in self._partition_stats:
            new_id = old_id % new_num_partitions
            reassignments[old_id].append(new_id)
        
        return dict(reassignments)
    
    async def merge_partitions(
        self,
        partitions: list[int]
    ) -> int:
        """Merge multiple partitions into one."""
        if not partitions:
            return 0
        
        target = partitions[0]
        total_items = sum(self._partition_stats[p].item_count for p in partitions)
        total_bytes = sum(self._partition_stats[p].byte_size for p in partitions)
        
        self._partition_stats[target].item_count = total_items
        self._partition_stats[target].byte_size = total_bytes
        self._partition_stats[target].last_updated = datetime.now()
        
        return target
    
    def get_stats_summary(self) -> dict[str, Any]:
        """Get summary of partition statistics."""
        total_items = sum(s.item_count for s in self._partition_stats.values())
        total_bytes = sum(s.byte_size for s in self._partition_stats.values())
        
        return {
            "num_partitions": self.config.num_partitions,
            "strategy": self.config.strategy,
            "total_items": total_items,
            "total_bytes": total_bytes,
            "avg_items_per_partition": total_items / max(1, self.config.num_partitions),
            "partition_stats": {
                pid: {
                    "item_count": stats.item_count,
                    "byte_size": stats.byte_size,
                    "last_updated": stats.last_updated.isoformat()
                }
                for pid, stats in self._partition_stats.items()
            }
        }
