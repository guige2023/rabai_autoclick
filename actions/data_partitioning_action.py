"""
Data Partitioning Action Module.

Provides data partitioning capabilities including horizontal/vertical
partitioning, range-based partitioning, and hash partitioning for
distributed data processing.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import hashlib
from collections import defaultdict
from datetime import datetime


T = TypeVar('T')


class PartitionStrategy(Enum):
    """Partition strategies."""
    RANGE = "range"
    HASH = "hash"
    LIST = "list"
    ROUND_ROBIN = "round_robin"
    COMPOSITE = "composite"


@dataclass
class Partition:
    """Represents a data partition."""
    id: str
    name: str
    strategy: PartitionStrategy
    partition_key: str
    data: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionConfig:
    """Configuration for partitioning."""
    strategy: PartitionStrategy
    partition_key: str
    num_partitions: int = 4
    range_bounds: Optional[List[Any]] = None
    hash_modulo: Optional[int] = None


class RangePartitioner:
    """
    Range-based partitioner.
    
    Example:
        partitioner = RangePartitioner(
            partition_key="timestamp",
            bounds=["2024-01-01", "2024-02-01", "2024-03-01"]
        )
        
        partition = partitioner.partition(data_records)
    """
    
    def __init__(
        self,
        partition_key: str,
        bounds: Optional[List[Any]] = None,
        num_partitions: int = 4
    ):
        self.partition_key = partition_key
        self.bounds = bounds or self._compute_auto_bounds(num_partitions)
    
    def partition(self, data: List[Dict]) -> List[Partition]:
        """Partition data based on ranges."""
        partitions = []
        
        # Initialize partitions
        for i, bound in enumerate(self.bounds[:-1]):
            partitions.append(Partition(
                id=f"range_{i}",
                name=f"Range {bound} to {self.bounds[i+1]}",
                strategy=PartitionStrategy.RANGE,
                partition_key=self.partition_key,
                metadata={"lower": bound, "upper": self.bounds[i+1]}
            ))
        
        # Assign data to partitions
        for item in data:
            key_value = item.get(self.partition_key)
            partition_idx = self._find_partition(key_value)
            if partition_idx is not None and 0 <= partition_idx < len(partitions):
                partitions[partition_idx].data.append(item)
        
        return partitions
    
    def _find_partition(self, value: Any) -> Optional[int]:
        """Find which partition a value belongs to."""
        if value is None:
            return None
        
        for i, bound in enumerate(self.bounds[:-1]):
            if self.bounds[i] <= value < self.bounds[i+1]:
                return i
        
        # If value is >= last bound, put in last partition
        if value >= self.bounds[-1]:
            return len(self.bounds) - 2
        
        return None
    
    def _compute_auto_bounds(self, num_partitions: int) -> List[Any]:
        """Compute automatic bounds."""
        # Placeholder - would need data to compute actual bounds
        return list(range(num_partitions + 1))


class HashPartitioner:
    """
    Hash-based partitioner.
    
    Example:
        partitioner = HashPartitioner(
            partition_key="user_id",
            num_partitions=4
        )
        
        partitions = partitioner.partition(user_records)
    """
    
    def __init__(self, partition_key: str, num_partitions: int = 4):
        self.partition_key = partition_key
        self.num_partitions = num_partitions
    
    def partition(self, data: List[Dict]) -> List[Partition]:
        """Partition data based on hash values."""
        partitions = [
            Partition(
                id=f"hash_{i}",
                name=f"Hash Partition {i}",
                strategy=PartitionStrategy.HASH,
                partition_key=self.partition_key,
                metadata={"modulo": self.num_partitions}
            )
            for i in range(self.num_partitions)
        ]
        
        for item in data:
            key_value = item.get(self.partition_key)
            if key_value is not None:
                partition_idx = self._hash_partition(key_value)
                partitions[partition_idx].data.append(item)
        
        return partitions
    
    def _hash_partition(self, key_value: Any) -> int:
        """Compute hash and determine partition."""
        key_str = str(key_value)
        hash_value = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
        return hash_value % self.num_partitions


class ListPartitioner:
    """
    List-based partitioner with explicit value mapping.
    
    Example:
        partitioner = ListPartitioner(
            partition_key="region",
            partitions={
                "NA": ["US", "CA", "MX"],
                "EU": ["UK", "DE", "FR"],
                "APAC": ["JP", "CN", "AU"]
            }
        )
    """
    
    def __init__(
        self,
        partition_key: str,
        partitions: Dict[str, List[Any]]
    ):
        self.partition_key = partition_key
        self.value_to_partition: Dict[Any, str] = {}
        
        for partition_name, values in partitions.items():
            for value in values:
                self.value_to_partition[value] = partition_name
    
    def partition(self, data: List[Dict]) -> List[Partition]:
        """Partition data based on list membership."""
        partition_map: Dict[str, Partition] = {}
        
        for item in data:
            key_value = item.get(self.partition_key)
            partition_name = self.value_to_partition.get(key_value, "default")
            
            if partition_name not in partition_map:
                partition_map[partition_name] = Partition(
                    id=f"list_{partition_name}",
                    name=f"List Partition: {partition_name}",
                    strategy=PartitionStrategy.LIST,
                    partition_key=self.partition_key
                )
            
            partition_map[partition_name].data.append(item)
        
        return list(partition_map.values())


class RoundRobinPartitioner:
    """
    Round-robin partitioner for even distribution.
    
    Example:
        partitioner = RoundRobinPartitioner(num_partitions=4)
        partitions = partitioner.partition(data)
    """
    
    def __init__(self, num_partitions: int = 4):
        self.num_partitions = num_partitions
    
    def partition(self, data: List[T]) -> List[List[T]]:
        """Partition data in round-robin fashion."""
        partitions: List[List[T]] = [[] for _ in range(self.num_partitions)]
        
        for i, item in enumerate(data):
            partition_idx = i % self.num_partitions
            partitions[partition_idx].append(item)
        
        return partitions


class DataPartitioner:
    """
    Main data partitioning interface.
    
    Example:
        partitioner = DataPartitioner()
        partitioner.configure(strategy="hash", partition_key="id", num_partitions=4)
        
        result = partitioner.partition(data)
    """
    
    def __init__(self):
        self.config: Optional[PartitionConfig] = None
        self._partitioner: Optional[Any] = None
    
    def configure(
        self,
        strategy: str,
        partition_key: str,
        num_partitions: int = 4,
        **kwargs
    ) -> "DataPartitioner":
        """Configure the partitioner."""
        strategy_enum = PartitionStrategy(strategy)
        
        self.config = PartitionConfig(
            strategy=strategy_enum,
            partition_key=partition_key,
            num_partitions=num_partitions,
            **kwargs
        )
        
        if strategy_enum == PartitionStrategy.RANGE:
            self._partitioner = RangePartitioner(
                partition_key=partition_key,
                bounds=kwargs.get("bounds"),
                num_partitions=num_partitions
            )
        elif strategy_enum == PartitionStrategy.HASH:
            self._partitioner = HashPartitioner(
                partition_key=partition_key,
                num_partitions=num_partitions
            )
        elif strategy_enum == PartitionStrategy.LIST:
            self._partitioner = ListPartitioner(
                partition_key=partition_key,
                partitions=kwargs.get("list_mapping", {})
            )
        elif strategy_enum == PartitionStrategy.ROUND_ROBIN:
            self._partitioner = RoundRobinPartitioner(num_partitions=num_partitions)
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        return self
    
    def partition(self, data: List[Dict]) -> List[Partition]:
        """Partition data according to configuration."""
        if not self._partitioner:
            raise ValueError("Partitioner not configured")
        
        return self._partitioner.partition(data)
    
    def get_partition_stats(self, partitions: List[Partition]) -> Dict[str, Any]:
        """Get statistics about partitions."""
        sizes = [len(p.data) for p in partitions]
        
        return {
            "num_partitions": len(partitions),
            "total_records": sum(sizes),
            "min_partition_size": min(sizes) if sizes else 0,
            "max_partition_size": max(sizes) if sizes else 0,
            "avg_partition_size": sum(sizes) / len(sizes) if sizes else 0,
            "distribution": sizes
        }


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataPartitioningAction(BaseAction):
    """
    Data partitioning action for distributed processing.
    
    Parameters:
        data: List of records to partition
        strategy: Partition strategy (range/hash/list/round_robin)
        partition_key: Field to partition on
        num_partitions: Number of partitions
        bounds: Range bounds (for range strategy)
    
    Example:
        action = DataPartitioningAction()
        result = action.execute({}, {
            "data": [{"id": 1}, {"id": 2}, {"id": 3}],
            "strategy": "hash",
            "partition_key": "id",
            "num_partitions": 4
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data partitioning."""
        data = params.get("data", [])
        strategy = params.get("strategy", "hash")
        partition_key = params.get("partition_key", "id")
        num_partitions = params.get("num_partitions", 4)
        bounds = params.get("bounds")
        
        partitioner = DataPartitioner()
        partitioner.configure(
            strategy=strategy,
            partition_key=partition_key,
            num_partitions=num_partitions,
            bounds=bounds
        )
        
        partitions = partitioner.partition(data)
        stats = partitioner.get_partition_stats(partitions)
        
        return {
            "success": True,
            "strategy": strategy,
            "partition_key": partition_key,
            "num_partitions": len(partitions),
            "stats": stats,
            "partitioned_at": datetime.now().isoformat()
        }
