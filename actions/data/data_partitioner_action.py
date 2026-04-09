"""Data Partitioner Action Module.

Provides data partitioning capabilities for splitting datasets
into partitions based on size, key ranges, or custom functions.

Example:
    >>> from actions.data.data_partitioner_action import DataPartitionerAction
    >>> action = DataPartitionerAction()
    >>> partitions = action.partition_by_size(data, partition_size=1000)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class PartitionStrategy(Enum):
    """Partition strategy types."""
    BY_SIZE = "by_size"
    BY_COUNT = "by_count"
    BY_KEY = "by_key"
    BY_RANGE = "by_range"
    BY_HASH = "by_hash"
    CUSTOM = "custom"


@dataclass
class Partition:
    """Data partition.
    
    Attributes:
        partition_id: Unique partition identifier
        data: Partition data
        size: Number of items
        start_index: Start index in original data
        end_index: End index in original data
        metadata: Additional metadata
    """
    partition_id: str
    data: List[Any]
    size: int
    start_index: int
    end_index: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionerConfig:
    """Configuration for partitioning.
    
    Attributes:
        strategy: Partition strategy
        partition_size: Size per partition
        num_partitions: Number of partitions
        preserve_order: Preserve original ordering
        overlap: Overlap between partitions
    """
    strategy: PartitionStrategy = PartitionStrategy.BY_SIZE
    partition_size: int = 1000
    num_partitions: int = 4
    preserve_order: bool = True
    overlap: int = 0


@dataclass
class PartitioningResult:
    """Result of partitioning operation.
    
    Attributes:
        partitions: List of partitions
        strategy: Strategy used
        original_size: Original dataset size
        num_partitions: Number of partitions created
    """
    partitions: List[Partition]
    strategy: PartitionStrategy
    original_size: int
    num_partitions: int
    duration: float = 0.0


class DataPartitionerAction:
    """Data partitioner for datasets.
    
    Provides various partitioning strategies to split
    large datasets into manageable chunks.
    
    Attributes:
        config: Partitioner configuration
        _partitions: Created partitions
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[PartitionerConfig] = None,
    ) -> None:
        """Initialize partitioner action.
        
        Args:
            config: Partitioner configuration
        """
        self.config = config or PartitionerConfig()
        self._partitions: Dict[str, Partition] = {}
        self._lock = threading.Lock()
    
    def partition(
        self,
        data: List[Any],
        strategy: Optional[PartitionStrategy] = None,
        **kwargs: Any,
    ) -> PartitioningResult:
        """Partition data using specified strategy.
        
        Args:
            data: Data to partition
            strategy: Partition strategy
            **kwargs: Strategy-specific arguments
        
        Returns:
            PartitioningResult
        """
        import time
        start = time.time()
        
        strategy = strategy or self.config.strategy
        
        if strategy == PartitionStrategy.BY_SIZE:
            result = self._partition_by_size(data, **kwargs)
        elif strategy == PartitionStrategy.BY_COUNT:
            result = self._partition_by_count(data, **kwargs)
        elif strategy == PartitionStrategy.BY_KEY:
            result = self._partition_by_key(data, **kwargs)
        elif strategy == PartitionStrategy.BY_RANGE:
            result = self._partition_by_range(data, **kwargs)
        elif strategy == PartitionStrategy.BY_HASH:
            result = self._partition_by_hash(data, **kwargs)
        elif strategy == PartitionStrategy.CUSTOM:
            result = self._partition_custom(data, **kwargs)
        else:
            result = self._partition_by_size(data, **kwargs)
        
        result.duration = time.time() - start
        return result
    
    def _partition_by_size(
        self,
        data: List[Any],
        partition_size: Optional[int] = None,
        overlap: Optional[int] = None,
    ) -> PartitioningResult:
        """Partition by size.
        
        Args:
            data: Data to partition
            partition_size: Size per partition
            overlap: Items to overlap between partitions
        
        Returns:
            PartitioningResult
        """
        partition_size = partition_size or self.config.partition_size
        overlap = overlap or self.config.overlap
        
        partitions: List[Partition] = []
        partition_id = 0
        
        for i in range(0, len(data), partition_size - overlap):
            end = min(i + partition_size, len(data))
            partition_data = data[i:end]
            
            partition = Partition(
                partition_id=f"p{partition_id}",
                data=partition_data,
                size=len(partition_data),
                start_index=i,
                end_index=end,
                metadata={"strategy": "by_size", "partition_size": partition_size},
            )
            
            partitions.append(partition)
            self._partitions[partition.partition_id] = partition
            partition_id += 1
            
            if end >= len(data):
                break
        
        return PartitioningResult(
            partitions=partitions,
            strategy=PartitionStrategy.BY_SIZE,
            original_size=len(data),
            num_partitions=len(partitions),
        )
    
    def _partition_by_count(
        self,
        data: List[Any],
        num_partitions: Optional[int] = None,
    ) -> PartitioningResult:
        """Partition by count.
        
        Args:
            data: Data to partition
            num_partitions: Number of partitions
        
        Returns:
            PartitioningResult
        """
        num_partitions = num_partitions or self.config.num_partitions
        num_partitions = max(1, num_partitions)
        
        partition_size = (len(data) + num_partitions - 1) // num_partitions
        
        partitions: List[Partition] = []
        
        for i in range(num_partitions):
            start = i * partition_size
            end = min(start + partition_size, len(data))
            
            if start >= len(data):
                break
            
            partition_data = data[start:end]
            
            partition = Partition(
                partition_id=f"p{i}",
                data=partition_data,
                size=len(partition_data),
                start_index=start,
                end_index=end,
                metadata={"strategy": "by_count", "num_partitions": num_partitions},
            )
            
            partitions.append(partition)
            self._partitions[partition.partition_id] = partition
        
        return PartitioningResult(
            partitions=partitions,
            strategy=PartitionStrategy.BY_COUNT,
            original_size=len(data),
            num_partitions=len(partitions),
        )
    
    def _partition_by_key(
        self,
        data: List[Any],
        key_func: Optional[Callable[[Any], Any]] = None,
    ) -> PartitioningResult:
        """Partition by key function.
        
        Args:
            data: Data to partition
            key_func: Function to extract partition key
        
        Returns:
            PartitioningResult
        """
        if not key_func:
            def default_key(x):
                return hash(str(x)) % self.config.num_partitions
            key_func = default_key
        
        key_groups: Dict[Any, List[Any]] = {}
        
        for i, item in enumerate(data):
            key = key_func(item)
            if key not in key_groups:
                key_groups[key] = []
            key_groups[key].append(item)
        
        partitions: List[Partition] = []
        partition_id = 0
        total_size = 0
        
        for key, items in key_groups.items():
            start = total_size
            end = total_size + len(items)
            total_size = end
            
            partition = Partition(
                partition_id=f"p{partition_id}",
                data=items,
                size=len(items),
                start_index=start,
                end_index=end,
                metadata={"strategy": "by_key", "key": str(key)},
            )
            
            partitions.append(partition)
            self._partitions[partition.partition_id] = partition
            partition_id += 1
        
        return PartitioningResult(
            partitions=partitions,
            strategy=PartitionStrategy.BY_KEY,
            original_size=len(data),
            num_partitions=len(partitions),
        )
    
    def _partition_by_range(
        self,
        data: List[Any],
        range_func: Optional[Callable[[Any], Tuple[Any, Any]]] = None,
        num_ranges: int = 4,
    ) -> PartitioningResult:
        """Partition by value ranges.
        
        Args:
            data: Data to partition
            range_func: Function to get (min, max) for item
            num_ranges: Number of ranges
        
        Returns:
            PartitioningResult
        """
        if not data:
            return PartitioningResult(
                partitions=[],
                strategy=PartitionStrategy.BY_RANGE,
                original_size=0,
                num_partitions=0,
            )
        
        if range_func:
            ranges = [(range_func(item), item) for item in data]
            values = [range_func(item) for item in data]
        else:
            try:
                values = sorted(data)
            except TypeError:
                values = sorted(data, key=str)
        
        min_val = min(values)
        max_val = max(values)
        
        range_size = (max_val - min_val) / num_ranges if max_val > min_val else 1
        
        partitions: List[List[Any]] = [[] for _ in range(num_ranges)]
        
        for item in data:
            val = range_func(item) if range_func else item
            idx = min(int((val - min_val) / range_size), num_ranges - 1) if range_size > 0 else 0
            partitions[idx].append(item)
        
        result_partitions: List[Partition] = []
        total_size = 0
        
        for i, part_data in enumerate(partitions):
            start = total_size
            end = total_size + len(part_data)
            total_size = end
            
            partition = Partition(
                partition_id=f"p{i}",
                data=part_data,
                size=len(part_data),
                start_index=start,
                end_index=end,
                metadata={
                    "strategy": "by_range",
                    "range_index": i,
                    "range_size": range_size,
                },
            )
            
            result_partitions.append(partition)
            self._partitions[partition.partition_id] = partition
        
        return PartitioningResult(
            partitions=result_partitions,
            strategy=PartitionStrategy.BY_RANGE,
            original_size=len(data),
            num_partitions=len(result_partitions),
        )
    
    def _partition_by_hash(
        self,
        data: List[Any],
        num_partitions: int = 4,
        hash_func: Optional[Callable[[Any], int]] = None,
    ) -> PartitioningResult:
        """Partition by hash function.
        
        Args:
            data: Data to partition
            num_partitions: Number of partitions
            hash_func: Custom hash function
        
        Returns:
            PartitioningResult
        """
        partitions: List[List[Any]] = [[] for _ in range(num_partitions)]
        
        for i, item in enumerate(data):
            if hash_func:
                hash_val = hash_func(item)
            else:
                hash_val = hash(str(item))
            
            partition_idx = abs(hash_val) % num_partitions
            partitions[partition_idx].append(item)
        
        result_partitions: List[Partition] = []
        total_size = 0
        
        for i, part_data in enumerate(partitions):
            start = total_size
            end = total_size + len(part_data)
            total_size = end
            
            partition = Partition(
                partition_id=f"p{i}",
                data=part_data,
                size=len(part_data),
                start_index=start,
                end_index=end,
                metadata={"strategy": "by_hash", "partition_index": i},
            )
            
            result_partitions.append(partition)
            self._partitions[partition.partition_id] = partition
        
        return PartitioningResult(
            partitions=result_partitions,
            strategy=PartitionStrategy.BY_HASH,
            original_size=len(data),
            num_partitions=len(result_partitions),
        )
    
    def _partition_custom(
        self,
        data: List[Any],
        partition_func: Optional[Callable[[List[Any]], List[List[Any]]]] = None,
    ) -> PartitioningResult:
        """Partition using custom function.
        
        Args:
            data: Data to partition
            partition_func: Custom partition function
        
        Returns:
            PartitioningResult
        """
        if not partition_func:
            return self._partition_by_size(data)
        
        partition_data = partition_func(data)
        
        partitions: List[Partition] = []
        total_size = 0
        
        for i, part in enumerate(partition_data):
            start = total_size
            end = total_size + len(part)
            total_size = end
            
            partition = Partition(
                partition_id=f"p{i}",
                data=part,
                size=len(part),
                start_index=start,
                end_index=end,
                metadata={"strategy": "custom"},
            )
            
            partitions.append(partition)
            self._partitions[partition.partition_id] = partition
        
        return PartitioningResult(
            partitions=partitions,
            strategy=PartitionStrategy.CUSTOM,
            original_size=len(data),
            num_partitions=len(partitions),
        )
    
    def get_partition(self, partition_id: str) -> Optional[Partition]:
        """Get partition by ID.
        
        Args:
            partition_id: Partition identifier
        
        Returns:
            Partition if found
        """
        with self._lock:
            return self._partitions.get(partition_id)
    
    def merge_partitions(
        self,
        partition_ids: List[str],
    ) -> List[Any]:
        """Merge multiple partitions.
        
        Args:
            partition_ids: IDs of partitions to merge
        
        Returns:
            Merged data list
        """
        result: List[Any] = []
        
        with self._lock:
            for pid in partition_ids:
                if pid in self._partitions:
                    result.extend(self._partitions[pid].data)
        
        return result
