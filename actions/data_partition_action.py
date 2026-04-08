"""Data Partition action module for RabAI AutoClick.

Provides data partitioning strategies for large datasets
with horizontal/vertical partitioning and partition management.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PartitionType(Enum):
    """Partition strategy types."""
    HORIZONTAL = "horizontal"  # Shard rows
    VERTICAL = "vertical"     # Shard columns
    RANGE = "range"
    LIST = "list"
    HASH = "hash"


@dataclass
class Partition:
    """Represents a data partition."""
    partition_id: str
    name: str
    partition_type: PartitionType
    storage_location: Optional[str] = None
    row_count: int = 0
    size_bytes: int = 0
    bounds: Dict[str, Any] = field(default_factory=dict)  # Range/list bounds
    columns: List[str] = field(default_factory=list)  # For vertical partitioning
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PartitionConfig:
    """Configuration for partitioning."""
    strategy: PartitionType = PartitionType.HORIZONTAL
    num_partitions: int = 4
    partition_column: Optional[str] = None
    columns_per_partition: Optional[List[List[str]]] = None


class DataPartitioner:
    """Data partitioning and partition management."""
    
    def __init__(self):
        self._partitions: Dict[str, Partition] = {}
        self._config: Optional[PartitionConfig] = None
        self._data_records: Dict[str, List[Dict]] = {}  # partition_id -> records
    
    def configure(self, config: PartitionConfig) -> None:
        """Configure partitioning strategy."""
        self._config = config
        # Initialize partitions
        for i in range(config.num_partitions):
            partition_id = f"partition_{i}"
            self._partitions[partition_id] = Partition(
                partition_id=partition_id,
                name=f"p{i}",
                partition_type=config.strategy,
                columns=config.columns_per_partition[i] if config.columns_per_partition else []
            )
    
    def get_partition(self, partition_id: str) -> Optional[Partition]:
        """Get a partition by ID."""
        return self._partitions.get(partition_id)
    
    def compute_partition(self, record: Dict[str, Any]) -> str:
        """Compute partition for a record."""
        if not self._config:
            raise ValueError("Partitioning not configured")
        
        if self._config.strategy == PartitionType.HASH and self._config.partition_column:
            key = str(record.get(self._config.partition_column, ""))
            hash_val = hash(key)
            idx = hash_val % self._config.num_partitions
            return f"partition_{idx}"
        
        elif self._config.strategy == PartitionType.RANGE and self._config.partition_column:
            key = record.get(self._config.partition_column)
            bounds = sorted(self._config.num_partitions)
            for i, bound in enumerate(bounds):
                if key < bound:
                    return f"partition_{i}"
            return f"partition_{self._config.num_partitions - 1}"
        
        elif self._config.strategy == PartitionType.VERTICAL:
            # Return first partition for vertical partitioning
            return "partition_0"
        
        return f"partition_{0}"
    
    def insert_record(self, record: Dict[str, Any]) -> str:
        """Insert a record into appropriate partition."""
        partition_id = self.compute_partition(record)
        
        if partition_id not in self._data_records:
            self._data_records[partition_id] = []
        self._data_records[partition_id].append(record)
        
        partition = self._partitions.get(partition_id)
        if partition:
            partition.row_count = len(self._data_records.get(partition_id, []))
        
        return partition_id
    
    def get_partition_records(self, partition_id: str) -> List[Dict[str, Any]]:
        """Get all records in a partition."""
        return self._data_records.get(partition_id, [])
    
    def get_record_partition(self, record_id: str) -> Optional[str]:
        """Find which partition contains a record."""
        # Simplified - would need record tracking in real impl
        for pid, records in self._data_records.items():
            for rec in records:
                if str(rec.get("id", "")) == str(record_id):
                    return pid
        return None
    
    def merge_partitions(self, source_ids: List[str], target_id: str) -> bool:
        """Merge multiple partitions into one."""
        if target_id not in self._partitions:
            return False
        
        target_records = []
        for pid in source_ids:
            if pid in self._data_records:
                target_records.extend(self._data_records.pop(pid))
                if pid in self._partitions:
                    del self._partitions[pid]
        
        self._data_records[target_id] = target_records
        partition = self._partitions.get(target_id)
        if partition:
            partition.row_count = len(target_records)
        
        return True
    
    def split_partition(self, partition_id: str, split_column: str) -> List[str]:
        """Split a partition into multiple based on a column."""
        if partition_id not in self._data_records:
            return []
        
        records = self._data_records[partition_id]
        if not records:
            return []
        
        # Group by column values
        groups: Dict[Any, List] = {}
        for rec in records:
            key = rec.get(split_column, "null")
            groups.setdefault(key, []).append(rec)
        
        new_partition_ids = []
        for i, (key, group) in enumerate(groups.items()):
            new_pid = f"partition_split_{partition_id}_{i}"
            self._data_records[new_pid] = group
            self._partitions[new_pid] = Partition(
                partition_id=new_pid,
                name=f"split_{key}",
                partition_type=self._config.strategy if self._config else PartitionType.HORIZONTAL,
                row_count=len(group)
            )
            new_partition_ids.append(new_pid)
        
        # Remove original
        del self._data_records[partition_id]
        if partition_id in self._partitions:
            del self._partitions[partition_id]
        
        return new_partition_ids
    
    def list_partitions(self) -> List[Partition]:
        """List all partitions."""
        return list(self._partitions.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get partition statistics."""
        total_rows = sum(p.row_count for p in self._partitions.values())
        total_size = sum(p.size_bytes for p in self._partitions.values())
        return {
            "strategy": self._config.strategy.value if self._config else None,
            "num_partitions": len(self._partitions),
            "total_rows": total_rows,
            "total_size_bytes": total_size,
            "partitions": [
                {
                    "partition_id": p.partition_id,
                    "name": p.name,
                    "row_count": p.row_count,
                    "size_bytes": p.size_bytes
                }
                for p in self._partitions.values()
            ]
        }


class DataPartitionAction(BaseAction):
    """Data partitioning for large datasets.
    
    Supports horizontal, vertical, range, list, and hash partitioning
    with partition management and statistics.
    """
    action_type = "data_partition"
    display_name = "数据分区"
    description = "大数据集分区，支持水平和垂直分区"
    
    def __init__(self):
        super().__init__()
        self._partitioner = DataPartitioner()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute partition operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "configure":
                return self._configure(params)
            elif operation == "insert":
                return self._insert(params)
            elif operation == "get_partition":
                return self._get_partition(params)
            elif operation == "merge":
                return self._merge(params)
            elif operation == "split":
                return self._split(params)
            elif operation == "list":
                return self._list(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure partitioning."""
        config = PartitionConfig(
            strategy=PartitionType(params.get("strategy", "horizontal")),
            num_partitions=params.get("num_partitions", 4),
            partition_column=params.get("partition_column")
        )
        self._partitioner.configure(config)
        return ActionResult(success=True, message="Configured")
    
    def _insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert a record."""
        record = params.get("record", {})
        pid = self._partitioner.insert_record(record)
        return ActionResult(success=True, message=f"Inserted into {pid}", data={"partition_id": pid})
    
    def _get_partition(self, params: Dict[str, Any]) -> ActionResult:
        """Get partition info."""
        partition_id = params.get("partition_id", "")
        partition = self._partitioner.get_partition(partition_id)
        if not partition:
            return ActionResult(success=False, message="Not found")
        return ActionResult(success=True, message=f"Partition: {partition_id}",
                         data={"partition_id": partition_id, "row_count": partition.row_count})
    
    def _merge(self, params: Dict[str, Any]) -> ActionResult:
        """Merge partitions."""
        source_ids = params.get("source_ids", [])
        target_id = params.get("target_id", "")
        merged = self._partitioner.merge_partitions(source_ids, target_id)
        return ActionResult(success=merged, message="Merged" if merged else "Merge failed")
    
    def _split(self, params: Dict[str, Any]) -> ActionResult:
        """Split a partition."""
        partition_id = params.get("partition_id", "")
        split_column = params.get("split_column", "")
        new_ids = self._partitioner.split_partition(partition_id, split_column)
        return ActionResult(success=True, message=f"Split into {len(new_ids)} partitions",
                         data={"new_partition_ids": new_ids})
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all partitions."""
        partitions = self._partitioner.list_partitions()
        return ActionResult(success=True, message=f"{len(partitions)} partitions",
                         data={"partitions": [{"id": p.partition_id, "name": p.name} for p in partitions]})
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get partition stats."""
        stats = self._partitioner.get_stats()
        return ActionResult(success=True, message="Stats retrieved", data=stats)
