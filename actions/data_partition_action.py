"""
Data Partition Action - Partitions data by various strategies.

This module provides data partitioning including
by value, range, and quantiles.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from collections import defaultdict


@dataclass
class Partition:
    """A data partition."""
    name: str
    range_start: Any = None
    range_end: Any = None
    data: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PartitionResult:
    """Result of partitioning operation."""
    partitions: list[Partition]
    partition_count: int
    record_count: int


class DataPartitioner:
    """Partitions data into segments."""
    
    def partition_by_value(
        self,
        data: list[dict[str, Any]],
        field: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Partition by unique values."""
        result = defaultdict(list)
        for record in data:
            key = record.get(field)
            result[key].append(record)
        return dict(result)
    
    def partition_by_range(
        self,
        data: list[dict[str, Any]],
        field: str,
        ranges: list[tuple[Any, Any]],
    ) -> list[Partition]:
        """Partition by value ranges."""
        partitions = [Partition(name=f"p_{i}", range_start=r[0], range_end=r[1]) for i, r in enumerate(ranges)]
        
        for record in data:
            value = record.get(field)
            for partition in partitions:
                if partition.range_start <= value < partition.range_end:
                    partition.data.append(record)
                    break
        
        return [p for p in partitions if p.data]
    
    def partition_by_quantiles(
        self,
        data: list[dict[str, Any]],
        field: str,
        num_partitions: int = 4,
    ) -> list[Partition]:
        """Partition by quantiles."""
        values = sorted([r.get(field) for r in data if field in r])
        if not values:
            return []
        
        quantile_size = len(values) // num_partitions
        ranges = []
        for i in range(num_partitions):
            start = values[i * quantile_size] if i * quantile_size < len(values) else values[-1]
            end = values[min((i + 1) * quantile_size - 1, len(values) - 1)]
            ranges.append((start, end))
        
        return self.partition_by_range(data, field, ranges)


class DataPartitionAction:
    """Data partition action for automation workflows."""
    
    def __init__(self) -> None:
        self.partitioner = DataPartitioner()
    
    async def partition_by_value(
        self,
        data: list[dict[str, Any]],
        field: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Partition by field values."""
        return self.partitioner.partition_by_value(data, field)
    
    async def partition_by_quantiles(
        self,
        data: list[dict[str, Any]],
        field: str,
        num_partitions: int = 4,
    ) -> PartitionResult:
        """Partition by quantiles."""
        partitions = self.partitioner.partition_by_quantiles(data, field, num_partitions)
        return PartitionResult(
            partitions=partitions,
            partition_count=len(partitions),
            record_count=len(data),
        )


__all__ = ["Partition", "PartitionResult", "DataPartitioner", "DataPartitionAction"]
