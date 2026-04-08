"""
Data Split Action - Splits data into chunks, folds, and groups.

This module provides data splitting capabilities including
chunking, partitioning, and stratified splitting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar
from collections import defaultdict


T = TypeVar("T")


@dataclass
class SplitResult:
    """Result of split operation."""
    splits: list[list[dict[str, Any]]]
    split_count: int
    record_count: int


class DataSplitter:
    """Splits data into chunks and partitions."""
    
    def chunk(self, data: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
        """Split data into chunks of specified size."""
        return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
    def partition(
        self,
        data: list[dict[str, Any]],
        field: str,
    ) -> dict[Any, list[dict[str, Any]]]:
        """Partition data by field value."""
        result = defaultdict(list)
        for record in data:
            key = record.get(field)
            result[key].append(record)
        return dict(result)
    
    def stratified_split(
        self,
        data: list[dict[str, Any]],
        field: str,
        ratios: list[float],
    ) -> list[list[dict[str, Any]]]:
        """Stratified split preserving field distribution."""
        partitions = self.partition(data, field)
        splits = [[] for _ in ratios]
        
        for partition_records in partitions.values():
            current_idx = 0
            for i, ratio in enumerate(ratios):
                split_size = int(len(partition_records) * ratio)
                splits[i].extend(partition_records[current_idx:current_idx + split_size])
                current_idx += split_size
        
        return splits


class DataSplitAction:
    """Data split action for automation workflows."""
    
    def __init__(self) -> None:
        self.splitter = DataSplitter()
    
    async def chunk(self, data: list[dict[str, Any]], size: int) -> SplitResult:
        """Split data into chunks."""
        splits = self.splitter.chunk(data, size)
        return SplitResult(splits=splits, split_count=len(splits), record_count=len(data))
    
    async def partition_by(self, data: list[dict[str, Any]], field: str) -> dict[Any, list[dict[str, Any]]]:
        """Partition data by field."""
        return self.splitter.partition(data, field)


__all__ = ["SplitResult", "DataSplitter", "DataSplitAction"]
