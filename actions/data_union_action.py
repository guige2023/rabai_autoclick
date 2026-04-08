"""
Data Union Action - Unions multiple data sources.

This module provides union capabilities for
combining datasets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class UnionResult:
    """Result of union operation."""
    data: list[dict[str, Any]]
    source_count: int
    record_count: int


class DataUnion:
    """Unions multiple data sources."""
    
    def __init__(self) -> None:
        pass
    
    def union_all(
        self,
        *datasets: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Union all datasets without deduplication."""
        result = []
        for dataset in datasets:
            result.extend(dataset)
        return result
    
    def union_distinct(
        self,
        *datasets: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Union datasets with deduplication."""
        seen = set()
        result = []
        for dataset in datasets:
            for record in dataset:
                import json
                key = json.dumps(record, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    result.append(record)
        return result


class DataUnionAction:
    """Data union action for automation workflows."""
    
    def __init__(self) -> None:
        self.union = DataUnion()
    
    async def union_all(
        self,
        *datasets: list[dict[str, Any]],
    ) -> UnionResult:
        """Union all datasets."""
        data = self.union.union_all(*datasets)
        return UnionResult(data=data, source_count=len(datasets), record_count=len(data))
    
    async def union_distinct(
        self,
        *datasets: list[dict[str, Any]],
    ) -> UnionResult:
        """Union datasets with deduplication."""
        data = self.union.union_distinct(*datasets)
        return UnionResult(data=data, source_count=len(datasets), record_count=len(data))


__all__ = ["UnionResult", "DataUnion", "DataUnionAction"]
