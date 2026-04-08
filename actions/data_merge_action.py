"""
Data Merge Action - Merges multiple datasets into one.

This module provides data merging capabilities including
union, concat, and reconcile operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar


T = TypeVar("T")


@dataclass
class MergeResult:
    """Result of merge operation."""
    data: list[dict[str, Any]]
    source_count: int
    record_count: int
    duplicates: int


class DataMerger:
    """Merges multiple data sources."""
    
    def __init__(self) -> None:
        pass
    
    def union(
        self,
        *datasets: list[dict[str, Any]],
        deduplicate: bool = True,
    ) -> list[dict[str, Any]]:
        """Union of multiple datasets."""
        result = []
        seen = set()
        
        for dataset in datasets:
            for record in dataset:
                record_key = self._record_key(record)
                if not deduplicate or record_key not in seen:
                    result.append(record)
                    seen.add(record_key)
        
        return result
    
    def concat(
        self,
        *datasets: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Concatenate datasets without deduplication."""
        result = []
        for dataset in datasets:
            result.extend(dataset)
        return result
    
    def reconcile(
        self,
        primary: list[dict[str, Any]],
        *sources: list[dict[str, Any]],
        key_field: str = "id",
        conflict_resolution: str = "primary",
    ) -> list[dict[str, Any]]:
        """Reconcile data from multiple sources."""
        result = []
        primary_index = {r.get(key_field): r for r in primary}
        
        for record in primary:
            result.append(record.copy())
        
        for source in sources:
            for record in source:
                key = record.get(key_field)
                if key in primary_index:
                    if conflict_resolution == "source":
                        result = [record if r.get(key_field) == key else r for r in result]
                else:
                    result.append(record.copy())
        
        return result
    
    def _record_key(self, record: dict[str, Any]) -> str:
        """Generate a unique key for a record."""
        import json
        return json.dumps(record, sort_keys=True, default=str)


class DataMergeAction:
    """Data merge action for automation workflows."""
    
    def __init__(self) -> None:
        self.merger = DataMerger()
    
    async def merge(
        self,
        *datasets: list[dict[str, Any]],
        mode: str = "union",
        **kwargs,
    ) -> MergeResult:
        """Merge multiple datasets."""
        if mode == "union":
            data = self.merger.union(*datasets, **kwargs)
        elif mode == "concat":
            data = self.merger.concat(*datasets)
        elif mode == "reconcile":
            data = self.merger.reconcile(datasets[0], *datasets[1:], **kwargs)
        else:
            data = list(datasets[0]) if datasets else []
        
        return MergeResult(
            data=data,
            source_count=len(datasets),
            record_count=len(data),
            duplicates=sum(len(d) for d in datasets) - len(data),
        )


__all__ = ["MergeResult", "DataMerger", "DataMergeAction"]
