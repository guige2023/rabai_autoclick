"""
Data Group Action - Groups data by fields.

This module provides data grouping capabilities for
aggregation and summarization.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from collections import defaultdict


@dataclass
class GroupResult:
    """Result of grouping operation."""
    groups: dict[Any, list[dict[str, Any]]]
    group_count: int
    record_count: int


class DataGrouper:
    """Groups data by fields."""
    
    def __init__(self) -> None:
        pass
    
    def group_by(
        self,
        data: list[dict[str, Any]],
        field: str,
    ) -> GroupResult:
        """Group data by field."""
        groups = defaultdict(list)
        for record in data:
            key = record.get(field)
            groups[key].append(record)
        return GroupResult(
            groups=dict(groups),
            group_count=len(groups),
            record_count=len(data),
        )
    
    def group_by_multiple(
        self,
        data: list[dict[str, Any]],
        fields: list[str],
    ) -> dict[tuple, list[dict[str, Any]]]:
        """Group by multiple fields."""
        groups = defaultdict(list)
        for record in data:
            key = tuple(record.get(f) for f in fields)
            groups[key].append(record)
        return dict(groups)


class DataGroupAction:
    """Data group action for automation workflows."""
    
    def __init__(self) -> None:
        self.grouper = DataGrouper()
    
    async def group_by(
        self,
        data: list[dict[str, Any]],
        field: str,
    ) -> GroupResult:
        """Group data by field."""
        return self.grouper.group_by(data, field)


__all__ = ["GroupResult", "DataGrouper", "DataGroupAction"]
