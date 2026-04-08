"""
Data Profile Action - Profiles data for analysis.

This module provides data profiling capabilities for
understanding data characteristics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from collections import Counter


@dataclass
class FieldProfile:
    """Profile for a single field."""
    field_name: str
    total_count: int
    null_count: int
    unique_count: int
    top_values: list[tuple[Any, int]] = field(default_factory=list)


@dataclass
class DataProfile:
    """Profile of entire dataset."""
    record_count: int
    field_count: int
    field_profiles: list[FieldProfile]


class DataProfiler:
    """Profiles data."""
    
    def __init__(self) -> None:
        pass
    
    def profile(self, data: list[dict[str, Any]]) -> DataProfile:
        """Profile data."""
        if not data:
            return DataProfile(record_count=0, field_count=0, field_profiles=[])
        
        field_names = set()
        for record in data:
            field_names.update(record.keys())
        
        field_profiles = []
        for field_name in field_names:
            values = [r.get(field_name) for r in data]
            non_null = [v for v in values if v is not None]
            unique = set(non_null)
            counter = Counter(non_null)
            top = counter.most_common(5)
            
            field_profiles.append(FieldProfile(
                field_name=field_name,
                total_count=len(values),
                null_count=len(values) - len(non_null),
                unique_count=len(unique),
                top_values=top,
            ))
        
        return DataProfile(
            record_count=len(data),
            field_count=len(field_names),
            field_profiles=field_profiles,
        )


class DataProfileAction:
    """Data profile action for automation workflows."""
    
    def __init__(self) -> None:
        self.profiler = DataProfiler()
    
    async def profile(self, data: list[dict[str, Any]]) -> DataProfile:
        """Profile data."""
        return self.profiler.profile(data)


__all__ = ["FieldProfile", "DataProfile", "DataProfiler", "DataProfileAction"]
