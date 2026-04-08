# Copyright (c) 2024. coded by claude
"""Data Profiler Action Module.

Profiles API data structures to understand schema, types,
nullability, and value distributions.
"""
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    field_name: str
    data_type: type
    null_count: int = 0
    unique_count: int = 0
    sample_values: List[Any] = field(default_factory=list)
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None


@dataclass
class DataProfile:
    total_records: int
    field_profiles: Dict[str, FieldProfile]
    null_percentage: float
    duplicate_count: int


class DataProfiler:
    def profile(self, data: List[Dict[str, Any]]) -> DataProfile:
        if not data:
            return DataProfile(total_records=0, field_profiles={}, null_percentage=0.0, duplicate_count=0)
        field_names = set()
        for record in data:
            field_names.update(record.keys())
        field_profiles: Dict[str, FieldProfile] = {}
        for field_name in field_names:
            field_profiles[field_name] = FieldProfile(field_name=field_name, data_type=type(None))
        null_count_total = 0
        total_cells = len(data) * len(field_names)
        for record in data:
            for field_name in field_names:
                value = record.get(field_name)
                profile = field_profiles[field_name]
                if value is None:
                    profile.null_count += 1
                    null_count_total += 1
                else:
                    if profile.data_type == type(None):
                        profile.data_type = type(value)
                    if not profile.sample_values or len(profile.sample_values) < 5:
                        if value not in profile.sample_values:
                            profile.sample_values.append(value)
                    if isinstance(value, (int, float)):
                        if profile.min_value is None or value < profile.min_value:
                            profile.min_value = value
                        if profile.max_value is None or value > profile.max_value:
                            profile.max_value = value
        for profile in field_profiles.values():
            profile.unique_count = len(set(profile.sample_values))
        null_percentage = (null_count_total / total_cells * 100) if total_cells > 0 else 0.0
        duplicates = len(data) - len({str(d) for d in data})
        return DataProfile(
            total_records=len(data),
            field_profiles=field_profiles,
            null_percentage=null_percentage,
            duplicate_count=duplicates,
        )

    def get_summary(self, profile: DataProfile) -> Dict[str, Any]:
        return {
            "total_records": profile.total_records,
            "total_fields": len(profile.field_profiles),
            "null_percentage": f"{profile.null_percentage:.2f}%",
            "duplicate_count": profile.duplicate_count,
            "fields": {
                name: {
                    "type": p.data_type.__name__,
                    "nulls": p.null_count,
                    "samples": len(p.sample_values),
                }
                for name, p in profile.field_profiles.items()
            },
        }
