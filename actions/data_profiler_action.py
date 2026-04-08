"""Data Profiler Action Module.

Provides data profiling with statistics, type inference,
null detection, and distribution analysis.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar
import logging

logger = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    """Profile for a single field."""
    name: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    inferred_type: Optional[str] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    avg_value: Optional[float] = None
    top_values: List[tuple] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Profile for entire dataset."""
    field_count: int = 0
    row_count: int = 0
    total_cells: int = 0
    null_cells: int = 0
    null_percentage: float = 0.0
    fields: Dict[str, FieldProfile] = field(default_factory=dict)


class DataProfilerAction:
    """Data profiler for datasets.

    Example:
        profiler = DataProfilerAction()

        profile = profiler.profile([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": None, "age": 35},
        ])

        print(profile.row_count)
        print(profile.fields["name"].null_count)
    """

    def __init__(self) -> None:
        pass

    def profile(self, data: List[Dict[str, Any]]) -> DatasetProfile:
        """Profile dataset.

        Args:
            data: List of records

        Returns:
            DatasetProfile with statistics
        """
        if not data:
            return DatasetProfile()

        row_count = len(data)
        field_names = set()
        for record in data:
            field_names.update(record.keys())

        field_profiles: Dict[str, FieldProfile] = {}
        all_values: Dict[str, List] = defaultdict(list)

        for field_name in field_names:
            field_profiles[field_name] = FieldProfile(name=field_name)

        for record in data:
            for field_name in field_names:
                value = record.get(field_name)
                profile = field_profiles[field_name]
                profile.total_count += 1

                if value is None:
                    profile.null_count += 1
                else:
                    all_values[field_name].append(value)

        for field_name, values in all_values.items():
            profile = field_profiles[field_name]
            profile.unique_count = len(set(str(v) for v in values))

            if values:
                profile.inferred_type = self._infer_type(values)

                if all(isinstance(v, (int, float)) for v in values):
                    numeric_values = [v for v in values if v is not None]
                    if numeric_values:
                        profile.min_value = min(numeric_values)
                        profile.max_value = max(numeric_values)
                        profile.avg_value = sum(numeric_values) / len(numeric_values)

                value_counts = Counter(str(v) for v in values)
                profile.top_values = value_counts.most_common(5)

        null_cells = sum(f.null_count for f in field_profiles.values())
        total_cells = row_count * len(field_names)

        return DatasetProfile(
            field_count=len(field_names),
            row_count=row_count,
            total_cells=total_cells,
            null_cells=null_cells,
            null_percentage=(null_cells / total_cells * 100) if total_cells > 0 else 0.0,
            fields=field_profiles,
        )

    def _infer_type(self, values: List[Any]) -> str:
        """Infer type from values."""
        if not values:
            return "unknown"

        type_counts: Dict[str, int] = defaultdict(int)

        for value in values:
            if value is None:
                continue

            if isinstance(value, bool):
                type_counts["boolean"] += 1
            elif isinstance(value, int):
                type_counts["integer"] += 1
            elif isinstance(value, float):
                type_counts["float"] += 1
            elif isinstance(value, str):
                if self._is_datetime(value):
                    type_counts["datetime"] += 1
                elif self._is_email(value):
                    type_counts["email"] += 1
                elif value.isdigit():
                    type_counts["numeric_string"] += 1
                else:
                    type_counts["string"] += 1
            elif isinstance(value, dict):
                type_counts["object"] += 1
            elif isinstance(value, list):
                type_counts["array"] += 1

        if not type_counts:
            return "null"

        return max(type_counts.items(), key=lambda x: x[1])[0]

    def _is_datetime(self, value: str) -> bool:
        """Check if string looks like datetime."""
        patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"\d{2}/\d{2}/\d{4}",
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",
        ]
        return any(re.search(p, str(value)) for p in patterns)

    def _is_email(self, value: str) -> bool:
        """Check if string looks like email."""
        return "@" in str(value) and "." in str(value)

    def compare_profiles(
        self,
        profile1: DatasetProfile,
        profile2: DatasetProfile,
    ) -> Dict[str, Any]:
        """Compare two dataset profiles.

        Returns:
            Dictionary of differences
        """
        return {
            "row_count_diff": profile2.row_count - profile1.row_count,
            "field_count_diff": profile2.field_count - profile1.field_count,
            "null_percentage_diff": profile2.null_percentage - profile1.null_percentage,
        }
