"""
Data Profile Action Module.

Generates statistical profiles of data,
computes descriptive statistics and data quality metrics.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass
import logging
import math
from collections import Counter

logger = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    """Statistical profile for a single field."""
    name: str
    field_type: str
    count: int
    null_count: int
    unique_count: int
    fill_rate: float
    min_value: Any = None
    max_value: Any = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    top_values: list[tuple[Any, int]] = None


@dataclass
class DataProfile:
    """Complete data profile."""
    total_rows: int
    total_columns: int
    field_profiles: dict[str, FieldProfile]
    data_quality_score: float


class DataProfileAction:
    """
    Statistical profiling of data structures.

    Computes descriptive statistics, detects types,
    and generates data quality scores.

    Example:
        profiler = DataProfileAction()
        profile = profiler.profile(data)
        print(profile.data_quality_score)
    """

    def __init__(
        self,
        top_values_count: int = 5,
    ) -> None:
        self.top_values_count = top_values_count

    def profile(
        self,
        data: list[dict],
        fields: Optional[list[str]] = None,
    ) -> DataProfile:
        """Generate comprehensive data profile."""
        if not data:
            return DataProfile(
                total_rows=0,
                total_columns=0,
                field_profiles={},
                data_quality_score=0.0,
            )

        fields = fields or list(data[0].keys())
        field_profiles = {}

        for field in fields:
            field_profiles[field] = self._profile_field(data, field)

        quality_score = self._compute_quality_score(field_profiles, len(data))

        return DataProfile(
            total_rows=len(data),
            total_columns=len(fields),
            field_profiles=field_profiles,
            data_quality_score=quality_score,
        )

    def _profile_field(
        self,
        data: list[dict],
        field: str,
    ) -> FieldProfile:
        """Profile a single field."""
        values = [row.get(field) for row in data]
        non_null = [v for v in values if v is not None and v != ""]

        field_type = self._infer_type(non_null)

        numeric_values = None
        if field_type in ("int", "float"):
            numeric_values = [v for v in non_null if isinstance(v, (int, float))]
            if not numeric_values and field_type == "int":
                numeric_values = [float(v) for v in non_null if self._is_numeric(v)]

        profile = FieldProfile(
            name=field,
            field_type=field_type,
            count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(set(non_null)) if non_null else 0,
            fill_rate=len(non_null) / len(values) if values else 0,
            top_values=[],
        )

        if numeric_values:
            profile.min_value = min(numeric_values)
            profile.max_value = max(numeric_values)
            profile.mean = sum(numeric_values) / len(numeric_values)
            profile.median = self._median(numeric_values)
            profile.std_dev = self._std_dev(numeric_values)

        if non_null:
            counter = Counter(non_null)
            profile.top_values = counter.most_common(self.top_values_count)

        return profile

    def _infer_type(self, values: list) -> str:
        """Infer the type of values."""
        if not values:
            return "unknown"

        type_counts: dict[str, int] = {}

        for v in values[:100]:
            vtype = self._get_value_type(v)
            type_counts[vtype] = type_counts.get(vtype, 0) + 1

        if not type_counts:
            return "unknown"

        return max(type_counts.items(), key=lambda x: x[1])[0]

    def _get_value_type(self, value: Any) -> str:
        """Get type name for a value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "string"
        if isinstance(value, dict):
            return "dict"
        if isinstance(value, (list, tuple)):
            return "list"
        return "unknown"

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _median(self, values: list) -> float:
        """Calculate median."""
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
        return sorted_vals[mid]

    def _std_dev(self, values: list) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return math.sqrt(variance)

    def _compute_quality_score(
        self,
        field_profiles: dict[str, FieldProfile],
        total_rows: int,
    ) -> float:
        """Compute overall data quality score."""
        if not field_profiles:
            return 0.0

        scores = []

        for field, profile in field_profiles.items():
            field_score = profile.fill_rate * 100

            if profile.null_count == 0:
                field_score += 10

            if profile.unique_count > 0:
                uniqueness = profile.unique_count / max(profile.count, 1)
                field_score += uniqueness * 10

            scores.append(field_score)

        return sum(scores) / len(scores) if scores else 0.0
