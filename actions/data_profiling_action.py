"""Data profiling action for analyzing dataset characteristics.

Provides statistical analysis, pattern detection, and
data quality profiling for datasets.
"""

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    field_name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: int
    statistics: dict[str, Any] = field(default_factory=dict)
    patterns: list[str] = field(default_factory=list)
    top_values: list[tuple[Any, int]] = field(default_factory=list)


class DataProfilingAction:
    """Profile datasets and compute statistics.

    Args:
        top_values_limit: Number of top values to track.
        pattern_sample_size: Sample size for pattern detection.
    """

    def __init__(
        self,
        top_values_limit: int = 10,
        pattern_sample_size: int = 1000,
    ) -> None:
        self._top_values_limit = top_values_limit
        self._pattern_sample_size = pattern_sample_size
        self._profiles: dict[str, FieldProfile] = {}

    def profile_dataset(
        self,
        records: list[dict[str, Any]],
        field_names: Optional[list[str]] = None,
    ) -> dict[str, FieldProfile]:
        """Profile a dataset.

        Args:
            records: List of records.
            field_names: Fields to profile (all if None).

        Returns:
            Dictionary mapping field names to profiles.
        """
        if not records:
            return {}

        fields = field_names or list(records[0].keys())

        for field_name in fields:
            values = [record.get(field_name) for record in records]
            profile = self._profile_field(field_name, values)
            self._profiles[field_name] = profile

        return self._profiles

    def _profile_field(self, field_name: str, values: list[Any]) -> FieldProfile:
        """Profile a single field.

        Args:
            field_name: Name of the field.
            values: List of field values.

        Returns:
            Field profile.
        """
        non_null = [v for v in values if v is not None]
        unique_values = list(set(non_null))

        data_type = self._infer_type(non_null)

        profile = FieldProfile(
            field_name=field_name,
            data_type=data_type,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(unique_values),
        )

        if data_type == "numeric" and non_null:
            profile.statistics = self._compute_numeric_stats(non_null)

        if data_type == "string" and non_null:
            profile.statistics = self._compute_string_stats(non_null)
            profile.patterns = self._detect_patterns(non_null[:self._pattern_sample_size])

        profile.top_values = self._compute_top_values(non_null)

        return profile

    def _infer_type(self, values: list[Any]) -> str:
        """Infer the data type of a field.

        Args:
            values: Non-null values.

        Returns:
            Data type string.
        """
        if not values:
            return "unknown"

        type_counts = {"numeric": 0, "string": 0, "boolean": 0, "other": 0}

        for value in values[:100]:
            if isinstance(value, bool):
                type_counts["boolean"] += 1
            elif isinstance(value, (int, float)):
                type_counts["numeric"] += 1
            elif isinstance(value, str):
                type_counts["string"] += 1
            else:
                type_counts["other"] += 1

        dominant = max(type_counts, key=type_counts.get)
        return dominant if type_counts[dominant] > 0 else "string"

    def _compute_numeric_stats(self, values: list[Any]) -> dict[str, float]:
        """Compute numeric statistics.

        Args:
            values: Numeric values.

        Returns:
            Statistics dictionary.
        """
        numeric = [v for v in values if isinstance(v, (int, float))]
        if not numeric:
            return {}

        sorted_values = sorted(numeric)
        n = len(sorted_values)

        mean = sum(numeric) / n
        variance = sum((x - mean) ** 2 for x in numeric) / n
        std_dev = math.sqrt(variance)

        return {
            "min": min(numeric),
            "max": max(numeric),
            "mean": mean,
            "median": sorted_values[n // 2],
            "std_dev": std_dev,
            "variance": variance,
            "q1": sorted_values[n // 4],
            "q3": sorted_values[3 * n // 4],
        }

    def _compute_string_stats(self, values: list[str]) -> dict[str, Any]:
        """Compute string statistics.

        Args:
            values: String values.

        Returns:
            Statistics dictionary.
        """
        lengths = [len(str(v)) for v in values]
        return {
            "min_length": min(lengths),
            "max_length": max(lengths),
            "avg_length": sum(lengths) / len(lengths),
            "total_chars": sum(lengths),
        }

    def _detect_patterns(self, values: list[str]) -> list[str]:
        """Detect common patterns in string values.

        Args:
            values: String values.

        Returns:
            List of detected patterns.
        """
        import re

        patterns = []

        if values:
            first = str(values[0])

            if re.match(r'^\d{4}-\d{2}-\d{2}$', first):
                patterns.append("date_iso")
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', first):
                patterns.append("date_us")
            elif re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', first):
                patterns.append("ipv4")
            elif re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', first):
                patterns.append("email")
            elif re.match(r'^https?://', first):
                patterns.append("url")
            elif re.match(r'^\+\d{1,3}\s*\d{3,}$', first):
                patterns.append("phone")

        return patterns

    def _compute_top_values(self, values: list[Any]) -> list[tuple[Any, int]]:
        """Compute most frequent values.

        Args:
            values: Values to analyze.

        Returns:
            List of (value, count) tuples.
        """
        from collections import Counter
        counter = Counter(values)
        return counter.most_common(self._top_values_limit)

    def get_profile(self, field_name: str) -> Optional[FieldProfile]:
        """Get a field profile.

        Args:
            field_name: Field name.

        Returns:
            Field profile or None.
        """
        return self._profiles.get(field_name)

    def get_all_profiles(self) -> dict[str, FieldProfile]:
        """Get all field profiles.

        Returns:
            Dictionary of profiles.
        """
        return self._profiles

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of all profiles.

        Returns:
            Summary dictionary.
        """
        total_fields = len(self._profiles)
        total_records = 0

        for profile in self._profiles.values():
            if profile.total_count > total_records:
                total_records = profile.total_count

        return {
            "total_fields": total_fields,
            "total_records": total_records,
            "field_types": {
                profile.field_name: profile.data_type
                for profile in self._profiles.values()
            },
        }
