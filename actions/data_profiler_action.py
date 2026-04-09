"""Data profiling and statistics analysis action."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional, Sequence

import statistics


@dataclass
class FieldProfile:
    """Profile statistics for a single field."""

    field_name: str
    total_count: int
    null_count: int
    unique_count: int
    null_percentage: float
    completeness: float

    # Numeric stats
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    p25: Optional[float] = None
    p75: Optional[float] = None
    p90: Optional[float] = None
    p99: Optional[float] = None

    # String stats
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    empty_count: int = 0

    # Type info
    inferred_type: str = "unknown"
    sample_values: list[Any] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Overall dataset profile."""

    total_rows: int
    total_columns: int
    analyzed_at: datetime
    field_profiles: dict[str, FieldProfile]
    metadata: dict[str, Any] = field(default_factory=dict)


class DataProfilerAction:
    """Profiles datasets and computes statistics."""

    def __init__(
        self,
        sample_size: int = 10000,
        compute_percentiles: bool = True,
        compute_std_dev: bool = True,
    ):
        """Initialize the profiler.

        Args:
            sample_size: Max rows to analyze.
            compute_percentiles: Whether to compute percentiles.
            compute_std_dev: Whether to compute standard deviation.
        """
        self._sample_size = sample_size
        self._compute_percentiles = compute_percentiles
        self._compute_std_dev = compute_std_dev

    def _infer_type(self, value: Any) -> str:
        """Infer the type of a value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "string"
        if isinstance(value, (list, tuple)):
            return "array"
        if isinstance(value, dict):
            return "object"
        return type(value).__name__

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def _to_numeric(self, value: Any) -> Optional[float]:
        """Convert value to numeric."""
        if self._is_numeric(value):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        return None

    def _compute_percentile(self, sorted_values: list[float], percentile: float) -> float:
        """Compute percentile from sorted values."""
        if not sorted_values:
            return 0.0
        idx = int(len(sorted_values) * percentile / 100)
        idx = min(idx, len(sorted_values) - 1)
        return sorted_values[idx]

    def profile_field(self, values: Sequence[Any]) -> FieldProfile:
        """Profile a single field."""
        non_null = [v for v in values if v is not None]
        empty_strings = [v for v in non_null if isinstance(v, str) and v == ""]
        unique_values = set(non_null)

        field_name = "unknown"
        if hasattr(values, "__name__"):
            field_name = getattr(values, "__name__", field_name)

        numeric_values: list[float] = []
        string_lengths: list[int] = []

        for v in non_null:
            if self._is_numeric(v):
                numeric_values.append(float(v))
            if isinstance(v, str):
                string_lengths.append(len(v))

        profile = FieldProfile(
            field_name=field_name,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(unique_values),
            null_percentage=((len(values) - len(non_null)) / len(values) * 100)
            if len(values) > 0
            else 0,
            completeness=len(non_null) / len(values) if len(values) > 0 else 0,
            min_length=min(string_lengths) if string_lengths else None,
            max_length=max(string_lengths) if string_lengths else None,
            avg_length=statistics.mean(string_lengths) if string_lengths else None,
            empty_count=len(empty_strings),
            inferred_type=self._infer_type(non_null[0]) if non_null else "null",
            sample_values=list(non_null[:5]),
        )

        if numeric_values:
            numeric_values_sorted = sorted(numeric_values)
            profile.min_value = min(numeric_values)
            profile.max_value = max(numeric_values)
            profile.mean = statistics.mean(numeric_values)
            profile.median = statistics.median(numeric_values)

            if self._compute_std_dev and len(numeric_values) > 1:
                profile.std_dev = statistics.stdev(numeric_values)

            if self._compute_percentiles and len(numeric_values) > 1:
                profile.p25 = self._compute_percentile(numeric_values_sorted, 25)
                profile.p75 = self._compute_percentile(numeric_values_sorted, 75)
                profile.p90 = self._compute_percentile(numeric_values_sorted, 90)
                profile.p99 = self._compute_percentile(numeric_values_sorted, 99)

        return profile

    def profile_dataset(
        self,
        data: list[dict[str, Any]],
        field_names: Optional[list[str]] = None,
    ) -> DatasetProfile:
        """Profile an entire dataset.

        Args:
            data: List of records.
            field_names: Specific fields to profile (None = all).

        Returns:
            DatasetProfile with statistics.
        """
        if not data:
            return DatasetProfile(
                total_rows=0,
                total_columns=0,
                analyzed_at=datetime.now(),
                field_profiles={},
            )

        sample = data[: self._sample_size]

        if field_names is None:
            field_names = list(sample[0].keys()) if sample else []

        field_profiles = {}
        for field_name in field_names:
            values = [row.get(field_name) for row in sample]
            profile = self.profile_field(values)
            profile.field_name = field_name
            field_profiles[field_name] = profile

        return DatasetProfile(
            total_rows=len(data),
            total_columns=len(field_names),
            analyzed_at=datetime.now(),
            field_profiles=field_profiles,
            metadata={
                "sample_size": len(sample),
                "sampled": len(data) > self._sample_size,
            },
        )

    def get_summary(self, profile: DatasetProfile) -> dict[str, Any]:
        """Get a summary dict from a profile."""
        return {
            "total_rows": profile.total_rows,
            "total_columns": profile.total_columns,
            "analyzed_at": profile.analyzed_at.isoformat(),
            "completeness_avg": statistics.mean(
                fp.completeness for fp in profile.field_profiles.values()
            )
            if profile.field_profiles else 0,
            "types": {name: fp.inferred_type for name, fp in profile.field_profiles.items()},
        }
