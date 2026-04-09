"""Data profiling action for analyzing dataset characteristics.

Analyzes datasets to extract statistics, detect patterns,
identify data quality issues, and generate profiles.
"""

import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    """Profile for a single data field."""
    name: str
    field_type: str
    total_count: int
    null_count: int
    unique_count: int
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[Any] = None
    top_values: list[tuple[Any, int]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProfile:
    """Complete data profile."""
    total_rows: int
    total_columns: int
    field_profiles: dict[str, FieldProfile]
    generated_at: float
    processing_time_ms: float
    quality_score: float
    issues: list[str] = field(default_factory=list)


class DataProfilingAction:
    """Profile datasets and extract statistics.

    Example:
        >>> profiler = DataProfilingAction()
        >>> profile = profiler.profile_dataset(data)
        >>> print(f"Quality score: {profile.quality_score}")
    """

    def __init__(self) -> None:
        self._profile_cache: dict[str, DataProfile] = {}

    def profile_dataset(
        self,
        data: list[dict[str, Any]],
        sample_size: Optional[int] = None,
    ) -> DataProfile:
        """Profile a dataset.

        Args:
            data: List of dictionaries representing rows.
            sample_size: Optional limit on rows to profile.

        Returns:
            Complete data profile.
        """
        import time
        start_time = time.time()

        if not data:
            return DataProfile(
                total_rows=0,
                total_columns=0,
                field_profiles={},
                generated_at=time.time(),
                processing_time_ms=0.0,
                quality_score=0.0,
            )

        rows = data if sample_size is None else data[:sample_size]

        columns = set()
        for row in rows:
            columns.update(row.keys())

        field_profiles: dict[str, FieldProfile] = {}
        issues: list[str] = []

        for col in columns:
            values = [row.get(col) for row in rows]
            profile = self._profile_field(col, values)
            field_profiles[col] = profile

            null_pct = profile.null_count / profile.total_count * 100
            if null_pct > 50:
                issues.append(f"Field '{col}' has {null_pct:.1f}% null values")
            if profile.unique_count == 1:
                issues.append(f"Field '{col}' has only one unique value")

        quality_score = self._calculate_quality_score(field_profiles, issues)

        return DataProfile(
            total_rows=len(data),
            total_columns=len(columns),
            field_profiles=field_profiles,
            generated_at=time.time(),
            processing_time_ms=(time.time() - start_time) * 1000,
            quality_score=quality_score,
            issues=issues,
        )

    def _profile_field(self, name: str, values: list[Any]) -> FieldProfile:
        """Profile a single field.

        Args:
            name: Field name.
            values: List of field values.

        Returns:
            Field profile.
        """
        total_count = len(values)
        non_null = [v for v in values if v is not None]
        null_count = total_count - len(non_null)
        unique_values = set(non_null)
        unique_count = len(unique_values)

        field_type = self._infer_type(non_null)

        numeric_values = [
            v for v in non_null
            if isinstance(v, (int, float)) and not isinstance(v, bool)
        ]

        min_val = None
        max_val = None
        mean_val = None
        median_val = None

        if numeric_values:
            numeric_values.sort()
            min_val = numeric_values[0]
            max_val = numeric_values[-1]
            mean_val = sum(numeric_values) / len(numeric_values)
            mid = len(numeric_values) // 2
            median_val = numeric_values[mid]

        value_counts = Counter(non_null)
        top_values = value_counts.most_common(5)

        return FieldProfile(
            name=name,
            field_type=field_type,
            total_count=total_count,
            null_count=null_count,
            unique_count=unique_count,
            min_value=min_val,
            max_value=max_val,
            mean_value=mean_val,
            median_value=median_val,
            top_values=top_values,
            metadata={
                "completeness": (total_count - null_count) / total_count,
                "uniqueness": unique_count / total_count if total_count > 0 else 0,
            },
        )

    def _infer_type(self, values: list[Any]) -> str:
        """Infer the type of a field.

        Args:
            values: Non-null values.

        Returns:
            Type name string.
        """
        if not values:
            return "unknown"

        type_counts: Counter[str] = Counter()

        for v in values:
            if isinstance(v, bool):
                type_counts["boolean"] += 1
            elif isinstance(v, int):
                type_counts["integer"] += 1
            elif isinstance(v, float):
                type_counts["float"] += 1
            elif isinstance(v, str):
                if v.lower() in ("true", "false"):
                    type_counts["boolean"] += 1
                elif v.isdigit():
                    type_counts["integer"] += 1
                else:
                    type_counts["string"] += 1
            elif isinstance(v, list):
                type_counts["array"] += 1
            elif isinstance(v, dict):
                type_counts["object"] += 1
            else:
                type_counts["unknown"] += 1

        return type_counts.most_common(1)[0][0]

    def _calculate_quality_score(
        self,
        field_profiles: dict[str, FieldProfile],
        issues: list[str],
    ) -> float:
        """Calculate overall data quality score.

        Args:
            field_profiles: Profiles for all fields.
            issues: List of identified issues.

        Returns:
            Quality score between 0 and 1.
        """
        if not field_profiles:
            return 0.0

        completeness_scores = []
        for profile in field_profiles.values():
            completeness_scores.append(
                (profile.total_count - profile.null_count) / profile.total_count
            )

        avg_completeness = sum(completeness_scores) / len(completeness_scores)

        issue_penalty = min(len(issues) * 0.05, 0.5)

        return max(0.0, avg_completeness - issue_penalty)

    def get_cached_profile(self, key: str) -> Optional[DataProfile]:
        """Get a cached profile by key.

        Args:
            key: Cache key.

        Returns:
            Cached profile or None.
        """
        return self._profile_cache.get(key)

    def cache_profile(self, key: str, profile: DataProfile) -> None:
        """Cache a profile.

        Args:
            key: Cache key.
            profile: Profile to cache.
        """
        self._profile_cache[key] = profile

    def clear_cache(self) -> None:
        """Clear the profile cache."""
        self._profile_cache.clear()
