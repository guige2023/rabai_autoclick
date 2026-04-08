"""
Data Profiler Action Module.

Profiles datasets to understand schema, distributions, quality,
 and statistical characteristics of data fields.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from collections import Counter
from enum import Enum
import statistics
import logging

logger = logging.getLogger(__name__)


class FieldType(Enum):
    """Inferred type of a data field."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    LIST = "list"
    DICT = "dict"
    NULL = "null"
    MIXED = "mixed"


@dataclass
class FieldProfile:
    """Statistical profile of a single field."""
    name: str
    field_type: FieldType
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    empty_count: int = 0
    min_value: Any = None
    max_value: Any = None
    mean_value: Optional[float] = None
    median_value: Any = None
    std_dev: Optional[float] = None
    top_values: list[tuple[Any, int]] = field(default_factory=list)
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    patterns: list[str] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Complete profile of a dataset."""
    record_count: int = 0
    field_count: int = 0
    total_cells: int = 0
    null_cells: int = 0
    completeness: float = 0.0
    field_profiles: dict[str, FieldProfile] = field(default_factory=dict)
    quality_score: float = 0.0
    issues: list[str] = field(default_factory=list)


class DataProfilerAction:
    """
    Dataset profiling and quality analysis.

    Analyzes datasets to generate comprehensive statistical profiles
    including type inference, distributions, and quality metrics.

    Example:
        profiler = DataProfilerAction()
        profile = profiler.profile_dataset(data)
        print(f"Quality score: {profile.quality_score}")
        for field_name, fp in profile.field_profiles.items():
            print(f"{field_name}: {fp.field_type.value}, {fp.unique_count} unique")
    """

    def __init__(
        self,
        sample_size: Optional[int] = None,
        top_values_limit: int = 10,
    ) -> None:
        self.sample_size = sample_size
        self.top_values_limit = top_values_limit

    def profile_dataset(
        self,
        data: list[dict[str, Any]],
    ) -> DatasetProfile:
        """Profile an entire dataset."""
        if not data:
            return DatasetProfile()

        sample = data[:self.sample_size] if self.sample_size else data

        field_names: set[str] = set()
        for record in sample:
            field_names.update(record.keys())

        field_profiles: dict[str, FieldProfile] = {}
        total_cells = 0
        null_cells = 0

        for field_name in field_names:
            values = [record.get(field_name) for record in sample]
            profile = self._profile_field(field_name, values)
            field_profiles[field_name] = profile
            total_cells += len(values)
            null_cells += profile.null_count

        completeness = 1.0 - (null_cells / total_cells) if total_cells > 0 else 0.0

        issues = self._detect_issues(field_profiles, completeness)
        quality_score = self._calculate_quality_score(completeness, issues)

        return DatasetProfile(
            record_count=len(data),
            field_count=len(field_names),
            total_cells=total_cells,
            null_cells=null_cells,
            completeness=completeness,
            field_profiles=field_profiles,
            quality_score=quality_score,
            issues=issues,
        )

    def _profile_field(
        self,
        field_name: str,
        values: list[Any],
    ) -> FieldProfile:
        """Profile a single field."""
        non_null = [v for v in values if v is not None]
        empty = [v for v in non_null if self._is_empty(v)]

        field_type = self._infer_type(non_null)

        unique_values = set(v for v in non_null if not self._is_empty(v))

        top_values: list[tuple[Any, int]] = []
        if len(unique_values) <= self.top_values_limit * 2:
            counter = Counter(non_null)
            top_values = counter.most_common(self.top_values_limit)

        numeric_values = [v for v in non_null if isinstance(v, (int, float)) and not isinstance(v, bool)]

        profile = FieldProfile(
            name=field_name,
            field_type=field_type,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(unique_values),
            empty_count=len(empty),
        )

        if numeric_values:
            profile.min_value = min(numeric_values)
            profile.max_value = max(numeric_values)
            profile.mean_value = statistics.mean(numeric_values)
            profile.median_value = statistics.median(numeric_values)
            if len(numeric_values) > 1:
                profile.std_dev = statistics.stdev(numeric_values)

        if field_type == FieldType.STRING:
            string_values = [v for v in non_null if isinstance(v, str)]
            if string_values:
                profile.min_length = min(len(v) for v in string_values)
                profile.max_length = max(len(v) for v in string_values)
                profile.avg_length = statistics.mean(len(v) for v in string_values)
                profile.patterns = self._detect_patterns(string_values[:100])

        profile.top_values = top_values
        return profile

    def _infer_type(self, values: list[Any]) -> FieldType:
        """Infer the type of a field."""
        if not values:
            return FieldType.NULL

        types_present: set[str] = set()
        for v in values:
            if v is None:
                continue
            elif isinstance(v, bool):
                types_present.add("boolean")
            elif isinstance(v, int):
                types_present.add("integer")
            elif isinstance(v, float):
                types_present.add("float")
            elif isinstance(v, str):
                if self._looks_like_datetime(v):
                    types_present.add("datetime")
                else:
                    types_present.add("string")
            elif isinstance(v, list):
                types_present.add("list")
            elif isinstance(v, dict):
                types_present.add("dict")
            else:
                types_present.add("mixed")

        if len(types_present) == 0:
            return FieldType.NULL
        if len(types_present) == 1:
            t = types_present.pop()
            if t == "boolean":
                return FieldType.BOOLEAN
            elif t == "integer":
                return FieldType.INTEGER
            elif t == "float":
                return FieldType.FLOAT
            elif t == "datetime":
                return FieldType.DATETIME
            elif t == "string":
                return FieldType.STRING
            elif t == "list":
                return FieldType.LIST
            elif t == "dict":
                return FieldType.DICT

        return FieldType.MIXED

    def _is_empty(self, value: Any) -> bool:
        """Check if a value is empty."""
        if value is None:
            return True
        if isinstance(value, str) and value.strip() == "":
            return True
        if isinstance(value, (list, dict)) and len(value) == 0:
            return True
        return False

    def _looks_like_datetime(self, value: str) -> bool:
        """Check if a string looks like a datetime."""
        import re
        patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
            r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}',
        ]
        return any(re.match(p, str(value)) for p in patterns)

    def _detect_patterns(self, values: list[str]) -> list[str]:
        """Detect common patterns in string values."""
        import re
        patterns_found: dict[str, int] = {}

        for value in values[:50]:
            if re.match(r'^[A-Z]{2,}$', value):
                patterns_found["ALL_CAPS"] = patterns_found.get("ALL_CAPS", 0) + 1
            elif re.match(r'^[a-z]+$', value):
                patterns_found["lowercase"] = patterns_found.get("lowercase", 0) + 1
            elif re.match(r'^[A-Za-z\s]+$', value):
                patterns_found["Title Case"] = patterns_found.get("Title Case", 0) + 1
            elif re.match(r'^\d+$', value):
                patterns_found["numeric"] = patterns_found.get("numeric", 0) + 1
            elif re.match(r'^\S+@\S+\.\S+$', value):
                patterns_found["email"] = patterns_found.get("email", 0) + 1
            elif re.match(r'^https?://', value):
                patterns_found["url"] = patterns_found.get("url", 0) + 1

        return [p for p, c in sorted(patterns_found.items(), key=lambda x: -x[1]) if c > len(values) * 0.1]

    def _detect_issues(
        self,
        field_profiles: dict[str, FieldProfile],
        completeness: float,
    ) -> list[str]:
        """Detect data quality issues."""
        issues: list[str] = []

        if completeness < 0.5:
            issues.append(f"Low completeness: {completeness:.1%}")

        for name, profile in field_profiles.items():
            if profile.null_count > profile.total_count * 0.5:
                issues.append(f"Field '{name}' has >50% null values")

            if profile.unique_count == 1 and profile.total_count > 10:
                issues.append(f"Field '{name}' has only one unique value")

            if profile.field_type == FieldType.MIXED:
                issues.append(f"Field '{name}' has mixed types")

        return issues

    def _calculate_quality_score(
        self,
        completeness: float,
        issues: list[str],
    ) -> float:
        """Calculate overall data quality score."""
        score = completeness

        for issue in issues:
            if "Low completeness" in issue:
                score -= 0.2
            elif ">50% null" in issue:
                score -= 0.1
            elif "mixed types" in issue:
                score -= 0.05

        return max(0.0, min(1.0, score))
