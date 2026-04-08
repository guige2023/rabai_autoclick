"""
Data Quality Action Module.

Provides data quality checks, profiling, validation,
and cleansing capabilities for data pipelines.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import re
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


@dataclass
class QualityCheck:
    """Single quality check result."""
    check_name: str
    passed: bool
    value: Any
    threshold: Any
    severity: str
    message: str


@dataclass
class DataProfile:
    """Profile of a dataset."""
    total_rows: int
    total_columns: int
    column_profiles: Dict[str, "ColumnProfile"]
    quality_score: float
    issues: List[str]


@dataclass
class ColumnProfile:
    """Profile of a single column."""
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    median_value: Optional[Any] = None


class DataValidator:
    """Validates data against rules."""

    def __init__(self):
        self.rules: List[Tuple[str, Callable, Any]] = []

    def add_rule(
        self,
        name: str,
        validator: Callable[[Any], bool],
        threshold: Any = None
    ):
        """Add validation rule."""
        self.rules.append((name, validator, threshold))

    def validate(self, data: Any) -> List[QualityCheck]:
        """Validate data against all rules."""
        results = []
        for name, validator, threshold in self.rules:
            try:
                passed = validator(data)
                results.append(QualityCheck(
                    check_name=name,
                    passed=passed,
                    value=data,
                    threshold=threshold,
                    severity="high" if not passed else "info",
                    message=f"{name}: {'PASS' if passed else 'FAIL'}"
                ))
            except Exception as e:
                results.append(QualityCheck(
                    check_name=name,
                    passed=False,
                    value=data,
                    threshold=threshold,
                    severity="high",
                    message=f"{name}: ERROR - {str(e)}"
                ))
        return results


class DataProfiler:
    """Profiles datasets and columns."""

    def profile_column(self, values: List[Any]) -> ColumnProfile:
        """Profile a single column."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)

        unique_values = set(non_null)

        try:
            numeric_values = [float(v) for v in non_null if self._is_numeric(v)]
            mean_val = sum(numeric_values) / len(numeric_values) if numeric_values else None
        except:
            mean_val = None

        return ColumnProfile(
            column_name="unknown",
            data_type=self._infer_type(non_null),
            null_count=null_count,
            null_percentage=(null_count / len(values) * 100) if values else 0,
            unique_count=len(unique_values),
            unique_percentage=(len(unique_values) / len(non_null) * 100) if non_null else 0,
            min_value=min(non_null) if non_null else None,
            max_value=max(non_null) if non_null else None,
            mean_value=mean_val
        )

    def _infer_type(self, values: List[Any]) -> str:
        """Infer data type from values."""
        if not values:
            return "unknown"
        sample = values[:100]
        if all(isinstance(v, bool) for v in sample):
            return "boolean"
        if all(isinstance(v, int) for v in sample):
            return "integer"
        if all(isinstance(v, float) for v in sample):
            return "float"
        if all(isinstance(v, str) for v in sample):
            return "string"
        return "mixed"

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        try:
            float(value)
            return True
        except:
            return False


class DataCleanser:
    """Cleans data issues."""

    def __init__(self):
        self.transforms: List[Tuple[str, Callable]] = []

    def add_transform(self, name: str, transform: Callable):
        """Add data transform."""
        self.transforms.append((name, transform))

    def cleanse(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Cleanse data and return issues."""
        cleansed = []
        issues = []

        for i, row in enumerate(data):
            clean_row = row.copy()
            for name, transform in self.transforms:
                try:
                    clean_row = transform(clean_row)
                except Exception as e:
                    issues.append(f"Row {i}: {name} failed - {str(e)}")
            cleansed.append(clean_row)

        return cleansed, issues


class QualityScorer:
    """Scores data quality."""

    def score(self, profile: DataProfile) -> float:
        """Calculate quality score 0-100."""
        if profile.total_rows == 0:
            return 0.0

        score = 100.0

        for col_profile in profile.column_profiles.values():
            if col_profile.null_percentage > 50:
                score -= 20
            elif col_profile.null_percentage > 20:
                score -= 10

            if col_profile.unique_percentage > 95 and col_profile.null_percentage == 0:
                score -= 5

        return max(0.0, score)


def main():
    """Demonstrate data quality."""
    profiler = DataProfiler()
    profile = profiler.profile_column([1, 2, 3, 4, 5, None, 7, 8, 9, 10])
    print(f"Profile: {profile}")

    validator = DataValidator()
    validator.add_rule("positive", lambda x: x > 0)
    checks = validator.validate(5)
    print(f"Checks: {[c.message for c in checks]}")


if __name__ == "__main__":
    main()
