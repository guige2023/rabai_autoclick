"""
Data Quality Action Module.

Provides data quality checks, profiling, and validation
for ensuring data integrity across pipelines.

Author: RabAi Team
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class QualityLevel(Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


@dataclass
class QualityIssue:
    """A data quality issue."""
    field: str
    issue_type: str
    severity: str  # "error", "warning", "info"
    message: str
    affected_count: int = 0
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class QualityReport:
    """Data quality report."""
    dataset_name: str
    total_records: int
    total_fields: int
    issues: List[QualityIssue] = field(default_factory=list)
    field_profiles: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    overall_score: float = 0.0
    quality_level: QualityLevel = QualityLevel.GOOD
    timestamp: float = field(default_factory=datetime.now().timestamp)


class DataValidator:
    """Data validation engine."""

    def __init__(self) -> None:
        self.rules: Dict[str, List[Callable]] = defaultdict(list)

    def add_rule(
        self,
        field_name: str,
        rule_func: Callable[[Any], Tuple[bool, Optional[str]]],
    ) -> "DataValidator":
        """Add validation rule for a field."""
        self.rules[field_name].append(rule_func)
        return self

    def validate_record(
        self,
        record: Dict[str, Any],
    ) -> List[Tuple[str, str]]:
        """Validate a single record. Returns list of (field, error) tuples."""
        errors = []

        for field_name, rules in self.rules.items():
            value = record.get(field_name)
            for rule in rules:
                is_valid, error_msg = rule(value)
                if not is_valid:
                    errors.append((field_name, error_msg or "Validation failed"))

        return errors

    def validate_batch(
        self,
        data: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate batch of records. Returns (valid, invalid) lists."""
        valid = []
        invalid = []

        for record in data:
            errors = self.validate_record(record)
            if errors:
                record["_validation_errors"] = errors
                invalid.append(record)
            else:
                valid.append(record)

        return valid, invalid


class DataProfiler:
    """Data profiling for understanding dataset characteristics."""

    @staticmethod
    def profile_field(values: List[Any]) -> Dict[str, Any]:
        """Profile a single field."""
        non_null = [v for v in values if v is not None]

        profile = {
            "total_count": len(values),
            "null_count": len(values) - len(non_null),
            "null_percentage": (len(values) - len(non_null)) / len(values) * 100 if values else 0,
            "unique_count": len(set(str(v) for v in non_null)) if non_null else 0,
            "unique_percentage": len(set(str(v) for v in non_null)) / len(non_null) * 100 if non_null else 0,
        }

        if non_null:
            # Type analysis
            types = defaultdict(int)
            for v in non_null:
                types[type(v).__name__] += 1
            profile["types"] = dict(types)

            # String-specific
            str_values = [v for v in non_null if isinstance(v, str)]
            if str_values:
                profile["min_length"] = min(len(v) for v in str_values)
                profile["max_length"] = max(len(v) for v in str_values)
                profile["avg_length"] = sum(len(v) for v in str_values) / len(str_values)

            # Numeric-specific
            numeric_values = [v for v in non_null if isinstance(v, (int, float))]
            if numeric_values:
                profile["min"] = min(numeric_values)
                profile["max"] = max(numeric_values)
                profile["avg"] = sum(numeric_values) / len(numeric_values)

        return profile

    @classmethod
    def profile_dataset(
        cls,
        data: List[Dict[str, Any]],
        dataset_name: str = "dataset",
    ) -> QualityReport:
        """Profile entire dataset."""
        if not data:
            return QualityReport(
                dataset_name=dataset_name,
                total_records=0,
                total_fields=0,
                overall_score=0.0,
            )

        # Collect all fields
        all_fields: Set[str] = set()
        for record in data:
            all_fields.update(record.keys())

        field_profiles = {}
        for field_name in all_fields:
            values = [record.get(field_name) for record in data]
            field_profiles[field_name] = cls.profile_field(values)

        return QualityReport(
            dataset_name=dataset_name,
            total_records=len(data),
            total_fields=len(all_fields),
            field_profiles=field_profiles,
        )


class DataQualityChecker:
    """Main data quality checking system."""

    def __init__(self) -> None:
        self.validator = DataValidator()
        self.issues: List[QualityIssue] = []

    def check_nulls(
        self,
        data: List[Dict[str, Any]],
        fields: List[str],
        threshold: float = 0.0,
    ) -> List[QualityIssue]:
        """Check for null values exceeding threshold."""
        issues = []

        for field_name in fields:
            null_count = sum(1 for record in data if record.get(field_name) is None)
            null_percentage = null_count / len(data) * 100 if data else 0

            if null_percentage > threshold:
                null_values = [
                    record.get(field_name)
                    for record in data[:100]
                    if record.get(field_name) is None
                ]
                issues.append(QualityIssue(
                    field=field_name,
                    issue_type="null_excessive",
                    severity="error" if null_percentage > 50 else "warning",
                    message=f"Null values: {null_count} ({null_percentage:.1f}%)",
                    affected_count=null_count,
                    sample_values=null_values[:5],
                ))

        return issues

    def check_duplicates(
        self,
        data: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> List[QualityIssue]:
        """Check for duplicate records."""
        seen: Set[Tuple] = set()
        duplicate_count = 0
        sample_duplicates = []

        for record in data:
            key_values = tuple(record.get(f) for f in key_fields)
            if key_values in seen:
                duplicate_count += 1
                if len(sample_duplicates) < 5:
                    sample_duplicates.append(dict(zip(key_fields, key_values)))
            else:
                seen.add(key_values)

        issues = []
        if duplicate_count > 0:
            issues.append(QualityIssue(
                field=", ".join(key_fields),
                issue_type="duplicates",
                severity="warning",
                message=f"Duplicate records: {duplicate_count}",
                affected_count=duplicate_count,
                sample_values=sample_duplicates,
            ))

        return issues

    def check_pattern(
        self,
        data: List[Dict[str, Any]],
        field_name: str,
        pattern: str,
    ) -> List[QualityIssue]:
        """Check field values match regex pattern."""
        regex = re.compile(pattern)
        non_matching = []

        for record in data:
            value = record.get(field_name)
            if value is not None and not regex.match(str(value)):
                non_matching.append(value)

        issues = []
        if non_matching:
            issues.append(QualityIssue(
                field=field_name,
                issue_type="pattern_mismatch",
                severity="warning",
                message=f"Values not matching pattern '{pattern}': {len(non_matching)}",
                affected_count=len(non_matching),
                sample_values=non_matching[:5],
            ))

        return issues

    def check_range(
        self,
        data: List[Dict[str, Any]],
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> List[QualityIssue]:
        """Check numeric values are within range."""
        out_of_range = []

        for record in data:
            value = record.get(field_name)
            if value is not None and isinstance(value, (int, float)):
                if (min_value is not None and value < min_value) or \
                   (max_value is not None and value > max_value):
                    out_of_range.append(value)

        issues = []
        if out_of_range:
            issues.append(QualityIssue(
                field=field_name,
                issue_type="out_of_range",
                severity="error",
                message=f"Values out of range [{min_value}, {max_value}]: {len(out_of_range)}",
                affected_count=len(out_of_range),
                sample_values=out_of_range[:5],
            ))

        return issues

    def check_uniqueness(
        self,
        data: List[Dict[str, Any]],
        field_name: str,
    ) -> List[QualityIssue]:
        """Check field values are unique."""
        seen: Set[Any] = set()
        duplicates = []

        for record in data:
            value = record.get(field_name)
            if value is not None:
                if value in seen:
                    duplicates.append(value)
                else:
                    seen.add(value)

        issues = []
        if duplicates:
            issues.append(QualityIssue(
                field=field_name,
                issue_type="not_unique",
                severity="error",
                message=f"Non-unique values: {len(duplicates)} duplicates",
                affected_count=len(duplicates),
                sample_values=duplicates[:5],
            ))

        return issues

    def calculate_score(
        self,
        report: QualityReport,
    ) -> Tuple[float, QualityLevel]:
        """Calculate overall quality score."""
        if report.total_records == 0:
            return 0.0, QualityLevel.BAD

        base_score = 100.0

        # Deduct for issues
        for issue in report.issues:
            if issue.severity == "error":
                deduction = min(30, issue.affected_count / report.total_records * 100)
            else:
                deduction = min(10, issue.affected_count / report.total_records * 50)
            base_score -= deduction

        # Deduct for null percentages
        for field_name, profile in report.field_profiles.items():
            if profile["null_percentage"] > 50:
                base_score -= 20
            elif profile["null_percentage"] > 20:
                base_score -= 10
            elif profile["null_percentage"] > 5:
                base_score -= 5

        base_score = max(0.0, base_score)

        # Determine quality level
        if base_score >= 95:
            level = QualityLevel.EXCELLENT
        elif base_score >= 85:
            level = QualityLevel.GOOD
        elif base_score >= 70:
            level = QualityLevel.FAIR
        elif base_score >= 50:
            level = QualityLevel.POOR
        else:
            level = QualityLevel.BAD

        return base_score, level

    def generate_report(
        self,
        data: List[Dict[str, Any]],
        dataset_name: str = "dataset",
        checks: Optional[List[str]] = None,
    ) -> QualityReport:
        """Generate full quality report."""
        checks = checks or ["nulls", "duplicates", "uniqueness"]

        profiler = DataProfiler()
        profile = profiler.profile_dataset(data, dataset_name)
        report = profile

        issues = []

        if "nulls" in checks:
            fields = list(set().union(*(r.keys() for r in data)))
            issues.extend(self.check_nulls(data, fields))

        if "duplicates" in checks:
            if data:
                fields = list(data[0].keys())
                issues.extend(self.check_duplicates(data, [fields[0]]))

        if "uniqueness" in checks:
            for field_name in list(data[0].keys()) if data else []:
                issues.extend(self.check_uniqueness(data, field_name))

        report.issues = issues

        score, level = self.calculate_score(report)
        report.overall_score = score
        report.quality_level = level

        return report
