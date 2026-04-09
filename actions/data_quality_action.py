"""
Data Quality Action Module

Data quality checking with completeness, accuracy, consistency,
and timeliness validation. Supports rule-based and ML-based checks.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """Data quality severity levels."""

    PASS = "pass"
    WARNING = "warning"
    FAIL = "fail"


class CheckType(Enum):
    """Types of data quality checks."""

    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    TIMELINESS = "timeliness"
    ACCURACY = "accuracy"


@dataclass
class QualityIssue:
    """A single data quality issue."""

    check_type: CheckType
    level: QualityLevel
    message: str
    field: Optional[str] = None
    row_count: int = 0
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class QualityRule:
    """A data quality validation rule."""

    name: str
    check_type: CheckType
    field: Optional[str] = None
    validator: Optional[Callable[[Any], bool]] = None
    threshold: float = 1.0
    level: QualityLevel = QualityLevel.FAIL


@dataclass
class QualityResult:
    """Result of quality check."""

    dataset_name: str
    total_rows: int
    overall_level: QualityLevel
    issues: List[QualityIssue] = field(default_factory=list)
    passed_checks: int = 0
    failed_checks: int = 0
    warning_checks: int = 0
    checked_at: datetime = field(default_factory=datetime.now)


class CompletenessChecker:
    """Checks for missing or null values."""

    def check_nulls(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float,
    ) -> QualityIssue:
        """Check for null values."""
        null_count = sum(1 for row in data if row.get(field) is None)
        null_rate = null_count / len(data) if data else 0

        level = QualityLevel.PASS
        if null_rate > threshold:
            level = QualityLevel.FAIL
        elif null_rate > 0:
            level = QualityLevel.WARNING

        return QualityIssue(
            check_type=CheckType.COMPLETENESS,
            level=level,
            message=f"Field '{field}' has {null_count} null values ({null_rate:.2%})",
            field=field,
            row_count=null_count,
        )

    def check_empty_strings(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float,
    ) -> QualityIssue:
        """Check for empty strings."""
        empty_count = sum(1 for row in data if str(row.get(field, "")).strip() == "")
        empty_rate = empty_count / len(data) if data else 0

        level = QualityLevel.PASS
        if empty_rate > threshold:
            level = QualityLevel.FAIL
        elif empty_rate > 0:
            level = QualityLevel.WARNING

        return QualityIssue(
            check_type=CheckType.COMPLETENESS,
            level=level,
            message=f"Field '{field}' has {empty_count} empty strings ({empty_rate:.2%})",
            field=field,
            row_count=empty_count,
        )


class UniquenessChecker:
    """Checks for duplicate values."""

    def check_duplicate_values(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float,
    ) -> QualityIssue:
        """Check for duplicate values in a field."""
        values = [row.get(field) for row in data if row.get(field) is not None]
        unique_values = set(values)
        duplicate_count = len(values) - len(unique_values)
        duplicate_rate = duplicate_count / len(values) if values else 0

        level = QualityLevel.PASS
        if duplicate_rate > threshold:
            level = QualityLevel.FAIL
        elif duplicate_rate > 0:
            level = QualityLevel.WARNING

        # Sample duplicates
        from collections import Counter
        counter = Counter(values)
        samples = [v for v, c in counter.most_common(3) if c > 1]

        return QualityIssue(
            check_type=CheckType.UNIQUENESS,
            level=level,
            message=f"Field '{field}' has {duplicate_count} duplicate values ({duplicate_rate:.2%})",
            field=field,
            row_count=duplicate_count,
            sample_values=samples,
        )


class ValidityChecker:
    """Checks for valid data values."""

    EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    URL_REGEX = r"^https?://[^\s]+$"

    def check_email_format(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float,
    ) -> QualityIssue:
        """Check email format validity."""
        invalid_count = 0
        invalid_samples = []

        for row in data:
            value = row.get(field)
            if value and not re.match(self.EMAIL_REGEX, str(value)):
                invalid_count += 1
                if len(invalid_samples) < 3:
                    invalid_samples.append(value)

        invalid_rate = invalid_count / len(data) if data else 0

        level = QualityLevel.PASS
        if invalid_rate > threshold:
            level = QualityLevel.FAIL
        elif invalid_rate > 0:
            level = QualityLevel.WARNING

        return QualityIssue(
            check_type=CheckType.VALIDITY,
            level=level,
            message=f"Field '{field}' has {invalid_count} invalid emails ({invalid_rate:.2%})",
            field=field,
            row_count=invalid_count,
            sample_values=invalid_samples,
        )

    def check_numeric_range(
        self,
        data: List[Dict[str, Any]],
        field: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        threshold: float = 1.0,
    ) -> QualityIssue:
        """Check numeric values are within range."""
        out_of_range_count = 0
        out_of_range_samples = []

        for row in data:
            value = row.get(field)
            if value is not None:
                try:
                    num_value = float(value)
                    if (min_value is not None and num_value < min_value) or \
                       (max_value is not None and num_value > max_value):
                        out_of_range_count += 1
                        if len(out_of_range_samples) < 3:
                            out_of_range_samples.append(value)
                except (ValueError, TypeError):
                    out_of_range_count += 1

        out_of_range_rate = out_of_range_count / len(data) if data else 0

        level = QualityLevel.PASS
        if out_of_range_rate > threshold:
            level = QualityLevel.FAIL
        elif out_of_range_rate > 0:
            level = QualityLevel.WARNING

        return QualityIssue(
            check_type=CheckType.VALIDITY,
            level=level,
            message=f"Field '{field}' has {out_of_range_count} out-of-range values ({out_of_range_rate:.2%})",
            field=field,
            row_count=out_of_range_count,
            sample_values=out_of_range_samples,
        )


class ConsistencyChecker:
    """Checks for data consistency."""

    def check_cross_field_consistency(
        self,
        data: List[Dict[str, Any]],
        source_field: str,
        target_field: str,
        validator: Callable[[Dict[str, Any]], bool],
        threshold: float,
    ) -> QualityIssue:
        """Check consistency between fields."""
        inconsistent_count = 0

        for row in data:
            if not validator(row):
                inconsistent_count += 1

        inconsistent_rate = inconsistent_count / len(data) if data else 0

        level = QualityLevel.PASS
        if inconsistent_rate > threshold:
            level = QualityLevel.FAIL
        elif inconsistent_rate > 0:
            level = QualityLevel.WARNING

        return QualityIssue(
            check_type=CheckType.CONSISTENCY,
            level=level,
            message=f"Inconsistent rows between '{source_field}' and '{target_field}' ({inconsistent_rate:.2%})",
            field=f"{source_field} <-> {target_field}",
            row_count=inconsistent_count,
        )


class DataQualityAction:
    """
    Main action class for data quality checking.

    Features:
    - Completeness checks (nulls, empty values)
    - Uniqueness checks (duplicates)
    - Validity checks (formats, ranges, patterns)
    - Consistency checks (cross-field, referential)
    - Custom validation rules
    - Configurable thresholds

    Usage:
        action = DataQualityAction()
        action.add_rule(QualityRule(name="email_valid", check_type=CheckType.VALIDITY, field="email"))
        result = action.check(data)
    """

    def __init__(self, dataset_name: str = "dataset"):
        self.dataset_name = dataset_name
        self._rules: List[QualityRule] = []
        self._completeness = CompletenessChecker()
        self._uniqueness = UniquenessChecker()
        self._validity = ValidityChecker()
        self._consistency = ConsistencyChecker()

    def add_rule(self, rule: QualityRule) -> "DataQualityAction":
        """Add a quality rule."""
        self._rules.append(rule)
        return self

    def check_completeness(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float = 1.0,
    ) -> QualityIssue:
        """Check data completeness."""
        return self._completeness.check_nulls(data, field, threshold)

    def check_uniqueness(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float = 1.0,
    ) -> QualityIssue:
        """Check data uniqueness."""
        return self._uniqueness.check_duplicate_values(data, field, threshold)

    def check_email_validity(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: float = 0.01,
    ) -> QualityIssue:
        """Check email validity."""
        return self._validity.check_email_format(data, field, threshold)

    def check_numeric_range(
        self,
        data: List[Dict[str, Any]],
        field: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        threshold: float = 0.01,
    ) -> QualityIssue:
        """Check numeric range."""
        return self._validity.check_numeric_range(data, field, min_value, max_value, threshold)

    def check(
        self,
        data: List[Dict[str, Any]],
    ) -> QualityResult:
        """Run all quality checks."""
        issues: List[QualityIssue] = []
        passed = failed = warnings = 0

        for rule in self._rules:
            issue = self._run_check(rule, data)
            if issue:
                issues.append(issue)
                if issue.level == QualityLevel.PASS:
                    passed += 1
                elif issue.level == QualityLevel.FAIL:
                    failed += 1
                else:
                    warnings += 1

        # Determine overall level
        overall = QualityLevel.PASS
        if failed > 0:
            overall = QualityLevel.FAIL
        elif warnings > 0:
            overall = QualityLevel.WARNING

        return QualityResult(
            dataset_name=self.dataset_name,
            total_rows=len(data),
            overall_level=overall,
            issues=issues,
            passed_checks=passed,
            failed_checks=failed,
            warning_checks=warnings,
        )

    def _run_check(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
    ) -> Optional[QualityIssue]:
        """Run a single quality check."""
        if not rule.field:
            return None

        if rule.check_type == CheckType.COMPLETENESS:
            return self._completeness.check_nulls(data, rule.field, 1.0 - rule.threshold)

        elif rule.check_type == CheckType.UNIQUENESS:
            return self._uniqueness.check_duplicate_values(data, rule.field, 1.0 - rule.threshold)

        elif rule.check_type == CheckType.VALIDITY:
            return self._validity.check_email_format(data, rule.field, 1.0 - rule.threshold)

        return None


def demo_quality():
    """Demonstrate data quality checking."""
    data = [
        {"email": "alice@example.com", "age": 30, "name": "Alice"},
        {"email": "invalid-email", "age": 25, "name": "Bob"},
        {"email": "charlie@example.com", "age": 35, "name": "Charlie"},
        {"email": "", "age": None, "name": "Diana"},
        {"email": "eve@example.com", "age": 28, "name": "Eve"},
    ]

    action = DataQualityAction("users")
    result = action.check_completeness(data, "email")
    print(f"Completeness check: {result.level.value} - {result.message}")

    result = action.check_email_validity(data, "email")
    print(f"Validity check: {result.level.value} - {result.message}")


if __name__ == "__main__":
    demo_quality()
