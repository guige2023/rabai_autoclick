"""Data Quality Checker.

This module provides data quality validation:
- Schema validation
- Value range checking
- Pattern matching
- Custom quality rules

Example:
    >>> from actions.data_quality_action import DataQualityChecker
    >>> checker = DataQualityChecker()
    >>> checker.add_rule("email", lambda x: "@" in str(x))
    >>> result = checker.check(records)
"""

from __future__ import annotations

import re
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class QualityRule:
    """A data quality rule."""
    name: str
    column: Optional[str]
    validator: Callable[[Any], bool]
    severity: str = "error"
    description: str = ""


@dataclass
class QualityIssue:
    """A data quality issue found."""
    rule_name: str
    column: Optional[str]
    row_index: Optional[int]
    value: Any
    severity: str
    message: str


@dataclass
class QualityResult:
    """Result of a quality check."""
    passed: bool
    total_records: int
    total_issues: int
    issues_by_severity: dict[str, int]
    issues_by_column: dict[str, int]
    issues: list[QualityIssue]
    pass_rate: float


class DataQualityChecker:
    """Checks data quality against defined rules."""

    def __init__(self) -> None:
        """Initialize the data quality checker."""
        self._rules: list[QualityRule] = []
        self._lock = threading.RLock()
        self._stats: dict[str, int] = defaultdict(int)

    def add_rule(
        self,
        name: str,
        validator: Callable[[Any], bool],
        column: Optional[str] = None,
        severity: str = "error",
        description: str = "",
    ) -> None:
        """Add a quality rule.

        Args:
            name: Rule name.
            validator: Function that returns True if value passes.
            column: Column to apply rule to. None = apply to entire row.
            severity: "error", "warning", or "info".
            description: Human-readable description.
        """
        with self._lock:
            rule = QualityRule(
                name=name,
                column=column,
                validator=validator,
                severity=severity,
                description=description,
            )
            self._rules.append(rule)
            logger.info("Added quality rule: %s (severity=%s)", name, severity)

    def add_regex_rule(
        self,
        name: str,
        pattern: str,
        column: str,
        severity: str = "error",
    ) -> None:
        """Add a regex-based quality rule.

        Args:
            name: Rule name.
            pattern: Regex pattern to match.
            column: Column to validate.
            severity: Issue severity.
        """
        regex = re.compile(pattern)
        self.add_rule(
            name=name,
            validator=lambda v: bool(regex.match(str(v))) if v is not None else False,
            column=column,
            severity=severity,
            description=f"Must match pattern: {pattern}",
        )

    def add_range_rule(
        self,
        name: str,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: str = "error",
    ) -> None:
        """Add a numeric range validation rule.

        Args:
            name: Rule name.
            column: Column to validate.
            min_val: Minimum allowed value.
            max_val: Maximum allowed value.
            severity: Issue severity.
        """
        def validator(v: Any) -> bool:
            try:
                num = float(v)
                if min_val is not None and num < min_val:
                    return False
                if max_val is not None and num > max_val:
                    return False
                return True
            except (ValueError, TypeError):
                return False

        desc = f"Must be between {min_val} and {max_val}"
        self.add_rule(name=name, validator=validator, column=column, severity=severity, description=desc)

    def add_not_null_rule(
        self,
        name: str,
        column: str,
        severity: str = "error",
    ) -> None:
        """Add a not-null validation rule.

        Args:
            name: Rule name.
            column: Column to validate.
            severity: Issue severity.
        """
        self.add_rule(
            name=name,
            validator=lambda v: v is not None and str(v).strip() != "",
            column=column,
            severity=severity,
            description="Value must not be null or empty",
        )

    def check(
        self,
        records: list[dict[str, Any]],
        stop_on_error: bool = False,
        max_issues: int = 1000,
    ) -> QualityResult:
        """Check data quality against all rules.

        Args:
            records: List of record dicts.
            stop_on_error: Stop checking after first error.
            max_issues: Maximum issues to collect.

        Returns:
            QualityResult with all issues found.
        """
        issues: list[QualityIssue] = []
        issues_by_severity: dict[str, int] = defaultdict(int)
        issues_by_column: dict[str, int] = defaultdict(int)

        self._stats["records_checked"] += len(records)

        for row_idx, record in enumerate(records):
            for rule in self._rules:
                value = None
                if rule.column:
                    value = record.get(rule.column)
                else:
                    value = record

                try:
                    passed = rule.validator(value)
                except Exception as e:
                    passed = False

                if not passed:
                    issue = QualityIssue(
                        rule_name=rule.name,
                        column=rule.column,
                        row_index=row_idx,
                        value=value,
                        severity=rule.severity,
                        message=f"Rule '{rule.name}' failed: {rule.description}",
                    )
                    issues.append(issue)
                    issues_by_severity[rule.severity] += 1
                    if rule.column:
                        issues_by_column[rule.column] += 1

                    self._stats["issues_found"] += 1

                    if stop_on_error and rule.severity == "error":
                        return QualityResult(
                            passed=False,
                            total_records=len(records),
                            total_issues=len(issues),
                            issues_by_severity=dict(issues_by_severity),
                            issues_by_column=dict(issues_by_column),
                            issues=issues,
                            pass_rate=0.0,
                        )

                    if len(issues) >= max_issues:
                        break

            if len(issues) >= max_issues:
                break

        total_issues = len(issues)
        pass_rate = max(0.0, 1.0 - (total_issues / len(records))) if records else 1.0

        return QualityResult(
            passed=total_issues == 0,
            total_records=len(records),
            total_issues=total_issues,
            issues_by_severity=dict(issues_by_severity),
            issues_by_column=dict(issues_by_column),
            issues=issues,
            pass_rate=round(pass_rate, 4),
        )

    def list_rules(self) -> list[QualityRule]:
        """List all registered rules."""
        with self._lock:
            return list(self._rules)

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name.

        Args:
            name: Rule name to remove.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            for i, rule in enumerate(self._rules):
                if rule.name == name:
                    self._rules.pop(i)
                    logger.info("Removed quality rule: %s", name)
                    return True
            return False

    def get_stats(self) -> dict[str, int]:
        """Get quality check statistics."""
        with self._lock:
            return dict(self._stats)
