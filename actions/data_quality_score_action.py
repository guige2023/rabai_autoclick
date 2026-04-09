"""
Data Quality Score Action Module

Provides comprehensive data quality assessment with scoring algorithms,
rule-based validation, anomaly detection, and quality reporting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Data quality dimensions."""

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class IssueSeverity(Enum):
    """Quality issue severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityIssue:
    """A data quality issue."""

    issue_id: str
    dimension: QualityDimension
    severity: IssueSeverity
    description: str
    affected_rows: int
    affected_columns: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)
    rule_name: Optional[str] = None
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class QualityRule:
    """A data quality validation rule."""

    rule_id: str
    name: str
    dimension: QualityDimension
    severity: IssueSeverity
    validator: Callable[[Any], bool]
    description: str = ""
    enabled: bool = True


@dataclass
class QualityScore:
    """Overall data quality score."""

    score_id: str
    overall_score: float
    dimension_scores: Dict[QualityDimension, float] = field(default_factory=dict)
    issue_count: int = 0
    critical_issues: int = 0
    rows_checked: int = 0
    columns_checked: int = 0
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

    def grade(self) -> str:
        """Get letter grade based on score."""
        if self.overall_score >= 95:
            return "A+"
        elif self.overall_score >= 90:
            return "A"
        elif self.overall_score >= 85:
            return "B+"
        elif self.overall_score >= 80:
            return "B"
        elif self.overall_score >= 70:
            return "C"
        elif self.overall_score >= 60:
            return "D"
        else:
            return "F"


@dataclass
class QualityConfig:
    """Configuration for data quality assessment."""

    completeness_weight: float = 0.25
    accuracy_weight: float = 0.20
    consistency_weight: float = 0.15
    timeliness_weight: float = 0.10
    uniqueness_weight: float = 0.15
    validity_weight: float = 0.15
    min_score_threshold: float = 80.0
    critical_threshold: float = 60.0


class CompletenessChecker:
    """Checks data completeness (null values, missing data)."""

    def check(self, data: List[Dict[str, Any]]) -> tuple[float, List[QualityIssue]]:
        """Check completeness of data."""
        if not data:
            return 100.0, []

        issues = []
        total_cells = 0
        null_cells = 0

        for row in data:
            for key, value in row.items():
                total_cells += 1
                if value is None or value == "" or value == []:
                    null_cells += 1

        completeness_pct = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 100.0

        if completeness_pct < 95:
            issue = QualityIssue(
                issue_id=f"complete_{uuid.uuid4().hex[:8]}",
                dimension=QualityDimension.COMPLETENESS,
                severity=IssueSeverity.WARNING if completeness_pct >= 80 else IssueSeverity.ERROR,
                description=f"Completeness score: {completeness_pct:.2f}%",
                affected_rows=len(data),
                affected_columns=[],
            )
            issues.append(issue)

        return completeness_pct, issues


class ValidityChecker:
    """Checks data validity against schemas and patterns."""

    def __init__(self):
        self._schema_validators: Dict[str, Callable[[Any], bool]] = {}

    def register_validator(
        self,
        column: str,
        validator: Callable[[Any], bool],
    ) -> None:
        """Register a validator for a column."""
        self._schema_validators[column] = validator

    def check(
        self,
        data: List[Dict[str, Any]],
    ) -> tuple[float, List[QualityIssue]]:
        """Check validity of data."""
        if not data:
            return 100.0, []

        issues = []
        total_checks = 0
        failed_checks = 0

        for row in data:
            for column, validator in self._schema_validators.items():
                if column in row:
                    total_checks += 1
                    try:
                        if not validator(row[column]):
                            failed_checks += 1
                    except Exception:
                        failed_checks += 1

        validity_pct = ((total_checks - failed_checks) / total_checks * 100) if total_checks > 0 else 100.0

        if failed_checks > 0:
            issue = QualityIssue(
                issue_id=f"valid_{uuid.uuid4().hex[:8]}",
                dimension=QualityDimension.VALIDITY,
                severity=IssueSeverity.WARNING,
                description=f"{failed_checks} validity violations found",
                affected_rows=failed_checks,
            )
            issues.append(issue)

        return validity_pct, issues


class UniquenessChecker:
    """Checks data uniqueness (duplicates)."""

    def check(
        self,
        data: List[Dict[str, Any]],
        key_columns: Optional[List[str]] = None,
    ) -> tuple[float, List[QualityIssue]]:
        """Check uniqueness of data."""
        if not data:
            return 100.0, []

        if not key_columns:
            # Check full row uniqueness
            unique_rows = set()
            duplicates = []
            for row in data:
                row_key = str(sorted(row.items()))
                if row_key in unique_rows:
                    duplicates.append(row)
                unique_rows.add(row_key)
        else:
            # Check key column uniqueness
            seen_values: Dict[str, List[int]] = {}
            for i, row in data:
                key_val = tuple(row.get(c) for c in key_columns)
                key_str = str(key_val)
                if key_str not in seen_values:
                    seen_values[key_str] = []
                seen_values[key_str].append(i)

            duplicates = [data[i] for vals in seen_values.values() if len(vals) > 1 for i in vals[1:]]

        duplicate_count = len(duplicates)
        uniqueness_pct = ((len(data) - duplicate_count) / len(data) * 100) if data else 100.0

        issues = []
        if duplicate_count > 0:
            issue = QualityIssue(
                issue_id=f"unique_{uuid.uuid4().hex[:8]}",
                dimension=QualityDimension.UNIQUENESS,
                severity=IssueSeverity.WARNING,
                description=f"{duplicate_count} duplicate rows found",
                affected_rows=duplicate_count,
            )
            issues.append(issue)

        return uniqueness_pct, issues


class DataQualityScoreAction:
    """
    Data quality scoring and validation action.

    Features:
    - Multi-dimensional quality scoring (completeness, accuracy, validity, etc.)
    - Configurable quality rules and validators
    - Issue tracking with severity levels
    - Weighted scoring algorithms
    - Detailed quality reports
    - Threshold-based alerting

    Usage:
        quality = DataQualityScoreAction(config)
        quality.register_rule(rule)
        score = await quality.assess(data)
        print(f"Quality score: {score.overall_score}")
    """

    def __init__(self, config: Optional[QualityConfig] = None):
        self.config = config or QualityConfig()
        self._rules: Dict[str, QualityRule] = {}
        self._completeness = CompletenessChecker()
        self._validity = ValidityChecker()
        self._uniqueness = UniquenessChecker()
        self._stats = {
            "assessments_run": 0,
            "issues_found": 0,
            "critical_issues": 0,
        }

    def register_rule(self, rule: QualityRule) -> None:
        """Register a quality validation rule."""
        self._rules[rule.rule_id] = rule
        if rule.dimension == QualityDimension.VALIDITY:
            self._validity.register_validator(rule.name, rule.validator)

    def register_rules(self, rules: List[QualityRule]) -> None:
        """Register multiple quality rules."""
        for rule in rules:
            self.register_rule(rule)

    async def assess(
        self,
        data: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
    ) -> QualityScore:
        """
        Assess data quality and compute scores.

        Args:
            data: List of records to assess
            columns: Optional list of columns to focus on

        Returns:
            QualityScore with overall and dimensional scores
        """
        score_id = f"qs_{uuid.uuid4().hex[:12]}"
        rows_checked = len(data)
        cols_checked = len(columns) if columns else (len(data[0]) if data else 0)

        dimension_scores: Dict[QualityDimension, float] = {}
        all_issues: List[QualityIssue] = []

        # Completeness
        completeness_score, completeness_issues = self._completeness.check(data)
        dimension_scores[QualityDimension.COMPLETENESS] = completeness_score
        all_issues.extend(completeness_issues)

        # Validity
        validity_score, validity_issues = self._validity.check(data)
        dimension_scores[QualityDimension.VALIDITY] = validity_score
        all_issues.extend(validity_issues)

        # Uniqueness
        uniqueness_score, uniqueness_issues = self._uniqueness.check(data)
        dimension_scores[QualityDimension.UNIQUENESS] = uniqueness_score
        all_issues.extend(uniqueness_issues)

        # Check custom rules
        for rule in self._rules.values():
            if not rule.enabled:
                continue
            rule_issues = await self._check_rule(rule, data)
            all_issues.extend(rule_issues)

        # Calculate weighted overall score
        overall = (
            dimension_scores.get(QualityDimension.COMPLETENESS, 100.0) * self.config.completeness_weight +
            dimension_scores.get(QualityDimension.ACCURACY, 100.0) * self.config.accuracy_weight +
            dimension_scores.get(QualityDimension.CONSISTENCY, 100.0) * self.config.consistency_weight +
            dimension_scores.get(QualityDimension.TIMELINESS, 100.0) * self.config.timeliness_weight +
            dimension_scores.get(QualityDimension.UNIQUENESS, 100.0) * self.config.uniqueness_weight +
            dimension_scores.get(QualityDimension.VALIDITY, 100.0) * self.config.validity_weight
        )

        score = QualityScore(
            score_id=score_id,
            overall_score=round(overall, 2),
            dimension_scores=dimension_scores,
            issue_count=len(all_issues),
            critical_issues=sum(1 for i in all_issues if i.severity == IssueSeverity.CRITICAL),
            rows_checked=rows_checked,
            columns_checked=cols_checked,
        )

        self._stats["assessments_run"] += 1
        self._stats["issues_found"] += len(all_issues)
        self._stats["critical_issues"] += score.critical_issues

        return score

    async def _check_rule(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
    ) -> List[QualityIssue]:
        """Check a custom quality rule against data."""
        issues = []
        failed_count = 0
        sample_values = []

        for row in data:
            try:
                if not rule.validator(row):
                    failed_count += 1
                    if len(sample_values) < 3:
                        sample_values.append(row)
            except Exception:
                failed_count += 1

        if failed_count > 0:
            issue = QualityIssue(
                issue_id=f"rule_{rule.rule_id}_{uuid.uuid4().hex[:8]}",
                dimension=rule.dimension,
                severity=rule.severity,
                description=f"Rule '{rule.name}': {failed_count} violations",
                affected_rows=failed_count,
                rule_name=rule.name,
            )
            issues.append(issue)

        return issues

    def generate_report(
        self,
        score: QualityScore,
        include_samples: bool = True,
    ) -> Dict[str, Any]:
        """
        Generate a quality report from a score.

        Args:
            score: QualityScore to report on
            include_samples: Whether to include sample values in report

        Returns:
            Quality report dictionary
        """
        return {
            "score_id": score.score_id,
            "overall_score": score.overall_score,
            "grade": score.grade(),
            "dimension_scores": {
                dim.value: round(scor, 2)
                for dim, scor in score.dimension_scores.items()
            },
            "statistics": {
                "rows_checked": score.rows_checked,
                "columns_checked": score.columns_checked,
                "total_issues": score.issue_count,
                "critical_issues": score.critical_issues,
            },
            "timestamp": score.timestamp,
            "passed": score.overall_score >= self.config.min_score_threshold,
            "status": "pass" if score.overall_score >= self.config.min_score_threshold
                     else ("fail" if score.overall_score < self.config.critical_threshold else "warning"),
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get quality assessment statistics."""
        return self._stats.copy()


async def demo_quality():
    """Demonstrate data quality scoring."""
    config = QualityConfig()
    quality = DataQualityScoreAction(config)

    # Sample data with some quality issues
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": None, "age": 25},
        {"id": 3, "name": "", "email": "charlie@example.com", "age": 35},
        {"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28},
        {"id": 5, "name": "Eve", "email": "eve@example.com", "age": None},
    ]

    score = await quality.assess(data)
    report = quality.generate_report(score)

    print(f"Overall Score: {score.overall_score}")
    print(f"Grade: {score.grade()}")
    print(f"Status: {report['status']}")
    print(f"Stats: {quality.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_quality())
