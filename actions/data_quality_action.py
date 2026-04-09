"""Data Quality Action with profiling, validation, and anomaly detection.

This module provides comprehensive data quality management:
- Data profiling (statistics, distributions, patterns)
- Rule-based validation
- Anomaly detection
- Quality scoring
- Data cleansing suggestions
"""

from __future__ import annotations

import logging
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class QualityLevel(Enum):
    """Data quality levels."""

    EXCELLENT = "excellent"  # 95-100%
    GOOD = "good"  # 85-95%
    FAIR = "fair"  # 70-85%
    POOR = "poor"  # 50-70%
    BAD = "bad"  # <50%


@dataclass
class FieldProfile:
    """Profile for a single field."""

    field_name: str
    field_type: str = "unknown"
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    empty_count: int = 0

    # Numeric stats
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_mean: float | None = None
    numeric_median: float | None = None
    numeric_std_dev: float | None = None

    # String stats
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None
    empty_string_count: int = 0

    # Value distributions
    top_values: list[tuple[Any, int]] = field(default_factory=list)
    value_frequencies: dict[str, int] = field(default_factory=dict)

    # Pattern analysis
    patterns: dict[str, int] = field(default_factory=dict)

    # Compliance
    null_ratio: float = 0.0
    completeness: float = 0.0


@dataclass
class ValidationRule:
    """A data validation rule."""

    name: str
    field: str
    rule_type: str  # "not_null", "unique", "range", "pattern", "custom"
    params: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    severity: str = "error"  # "error", "warning", "info"
    validator: Callable[[Any], bool] | None = None


@dataclass
class ValidationResult:
    """Result of validating a record against rules."""

    passed: bool
    rule_name: str
    field: str
    value: Any
    error_message: str = ""
    severity: str = "error"


@dataclass
class QualityReport:
    """Data quality report."""

    total_records: int = 0
    total_fields: int = 0
    overall_score: float = 0.0
    quality_level: QualityLevel = QualityLevel.GOOD
    field_profiles: dict[str, FieldProfile] = field(default_factory=dict)
    validation_results: list[ValidationResult] = field(default_factory=list)
    failed_rules_count: int = 0
    passed_rules_count: int = 0
    anomaly_records: list[dict] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class DataQualityAction:
    """Data quality profiling, validation, and anomaly detection."""

    def __init__(
        self,
        rules: list[ValidationRule] | None = None,
        quality_thresholds: dict[str, float] | None = None,
    ):
        """Initialize data quality action.

        Args:
            rules: Optional validation rules
            quality_thresholds: Quality score thresholds per level
        """
        self.rules = rules or []
        self.quality_thresholds = quality_thresholds or {
            "excellent": 95.0,
            "good": 85.0,
            "fair": 70.0,
            "poor": 50.0,
        }

    def add_rule(self, rule: ValidationRule) -> "DataQualityAction":
        """Add a validation rule.

        Args:
            rule: Validation rule to add

        Returns:
            Self for chaining
        """
        self.rules.append(rule)
        return self

    def add_not_null_rule(
        self,
        field: str,
        severity: str = "error",
    ) -> "DataQualityAction":
        """Add a not-null validation rule.

        Args:
            field: Field name
            severity: Rule severity

        Returns:
            Self for chaining
        """
        rule = ValidationRule(
            name=f"{field}_not_null",
            field=field,
            rule_type="not_null",
            error_message=f"Field {field} cannot be null",
            severity=severity,
        )
        return self.add_rule(rule)

    def add_unique_rule(
        self,
        field: str,
        severity: str = "error",
    ) -> "DataQualityAction":
        """Add a uniqueness validation rule.

        Args:
            field: Field name
            severity: Rule severity

        Returns:
            Self for chaining
        """
        rule = ValidationRule(
            name=f"{field}_unique",
            field=field,
            rule_type="unique",
            error_message=f"Field {field} must be unique",
            severity=severity,
        )
        return self.add_rule(rule)

    def add_range_rule(
        self,
        field: str,
        min_value: float | None = None,
        max_value: float | None = None,
        severity: str = "error",
    ) -> "DataQualityAction":
        """Add a range validation rule.

        Args:
            field: Field name
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            severity: Rule severity

        Returns:
            Self for chaining
        """
        rule = ValidationRule(
            name=f"{field}_range",
            field=field,
            rule_type="range",
            params={"min": min_value, "max": max_value},
            error_message=f"Field {field} must be between {min_value} and {max_value}",
            severity=severity,
        )
        return self.add_rule(rule)

    def add_pattern_rule(
        self,
        field: str,
        pattern: str,
        severity: str = "error",
    ) -> "DataQualityAction":
        """Add a pattern validation rule.

        Args:
            field: Field name
            pattern: Regex pattern
            severity: Rule severity

        Returns:
            Self for chaining
        """
        rule = ValidationRule(
            name=f"{field}_pattern",
            field=field,
            rule_type="pattern",
            params={"pattern": pattern},
            error_message=f"Field {field} does not match required pattern",
            severity=severity,
        )
        return self.add_rule(rule)

    def profile(self, data: list[dict]) -> dict[str, FieldProfile]:
        """Profile data and compute statistics.

        Args:
            data: List of records to profile

        Returns:
            Dictionary of field profiles
        """
        if not data:
            return {}

        profiles: dict[str, FieldProfile] = {}
        fields = list(data[0].keys())

        for field_name in fields:
            profile = self._profile_field(field_name, data)
            profiles[field_name] = profile

        return profiles

    def _profile_field(self, field_name: str, data: list[dict]) -> FieldProfile:
        """Profile a single field.

        Args:
            field_name: Name of field to profile
            data: All records

        Returns:
            FieldProfile with statistics
        """
        profile = FieldProfile(field_name=field_name)
        values = [record.get(field_name) for record in data]

        profile.total_count = len(values)
        profile.null_count = sum(1 for v in values if v is None)
        profile.empty_string_count = sum(1 for v in values if v == "")
        profile.empty_count = profile.null_count + profile.empty_string_count

        profile.null_ratio = profile.null_count / profile.total_count if profile.total_count > 0 else 0
        profile.completeness = 1.0 - profile.null_ratio

        # Non-null values for further analysis
        non_null = [v for v in values if v is not None]
        non_empty = [v for v in non_null if v != ""]

        if non_empty:
            profile.unique_count = len(set(str(v) for v in non_empty))

            # Determine field type
            sample = non_empty[0]
            if isinstance(sample, (int, float)):
                profile.field_type = "numeric"
                self._profile_numeric(profile, non_empty)
            else:
                profile.field_type = "string"
                self._profile_string(profile, non_empty)

            # Value frequencies
            freq = Counter(str(v) for v in non_empty)
            profile.top_values = freq.most_common(10)
            profile.value_frequencies = dict(freq)

        return profile

    def _profile_numeric(self, profile: FieldProfile, values: list) -> None:
        """Profile numeric field."""
        numeric_values = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except (ValueError, TypeError):
                pass

        if numeric_values:
            profile.numeric_min = min(numeric_values)
            profile.numeric_max = max(numeric_values)
            profile.numeric_mean = statistics.mean(numeric_values)
            profile.numeric_median = statistics.median(numeric_values)

            if len(numeric_values) > 1:
                profile.numeric_std_dev = statistics.stdev(numeric_values)

    def _profile_string(self, profile: FieldProfile, values: list) -> None:
        """Profile string field."""
        lengths = [len(str(v)) for v in values]
        if lengths:
            profile.min_length = min(lengths)
            profile.max_length = max(lengths)
            profile.avg_length = statistics.mean(lengths)

        # Pattern detection
        patterns: dict[str, int] = defaultdict(int)
        for v in values:
            s = str(v)
            if re.match(r"^\d+$", s):
                patterns["numeric"] += 1
            elif re.match(r"^[a-zA-Z]+$", s):
                patterns["alphabetic"] += 1
            elif re.match(r"^[a-zA-Z0-9]+$", s):
                patterns["alphanumeric"] += 1
            elif re.match(r"^[\w.-]+@[\w.-]+\.\w+$", s):
                patterns["email"] += 1
            elif re.match(r"^\d{4}-\d{2}-\d{2}$", s):
                patterns["date_iso"] += 1
            else:
                patterns["other"] += 1

        profile.patterns = dict(patterns)

    def validate_record(self, record: dict) -> list[ValidationResult]:
        """Validate a single record against all rules.

        Args:
            record: Record to validate

        Returns:
            List of validation results
        """
        results = []

        for rule in self.rules:
            result = self._validate_rule(rule, record)
            if result:
                results.append(result)

        return results

    def _validate_rule(self, rule: ValidationRule, record: dict) -> ValidationResult | None:
        """Validate a record against a single rule.

        Args:
            rule: Validation rule
            record: Record to validate

        Returns:
            ValidationResult if rule failed, None if passed
        """
        value = record.get(rule.field)

        # Custom validator
        if rule.validator:
            try:
                passed = rule.validator(value)
            except Exception:
                passed = False

            if not passed:
                return ValidationResult(
                    passed=False,
                    rule_name=rule.name,
                    field=rule.field,
                    value=value,
                    error_message=rule.error_message or f"Custom validation failed for {rule.field}",
                    severity=rule.severity,
                )
            return None

        # Built-in validators
        if rule.rule_type == "not_null":
            if value is None or value == "":
                return ValidationResult(
                    passed=False,
                    rule_name=rule.name,
                    field=rule.field,
                    value=value,
                    error_message=rule.error_message,
                    severity=rule.severity,
                )

        elif rule.rule_type == "unique":
            # Note: Uniqueness requires checking against all records
            pass

        elif rule.rule_type == "range":
            try:
                num_value = float(value)
                min_val = rule.params.get("min")
                max_val = rule.params.get("max")

                if min_val is not None and num_value < min_val:
                    return ValidationResult(
                        passed=False,
                        rule_name=rule.name,
                        field=rule.field,
                        value=value,
                        error_message=rule.error_message,
                        severity=rule.severity,
                    )

                if max_val is not None and num_value > max_val:
                    return ValidationResult(
                        passed=False,
                        rule_name=rule.name,
                        field=rule.field,
                        value=value,
                        error_message=rule.error_message,
                        severity=rule.severity,
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    passed=False,
                    rule_name=rule.name,
                    field=rule.field,
                    value=value,
                    error_message=rule.error_message,
                    severity=rule.severity,
                )

        elif rule.rule_type == "pattern":
            pattern = rule.params.get("pattern", "")
            if value is not None and not re.match(pattern, str(value)):
                return ValidationResult(
                    passed=False,
                    rule_name=rule.name,
                    field=rule.field,
                    value=value,
                    error_message=rule.error_message,
                    severity=rule.severity,
                )

        return None

    def detect_anomalies(
        self,
        data: list[dict],
        threshold: float = 3.0,
    ) -> list[dict]:
        """Detect anomalous records using statistical methods.

        Args:
            data: List of records
            threshold: Z-score threshold for anomaly detection

        Returns:
            List of anomalous records with anomaly scores
        """
        anomalies = []

        if not data:
            return anomalies

        fields = list(data[0].keys())

        for field_name in fields:
            field_anomalies = self._detect_field_anomalies(data, field_name, threshold)
            anomalies.extend(field_anomalies)

        # Deduplicate by record
        seen_records = set()
        unique_anomalies = []

        for anomaly in anomalies:
            record_idx = anomaly.get("record_index")
            if record_idx not in seen_records:
                seen_records.add(record_idx)
                unique_anomalies.append(anomaly)

        return unique_anomalies

    def _detect_field_anomalies(
        self,
        data: list[dict],
        field_name: str,
        threshold: float,
    ) -> list[dict]:
        """Detect anomalies in a single field."""
        anomalies = []

        values = []
        for record in data:
            try:
                values.append(float(record.get(field_name, 0)))
            except (ValueError, TypeError):
                continue

        if len(values) < 3:
            return anomalies

        mean = statistics.mean(values)
        std_dev = statistics.stdev(values)

        if std_dev == 0:
            return anomalies

        for i, record in enumerate(data):
            try:
                value = float(record.get(field_name, 0))
                z_score = abs((value - mean) / std_dev)

                if z_score > threshold:
                    anomalies.append({
                        "record_index": i,
                        "record": record,
                        "field": field_name,
                        "value": value,
                        "z_score": z_score,
                        "expected_range": (mean - threshold * std_dev, mean + threshold * std_dev),
                    })
            except (ValueError, TypeError):
                continue

        return anomalies

    def compute_quality_score(
        self,
        profiles: dict[str, FieldProfile],
        validation_results: list[ValidationResult],
    ) -> float:
        """Compute overall data quality score.

        Args:
            profiles: Field profiles
            validation_results: All validation results

        Returns:
            Quality score (0-100)
        """
        if not profiles:
            return 0.0

        scores = []

        # Completeness score
        completeness_scores = [p.completeness * 100 for p in profiles.values()]
        scores.append(statistics.mean(completeness_scores))

        # Validity score (based on validation failures)
        total_rules = len(validation_results)
        failed_rules = sum(1 for r in validation_results if not r.passed)
        validity_score = ((total_rules - failed_rules) / total_rules * 100) if total_rules > 0 else 100
        scores.append(validity_score)

        # Consistency score (based on unique counts)
        consistency_scores = []
        for profile in profiles.values():
            if profile.total_count > 0:
                uniqueness_ratio = profile.unique_count / profile.total_count
                consistency_scores.append(uniqueness_ratio * 100)
        if consistency_scores:
            scores.append(statistics.mean(consistency_scores))

        return statistics.mean(scores) if scores else 0.0

    def assess_quality_level(self, score: float) -> QualityLevel:
        """Assess quality level from score.

        Args:
            score: Quality score

        Returns:
            QualityLevel enum
        """
        if score >= self.quality_thresholds["excellent"]:
            return QualityLevel.EXCELLENT
        elif score >= self.quality_thresholds["good"]:
            return QualityLevel.GOOD
        elif score >= self.quality_thresholds["fair"]:
            return QualityLevel.FAIR
        elif score >= self.quality_thresholds["poor"]:
            return QualityLevel.POOR
        return QualityLevel.BAD

    def generate_report(
        self,
        data: list[dict],
        detect_anomalies_flag: bool = True,
    ) -> QualityReport:
        """Generate comprehensive data quality report.

        Args:
            data: List of records
            detect_anomalies_flag: Whether to detect anomalies

        Returns:
            QualityReport with full analysis
        """
        report = QualityReport(
            total_records=len(data),
            total_fields=len(data[0].keys()) if data else 0,
        )

        if not data:
            return report

        # Profile all fields
        report.field_profiles = self.profile(data)

        # Validate all records
        for i, record in enumerate(data):
            results = self.validate_record(record)
            for result in results:
                result_dict = {
                    "record_index": i,
                    "record": record,
                    "passed": result.passed,
                    "rule_name": result.rule_name,
                    "field": result.field,
                    "value": result.value,
                    "error_message": result.error_message,
                    "severity": result.severity,
                }
                report.validation_results.append(result_dict)

                if not result.passed:
                    report.failed_rules_count += 1
                    if result.severity == "error":
                        report.anomaly_records.append(result_dict)

        # Compute quality score
        report.overall_score = self.compute_quality_score(
            report.field_profiles,
            report.validation_results,
        )
        report.quality_level = self.assess_quality_level(report.overall_score)
        report.passed_rules_count = len(self.rules) * len(data) - report.failed_rules_count

        # Detect anomalies
        if detect_anomalies_flag:
            report.anomaly_records.extend(self.detect_anomalies(data))

        # Generate suggestions
        report.suggestions = self._generate_suggestions(report)

        return report

    def _generate_suggestions(self, report: QualityReport) -> list[str]:
        """Generate data quality improvement suggestions.

        Args:
            report: Quality report

        Returns:
            List of suggestions
        """
        suggestions = []

        for field_name, profile in report.field_profiles.items():
            if profile.null_ratio > 0.1:
                suggestions.append(
                    f"Field '{field_name}' has {profile.null_ratio:.1%} null values. "
                    "Consider imputing missing values or marking as optional."
                )

            if profile.field_type == "numeric" and profile.numeric_std_dev:
                if profile.numeric_std_dev > (profile.numeric_max - profile.numeric_min) * 0.5:
                    suggestions.append(
                        f"Field '{field_name}' has high variance. Check for data entry errors."
                    )

        if report.overall_score < 70:
            suggestions.append(
                "Overall data quality is poor. Review validation rules and data sources."
            )

        if report.anomaly_records:
            suggestions.append(
                f"Found {len(report.anomaly_records)} anomalous records. "
                "Consider investigating or excluding outliers."
            )

        return suggestions


def create_quality_checker(
    rules: list[ValidationRule] | None = None,
) -> DataQualityAction:
    """Create a configured data quality checker.

    Args:
        rules: Optional validation rules

    Returns:
        Configured DataQualityAction instance
    """
    return DataQualityAction(rules=rules)
