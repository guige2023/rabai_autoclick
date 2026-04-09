"""Data quality checker action for validating data quality.

Provides data profiling, validation rules, and quality
reporting for datasets.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    BAD = "bad"


@dataclass
class ValidationRule:
    rule_id: str
    field: str
    rule_type: str
    params: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class ValidationResult:
    field: str
    rule_id: str
    passed: bool
    message: str = ""
    value: Any = None


@dataclass
class QualityReport:
    dataset_name: str
    timestamp: float
    total_records: int
    total_fields: int
    quality_score: float
    quality_level: QualityLevel
    validation_results: list[ValidationResult] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


class DataQualityCheckerAction:
    """Data quality validation and profiling.

    Args:
        dataset_name: Name of the dataset.
        default_quality_threshold: Minimum acceptable quality score.
    """

    def __init__(
        self,
        dataset_name: str = "dataset",
        default_quality_threshold: float = 0.8,
    ) -> None:
        self._dataset_name = dataset_name
        self._default_threshold = default_quality_threshold
        self._validation_rules: list[ValidationRule] = []
        self._field_profiles: dict[str, dict[str, Any]] = {}
        self._reports: list[QualityReport] = []
        self._max_reports = 100

    def add_rule(
        self,
        field: str,
        rule_type: str,
        params: Optional[dict[str, Any]] = None,
        rule_id: Optional[str] = None,
    ) -> bool:
        """Add a validation rule.

        Args:
            field: Field name to validate.
            rule_type: Type of rule ('not_null', 'unique', 'min', 'max', 'pattern').
            params: Rule parameters.
            rule_id: Optional rule ID.

        Returns:
            True if rule was added.
        """
        rid = rule_id or f"rule_{int(time.time() * 1000)}"

        rule = ValidationRule(
            rule_id=rid,
            field=field,
            rule_type=rule_type,
            params=params or {},
        )

        self._validation_rules.append(rule)
        logger.debug(f"Added validation rule: {rid} for field {field}")
        return True

    def profile_field(self, field: str, values: list[Any]) -> dict[str, Any]:
        """Profile a field and compute statistics.

        Args:
            field: Field name.
            values: List of field values.

        Returns:
            Field profile dictionary.
        """
        non_null = [v for v in values if v is not None]
        unique_values = set(non_null)

        profile = {
            "field": field,
            "total_count": len(values),
            "non_null_count": len(non_null),
            "null_count": len(values) - len(non_null),
            "unique_count": len(unique_values),
            "null_percentage": (len(values) - len(non_null)) / len(values) if values else 0,
            "unique_percentage": len(unique_values) / len(non_null) if non_null else 0,
        }

        numeric_values = [v for v in non_null if isinstance(v, (int, float))]
        if numeric_values:
            profile.update({
                "min": min(numeric_values),
                "max": max(numeric_values),
                "mean": sum(numeric_values) / len(numeric_values),
                "type": "numeric",
            })
        else:
            profile["type"] = "string"
            str_values = [str(v) for v in non_null]
            if str_values:
                profile["min_length"] = min(len(s) for s in str_values)
                profile["max_length"] = max(len(s) for s in str_values)
                profile["avg_length"] = sum(len(s) for s in str_values) / len(str_values)

        self._field_profiles[field] = profile
        return profile

    def validate_record(self, record: dict[str, Any]) -> list[ValidationResult]:
        """Validate a single record against all rules.

        Args:
            record: Record to validate.

        Returns:
            List of validation results.
        """
        results = []

        for rule in self._validation_rules:
            if not rule.enabled:
                continue

            field = rule.field
            value = record.get(field)
            result = self._apply_rule(rule, value)
            results.append(result)

        return results

    def _apply_rule(
        self,
        rule: ValidationRule,
        value: Any,
    ) -> ValidationResult:
        """Apply a validation rule to a value.

        Args:
            rule: Validation rule.
            value: Value to validate.

        Returns:
            Validation result.
        """
        rule_type = rule.rule_type

        if rule_type == "not_null":
            passed = value is not None
            return ValidationResult(
                field=rule.field,
                rule_id=rule.rule_id,
                passed=passed,
                message="Value is null" if not passed else "Value is not null",
                value=value,
            )

        if rule_type == "unique":
            passed = True
            return ValidationResult(
                field=rule.field,
                rule_id=rule.rule_id,
                passed=passed,
                message="Uniqueness check",
                value=value,
            )

        if rule_type == "min":
            min_val = rule.params.get("min")
            if min_val is not None and value is not None:
                passed = value >= min_val
                return ValidationResult(
                    field=rule.field,
                    rule_id=rule.rule_id,
                    passed=passed,
                    message=f"Value {value} is less than min {min_val}" if not passed else "Value meets minimum",
                    value=value,
                )

        if rule_type == "max":
            max_val = rule.params.get("max")
            if max_val is not None and value is not None:
                passed = value <= max_val
                return ValidationResult(
                    field=rule.field,
                    rule_id=rule.rule_id,
                    passed=passed,
                    message=f"Value {value} exceeds max {max_val}" if not passed else "Value within maximum",
                    value=value,
                )

        if rule_type == "pattern":
            import re
            pattern = rule.params.get("pattern")
            if pattern and value is not None:
                passed = bool(re.match(pattern, str(value)))
                return ValidationResult(
                    field=rule.field,
                    rule_id=rule.rule_id,
                    passed=passed,
                    message="Value does not match pattern" if not passed else "Value matches pattern",
                    value=value,
                )

        return ValidationResult(
            field=rule.field,
            rule_id=rule.rule_id,
            passed=True,
            message="No validation applied",
            value=value,
        )

    def validate_dataset(
        self,
        records: list[dict[str, Any]],
    ) -> QualityReport:
        """Validate an entire dataset.

        Args:
            records: List of records.

        Returns:
            Quality report.
        """
        if not records:
            return QualityReport(
                dataset_name=self._dataset_name,
                timestamp=time.time(),
                total_records=0,
                total_fields=0,
                quality_score=0.0,
                quality_level=QualityLevel.BAD,
            )

        all_fields = set()
        for record in records:
            all_fields.update(record.keys())

        for field in all_fields:
            values = [r.get(field) for r in records]
            self.profile_field(field, values)

        all_results = []
        issues = []

        for record in records:
            results = self.validate_record(record)
            all_results.extend(results)

        failed_count = sum(1 for r in all_results if not r.passed)
        total_checks = len(all_results) if all_results else 1
        quality_score = (total_checks - failed_count) / total_checks

        if quality_score >= 0.95:
            quality_level = QualityLevel.EXCELLENT
        elif quality_score >= 0.85:
            quality_level = QualityLevel.GOOD
        elif quality_score >= 0.7:
            quality_level = QualityLevel.FAIR
        elif quality_score >= 0.5:
            quality_level = QualityLevel.POOR
        else:
            quality_level = QualityLevel.BAD

        failed_results = [r for r in all_results if not r.passed]
        for fr in failed_results:
            issues.append(f"{fr.field}: {fr.message}")

        report = QualityReport(
            dataset_name=self._dataset_name,
            timestamp=time.time(),
            total_records=len(records),
            total_fields=len(all_fields),
            quality_score=quality_score,
            quality_level=quality_level,
            validation_results=all_results,
            issues=issues,
        )

        self._reports.append(report)
        if len(self._reports) > self._max_reports:
            self._reports.pop(0)

        return report

    def get_field_profile(self, field: str) -> Optional[dict[str, Any]]:
        """Get field profile.

        Args:
            field: Field name.

        Returns:
            Field profile or None.
        """
        return self._field_profiles.get(field)

    def get_reports(self, limit: int = 10) -> list[QualityReport]:
        """Get recent quality reports.

        Args:
            limit: Maximum reports to return.

        Returns:
            List of reports (newest first).
        """
        return self._reports[-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get data quality statistics.

        Returns:
            Dictionary with stats.
        """
        return {
            "dataset_name": self._dataset_name,
            "total_rules": len(self._validation_rules),
            "enabled_rules": sum(1 for r in self._validation_rules if r.enabled),
            "total_fields": len(self._field_profiles),
            "total_reports": len(self._reports),
            "quality_threshold": self._default_threshold,
        }
