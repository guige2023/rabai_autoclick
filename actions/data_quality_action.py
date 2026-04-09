"""
Data Quality Checker Action Module

Provides data quality validation and profiling for UI automation workflows.
Supports schema validation, null checks, uniqueness, and statistical analysis.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """Data quality levels."""
    EXCELLENT = auto()
    GOOD = auto()
    FAIR = auto()
    POOR = auto()
    FAIL = auto()


class ValidationType(Enum):
    """Validation types."""
    SCHEMA = auto()
    NULL = auto()
    UNIQUE = auto()
    RANGE = auto()
    PATTERN = auto()
    TYPE = auto()
    CUSTOM = auto()


@dataclass
class ValidationError:
    """Single validation error."""
    field: str
    validation_type: ValidationType
    message: str
    value: Any = None
    row_index: Optional[int] = None


@dataclass
class QualityMetrics:
    """Data quality metrics."""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    null_count: int = 0
    unique_count: int = 0
    duplicate_count: int = 0
    completeness: float = 0.0
    accuracy: float = 0.0
    consistency: float = 0.0
    timeliness: float = 0.0
    validity: float = 0.0

    @property
    def quality_score(self) -> float:
        """Calculate overall quality score."""
        weights = {
            "completeness": 0.25,
            "accuracy": 0.25,
            "consistency": 0.20,
            "timeliness": 0.10,
            "validity": 0.20,
        }
        score = sum(getattr(self, k) * v for k, v in weights.items())
        return min(1.0, max(0.0, score))

    @property
    def quality_level(self) -> QualityLevel:
        """Get quality level."""
        score = self.quality_score
        if score >= 0.95:
            return QualityLevel.EXCELLENT
        if score >= 0.85:
            return QualityLevel.GOOD
        if score >= 0.70:
            return QualityLevel.FAIR
        if score >= 0.50:
            return QualityLevel.POOR
        return QualityLevel.FAIL


@dataclass
class SchemaRule:
    """Schema validation rule."""
    field: str
    field_type: type
    required: bool = False
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[list[Any]] = None
    custom_validator: Optional[Callable] = None


class DataQualityChecker:
    """
    Data quality checker with multiple validation types.

    Example:
        >>> checker = DataQualityChecker()
        >>> checker.add_rule(SchemaRule("email", str, required=True, pattern=r".+@.+\\..+"))
        >>> result = checker.validate(data)
    """

    def __init__(self) -> None:
        self._rules: list[SchemaRule] = []
        self._custom_validators: dict[str, Callable] = {}

    def add_rule(self, rule: SchemaRule) -> "DataQualityChecker":
        """Add validation rule."""
        self._rules.append(rule)
        return self

    def add_custom_validator(
        self,
        name: str,
        validator: Callable[[Any], tuple[bool, str]],
    ) -> "DataQualityChecker":
        """Add custom validator function."""
        self._custom_validators[name] = validator
        return self

    def validate(self, data: list[dict]) -> tuple[bool, list[ValidationError], QualityMetrics]:
        """Validate data and return results."""
        errors: list[ValidationError] = []
        metrics = QualityMetrics(total_records=len(data))

        if not data:
            return True, [], metrics

        field_names = set()
        for record in data:
            field_names.update(record.keys())

        for idx, record in enumerate(data):
            record_errors = self._validate_record(record, idx)
            errors.extend(record_errors)

            for field_name in field_names:
                if field_name not in record or record[field_name] is None:
                    metrics.null_count += 1

        metrics.invalid_records = len(set(e.row_index for e in errors if e.row_index is not None))
        metrics.valid_records = metrics.total_records - metrics.invalid_records

        all_values = []
        for record in data:
            all_values.extend(str(v) for v in record.values() if v is not None)
        metrics.unique_count = len(set(all_values))
        metrics.duplicate_count = len(all_values) - metrics.unique_count

        metrics.completeness = 1.0 - (metrics.null_count / max(1, metrics.total_records * len(field_names)))
        metrics.validity = metrics.valid_records / metrics.total_records if metrics.total_records > 0 else 0.0

        return len(errors) == 0, errors, metrics

    def _validate_record(self, record: dict, row_index: int) -> list[ValidationError]:
        """Validate single record."""
        errors: list[ValidationError] = []

        for rule in self._rules:
            value = record.get(rule.field)
            field_errors = self._validate_field(rule, value, row_index)
            errors.extend(field_errors)

        for name, validator in self._custom_validators.items():
            try:
                valid, message = validator(record)
                if not valid:
                    errors.append(ValidationError(
                        field="__custom__",
                        validation_type=ValidationType.CUSTOM,
                        message=f"Custom validation '{name}': {message}",
                        value=record,
                        row_index=row_index,
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field="__custom__",
                    validation_type=ValidationType.CUSTOM,
                    message=f"Custom validator error: {e}",
                    row_index=row_index,
                ))

        return errors

    def _validate_field(
        self,
        rule: SchemaRule,
        value: Any,
        row_index: int,
    ) -> list[ValidationError]:
        """Validate single field."""
        errors: list[ValidationError] = []

        if value is None:
            if rule.required and not rule.nullable:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.NULL,
                    message=f"Required field is null",
                    value=value,
                    row_index=row_index,
                ))
            return errors

        if not isinstance(value, rule.field_type):
            if not (rule.nullable and value is None):
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.TYPE,
                    message=f"Expected type {rule.field_type.__name__}, got {type(value).__name__}",
                    value=value,
                    row_index=row_index,
                ))
            return errors

        if rule.min_value is not None and isinstance(value, (int, float)):
            if value < rule.min_value:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.RANGE,
                    message=f"Value {value} is less than minimum {rule.min_value}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.max_value is not None and isinstance(value, (int, float)):
            if value > rule.max_value:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.RANGE,
                    message=f"Value {value} is greater than maximum {rule.max_value}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.min_length is not None and isinstance(value, (str, list)):
            if len(value) < rule.min_length:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.RANGE,
                    message=f"Length {len(value)} is less than minimum {rule.min_length}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.max_length is not None and isinstance(value, (str, list)):
            if len(value) > rule.max_length:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.RANGE,
                    message=f"Length {len(value)} is greater than maximum {rule.max_length}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.pattern is not None and isinstance(value, str):
            if not re.match(rule.pattern, value):
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.PATTERN,
                    message=f"Value does not match pattern: {rule.pattern}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.allowed_values is not None:
            if value not in rule.allowed_values:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.SCHEMA,
                    message=f"Value not in allowed values: {rule.allowed_values}",
                    value=value,
                    row_index=row_index,
                ))

        if rule.custom_validator is not None:
            try:
                valid, message = rule.custom_validator(value)
                if not valid:
                    errors.append(ValidationError(
                        field=rule.field,
                        validation_type=ValidationType.CUSTOM,
                        message=f"Custom validation failed: {message}",
                        value=value,
                        row_index=row_index,
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field=rule.field,
                    validation_type=ValidationType.CUSTOM,
                    message=f"Custom validator error: {e}",
                    value=value,
                    row_index=row_index,
                ))

        return errors


class DataProfiler:
    """
    Data profiling with statistics.

    Example:
        >>> profiler = DataProfiler()
        >>> stats = profiler.profile(data)
    """

    def profile(self, data: list[dict]) -> dict[str, Any]:
        """Profile data and return statistics."""
        if not data:
            return {}

        field_names = list(data[0].keys())
        profile: dict[str, Any] = {
            "record_count": len(data),
            "field_count": len(field_names),
            "fields": {},
            "timestamp": datetime.utcnow().timestamp(),
        }

        for field_name in field_names:
            values = [record.get(field_name) for record in data if field_name in record]
            profile["fields"][field_name] = self._profile_field(field_name, values)

        return profile

    def _profile_field(self, name: str, values: list[Any]) -> dict[str, Any]:
        """Profile single field."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)

        field_profile: dict[str, Any] = {
            "name": name,
            "total_count": len(values),
            "null_count": null_count,
            "null_percentage": null_count / len(values) * 100 if values else 0,
            "unique_count": len(set(str(v) for v in non_null)),
        }

        if non_null:
            if all(isinstance(v, (int, float)) for v in non_null):
                nums = [float(v) for v in non_null]
                field_profile.update({
                    "type": "numeric",
                    "min": min(nums),
                    "max": max(nums),
                    "mean": sum(nums) / len(nums),
                    "median": self._median(nums),
                    "std_dev": self._std_dev(nums),
                })
            else:
                str_values = [str(v) for v in non_null]
                value_counts = Counter(str_values)
                field_profile.update({
                    "type": "categorical",
                    "min_length": min(len(v) for v in str_values),
                    "max_length": max(len(v) for v in str_values),
                    "avg_length": sum(len(v) for v in str_values) / len(str_values),
                    "top_values": value_counts.most_common(10),
                })

        return field_profile

    def _median(self, values: list[float]) -> float:
        """Calculate median."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n == 0:
            return 0.0
        if n % 2 == 0:
            return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
        return sorted_vals[n // 2]

    def _std_dev(self, values: list[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return variance ** 0.5
