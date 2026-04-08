"""
Data Quality Action - Data validation and quality checks.

This module provides data quality validation including schema
checking, constraint validation, and anomaly detection.
"""

from __future__ import annotations

import re
import math
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum
from datetime import datetime


class ValidationType(Enum):
    """Type of validation to perform."""
    SCHEMA = "schema"
    CONSTRAINT = "constraint"
    RANGE = "range"
    FORMAT = "format"
    CUSTOM = "custom"


class QualityLevel(Enum):
    """Data quality level."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    FAILING = "failing"


@dataclass
class ValidationRule:
    """A single validation rule."""
    rule_id: str
    name: str
    validation_type: ValidationType
    field_path: str
    params: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None


@dataclass
class ValidationError:
    """A validation error."""
    rule_id: str
    rule_name: str
    field_path: str
    value: Any
    expected: str
    actual: str


@dataclass
class QualityReport:
    """Data quality report."""
    level: QualityLevel
    score: float
    total_records: int
    valid_records: int
    invalid_records: int
    errors: list[ValidationError]
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class SchemaValidator:
    """Validates data against a schema."""
    
    def __init__(self) -> None:
        self._schemas: dict[str, dict[str, Any]] = {}
    
    def define_schema(
        self,
        schema_name: str,
        fields: dict[str, dict[str, Any]],
    ) -> None:
        """
        Define a schema for validation.
        
        Fields can have: type, required, min, max, pattern, enum
        """
        self._schemas[schema_name] = fields
    
    def validate(
        self,
        data: dict[str, Any],
        schema_name: str,
    ) -> tuple[bool, list[ValidationError]]:
        """Validate data against a named schema."""
        if schema_name not in self._schemas:
            return False, [ValidationError(
                rule_id="schema_not_found",
                rule_name="Schema Not Found",
                field_path="",
                value=None,
                expected=schema_name,
                actual="",
            )]
        
        schema = self._schemas[schema_name]
        errors = []
        
        for field_name, rules in schema.items():
            value = data.get(field_name)
            
            if rules.get("required", False) and value is None:
                errors.append(ValidationError(
                    rule_id=f"{field_name}_required",
                    rule_name=f"{field_name} Required",
                    field_path=field_name,
                    value=value,
                    expected="non-null value",
                    actual="null",
                ))
                continue
            
            if value is None:
                continue
            
            expected_type = rules.get("type")
            if expected_type and not isinstance(value, expected_type):
                errors.append(ValidationError(
                    rule_id=f"{field_name}_type",
                    rule_name=f"{field_name} Type",
                    field_path=field_name,
                    value=value,
                    expected=str(expected_type),
                    actual=type(value).__name__,
                ))
            
            if "min" in rules and isinstance(value, (int, float)):
                if value < rules["min"]:
                    errors.append(ValidationError(
                        rule_id=f"{field_name}_min",
                        rule_name=f"{field_name} Minimum",
                        field_path=field_name,
                        value=value,
                        expected=f">= {rules['min']}",
                        actual=str(value),
                    ))
            
            if "max" in rules and isinstance(value, (int, float)):
                if value > rules["max"]:
                    errors.append(ValidationError(
                        rule_id=f"{field_name}_max",
                        rule_name=f"{field_name} Maximum",
                        field_path=field_name,
                        value=value,
                        expected=f"<= {rules['max']}",
                        actual=str(value),
                    ))
            
            if "pattern" in rules:
                pattern = rules["pattern"]
                if not isinstance(value, str) or not re.match(pattern, value):
                    errors.append(ValidationError(
                        rule_id=f"{field_name}_pattern",
                        rule_name=f"{field_name} Format",
                        field_path=field_name,
                        value=value,
                        expected=f"matching pattern {pattern}",
                        actual=str(value)[:50],
                    ))
            
            if "enum" in rules:
                if value not in rules["enum"]:
                    errors.append(ValidationError(
                        rule_id=f"{field_name}_enum",
                        rule_name=f"{field_name} Enum",
                        field_path=field_name,
                        value=value,
                        expected=f"one of {rules['enum']}",
                        actual=str(value),
                    ))
        
        return len(errors) == 0, errors


class ConstraintValidator:
    """Validates data against custom constraints."""
    
    def __init__(self) -> None:
        self._constraints: list[ValidationRule] = []
    
    def add_constraint(
        self,
        rule_id: str,
        name: str,
        predicate: Callable[[Any], bool],
        error_message: str,
        field_path: str = "",
    ) -> None:
        """Add a custom constraint."""
        rule = ValidationRule(
            rule_id=rule_id,
            name=name,
            validation_type=ValidationType.CUSTOM,
            field_path=field_path,
            error_message=error_message,
        )
        rule._predicate = predicate
        self._constraints.append(rule)
    
    def validate_record(
        self,
        record: dict[str, Any],
    ) -> list[ValidationError]:
        """Validate a record against all constraints."""
        errors = []
        
        for rule in self._constraints:
            try:
                if rule.validation_type == ValidationType.CUSTOM:
                    value = self._get_nested(record, rule.field_path) if rule.field_path else record
                    if not rule._predicate(value):
                        errors.append(ValidationError(
                            rule_id=rule.rule_id,
                            rule_name=rule.name,
                            field_path=rule.field_path,
                            value=value,
                            expected=rule.error_message,
                            actual="constraint failed",
                        ))
            except Exception:
                pass
        
        return errors
    
    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        """Get nested value."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class DataQualityScorer:
    """Computes data quality scores."""
    
    def __init__(
        self,
        excellent_threshold: float = 0.95,
        good_threshold: float = 0.85,
        fair_threshold: float = 0.70,
    ) -> None:
        self.excellent_threshold = excellent_threshold
        self.good_threshold = good_threshold
        self.fair_threshold = fair_threshold
    
    def compute_score(
        self,
        total_records: int,
        valid_records: int,
        error_count: int,
    ) -> tuple[QualityLevel, float]:
        """Compute overall quality score."""
        if total_records == 0:
            return QualityLevel.FAILING, 0.0
        
        completeness = valid_records / total_records
        correctness = max(0, 1 - (error_count / total_records))
        
        score = (completeness + correctness) / 2
        
        if score >= self.excellent_threshold:
            level = QualityLevel.EXCELLENT
        elif score >= self.good_threshold:
            level = QualityLevel.GOOD
        elif score >= self.fair_threshold:
            level = QualityLevel.FAIR
        elif score > 0:
            level = QualityLevel.POOR
        else:
            level = QualityLevel.FAILING
        
        return level, score


class DataQualityAction:
    """
    Data quality validation action.
    
    Example:
        action = DataQualityAction()
        action.define_schema("user", {
            "email": {"type": str, "required": True, "pattern": r"^[^@]+@[^@]+$"},
            "age": {"type": int, "min": 0, "max": 150},
        })
        report = await action.validate_records(records, "user")
    """
    
    def __init__(self) -> None:
        self.schema_validator = SchemaValidator()
        self.constraint_validator = ConstraintValidator()
        self.quality_scorer = DataQualityScorer()
    
    def define_schema(
        self,
        schema_name: str,
        fields: dict[str, dict[str, Any]],
    ) -> None:
        """Define a validation schema."""
        self.schema_validator.define_schema(schema_name, fields)
    
    def add_rule(
        self,
        rule_id: str,
        name: str,
        predicate: Callable[[Any], bool],
        error_message: str,
        field_path: str = "",
    ) -> None:
        """Add a custom validation rule."""
        self.constraint_validator.add_constraint(
            rule_id, name, predicate, error_message, field_path
        )
    
    async def validate_records(
        self,
        records: list[dict[str, Any]],
        schema_name: str | None = None,
        apply_constraints: bool = True,
    ) -> QualityReport:
        """
        Validate a batch of records.
        
        Args:
            records: List of records to validate
            schema_name: Optional schema name for schema validation
            apply_constraints: Whether to apply custom constraints
            
        Returns:
            QualityReport with validation results
        """
        all_errors: list[ValidationError] = []
        warnings: list[str] = []
        
        valid_count = 0
        
        for i, record in enumerate(records):
            record_errors = []
            
            if schema_name:
                _, schema_errors = self.schema_validator.validate(record, schema_name)
                record_errors.extend(schema_errors)
            
            if apply_constraints:
                constraint_errors = self.constraint_validator.validate_record(record)
                record_errors.extend(constraint_errors)
            
            if record_errors:
                all_errors.extend(record_errors)
            else:
                valid_count += 1
        
        level, score = self.quality_scorer.compute_score(
            len(records),
            valid_count,
            len(all_errors),
        )
        
        return QualityReport(
            level=level,
            score=score,
            total_records=len(records),
            valid_records=valid_count,
            invalid_records=len(records) - valid_count,
            errors=all_errors,
            warnings=warnings,
        )
    
    def validate_single(
        self,
        record: dict[str, Any],
        schema_name: str | None = None,
    ) -> tuple[bool, list[ValidationError]]:
        """Validate a single record."""
        errors = []
        
        if schema_name:
            _, schema_errors = self.schema_validator.validate(record, schema_name)
            errors.extend(schema_errors)
        
        constraint_errors = self.constraint_validator.validate_record(record)
        errors.extend(constraint_errors)
        
        return len(errors) == 0, errors


# Export public API
__all__ = [
    "ValidationType",
    "QualityLevel",
    "ValidationRule",
    "ValidationError",
    "QualityReport",
    "SchemaValidator",
    "ConstraintValidator",
    "DataQualityScorer",
    "DataQualityAction",
]
