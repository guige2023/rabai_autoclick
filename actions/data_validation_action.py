"""Data Validation Action Module.

Provides data validation, schema enforcement, and
data quality checks for processing pipelines.
"""

from typing import Any, Dict, List, Optional, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from datetime import datetime


class ValidationLevel(Enum):
    """Validation severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationResult:
    """Result of data validation."""

    def __init__(self, valid: bool):
        self.valid = valid
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []

    def add_error(self, field: str, message: str, value: Any = None):
        """Add an error."""
        self.errors.append({
            "field": field,
            "message": message,
            "value": str(value) if value is not None else None,
        })
        self.valid = False

    def add_warning(self, field: str, message: str, value: Any = None):
        """Add a warning."""
        self.warnings.append({
            "field": field,
            "message": message,
            "value": str(value) if value is not None else None,
        })

    def merge(self, other: "ValidationResult"):
        """Merge another result into this one."""
        self.valid = self.valid and other.valid
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
        }


@dataclass
class FieldSchema:
    """Schema definition for a single field."""
    name: str
    field_type: Type
    required: bool = False
    nullable: bool = True
    default: Any = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    choices: Optional[List[Any]] = None
    custom_validator: Optional[Callable] = None
    transform: Optional[Callable] = None

    def validate(self, value: Any) -> ValidationResult:
        """Validate a value against this schema."""
        result = ValidationResult(valid=True)

        if value is None:
            if self.required:
                result.add_error(self.name, "Field is required")
            return result

        if not self.nullable and value is None:
            result.add_error(self.name, "Field cannot be null")
            return result

        if self.field_type != Any and not isinstance(value, self.field_type):
            result.add_error(
                self.name,
                f"Expected {self.field_type.__name__}, got {type(value).__name__}",
                value
            )
            return result

        if isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                result.add_error(
                    self.name,
                    f"Value {value} is less than minimum {self.min_value}",
                    value
                )
            if self.max_value is not None and value > self.max_value:
                result.add_error(
                    self.name,
                    f"Value {value} is greater than maximum {self.max_value}",
                    value
                )

        if isinstance(value, str):
            if self.min_length is not None and len(value) < self.min_length:
                result.add_error(
                    self.name,
                    f"Length {len(value)} is less than minimum {self.min_length}",
                    value
                )
            if self.max_length is not None and len(value) > self.max_length:
                result.add_error(
                    self.name,
                    f"Length {len(value)} exceeds maximum {self.max_length}",
                    value
                )
            if self.pattern:
                if not re.match(self.pattern, value):
                    result.add_error(
                        self.name,
                        f"Value does not match pattern {self.pattern}",
                        value
                    )

        if self.choices and value not in self.choices:
            result.add_error(
                self.name,
                f"Value must be one of {self.choices}",
                value
            )

        if self.custom_validator:
            try:
                self.custom_validator(value)
            except Exception as e:
                result.add_error(self.name, f"Custom validation failed: {str(e)}", value)

        return result


@dataclass
class Schema:
    """Schema definition for structured data."""
    name: str
    fields: Dict[str, FieldSchema]
    allow_extra_fields: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against schema."""
        result = ValidationResult(valid=True)

        for field_name, field_schema in self.fields.items():
            field_result = field_schema.validate(data.get(field_name))
            result.merge(field_result)

        if not self.allow_extra_fields:
            extra_fields = set(data.keys()) - set(self.fields.keys())
            if extra_fields:
                for field_name in extra_fields:
                    result.add_warning(
                        field_name,
                        f"Extra field not in schema",
                        data.get(field_name)
                    )

        return result

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply schema transformations to data."""
        result = {}
        for field_name, field_schema in self.fields.items():
            value = data.get(field_name, field_schema.default)
            if field_schema.transform:
                try:
                    value = field_schema.transform(value)
                except Exception:
                    pass
            result[field_name] = value
        return result


class DataValidator:
    """Validates data against schemas."""

    def __init__(self):
        self._schemas: Dict[str, Schema] = {}

    def register_schema(self, schema: Schema):
        """Register a schema."""
        self._schemas[schema.name] = schema

    def get_schema(self, name: str) -> Optional[Schema]:
        """Get schema by name."""
        return self._schemas.get(name)

    def validate(
        self,
        data: Dict[str, Any],
        schema_name: Optional[str] = None,
        schema: Optional[Schema] = None,
    ) -> ValidationResult:
        """Validate data against schema."""
        if schema:
            return schema.validate(data)
        elif schema_name:
            schema = self._schemas.get(schema_name)
            if not schema:
                result = ValidationResult(valid=False)
                result.add_error("_schema", f"Schema '{schema_name}' not found")
                return result
            return schema.validate(data)
        else:
            result = ValidationResult(valid=False)
            result.add_error("_schema", "No schema provided")
            return result

    def validate_batch(
        self,
        data_list: List[Dict[str, Any]],
        schema_name: Optional[str] = None,
        schema: Optional[Schema] = None,
        stop_on_first_error: bool = False,
    ) -> List[ValidationResult]:
        """Validate batch of data."""
        results = []
        for data in data_list:
            result = self.validate(data, schema_name, schema)
            results.append(result)
            if stop_on_first_error and not result.valid:
                break
        return results


class DataQualityChecker:
    """Performs data quality checks."""

    def __init__(self):
        self._rules: List[Callable] = []

    def add_rule(self, rule: Callable):
        """Add a quality rule."""
        self._rules.append(rule)

    def check_completeness(
        self,
        data: Dict[str, Any],
        required_fields: List[str],
    ) -> ValidationResult:
        """Check data completeness."""
        result = ValidationResult(valid=True)
        for field_name in required_fields:
            if field_name not in data or data[field_name] is None:
                result.add_error(field_name, "Required field is missing or null")
        return result

    def check_uniqueness(
        self,
        data_list: List[Dict[str, Any]],
        fields: List[str],
    ) -> ValidationResult:
        """Check uniqueness constraint."""
        result = ValidationResult(valid=True)
        seen: Dict[Tuple, List[int]] = {}

        for idx, data in enumerate(data_list):
            key = tuple(data.get(f) for f in fields)
            if key in seen:
                result.add_error(
                    "_unique",
                    f"Duplicate key {key} at rows {seen[key]} and {idx}"
                )
            else:
                seen[key] = [idx]

        return result

    def check_consistency(
        self,
        data: Dict[str, Any],
        rules: Dict[str, Callable],
    ) -> ValidationResult:
        """Check consistency rules."""
        result = ValidationResult(valid=True)
        for field_name, rule in rules.items():
            try:
                if field_name in data and not rule(data):
                    result.add_error(field_name, "Consistency check failed")
            except Exception as e:
                result.add_error(field_name, f"Consistency rule error: {str(e)}")
        return result

    def run_quality_report(
        self,
        data_list: List[Dict[str, Any]],
        schema: Optional[Schema] = None,
    ) -> Dict[str, Any]:
        """Generate data quality report."""
        total = len(data_list)
        if total == 0:
            return {"error": "No data to analyze"}

        all_fields = set()
        for data in data_list:
            all_fields.update(data.keys())

        null_counts: Dict[str, int] = {f: 0 for f in all_fields}
        type_counts: Dict[str, Dict[str, int]] = {f: {} for f in all_fields}

        for data in data_list:
            for field_name in all_fields:
                value = data.get(field_name)
                if value is None:
                    null_counts[field_name] += 1
                else:
                    type_name = type(value).__name__
                    if type_name not in type_counts[field_name]:
                        type_counts[field_name][type_name] = 0
                    type_counts[field_name][type_name] += 1

        return {
            "total_records": total,
            "total_fields": len(all_fields),
            "fields": list(all_fields),
            "null_percentage": {
                f: (null_counts[f] / total * 100) for f in all_fields
            },
            "type_distribution": {
                f: type_counts[f] for f in all_fields
            },
            "completeness_score": (
                (total * len(all_fields) - sum(null_counts.values())) /
                (total * len(all_fields)) * 100
            ) if all_fields else 100,
        }


class DataValidationAction:
    """High-level data validation action."""

    def __init__(
        self,
        validator: Optional[DataValidator] = None,
        quality_checker: Optional[DataQualityChecker] = None,
    ):
        self.validator = validator or DataValidator()
        self.quality_checker = quality_checker or DataQualityChecker()

    def create_schema(
        self,
        name: str,
        fields: Dict[str, Dict[str, Any]],
    ) -> Schema:
        """Create a schema from field definitions."""
        field_schemas = {}
        for field_name, field_def in fields.items():
            field_schemas[field_name] = FieldSchema(
                name=field_name,
                field_type=field_def.get("type", str),
                required=field_def.get("required", False),
                nullable=field_def.get("nullable", True),
                default=field_def.get("default"),
                min_value=field_def.get("min_value"),
                max_value=field_def.get("max_value"),
                min_length=field_def.get("min_length"),
                max_length=field_def.get("max_length"),
                pattern=field_def.get("pattern"),
                choices=field_def.get("choices"),
            )
        schema = Schema(name=name, fields=field_schemas)
        self.validator.register_schema(schema)
        return schema

    def validate(
        self,
        data: Dict[str, Any],
        schema_name: Optional[str] = None,
    ) -> ValidationResult:
        """Validate data."""
        return self.validator.validate(data, schema_name=schema_name)

    def validate_batch(
        self,
        data_list: List[Dict[str, Any]],
        schema_name: str,
    ) -> List[ValidationResult]:
        """Validate batch of data."""
        return self.validator.validate_batch(data_list, schema_name=schema_name)

    def check_quality(
        self,
        data_list: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Run quality check on data."""
        return self.quality_checker.run_quality_report(data_list)


# Module exports
__all__ = [
    "DataValidationAction",
    "DataValidator",
    "DataQualityChecker",
    "Schema",
    "FieldSchema",
    "ValidationResult",
    "ValidationLevel",
]
