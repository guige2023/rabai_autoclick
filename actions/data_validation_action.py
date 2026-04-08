"""
Data Validation Action Module

Provides comprehensive data validation, schema enforcement, and quality checks.
"""
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re
import json


class ValidationType(Enum):
    """Type of validation."""
    TYPE = "type"
    FORMAT = "format"
    RANGE = "range"
    PATTERN = "pattern"
    CUSTOM = "custom"
    SCHEMA = "schema"
    REFERENCE = "reference"


class Severity(Enum):
    """Severity level for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationRule:
    """A single validation rule."""
    name: str
    validation_type: ValidationType
    severity: Severity = Severity.ERROR
    message: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    rule: str
    message: str
    severity: Severity
    value: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[ValidationError]
    warnings: list[ValidationError]
    validated_at: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0


@dataclass
class SchemaField:
    """Schema definition for a field."""
    name: str
    field_type: type
    required: bool = False
    default: Any = None
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    enum_values: Optional[list[Any]] = None
    custom_validators: list[Callable] = field(default_factory=list)


@dataclass
class Schema:
    """Data schema definition."""
    name: str
    fields: dict[str, SchemaField]
    strict: bool = False  # Reject unknown fields


class DataValidationAction:
    """Main data validation action handler."""
    
    def __init__(self):
        self._schemas: dict[str, Schema] = {}
        self._rules: dict[str, list[ValidationRule]] = {}
        self._custom_validators: dict[str, Callable] = {}
    
    def add_schema(self, schema: Schema) -> "DataValidationAction":
        """Register a schema for validation."""
        self._schemas[schema.name] = schema
        return self
    
    def add_rule(
        self,
        schema_name: str,
        rule: ValidationRule
    ) -> "DataValidationAction":
        """Add a validation rule to a schema."""
        if schema_name not in self._rules:
            self._rules[schema_name] = []
        self._rules[schema_name].append(rule)
        return self
    
    def register_custom_validator(
        self,
        name: str,
        validator: Callable[[Any], bool]
    ) -> "DataValidationAction":
        """Register a custom validator function."""
        self._custom_validators[name] = validator
        return self
    
    async def validate(
        self,
        data: Any,
        schema_name: Optional[str] = None,
        rules: Optional[list[ValidationRule]] = None
    ) -> ValidationResult:
        """
        Validate data against schema or rules.
        
        Args:
            data: Data to validate
            schema_name: Name of registered schema
            rules: Optional inline validation rules
            
        Returns:
            ValidationResult with errors and warnings
        """
        start_time = datetime.now()
        errors = []
        warnings = []
        
        if schema_name and schema_name in self._schemas:
            errs, warns = await self._validate_with_schema(data, self._schemas[schema_name])
            errors.extend(errs)
            warnings.extend(warns)
        
        if rules:
            for rule in rules:
                errs, warns = await self._apply_rule(data, "", rule)
                errors.extend(errs)
                warnings.extend(warns)
        
        # Remove duplicates
        errors = self._deduplicate_errors(errors)
        warnings = self._deduplicate_errors(warnings)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return ValidationResult(
            valid=len([e for e in errors if e.severity == Severity.ERROR]) == 0,
            errors=errors,
            warnings=warnings,
            duration_ms=duration_ms
        )
    
    async def validate_dict(
        self,
        data: dict[str, Any],
        schema_name: str
    ) -> ValidationResult:
        """Validate a dictionary against a schema."""
        start_time = datetime.now()
        errors = []
        warnings = []
        
        if schema_name not in self._schemas:
            return ValidationResult(
                valid=False,
                errors=[ValidationError(
                    field="__schema__",
                    rule="schema_not_found",
                    message=f"Schema '{schema_name}' not found",
                    severity=Severity.ERROR
                )],
                warnings=[]
            )
        
        schema = self._schemas[schema_name]
        
        # Check required fields
        for field_name, field_def in schema.fields.items():
            if field_def.required and field_name not in data:
                if field_def.default is None:
                    errors.append(ValidationError(
                        field=field_name,
                        rule="required",
                        message=f"Required field '{field_name}' is missing",
                        severity=Severity.ERROR
                    ))
        
        # Validate each field
        for field_name, value in data.items():
            if field_name not in schema.fields:
                if schema.strict:
                    errors.append(ValidationError(
                        field=field_name,
                        rule="unknown_field",
                        message=f"Unknown field '{field_name}' in strict mode",
                        severity=Severity.ERROR
                    ))
                continue
            
            field_def = schema.fields[field_name]
            field_errors, field_warnings = await self._validate_field(
                field_name, value, field_def
            )
            errors.extend(field_errors)
            warnings.extend(field_warnings)
        
        # Apply schema-level rules
        if schema_name in self._rules:
            for rule in self._rules[schema_name]:
                rule_errors, rule_warnings = await self._apply_rule(data, "", rule)
                errors.extend(rule_errors)
                warnings.extend(rule_warnings)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return ValidationResult(
            valid=len([e for e in errors if e.severity == Severity.ERROR]) == 0,
            errors=errors,
            warnings=warnings,
            duration_ms=duration_ms
        )
    
    async def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_def: SchemaField
    ) -> tuple[list[ValidationError], list[ValidationError]]:
        """Validate a single field."""
        errors = []
        warnings = []
        
        # None handling
        if value is None:
            if not field_def.nullable:
                errors.append(ValidationError(
                    field=field_name,
                    rule="nullable",
                    message=f"Field '{field_name}' cannot be null",
                    severity=Severity.ERROR,
                    value=value
                ))
            return errors, warnings
        
        # Type checking
        if not isinstance(value, field_def.field_type):
            # Allow int for float
            if field_def.field_type == float and isinstance(value, int):
                return errors, warnings
            
            errors.append(ValidationError(
                field=field_name,
                rule="type",
                message=f"Field '{field_name}' must be of type {field_def.field_type.__name__}",
                severity=Severity.ERROR,
                value=value
            ))
            return errors, warnings
        
        # Range validation for numeric types
        if isinstance(value, (int, float)):
            if field_def.min_value is not None and value < field_def.min_value:
                errors.append(ValidationError(
                    field=field_name,
                    rule="min_value",
                    message=f"Field '{field_name}' must be >= {field_def.min_value}",
                    severity=Severity.ERROR,
                    value=value
                ))
            if field_def.max_value is not None and value > field_def.max_value:
                errors.append(ValidationError(
                    field=field_name,
                    rule="max_value",
                    message=f"Field '{field_name}' must be <= {field_def.max_value}",
                    severity=Severity.ERROR,
                    value=value
                ))
        
        # Length validation for sequences
        if hasattr(value, "__len__"):
            if field_def.min_length is not None and len(value) < field_def.min_length:
                errors.append(ValidationError(
                    field=field_name,
                    rule="min_length",
                    message=f"Field '{field_name}' length must be >= {field_def.min_length}",
                    severity=Severity.ERROR,
                    value=value
                ))
            if field_def.max_length is not None and len(value) > field_def.max_length:
                errors.append(ValidationError(
                    field=field_name,
                    rule="max_length",
                    message=f"Field '{field_name}' length must be <= {field_def.max_length}",
                    severity=Severity.ERROR,
                    value=value
                ))
        
        # Pattern validation
        if field_def.pattern and isinstance(value, str):
            if not re.match(field_def.pattern, value):
                errors.append(ValidationError(
                    field=field_name,
                    rule="pattern",
                    message=f"Field '{field_name}' does not match pattern '{field_def.pattern}'",
                    severity=Severity.ERROR,
                    value=value
                ))
        
        # Enum validation
        if field_def.enum_values is not None:
            if value not in field_def.enum_values:
                errors.append(ValidationError(
                    field=field_name,
                    rule="enum",
                    message=f"Field '{field_name}' must be one of {field_def.enum_values}",
                    severity=Severity.ERROR,
                    value=value
                ))
        
        # Custom validators
        for validator in field_def.custom_validators:
            try:
                if not validator(value):
                    errors.append(ValidationError(
                        field=field_name,
                        rule="custom",
                        message=f"Field '{field_name}' failed custom validation",
                        severity=Severity.ERROR,
                        value=value
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field=field_name,
                    rule="custom",
                    message=f"Custom validator error: {e}",
                    severity=Severity.ERROR,
                    value=value
                ))
        
        return errors, warnings
    
    async def _validate_with_schema(
        self,
        data: Any,
        schema: Schema
    ) -> tuple[list[ValidationError], list[ValidationError]]:
        """Validate data with a full schema."""
        if isinstance(data, dict):
            return await self.validate_dict(data, schema.name)
        return [], []
    
    async def _apply_rule(
        self,
        data: Any,
        field_path: str,
        rule: ValidationRule
    ) -> tuple[list[ValidationError], list[ValidationError]]:
        """Apply a validation rule."""
        errors = []
        warnings = []
        
        if rule.validation_type == ValidationType.CUSTOM:
            if rule.validator:
                try:
                    if not rule.validator(data):
                        errors.append(ValidationError(
                            field=field_path,
                            rule=rule.name,
                            message=rule.message or f"Custom validation failed for {rule.name}",
                            severity=rule.severity,
                            value=data
                        ))
                except Exception as e:
                    errors.append(ValidationError(
                        field=field_path,
                        rule=rule.name,
                        message=f"Validator error: {e}",
                        severity=Severity.ERROR,
                        value=data
                    ))
        
        return errors, warnings
    
    def _deduplicate_errors(self, errors: list[ValidationError]) -> list[ValidationError]:
        """Remove duplicate errors."""
        seen = set()
        result = []
        for error in errors:
            key = (error.field, error.rule, error.message)
            if key not in seen:
                seen.add(key)
                result.append(error)
        return result
    
    async def validate_json_schema(self, data: dict, schema: dict) -> ValidationResult:
        """Validate data against a JSON schema (simple implementation)."""
        errors = []
        warnings = []
        
        # Simple JSON schema validation
        if "required" in schema:
            for field_name in schema["required"]:
                if field_name not in data:
                    errors.append(ValidationError(
                        field=field_name,
                        rule="required",
                        message=f"Required field missing",
                        severity=Severity.ERROR
                    ))
        
        if "properties" in schema:
            for field_name, field_schema in schema["properties"].items():
                if field_name in data:
                    value = data[field_name]
                    
                    # Type validation
                    if "type" in field_schema:
                        expected_type = field_schema["type"]
                        type_map = {
                            "string": str,
                            "number": (int, float),
                            "integer": int,
                            "boolean": bool,
                            "array": list,
                            "object": dict
                        }
                        
                        if expected_type in type_map:
                            if not isinstance(value, type_map[expected_type]):
                                errors.append(ValidationError(
                                    field=field_name,
                                    rule="type",
                                    message=f"Must be {expected_type}",
                                    severity=Severity.ERROR,
                                    value=value
                                ))
        
        return ValidationResult(
            valid=len([e for e in errors if e.severity == Severity.ERROR]) == 0,
            errors=errors,
            warnings=warnings
        )
