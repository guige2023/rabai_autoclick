"""API Validator Action Module.

Provides comprehensive validation for API requests and responses,
including schema validation, type checking, and custom validation rules.

Example:
    >>> from actions.api.api_validator_action import APIValidatorAction
    >>> validator = APIValidatorAction()
    >>> result = validator.validate_request(data, schema)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Union
import threading


class ValidationLevel(Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class ValidationType(Enum):
    """Types of validation that can be performed."""
    SCHEMA = "schema"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    REQUIRED = "required"
    CUSTOM = "custom"
    SIZE = "size"
    ENUM = "enum"


@dataclass
class ValidationError:
    """Single validation error.
    
    Attributes:
        level: Severity level of the error
        validation_type: Type of validation that failed
        field: Field path where the error occurred
        message: Human-readable error message
        value: The invalid value
        expected: Expected value or format
    """
    level: ValidationLevel
    validation_type: ValidationType
    field: str
    message: str
    value: Any = None
    expected: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation.
    
    Attributes:
        valid: Whether validation passed
        errors: List of validation errors
        warnings: List of validation warnings
        metadata: Additional validation metadata
    """
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FieldSchema:
    """Schema definition for a single field.
    
    Attributes:
        name: Field name
        field_type: Expected data type
        required: Whether the field is required
        nullable: Whether the field can be null
        default: Default value if not provided
        min_value: Minimum value for numbers
        max_value: Maximum value for numbers
        min_length: Minimum length for strings/arrays
        max_length: Maximum length for strings/arrays
        pattern: Regex pattern for strings
        enum_values: Allowed values for enum fields
        custom_validator: Custom validation function
    """
    name: str
    field_type: type = str
    required: bool = False
    nullable: bool = True
    default: Any = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[Union[str, Pattern]] = None
    enum_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None


@dataclass
class Schema:
    """Schema definition for validating data structures.
    
    Attributes:
        name: Schema name
        fields: Dictionary of field schemas
        strict: Whether to reject unknown fields
    """
    name: str
    fields: Dict[str, FieldSchema] = field(default_factory=dict)
    strict: bool = False


class APIValidatorAction:
    """Handles API request and response validation.
    
    Provides comprehensive validation with support for custom
    validators, schema validation, and detailed error reporting.
    
    Attributes:
        schemas: Registered validation schemas
        custom_validators: Custom validator functions
    
    Example:
        >>> validator = APIValidatorAction()
        >>> validator.register_schema("user", user_schema)
        >>> result = validator.validate(data, "user")
    """
    
    def __init__(self):
        """Initialize the API validator action."""
        self._schemas: Dict[str, Schema] = {}
        self._custom_validators: Dict[str, Callable[[Any], bool]] = {}
        self._validation_history: List[ValidationResult] = []
        self._lock = threading.RLock()
    
    def register_schema(self, name: str, schema: Schema) -> "APIValidatorAction":
        """Register a validation schema.
        
        Args:
            name: Schema name
            schema: Schema definition
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._schemas[name] = schema
            return self
    
    def register_custom_validator(
        self,
        name: str,
        validator_fn: Callable[[Any], bool]
    ) -> "APIValidatorAction":
        """Register a custom validator function.
        
        Args:
            name: Validator name
            validator_fn: Validation function returning bool
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._custom_validators[name] = validator_fn
            return self
    
    def validate(
        self,
        data: Any,
        schema_name: str,
        level: ValidationLevel = ValidationLevel.ERROR
    ) -> ValidationResult:
        """Validate data against a registered schema.
        
        Args:
            data: Data to validate
            schema_name: Name of the registered schema
            level: Minimum validation level to report
        
        Returns:
            ValidationResult with any errors or warnings
        """
        schema = self._schemas.get(schema_name)
        if not schema:
            return ValidationResult(
                valid=False,
                errors=[ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.SCHEMA,
                    field="",
                    message=f"Schema '{schema_name}' not found"
                )]
            )
        
        return self._validate_against_schema(data, schema, level)
    
    def validate_request(
        self,
        data: Dict[str, Any],
        level: ValidationLevel = ValidationLevel.ERROR
    ) -> ValidationResult:
        """Validate an API request.
        
        Args:
            data: Request data to validate
            level: Minimum validation level to report
        
        Returns:
            ValidationResult with any errors or warnings
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        metadata: Dict[str, Any] = {
            "timestamp": datetime.now(),
            "field_count": len(data)
        }
        
        # Check content type
        if not isinstance(data, dict):
            errors.append(ValidationError(
                level=ValidationLevel.ERROR,
                validation_type=ValidationType.TYPE,
                field="root",
                message="Request body must be an object",
                value=type(data).__name__,
                expected="object"
            ))
        
        # Validate common request fields
        for field_name, value in data.items():
            field_errors, field_warnings = self._validate_field(
                field_name, value, level
            )
            errors.extend(field_errors)
            warnings.extend(field_warnings)
        
        result = ValidationResult(
            valid=all(e.level != ValidationLevel.ERROR for e in errors),
            errors=errors,
            warnings=warnings,
            metadata=metadata
        )
        
        self._record_result(result)
        return result
    
    def validate_response(
        self,
        data: Any,
        level: ValidationLevel = ValidationLevel.ERROR
    ) -> ValidationResult:
        """Validate an API response.
        
        Args:
            data: Response data to validate
            level: Minimum validation level to report
        
        Returns:
            ValidationResult with any errors or warnings
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        metadata: Dict[str, Any] = {
            "timestamp": datetime.now(),
            "data_type": type(data).__name__
        }
        
        # Basic response validation
        if data is None:
            warnings.append(ValidationError(
                level=ValidationLevel.WARNING,
                validation_type=ValidationType.NULL,
                field="root",
                message="Response body is null"
            ))
        
        result = ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            metadata=metadata
        )
        
        self._record_result(result)
        return result
    
    def _validate_against_schema(
        self,
        data: Any,
        schema: Schema,
        level: ValidationLevel
    ) -> ValidationResult:
        """Validate data against a schema.
        
        Args:
            data: Data to validate
            schema: Schema to validate against
            level: Minimum validation level to report
        
        Returns:
            ValidationResult with errors and warnings
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        
        if not isinstance(data, dict):
            errors.append(ValidationError(
                level=ValidationLevel.ERROR,
                validation_type=ValidationType.TYPE,
                field="root",
                message="Data must be an object",
                value=type(data).__name__,
                expected="object"
            ))
            return ValidationResult(valid=False, errors=errors)
        
        # Check required fields
        for field_name, field_schema in schema.fields.items():
            if field_schema.required and field_name not in data:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.REQUIRED,
                    field=field_name,
                    message=f"Required field '{field_name}' is missing"
                ))
        
        # Validate present fields
        for field_name, value in data.items():
            if field_name in schema.fields:
                field_errors, field_warnings = self._validate_field_value(
                    field_name, value, schema.fields[field_name]
                )
                errors.extend(field_errors)
                warnings.extend(field_warnings)
            elif schema.strict:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.SCHEMA,
                    field=field_name,
                    message=f"Unknown field '{field_name}' in strict mode"
                ))
        
        result = ValidationResult(
            valid=all(e.level != ValidationLevel.ERROR for e in errors),
            errors=errors,
            warnings=warnings
        )
        
        self._record_result(result)
        return result
    
    def _validate_field(
        self,
        field_name: str,
        value: Any,
        level: ValidationLevel
    ) -> tuple:
        """Validate a single field.
        
        Args:
            field_name: Name of the field
            value: Value to validate
            level: Minimum validation level to report
        
        Returns:
            Tuple of (errors, warnings)
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        
        # Type validation
        if value is None:
            warnings.append(ValidationError(
                level=ValidationLevel.WARNING,
                validation_type=ValidationType.TYPE,
                field=field_name,
                message=f"Field '{field_name}' has null value"
            ))
        
        return errors, warnings
    
    def _validate_field_value(
        self,
        field_name: str,
        value: Any,
        field_schema: FieldSchema
    ) -> tuple:
        """Validate a field value against its schema.
        
        Args:
            field_name: Name of the field
            value: Value to validate
            field_schema: Field schema definition
        
        Returns:
            Tuple of (errors, warnings)
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        
        # Null check
        if value is None:
            if not field_schema.nullable:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.NULL,
                    field=field_name,
                    message=f"Field '{field_name}' cannot be null"
                ))
            return errors, warnings
        
        # Type validation
        if not isinstance(value, field_schema.field_type):
            errors.append(ValidationError(
                level=ValidationLevel.ERROR,
                validation_type=ValidationType.TYPE,
                field=field_name,
                message=f"Field '{field_name}' has incorrect type",
                value=type(value).__name__,
                expected=field_schema.field_type.__name__
            ))
        
        # Range validation for numbers
        if isinstance(value, (int, float)) and field_schema.field_type in (int, float):
            if field_schema.min_value is not None and value < field_schema.min_value:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.RANGE,
                    field=field_name,
                    message=f"Field '{field_name}' is below minimum",
                    value=value,
                    expected=f">= {field_schema.min_value}"
                ))
            if field_schema.max_value is not None and value > field_schema.max_value:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.RANGE,
                    field=field_name,
                    message=f"Field '{field_name}' exceeds maximum",
                    value=value,
                    expected=f"<= {field_schema.max_value}"
                ))
        
        # Length validation for strings/arrays
        if isinstance(value, (str, list)):
            if field_schema.min_length is not None and len(value) < field_schema.min_length:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.SIZE,
                    field=field_name,
                    message=f"Field '{field_name}' is too short",
                    value=len(value),
                    expected=f">= {field_schema.min_length}"
                ))
            if field_schema.max_length is not None and len(value) > field_schema.max_length:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.SIZE,
                    field=field_name,
                    message=f"Field '{field_name}' is too long",
                    value=len(value),
                    expected=f"<= {field_schema.max_length}"
                ))
        
        # Pattern validation for strings
        if isinstance(value, str) and field_schema.pattern is not None:
            pattern = field_schema.pattern
            if isinstance(pattern, str):
                pattern = re.compile(pattern)
            if not pattern.match(value):
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.PATTERN,
                    field=field_name,
                    message=f"Field '{field_name}' does not match required pattern",
                    value=value,
                    expected=str(field_schema.pattern)
                ))
        
        # Enum validation
        if field_schema.enum_values is not None and value not in field_schema.enum_values:
            errors.append(ValidationError(
                level=ValidationLevel.ERROR,
                validation_type=ValidationType.ENUM,
                field=field_name,
                message=f"Field '{field_name}' has invalid value",
                value=value,
                expected=field_schema.enum_values
            ))
        
        # Custom validation
        if field_schema.custom_validator is not None:
            try:
                if not field_schema.custom_validator(value):
                    errors.append(ValidationError(
                        level=ValidationLevel.ERROR,
                        validation_type=ValidationType.CUSTOM,
                        field=field_name,
                        message=f"Field '{field_name}' failed custom validation",
                        value=value
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    level=ValidationLevel.ERROR,
                    validation_type=ValidationType.CUSTOM,
                    field=field_name,
                    message=f"Custom validation error: {str(e)}",
                    value=value
                ))
        
        return errors, warnings
    
    def _record_result(self, result: ValidationResult) -> None:
        """Record a validation result.
        
        Args:
            result: Validation result to record
        """
        with self._lock:
            self._validation_history.append(result)
            # Keep only last 1000 results
            if len(self._validation_history) > 1000:
                self._validation_history = self._validation_history[-1000:]
    
    def get_validation_history(
        self,
        limit: int = 100
    ) -> List[ValidationResult]:
        """Get recent validation history.
        
        Args:
            limit: Maximum number of results to return
        
        Returns:
            List of recent validation results
        """
        with self._lock:
            return self._validation_history[-limit:]
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics.
        
        Returns:
            Dictionary with validation statistics
        """
        with self._lock:
            if not self._validation_history:
                return {"total": 0, "valid": 0, "invalid": 0, "success_rate": 0}
            
            total = len(self._validation_history)
            valid = sum(1 for r in self._validation_history if r.valid)
            
            return {
                "total": total,
                "valid": valid,
                "invalid": total - valid,
                "success_rate": (valid / total) * 100 if total > 0 else 0,
                "error_count": sum(len(r.errors) for r in self._validation_history),
                "warning_count": sum(len(r.warnings) for r in self._validation_history)
            }
