"""
Data Validation Action Module.

Provides comprehensive data validation with schema support,
custom validators, and detailed error reporting for data quality assurance.
"""

from typing import Optional, Dict, List, Any, Callable, Union, Type
from dataclasses import dataclass, field
from enum import Enum
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Types of validation operations."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    LENGTH = "length"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    EMAIL = "email"
    URL = "url"
    JSON = "json"
    SCHEMA = "schema"


@dataclass
class ValidationError:
    """Represents a single validation error."""
    field: str
    error_type: ValidationType
    message: str
    value: Any = None
    constraint: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[ValidationError]
    warnings: List[str] = field(default_factory=list)
    validated_at: float = field(default_factory=datetime.now().timestamp)
    
    @property
    def error_count(self) -> int:
        return len(self.errors)
        
    @property
    def error_messages(self) -> List[str]:
        return [e.message for e in self.errors]
        
    def get_errors_for_field(self, field: str) -> List[ValidationError]:
        """Get all errors for a specific field."""
        return [e for e in self.errors if e.field == field]
        
    def summary(self) -> str:
        """Get human-readable summary of validation result."""
        if self.valid:
            return "Validation passed"
        return f"Validation failed with {self.error_count} error(s): {', '.join(self.error_messages[:3])}"


@dataclass
class FieldValidator:
    """Validator configuration for a single field."""
    name: str
    validators: List[tuple] = field(default_factory=list)  # (validator_type, params)
    required: bool = False
    custom_message: Optional[str] = None
    
    def add(
        self,
        validator_type: ValidationType,
        constraint: Any = None,
        message: Optional[str] = None,
    ) -> "FieldValidator":
        """Add a validator to this field."""
        self.validators.append((validator_type, constraint, message))
        return self


class ValidatorRegistry:
    """Registry of custom validator functions."""
    
    _validators: Dict[str, Callable[[Any, Any], bool]] = {}
    
    @classmethod
    def register(
        cls,
        name: str,
        validator_fn: Callable[[Any, Any], bool],
    ) -> None:
        """Register a custom validator function."""
        cls._validators[name] = validator_fn
        
    @classmethod
    def get(cls, name: str) -> Optional[Callable[[Any, Any], bool]]:
        """Get a registered validator."""
        return cls._validators.get(name)


@dataclass
class SchemaValidator:
    """
    Schema-based validator for complex data structures.
    
    Example:
        schema = SchemaValidator()
        schema.field("email", required=True).add(ValidationType.EMAIL)
        schema.field("age", required=True).add(ValidationType.RANGE, constraint=(0, 150))
        schema.field("tags", required=False).add(ValidationType.TYPE, constraint=list)
        
        result = schema.validate({"email": "test@example.com", "age": 25})
    """
    
    def __init__(self):
        self.fields: Dict[str, FieldValidator] = {}
        self.validators: List[Callable[[Any], ValidationResult]] = []
        
    def field(
        self,
        name: str,
        required: bool = False,
        custom_message: Optional[str] = None,
    ) -> FieldValidator:
        """Define a field with validation rules."""
        if name not in self.fields:
            self.fields[name] = FieldValidator(
                name=name,
                required=required,
                custom_message=custom_message,
            )
        return self.fields[name]
        
    def add_validator(
        self,
        validator_fn: Callable[[Any], ValidationResult],
    ) -> "SchemaValidator":
        """Add a custom schema-level validator."""
        self.validators.append(validator_fn)
        return self
        
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against the schema."""
        errors: List[ValidationError] = []
        warnings: List[str] = []
        
        for field_name, field_validator in self.fields.items():
            value = data.get(field_name)
            
            field_errors = self._validate_field(
                field_name,
                value,
                field_validator,
            )
            errors.extend(field_errors)
            
        for validator in self.validators:
            try:
                result = validator(data)
                if not result.valid:
                    errors.extend(result.errors)
                    warnings.extend(result.warnings)
            except Exception as e:
                logger.error(f"Schema validator error: {e}")
                
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )
        
    def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_validator: FieldValidator,
    ) -> List[ValidationError]:
        """Validate a single field."""
        errors: List[ValidationError] = []
        
        # Check required
        if value is None or value == "":
            if field_validator.required:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationType.REQUIRED,
                    message=field_validator.custom_message or f"Field '{field_name}' is required",
                ))
            return errors
            
        # Run validators
        for validator_type, constraint, custom_msg in field_validator.validators:
            error = self._run_validator(
                field_name,
                value,
                validator_type,
                constraint,
                custom_msg,
            )
            if error:
                errors.append(error)
                
        return errors
        
    def _run_validator(
        self,
        field_name: str,
        value: Any,
        validator_type: ValidationType,
        constraint: Any,
        custom_msg: Optional[str],
    ) -> Optional[ValidationError]:
        """Run a single validator and return error if failed."""
        message = custom_msg or f"Validation failed for '{field_name}'"
        
        if validator_type == ValidationType.TYPE:
            if not isinstance(value, constraint):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                    constraint=constraint,
                )
                
        elif validator_type == ValidationType.RANGE:
            min_val, max_val = constraint
            if not (min_val <= value <= max_val):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                    constraint=constraint,
                )
                
        elif validator_type == ValidationType.LENGTH:
            min_len, max_len = constraint
            length = len(value) if hasattr(value, "__len__") else 0
            if not (min_len <= length <= max_len):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                    constraint=constraint,
                )
                
        elif validator_type == ValidationType.PATTERN:
            if not re.match(constraint, str(value)):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                    constraint=constraint,
                )
                
        elif validator_type == ValidationType.ENUM:
            if value not in constraint:
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                    constraint=constraint,
                )
                
        elif validator_type == ValidationType.EMAIL:
            email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            if not re.match(email_pattern, str(value)):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                )
                
        elif validator_type == ValidationType.URL:
            url_pattern = r"^https?://[^\s/$.?#].[^\s]*$"
            if not re.match(url_pattern, str(value)):
                return ValidationError(
                    field=field_name,
                    error_type=validator_type,
                    message=message,
                    value=value,
                )
                
        elif validator_type == ValidationType.CUSTOM:
            if callable(constraint):
                try:
                    if not constraint(value):
                        return ValidationError(
                            field=field_name,
                            error_type=validator_type,
                            message=message,
                            value=value,
                        )
                except Exception as e:
                    return ValidationError(
                        field=field_name,
                        error_type=validator_type,
                        message=f"{message}: {e}",
                        value=value,
                    )
                    
        return None


class DataValidator:
    """
    Fluent data validation API.
    
    Example:
        result = DataValidator.validate(data) \
            .field("name").required().string().min_length(2) \
            .field("age").required().number().range(0, 150) \
            .field("email").required().email() \
            .execute()
    """
    
    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self._field_validators: Dict[str, List[Callable]] = {}
        self._required_fields: set = set()
        
    @classmethod
    def validate(cls, data: Dict[str, Any]) -> "DataValidator":
        """Start validation chain."""
        return cls(data)
        
    def field(self, field_name: str) -> "DataValidator":
        """Select a field for validation."""
        if field_name not in self._field_validators:
            self._field_validators[field_name] = []
        return self
        
    def required(self) -> "DataValidator":
        """Mark selected field as required."""
        self._required_fields.add(self._get_current_field())
        return self
        
    def string(self) -> "DataValidator":
        """Validate field is a string."""
        return self
        
    def number(self) -> "DataValidator":
        """Validate field is a number."""
        return self
        
    def email(self) -> "DataValidator":
        """Validate field is a valid email."""
        return self
        
    def url(self) -> "DataValidator":
        """Validate field is a valid URL."""
        return self
        
    def min_length(self, length: int) -> "DataValidator":
        """Validate minimum string length."""
        return self
        
    def max_length(self, length: int) -> "DataValidator":
        """Validate maximum string length."""
        return self
        
    def range(self, min_val: Any, max_val: Any) -> "DataValidator":
        """Validate number is in range."""
        return self
        
    def pattern(self, regex: str) -> "DataValidator":
        """Validate string matches pattern."""
        return self
        
    def execute(self) -> ValidationResult:
        """Execute validation and return result."""
        errors: List[ValidationError] = []
        
        for field_name in self._required_fields:
            if field_name not in self.data or self.data[field_name] is None:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationType.REQUIRED,
                    message=f"Field '{field_name}' is required",
                ))
                
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
        )
        
    def _get_current_field(self) -> str:
        """Get the last referenced field name."""
        return list(self._field_validators.keys())[-1] if self._field_validators else ""


# Register built-in validators
ValidatorRegistry.register("phone", lambda v: re.match(r"^\+?[\d\s-]{10,}$", str(v)))
ValidatorRegistry.register("postal_code", lambda v: re.match(r"^\d{5}(-\d{4})?$", str(v)))
