"""
API Request Validator Module.

Provides request validation, schema enforcement, and data sanitization
for API request/response handling.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Union
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ValidationType(Enum):
    """Type of validation."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    SCHEMA = "schema"


@dataclass
class ValidationRule:
    """Container for a validation rule."""
    field: str
    validation_type: ValidationType
    rule: Any
    message: str
    required: bool = False
    transform: Optional[Callable[[Any], Any]] = None


@dataclass
class ValidationError:
    """Validation error details."""
    field: str
    message: str
    value: Any
    rule: Any
    
    def __str__(self) -> str:
        return f"{self.field}: {self.message}"


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[ValidationError]
    validated_data: Optional[Dict[str, Any]]
    
    @property
    def error_messages(self) -> List[str]:
        return [str(e) for e in self.errors]


class SchemaValidator:
    """
    Schema-based request/response validator.
    
    Example:
        schema = {
            "user_id": {"type": int, "required": True},
            "email": {"type": str, "pattern": r"^[\w.-]+@[\w.-]+\.\w+$"},
            "age": {"type": int, "min": 0, "max": 150},
            "status": {"enum": ["active", "inactive", "pending"]},
        }
        
        validator = SchemaValidator(schema)
        result = validator.validate(request_data)
    """
    
    def __init__(
        self,
        schema: Dict[str, Dict[str, Any]],
        strict: bool = False,
    ) -> None:
        """
        Initialize the validator.
        
        Args:
            schema: Schema definition.
            strict: If True, reject unknown fields.
        """
        self.schema = schema
        self.strict = strict
        self._rules: List[ValidationRule] = []
        self._build_rules()
        
    def _build_rules(self) -> None:
        """Build validation rules from schema."""
        for field_name, field_spec in self.schema.items():
            # Required
            if field_spec.get("required", False):
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.REQUIRED,
                    rule=True,
                    message=f"{field_name} is required",
                    required=True,
                ))
                
            # Type check
            if "type" in field_spec:
                expected_type = field_spec["type"]
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.TYPE,
                    rule=expected_type,
                    message=f"{field_name} must be of type {expected_type.__name__}",
                ))
                
            # Range checks
            if "min" in field_spec or "max" in field_spec:
                min_val = field_spec.get("min")
                max_val = field_spec.get("max")
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.RANGE,
                    rule=(min_val, max_val),
                    message=f"{field_name} must be between {min_val} and {max_val}",
                ))
                
            # Pattern
            if "pattern" in field_spec:
                pattern = field_spec["pattern"]
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.PATTERN,
                    rule=re.compile(pattern) if isinstance(pattern, str) else pattern,
                    message=f"{field_name} does not match required pattern",
                ))
                
            # Enum
            if "enum" in field_spec:
                enum_values = field_spec["enum"]
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.ENUM,
                    rule=enum_values,
                    message=f"{field_name} must be one of {enum_values}",
                ))
                
            # Custom validator
            if "validator" in field_spec:
                custom = field_spec["validator"]
                self._rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.CUSTOM,
                    rule=custom,
                    message=f"{field_name} failed custom validation",
                ))
                
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate data against schema.
        
        Args:
            data: Data to validate.
            
        Returns:
            ValidationResult with errors if any.
        """
        errors = []
        validated_data = {}
        
        # Check required fields first
        for rule in self._rules:
            if rule.validation_type == ValidationType.REQUIRED:
                if rule.field not in data or data[rule.field] is None:
                    errors.append(ValidationError(
                        field=rule.field,
                        message=rule.message,
                        value=None,
                        rule=rule.rule,
                    ))
                    
        # Validate present fields
        for field_name, value in data.items():
            if field_name not in self.schema and self.strict:
                errors.append(ValidationError(
                    field=field_name,
                    message=f"Unknown field: {field_name}",
                    value=value,
                    rule=None,
                ))
                continue
                
            if field_name in self.schema:
                field_errors = self._validate_field(field_name, value)
                errors.extend(field_errors)
                
                if not field_errors:
                    validated_data[field_name] = value
                    
        # Apply transformations
        for rule in self._rules:
            if rule.transform and rule.field in validated_data:
                try:
                    validated_data[rule.field] = rule.transform(validated_data[rule.field])
                except Exception as e:
                    errors.append(ValidationError(
                        field=rule.field,
                        message=f"Transform failed: {e}",
                        value=validated_data[rule.field],
                        rule=rule.transform,
                    ))
                    
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            validated_data=validated_data if len(errors) == 0 else None,
        )
        
    def _validate_field(self, field_name: str, value: Any) -> List[ValidationError]:
        """Validate a single field."""
        errors = []
        spec = self.schema[field_name]
        
        for rule in self._rules:
            if rule.field != field_name:
                continue
                
            if rule.validation_type == ValidationType.TYPE:
                if not self._check_type(value, rule.rule):
                    errors.append(ValidationError(
                        field=field_name,
                        message=rule.message,
                        value=value,
                        rule=rule.rule,
                    ))
                    
            elif rule.validation_type == ValidationType.RANGE:
                min_val, max_val = rule.rule
                if min_val is not None and value < min_val:
                    errors.append(ValidationError(
                        field=field_name,
                        message=rule.message,
                        value=value,
                        rule=rule.rule,
                    ))
                if max_val is not None and value > max_val:
                    errors.append(ValidationError(
                        field=field_name,
                        message=rule.message,
                        value=value,
                        rule=rule.rule,
                    ))
                    
            elif rule.validation_type == ValidationType.PATTERN:
                if isinstance(value, str) and not rule.rule.match(value):
                    errors.append(ValidationError(
                        field=field_name,
                        message=rule.message,
                        value=value,
                        rule=rule.rule,
                    ))
                    
            elif rule.validation_type == ValidationType.ENUM:
                if value not in rule.rule:
                    errors.append(ValidationError(
                        field=field_name,
                        message=rule.message,
                        value=value,
                        rule=rule.rule,
                    ))
                    
            elif rule.validation_type == ValidationType.CUSTOM:
                try:
                    if not rule.rule(value):
                        errors.append(ValidationError(
                            field=field_name,
                            message=rule.message,
                            value=value,
                            rule=rule.rule,
                        ))
                except Exception as e:
                    errors.append(ValidationError(
                        field=field_name,
                        message=f"Validation error: {e}",
                        value=value,
                        rule=rule.rule,
                    ))
                    
        return errors
        
    def _check_type(self, value: Any, expected_type: type) -> bool:
        """Check if value matches expected type."""
        if expected_type == int:
            return isinstance(value, int) and not isinstance(value, bool)
        elif expected_type == float:
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif expected_type == str:
            return isinstance(value, str)
        elif expected_type == bool:
            return isinstance(value, bool)
        elif expected_type == list:
            return isinstance(value, list)
        elif expected_type == dict:
            return isinstance(value, dict)
        else:
            return isinstance(value, expected_type)


class RequestValidator:
    """
    HTTP request validator with sanitization.
    
    Example:
        validator = RequestValidator()
        
        validator.add_rule("email", ValidationType.PATTERN, 
                          r"^[\w.-]+@[\w.-]+\.\w+$", "Invalid email")
        validator.add_rule("age", ValidationType.RANGE, (0, 150), "Age out of range")
        
        result = validator.validate_request(request)
    """
    
    def __init__(self) -> None:
        """Initialize request validator."""
        self._rules: Dict[str, List[ValidationRule]] = {}
        self._sanitizers: Dict[str, Callable[[Any], Any]] = {}
        
    def add_rule(
        self,
        field: str,
        validation_type: ValidationType,
        rule: Any,
        message: str,
        transform: Optional[Callable[[Any], Any]] = None,
    ) -> "RequestValidator":
        """
        Add a validation rule.
        
        Args:
            field: Field name to validate.
            validation_type: Type of validation.
            rule: Validation rule.
            message: Error message.
            transform: Optional transformation function.
            
        Returns:
            Self for chaining.
        """
        if field not in self._rules:
            self._rules[field] = []
            
        self._rules[field].append(ValidationRule(
            field=field,
            validation_type=validation_type,
            rule=rule,
            message=message,
            transform=transform,
        ))
        
        return self
        
    def sanitize(self, field: str, sanitizer: Callable[[Any], Any]) -> "RequestValidator":
        """
        Add a sanitizer for a field.
        
        Args:
            field: Field to sanitize.
            sanitizer: Sanitization function.
            
        Returns:
            Self for chaining.
        """
        self._sanitizers[field] = sanitizer
        return self
        
    def validate_request(self, request: Dict[str, Any]) -> ValidationResult:
        """
        Validate an HTTP request.
        
        Args:
            request: Request dictionary.
            
        Returns:
            ValidationResult.
        """
        errors = []
        validated = {}
        
        for field_name, rules in self._rules.items():
            value = request.get(field_name)
            
            # Sanitize first
            if field_name in self._sanitizers:
                try:
                    value = self._sanitizers[field_name](value)
                except Exception as e:
                    errors.append(ValidationError(
                        field=field_name,
                        message=f"Sanitization failed: {e}",
                        value=value,
                        rule=self._sanitizers[field_name],
                    ))
                    continue
                    
            # Apply rules
            for rule in rules:
                error = self._apply_rule(field_name, value, rule)
                if error:
                    errors.append(error)
                    
            if not any(e.field == field_name for e in errors):
                validated[field_name] = value
                
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            validated_data=validated if len(errors) == 0 else None,
        )
        
    def _apply_rule(
        self,
        field: str,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Apply a single validation rule."""
        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or value == "":
                return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                
        elif rule.validation_type == ValidationType.TYPE:
            if value is not None and not isinstance(value, rule.rule):
                return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                
        elif rule.validation_type == ValidationType.PATTERN:
            if value and isinstance(value, str):
                if not re.match(rule.rule, value):
                    return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                    
        elif rule.validation_type == ValidationType.RANGE:
            if value is not None:
                min_val, max_val = rule.rule
                if min_val is not None and value < min_val:
                    return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                if max_val is not None and value > max_val:
                    return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                    
        elif rule.validation_type == ValidationType.ENUM:
            if value not in rule.rule:
                return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
                
        elif rule.validation_type == ValidationType.CUSTOM:
            try:
                if not rule.rule(value):
                    return ValidationError(field=field, message=rule.message, value=value, rule=rule.rule)
            except Exception as e:
                return ValidationError(field=field, message=str(e), value=value, rule=rule.rule)
                
        return None


class DataSanitizer:
    """
    Data sanitization utilities for security.
    
    Example:
        sanitizer = DataSanitizer()
        
        # Remove HTML tags
        clean = sanitizer.strip_html("<script>alert('xss')</script>")
        
        # Escape SQL
        safe = sanitizer.escape_sql("'; DROP TABLE users; --")
    """
    
    @staticmethod
    def strip_html(text: str) -> str:
        """Remove HTML tags from text."""
        if not isinstance(text, str):
            return text
        return re.sub(r"<[^>]+>", "", text)
        
    @staticmethod
    def escape_sql(text: str) -> str:
        """Escape SQL special characters."""
        if not isinstance(text, str):
            return text
        replacements = {
            "'": "''",
            "\\": "\\\\",
            "\n": "\\n",
            "\r": "\\r",
            "\x1a": "\\Z",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
        
    @staticmethod
    def escape_json(text: str) -> str:
        """Escape JSON special characters."""
        if not isinstance(text, str):
            return text
        replacements = {
            '"': '\\"',
            "\\": "\\\\",
            "\n": "\\n",
            "\r": "\\r",
            "\t": "\\t",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text
        
    @staticmethod
    def truncate(text: str, max_length: int, suffix: str = "...") -> str:
        """Truncate text to max length."""
        if not isinstance(text, str):
            return text
        if len(text) <= max_length:
            return text
        return text[:max_length - len(suffix)] + suffix
        
    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Normalize whitespace in text."""
        if not isinstance(text, str):
            return text
        return " ".join(text.split())
        
    @staticmethod
    def remove_control_chars(text: str) -> str:
        """Remove control characters from text."""
        if not isinstance(text, str):
            return text
        return "".join(char for char in text if ord(char) >= 32 or char in "\n\r\t")
