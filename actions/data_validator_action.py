"""
Data Validator Module.

Provides comprehensive data validation with schema support,
cross-field validation, and custom validator patterns.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar, Generic
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ValidationLevel(Enum):
    """Validation level."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Single validation issue."""
    field: str
    message: str
    level: ValidationLevel
    code: str
    value: Any = None
    constraints: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        return f"[{self.level.value}] {self.field}: {self.message}"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "field": self.field,
            "message": self.message,
            "level": self.level.value,
            "code": self.code,
            "value": self.value,
            "constraints": self.constraints,
        }


@dataclass
class ValidationReport:
    """Complete validation report."""
    valid: bool
    errors: List[ValidationIssue]
    warnings: List[ValidationIssue]
    infos: List[ValidationIssue]
    validated_data: Optional[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def error_count(self) -> int:
        return len(self.errors)
    
    @property
    def warning_count(self) -> int:
        return len(self.warnings)
    
    @property
    def issues(self) -> List[ValidationIssue]:
        return self.errors + self.warnings + self.infos


class FieldValidator:
    """Validator for a single field."""
    
    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self._rules: List[Tuple[str, Callable, Any, str]] = []
        
    def required(self, message: Optional[str] = None) -> "FieldValidator":
        """Mark field as required."""
        self._rules.append((
            "required",
            lambda v: v is not None and v != "",
            True,
            message or f"{self.field_name} is required"
        ))
        return self
        
    def type(self, expected_type: type, message: Optional[str] = None) -> "FieldValidator":
        """Validate field type."""
        def check_type(value: Any) -> bool:
            if expected_type == int:
                return isinstance(value, int) and not isinstance(value, bool)
            elif expected_type == float:
                return isinstance(value, (int, float)) and not isinstance(value, bool)
            return isinstance(value, expected_type)
            
        self._rules.append((
            "type",
            check_type,
            expected_type,
            message or f"{self.field_name} must be of type {expected_type.__name__}"
        ))
        return self
        
    def min_length(self, min_len: int, message: Optional[str] = None) -> "FieldValidator":
        """Validate minimum length."""
        self._rules.append((
            "min_length",
            lambda v: v is None or len(v) >= min_len,
            min_len,
            message or f"{self.field_name} must be at least {min_len} characters"
        ))
        return self
        
    def max_length(self, max_len: int, message: Optional[str] = None) -> "FieldValidator":
        """Validate maximum length."""
        self._rules.append((
            "max_length",
            lambda v: v is None or len(v) <= max_len,
            max_len,
            message or f"{self.field_name} must be at most {max_len} characters"
        ))
        return self
        
    def min_value(self, min_val: Any, message: Optional[str] = None) -> "FieldValidator":
        """Validate minimum value."""
        self._rules.append((
            "min_value",
            lambda v: v is None or v >= min_val,
            min_val,
            message or f"{self.field_name} must be at least {min_val}"
        ))
        return self
        
    def max_value(self, max_val: Any, message: Optional[str] = None) -> "FieldValidator":
        """Validate maximum value."""
        self._rules.append((
            "max_value",
            lambda v: v is None or v <= max_val,
            max_val,
            message or f"{self.field_name} must be at most {max_val}"
        ))
        return self
        
    def pattern(self, regex: str, message: Optional[str] = None) -> "FieldValidator":
        """Validate against regex pattern."""
        compiled = re.compile(regex)
        self._rules.append((
            "pattern",
            lambda v: v is None or compiled.match(str(v)),
            regex,
            message or f"{self.field_name} does not match required pattern"
        ))
        return self
        
    def email(self, message: Optional[str] = None) -> "FieldValidator":
        """Validate email format."""
        email_pattern = r"^[\w.-]+@[\w.-]+\.\w+$"
        return self.pattern(email_pattern, message or f"{self.field_name} must be a valid email")
        
    def url(self, message: Optional[str] = None) -> "FieldValidator":
        """Validate URL format."""
        url_pattern = r"^https?://[\w.-]+\.[a-z]{2,}.*$"
        return self.pattern(url_pattern, message or f"{self.field_name} must be a valid URL")
        
    def enum(self, allowed: List[Any], message: Optional[str] = None) -> "FieldValidator":
        """Validate against allowed values."""
        self._rules.append((
            "enum",
            lambda v: v is None or v in allowed,
            allowed,
            message or f"{self.field_name} must be one of {allowed}"
        ))
        return self
        
    def custom(
        self,
        validator: Callable[[Any], bool],
        message: str,
    ) -> "FieldValidator":
        """Add custom validation function."""
        self._rules.append((
            "custom",
            validator,
            None,
            message
        ))
        return self
        
    def validate(self, value: Any, level: ValidationLevel = ValidationLevel.ERROR) -> List[ValidationIssue]:
        """Validate field value."""
        issues = []
        
        for code, rule, constraint, message in self._rules:
            if code == "required":
                if not rule(value):
                    issues.append(ValidationIssue(
                        field=self.field_name,
                        message=message,
                        level=level,
                        code=code,
                        value=value,
                    ))
            else:
                if value is not None:  # Skip non-required empty fields
                    if not rule(value):
                        issues.append(ValidationIssue(
                            field=self.field_name,
                            message=message,
                            level=level,
                            code=code,
                            value=value,
                            constraints={"constraint": constraint} if constraint else {},
                        ))
                        
        return issues


class DataValidator:
    """
    Comprehensive data validator with schema support.
    
    Example:
        validator = DataValidator()
        
        # Add field validators
        validator.field("name").required().min_length(2).max_length(100)
        validator.field("email").required().email()
        validator.field("age").type(int).min_value(0).max_value(150)
        
        # Add cross-field validation
        validator.add_cross_field(
            lambda d: d.get("password") == d.get("confirm_password"),
            "passwords must match"
        )
        
        # Validate
        report = validator.validate(data)
    """
    
    def __init__(self) -> None:
        """Initialize data validator."""
        self._field_validators: Dict[str, FieldValidator] = {}
        self._cross_field_validators: List[Tuple[Callable[[Dict], bool], str]] = []
        self._transforms: Dict[str, Callable[[Any], Any]] = {}
        
    def field(self, field_name: str) -> FieldValidator:
        """
        Get or create field validator.
        
        Args:
            field_name: Name of field to validate.
            
        Returns:
            FieldValidator for chaining.
        """
        if field_name not in self._field_validators:
            self._field_validators[field_name] = FieldValidator(field_name)
        return self._field_validators[field_name]
        
    def add_cross_field(
        self,
        validator: Callable[[Dict[str, Any]], bool],
        message: str,
    ) -> "DataValidator":
        """
        Add cross-field validation.
        
        Args:
            validator: Function that takes full data dict and returns bool.
            message: Error message if validation fails.
            
        Returns:
            Self for chaining.
        """
        self._cross_field_validators.append((validator, message))
        return self
        
    def transform(self, field_name: str, transform: Callable[[Any], Any]) -> "DataValidator":
        """
        Add data transformation.
        
        Args:
            field_name: Field to transform.
            transform: Transformation function.
            
        Returns:
            Self for chaining.
        """
        self._transforms[field_name] = transform
        return self
        
    def validate(
        self,
        data: Dict[str, Any],
        error_level: ValidationLevel = ValidationLevel.ERROR,
    ) -> ValidationReport:
        """
        Validate data against all rules.
        
        Args:
            data: Data to validate.
            error_level: Level for validation issues.
            
        Returns:
            ValidationReport with results.
        """
        errors = []
        warnings = []
        infos = []
        validated_data = {}
        
        # Apply transforms and validate fields
        for field_name, validator in self._field_validators.items():
            value = data.get(field_name)
            
            # Apply transform if defined
            if field_name in self._transforms and value is not None:
                try:
                    value = self._transforms[field_name](value)
                    validated_data[field_name] = value
                except Exception as e:
                    errors.append(ValidationIssue(
                        field=field_name,
                        message=f"Transform failed: {e}",
                        level=ValidationLevel.ERROR,
                        code="transform",
                        value=value,
                    ))
                    continue
            else:
                validated_data[field_name] = value
                
            # Validate
            issues = validator.validate(value, error_level)
            for issue in issues:
                if issue.level == ValidationLevel.ERROR:
                    errors.append(issue)
                elif issue.level == ValidationLevel.WARNING:
                    warnings.append(issue)
                else:
                    infos.append(issue)
                    
        # Cross-field validation
        for validator, message in self._cross_field_validators:
            try:
                if not validator(validated_data):
                    errors.append(ValidationIssue(
                        field="*",
                        message=message,
                        level=error_level,
                        code="cross_field",
                    ))
            except Exception as e:
                errors.append(ValidationIssue(
                    field="*",
                    message=f"Cross-field validation error: {e}",
                    level=error_level,
                    code="cross_field_error",
                ))
                
        return ValidationReport(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            infos=infos,
            validated_data=validated_data if len(errors) == 0 else None,
        )
        
    def validate_partial(
        self,
        data: Dict[str, Any],
        required_fields: Optional[List[str]] = None,
    ) -> ValidationReport:
        """
        Validate only specified fields (for partial updates).
        
        Args:
            data: Data to validate.
            required_fields: List of required field names.
            
        Returns:
            ValidationReport.
        """
        required_fields = required_fields or list(data.keys())
        original_validators = self._field_validators.copy()
        
        # Temporarily modify validators
        for field_name, validator in self._field_validators.items():
            if field_name not in required_fields:
                # Remove required rule if field not in required_fields
                validator._rules = [
                    r for r in validator._rules
                    if r[0] != "required"
                ]
                
        result = self.validate(data)
        
        # Restore original validators
        self._field_validators = original_validators
        
        return result


class SchemaValidator:
    """
    JSON Schema-like validator.
    
    Example:
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "required": True},
                "age": {"type": "integer", "min": 0},
                "email": {"type": "string", "format": "email"},
            },
            "required": ["name"]
        }
        
        validator = SchemaValidator(schema)
        report = validator.validate(data)
    """
    
    def __init__(self, schema: Dict[str, Any]) -> None:
        """
        Initialize schema validator.
        
        Args:
            schema: JSON Schema-like definition.
        """
        self.schema = schema
        
    def validate(self, data: Any) -> ValidationReport:
        """
        Validate data against schema.
        
        Args:
            data: Data to validate.
            
        Returns:
            ValidationReport.
        """
        issues = []
        
        try:
            self._validate_value(data, self.schema, "", issues)
        except Exception as e:
            issues.append(ValidationIssue(
                field="",
                message=f"Validation error: {e}",
                level=ValidationLevel.ERROR,
                code="schema_error",
            ))
            
        return ValidationReport(
            valid=len([i for i in issues if i.level == ValidationLevel.ERROR]) == 0,
            errors=[i for i in issues if i.level == ValidationLevel.ERROR],
            warnings=[i for i in issues if i.level == ValidationLevel.WARNING],
            infos=[i for i in issues if i.level == ValidationLevel.INFO],
            validated_data=data if len([i for i in issues if i.level == ValidationLevel.ERROR]) == 0 else None,
        )
        
    def _validate_value(
        self,
        value: Any,
        schema: Dict[str, Any],
        path: str,
        issues: List[ValidationIssue],
    ) -> None:
        """Recursively validate value against schema."""
        schema_type = schema.get("type")
        
        if schema_type == "object":
            if not isinstance(value, dict):
                issues.append(ValidationIssue(
                    field=path or "root",
                    message="Expected object",
                    level=ValidationLevel.ERROR,
                    code="type_mismatch",
                    value=type(value).__name__,
                ))
                return
                
            # Check required fields
            for required in schema.get("required", []):
                if required not in value:
                    issues.append(ValidationIssue(
                        field=f"{path}.{required}" if path else required,
                        message="Required field missing",
                        level=ValidationLevel.ERROR,
                        code="required",
                    ))
                    
            # Validate properties
            for prop_name, prop_schema in schema.get("properties", {}).items():
                if prop_name in value:
                    self._validate_value(
                        value[prop_name],
                        prop_schema,
                        f"{path}.{prop_name}" if path else prop_name,
                        issues
                    )
                    
        elif schema_type == "array":
            if not isinstance(value, list):
                issues.append(ValidationIssue(
                    field=path or "root",
                    message="Expected array",
                    level=ValidationLevel.ERROR,
                    code="type_mismatch",
                    value=type(value).__name__,
                ))
                return
                
            items_schema = schema.get("items", {})
            for i, item in enumerate(value):
                self._validate_value(item, items_schema, f"{path}[{i}]", issues)
                
        elif schema_type == "string":
            if not isinstance(value, str):
                issues.append(ValidationIssue(
                    field=path or "root",
                    message="Expected string",
                    level=ValidationLevel.ERROR,
                    code="type_mismatch",
                ))
                return
                
            if "minLength" in schema and len(value) < schema["minLength"]:
                issues.append(ValidationIssue(
                    field=path or "root",
                    message=f"String too short (min: {schema['minLength']})",
                    level=ValidationLevel.ERROR,
                    code="minLength",
                ))
                
            if "maxLength" in schema and len(value) > schema["maxLength"]:
                issues.append(ValidationIssue(
                    field=path or "root",
                    message=f"String too long (max: {schema['maxLength']})",
                    level=ValidationLevel.ERROR,
                    code="maxLength",
                ))
                
            if "pattern" in schema:
                if not re.match(schema["pattern"], value):
                    issues.append(ValidationIssue(
                        field=path or "root",
                        message="Does not match pattern",
                        level=ValidationLevel.ERROR,
                        code="pattern",
                    ))
                    
            if "format" in schema:
                if schema["format"] == "email":
                    if not re.match(r"^[\w.-]+@[\w.-]+\.\w+$", value):
                        issues.append(ValidationIssue(
                            field=path or "root",
                            message="Invalid email format",
                            level=ValidationLevel.ERROR,
                            code="email",
                        ))
                elif schema["format"] == "uri":
                    if not re.match(r"^https?://", value):
                        issues.append(ValidationIssue(
                            field=path or "root",
                            message="Invalid URI format",
                            level=ValidationLevel.ERROR,
                            code="uri",
                        ))
                        
        elif schema_type in ("integer", "number"):
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                issues.append(ValidationIssue(
                    field=path or "root",
                    message=f"Expected number, got {type(value).__name__}",
                    level=ValidationLevel.ERROR,
                    code="type_mismatch",
                ))
                return
                
            if "minimum" in schema and value < schema["minimum"]:
                issues.append(ValidationIssue(
                    field=path or "root",
                    message=f"Value too small (min: {schema['minimum']})",
                    level=ValidationLevel.ERROR,
                    code="minimum",
                ))
                
            if "maximum" in schema and value > schema["maximum"]:
                issues.append(ValidationIssue(
                    field=path or "root",
                    message=f"Value too large (max: {schema['maximum']})",
                    level=ValidationLevel.ERROR,
                    code="maximum",
                ))
                
        elif schema_type == "boolean":
            if not isinstance(value, bool):
                issues.append(ValidationIssue(
                    field=path or "root",
                    message="Expected boolean",
                    level=ValidationLevel.ERROR,
                    code="type_mismatch",
                ))
