"""
API Request Validation Action Module.

Provides comprehensive request validation for API endpoints including
schema validation, parameter type checking, and constraint verification.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import re
import json
from datetime import datetime, timedelta


class ValidationLevel(Enum):
    """Validation strictness levels."""
    LENIENT = "lenient"
    MODERATE = "moderate"
    STRICT = "strict"


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


@dataclass
class ValidationRule:
    """Represents a single validation rule."""
    field: str
    rule_type: str
    constraint: Any
    message: str
    required: bool = True


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)


class SchemaValidator:
    """Validates data against JSON schemas."""
    
    TYPE_MAPPING = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None),
    }
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against schema."""
        errors = []
        warnings = []
        
        # Check required fields
        required = self.schema.get("required", [])
        for field in required:
            if field not in data:
                errors.append(f"Missing required field: {field}")
        
        # Check properties
        properties = self.schema.get("properties", {})
        for field, field_spec in properties.items():
            if field in data:
                field_errors = self._validate_field(field, data[field], field_spec)
                errors.extend(field_errors)
        
        # Check additional properties
        additional_props = self.schema.get("additionalProperties", True)
        if not additional_props:
            extra = set(data.keys()) - set(properties.keys())
            if extra:
                errors.append(f"Additional properties not allowed: {extra}")
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _validate_field(self, field: str, value: Any, spec: Dict) -> List[str]:
        """Validate a single field."""
        errors = []
        
        # Type validation
        expected_type = spec.get("type")
        if expected_type:
            type_class = self.TYPE_MAPPING.get(expected_type)
            if type_class and not isinstance(value, type_class):
                errors.append(
                    f"Field '{field}' expected type {expected_type}, "
                    f"got {type(value).__name__}"
                )
        
        # String constraints
        if isinstance(value, str):
            if "minLength" in spec and len(value) < spec["minLength"]:
                errors.append(
                    f"Field '{field}' must be at least {spec['minLength']} characters"
                )
            if "maxLength" in spec and len(value) > spec["maxLength"]:
                errors.append(
                    f"Field '{field}' must be at most {spec['maxLength']} characters"
                )
            if "pattern" in spec:
                pattern = re.compile(spec["pattern"])
                if not pattern.match(value):
                    errors.append(
                        f"Field '{field}' does not match pattern: {spec['pattern']}"
                    )
        
        # Number constraints
        if isinstance(value, (int, float)):
            if "minimum" in spec and value < spec["minimum"]:
                errors.append(
                    f"Field '{field}' must be >= {spec['minimum']}"
                )
            if "maximum" in spec and value > spec["maximum"]:
                errors.append(
                    f"Field '{field}' must be <= {spec['maximum']}"
                )
        
        # Array constraints
        if isinstance(value, list):
            if "minItems" in spec and len(value) < spec["minItems"]:
                errors.append(
                    f"Field '{field}' must have at least {spec['minItems']} items"
                )
            if "maxItems" in spec and len(value) > spec["maxItems"]:
                errors.append(
                    f"Field '{field}' must have at most {spec['maxItems']} items"
                )
            if "uniqueItems" in spec and spec["uniqueItems"] and len(value) != len(set(str(v) for v in value)):
                errors.append(f"Field '{field}' must have unique items")
        
        # Enum validation
        if "enum" in spec and value not in spec["enum"]:
            errors.append(
                f"Field '{field}' must be one of: {spec['enum']}"
            )
        
        return errors


class RequestValidator:
    """
    Main request validation handler.
    
    Example:
        validator = RequestValidator(level=ValidationLevel.STRICT)
        validator.add_rule(ValidationRule("user_id", "type", int, "user_id must be int"))
        validator.add_rule(ValidationRule("email", "pattern", r"^[^@]+@[^@]+$", "Invalid email"))
        result = validator.validate({"user_id": 123, "email": "test@example.com"})
    """
    
    def __init__(self, level: ValidationLevel = ValidationLevel.MODERATE):
        self.level = level
        self.rules: List[ValidationRule] = []
        self._schema_validator: Optional[SchemaValidator] = None
    
    def add_rule(self, rule: ValidationRule) -> "RequestValidator":
        """Add a validation rule."""
        self.rules.append(rule)
        return self
    
    def add_schema(self, schema: Dict[str, Any]) -> "RequestValidator":
        """Add a JSON schema for validation."""
        self._schema_validator = SchemaValidator(schema)
        return self
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate request data against all rules."""
        all_errors = []
        all_warnings = []
        
        # Schema validation
        if self._schema_validator:
            schema_result = self._schema_validator.validate(data)
            all_errors.extend(schema_result.errors)
            all_warnings.extend(schema_result.warnings)
        
        # Rule-based validation
        for rule in self.rules:
            field_value = data.get(rule.field)
            
            # Required field check
            if rule.required and field_value is None:
                all_errors.append(f"Missing required field: {rule.field}")
                continue
            
            if field_value is None:
                continue
            
            # Type-based validation
            if rule.rule_type == "type":
                if not isinstance(field_value, rule.constraint):
                    all_errors.append(
                        f"Field '{rule.field}' {rule.message}"
                    )
            
            # Pattern validation
            elif rule.rule_type == "pattern":
                if not isinstance(field_value, str):
                    all_errors.append(
                        f"Field '{rule.field}' must be string for pattern validation"
                    )
                elif not re.match(rule.constraint, field_value):
                    all_errors.append(
                        f"Field '{rule.field}' {rule.message}"
                    )
            
            # Range validation
            elif rule.rule_type == "range":
                min_val, max_val = rule.constraint
                if not (min_val <= field_value <= max_val):
                    all_errors.append(
                        f"Field '{rule.field}' {rule.message}"
                    )
            
            # Custom validator
            elif rule.rule_type == "custom":
                validator_fn: Callable = rule.constraint
                if not validator_fn(field_value):
                    all_errors.append(
                        f"Field '{rule.field}' {rule.message}"
                    )
            
            # Length validation
            elif rule.rule_type == "length":
                min_len, max_len = rule.constraint
                if not (min_len <= len(field_value) <= max_len):
                    all_errors.append(
                        f"Field '{rule.field}' {rule.message}"
                    )
        
        # Apply lenient mode filtering
        if self.level == ValidationLevel.LENIENT:
            # Only fail on critical errors
            critical = [e for e in all_errors if "required" in e.lower()]
            all_errors = critical
        
        return ValidationResult(
            valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=all_warnings
        )
    
    def validate_with_defaults(
        self,
        data: Dict[str, Any],
        defaults: Dict[str, Any]
    ) -> tuple[Dict[str, Any], ValidationResult]:
        """Validate and fill in defaults for missing fields."""
        merged = {**defaults, **data}
        result = self.validate(merged)
        return merged, result


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        """Execute the action."""
        raise NotImplementedError


class APIRequestValidationAction(BaseAction):
    """
    Validates API requests against schemas and rules.
    
    Parameters:
        schema: JSON schema definition
        level: Validation strictness (lenient/moderate/strict)
        rules: Additional validation rules
    
    Example:
        action = APIRequestValidationAction()
        result = action.execute({}, {
            "schema": {"type": "object", "properties": {"name": {"type": "string"}}},
            "data": {"name": "test"}
        })
    """
    
    def __init__(self):
        self.validator: Optional[RequestValidator] = None
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute request validation."""
        schema = params.get("schema")
        level_str = params.get("level", "moderate")
        rules = params.get("rules", [])
        data = params.get("data", {})
        
        level = ValidationLevel(level_str)
        validator = RequestValidator(level=level)
        
        if schema:
            validator.add_schema(schema)
        
        for rule_spec in rules:
            rule = ValidationRule(
                field=rule_spec["field"],
                rule_type=rule_spec["type"],
                constraint=rule_spec["constraint"],
                message=rule_spec.get("message", "validation failed"),
                required=rule_spec.get("required", True)
            )
            validator.add_rule(rule)
        
        result = validator.validate(data)
        
        return {
            "valid": result.valid,
            "errors": result.errors,
            "warnings": result.warnings,
            "validated_at": result.validated_at.isoformat()
        }
