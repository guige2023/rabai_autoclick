"""
Data Validator Action Module

Schema-based data validation with custom rules,
type checking, and comprehensive error reporting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Validation types."""
    
    REQUIRED = "required"
    TYPE = "type"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"


@dataclass
class ValidationRule:
    """A single validation rule."""
    
    field: str
    validation_type: ValidationType
    value: Any = None
    message: Optional[str] = None
    validator: Optional[Callable] = None


@dataclass
class ValidationError:
    """A single validation error."""
    
    field: str
    message: str
    value: Any = None
    rule: Optional[ValidationType] = None


@dataclass
class ValidationResult:
    """Result of validation."""
    
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: float = field(default_factory=datetime.now().timestamp)


class TypeValidator:
    """Validates data types."""
    
    TYPE_MAP = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
        "list_str": lambda x: isinstance(x, list) and all(isinstance(i, str) for i in x),
        "list_int": lambda x: isinstance(x, list) and all(isinstance(i, int) for i in x),
        "list_dict": lambda x: isinstance(x, list) and all(isinstance(i, dict) for i in x),
        "datetime": lambda x: isinstance(x, (datetime, str)) or (
            isinstance(x, (int, float)) and x > 0
        ),
        "email": lambda x: bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", str(x))),
        "url": lambda x: bool(re.match(r"^https?://", str(x))),
        "uuid": lambda x: bool(re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            str(x).lower()
        ))
    }
    
    @classmethod
    def validate(cls, value: Any, type_name: str) -> bool:
        """Validate value against type name."""
        validator = cls.TYPE_MAP.get(type_name)
        if validator:
            try:
                return validator(value)
            except Exception:
                return False
        return True


class DataValidator:
    """Core data validation logic."""
    
    def __init__(self, rules: List[ValidationRule]):
        self.rules = rules
        self._type_validator = TypeValidator()
    
    def validate(self, data: Dict) -> ValidationResult:
        """Validate data against rules."""
        errors = []
        warnings = []
        
        for rule in self.rules:
            error = self._validate_rule(rule, data)
            if error:
                errors.append(error)
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _validate_rule(self, rule: ValidationRule, data: Dict) -> Optional[ValidationError]:
        """Validate a single rule."""
        value = self._get_nested_value(data, rule.field)
        
        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or value == "":
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} is required",
                    value=value,
                    rule=rule.validation_type
                )
        
        if value is None:
            return None
        
        if rule.validation_type == ValidationType.TYPE:
            if not TypeValidator.validate(value, rule.value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be of type {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.MIN:
            if isinstance(value, (int, float)) and value < rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be >= {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.MAX:
            if isinstance(value, (int, float)) and value > rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be <= {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.MIN_LENGTH:
            if hasattr(value, "__len__") and len(value) < rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} length must be >= {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.MAX_LENGTH:
            if hasattr(value, "__len__") and len(value) > rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} length must be <= {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.PATTERN:
            if not re.match(rule.value, str(value)):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} does not match pattern",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.ENUM:
            if value not in rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be one of {rule.value}",
                    value=value,
                    rule=rule.validation_type
                )
        
        elif rule.validation_type == ValidationType.CUSTOM and rule.validator:
            try:
                if not rule.validator(value, data):
                    return ValidationError(
                        field=rule.field,
                        message=rule.message or f"{rule.field} failed custom validation",
                        value=value,
                        rule=rule.validation_type
                    )
            except Exception as e:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} validation error: {str(e)}",
                    value=value,
                    rule=rule.validation_type
                )
        
        return None
    
    def _get_nested_value(self, data: Dict, path: str) -> Any:
        """Get nested value from path."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value


class DataValidatorAction:
    """
    Main data validator action handler.
    
    Provides schema-based validation with custom rules,
    type checking, and comprehensive error reporting.
    """
    
    def __init__(self):
        self._validators: Dict[str, List[ValidationRule]] = defaultdict(list)
        self._middleware: List[Callable] = []
    
    def define_schema(
        self,
        schema_name: str,
        rules: List[Dict]
    ) -> None:
        """Define a validation schema."""
        validation_rules = []
        
        for rule_config in rules:
            rule = ValidationRule(
                field=rule_config["field"],
                validation_type=ValidationType(rule_config["type"]),
                value=rule_config.get("value"),
                message=rule_config.get("message"),
                validator=rule_config.get("validator")
            )
            validation_rules.append(rule)
        
        self._validators[schema_name] = validation_rules
    
    def add_rule(
        self,
        schema_name: str,
        field: str,
        validation_type: Union[ValidationType, str],
        value: Any = None,
        message: Optional[str] = None
    ) -> None:
        """Add a validation rule to a schema."""
        if isinstance(validation_type, str):
            validation_type = ValidationType(validation_type)
        
        rule = ValidationRule(
            field=field,
            validation_type=validation_type,
            value=value,
            message=message
        )
        
        self._validators[schema_name].append(rule)
    
    def validate(
        self,
        schema_name: str,
        data: Dict
    ) -> ValidationResult:
        """Validate data against a schema."""
        rules = self._validators.get(schema_name, [])
        validator = DataValidator(rules)
        result = validator.validate(data)
        
        for mw in self._middleware:
            mw(schema_name, result)
        
        return result
    
    def validate_batch(
        self,
        schema_name: str,
        records: List[Dict]
    ) -> List[ValidationResult]:
        """Validate multiple records."""
        return [
            self.validate(schema_name, record)
            for record in records
        ]
    
    def is_valid(
        self,
        schema_name: str,
        data: Dict
    ) -> bool:
        """Quick check if data is valid."""
        result = self.validate(schema_name, data)
        return result.valid
    
    def get_errors_dict(
        self,
        result: ValidationResult
    ) -> Dict[str, str]:
        """Convert validation errors to dictionary."""
        return {
            error.field: error.message
            for error in result.errors
        }
    
    def list_schemas(self) -> List[str]:
        """List all defined schemas."""
        return list(self._validators.keys())
    
    def add_middleware(self, func: Callable) -> None:
        """Add validation middleware."""
        self._middleware.append(func)
