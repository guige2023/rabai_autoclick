"""Data validator action module for RabAI AutoClick.

Provides data validation operations:
- DataValidator: General data validator
- SchemaValidator: Validate against schemas
- TypeValidator: Type checking validator
- RangeValidator: Range validation
- PatternValidator: Pattern/regex validation
- CustomValidator: Custom validation rules
"""

import re
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidationType(Enum):
    """Validation types."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    LENGTH = "length"
    ENUM = "enum"
    CUSTOM = "custom"
    SCHEMA = "schema"


@dataclass
class ValidationRule:
    """Validation rule definition."""
    field: str
    validation_type: ValidationType
    constraint: Any = None
    message: str = ""
    required: bool = True


@dataclass
class ValidationError:
    """Validation error."""
    field: str
    message: str
    value: Any = None
    rule: Optional[ValidationRule] = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[ValidationError]
    warnings: List[str]
    validated_at: float


class SchemaValidator:
    """Validate data against schema."""

    def __init__(self, schema: Dict[str, ValidationRule]):
        self.schema = schema

    def validate(self, data: Dict) -> Tuple[bool, List[ValidationError]]:
        """Validate data against schema."""
        errors = []

        for field, rule in self.schema.items():
            value = data.get(field)

            if rule.required and value is None:
                errors.append(ValidationError(
                    field=field,
                    message=rule.message or f"Field '{field}' is required",
                    value=value,
                    rule=rule,
                ))
                continue

            if value is None:
                continue

            if rule.validation_type == ValidationType.TYPE:
                if not self._validate_type(value, rule.constraint):
                    errors.append(ValidationError(
                        field=field,
                        message=rule.message or f"Field '{field}' must be of type {rule.constraint}",
                        value=value,
                        rule=rule,
                    ))

            elif rule.validation_type == ValidationType.RANGE:
                if not self._validate_range(value, rule.constraint):
                    errors.append(ValidationError(
                        field=field,
                        message=rule.message or f"Field '{field}' out of range",
                        value=value,
                        rule=rule,
                    ))

            elif rule.validation_type == ValidationType.PATTERN:
                if not self._validate_pattern(value, rule.constraint):
                    errors.append(ValidationError(
                        field=field,
                        message=rule.message or f"Field '{field}' does not match pattern",
                        value=value,
                        rule=rule,
                    ))

            elif rule.validation_type == ValidationType.LENGTH:
                if not self._validate_length(value, rule.constraint):
                    errors.append(ValidationError(
                        field=field,
                        message=rule.message or f"Field '{field}' length invalid",
                        value=value,
                        rule=rule,
                    ))

            elif rule.validation_type == ValidationType.ENUM:
                if not self._validate_enum(value, rule.constraint):
                    errors.append(ValidationError(
                        field=field,
                        message=rule.message or f"Field '{field}' not in allowed values",
                        value=value,
                        rule=rule,
                    ))

        return len(errors) == 0, errors

    def _validate_type(self, value: Any, expected_type: type) -> bool:
        """Validate type."""
        return isinstance(value, expected_type)

    def _validate_range(self, value: Union[int, float], constraint: Dict) -> bool:
        """Validate numeric range."""
        if not isinstance(value, (int, float)):
            return False
        min_val = constraint.get("min")
        max_val = constraint.get("max")
        if min_val is not None and value < min_val:
            return False
        if max_val is not None and value > max_val:
            return False
        return True

    def _validate_pattern(self, value: str, pattern: str) -> bool:
        """Validate pattern."""
        if not isinstance(value, str):
            return False
        return bool(re.match(pattern, value))

    def _validate_length(self, value: Any, constraint: Dict) -> bool:
        """Validate length."""
        length = len(value)
        min_len = constraint.get("min")
        max_len = constraint.get("max")
        if min_len is not None and length < min_len:
            return False
        if max_len is not None and length > max_len:
            return False
        return True

    def _validate_enum(self, value: Any, allowed: List) -> bool:
        """Validate enum."""
        return value in allowed


class TypeValidator:
    """Type-based validator."""

    @staticmethod
    def is_string(value: Any) -> bool:
        return isinstance(value, str)

    @staticmethod
    def is_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    @staticmethod
    def is_float(value: Any) -> bool:
        return isinstance(value, float)

    @staticmethod
    def is_number(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def is_bool(value: Any) -> bool:
        return isinstance(value, bool)

    @staticmethod
    def is_list(value: Any) -> bool:
        return isinstance(value, list)

    @staticmethod
    def is_dict(value: Any) -> bool:
        return isinstance(value, dict)

    @staticmethod
    def is_email(value: str) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, value))

    @staticmethod
    def is_url(value: str) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        return bool(re.match(pattern, value))

    @staticmethod
    def is_uuid(value: str) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(pattern, value.lower()))


class RangeValidator:
    """Range validator."""

    @staticmethod
    def validate_min(value: Union[int, float], min_val: float) -> bool:
        """Validate minimum."""
        return value >= min_val

    @staticmethod
    def validate_max(value: Union[int, float], max_val: float) -> bool:
        """Validate maximum."""
        return value <= max_val

    @staticmethod
    def validate_range(value: Union[int, float], min_val: float, max_val: float) -> bool:
        """Validate range."""
        return min_val <= value <= max_val

    @staticmethod
    def validate_positive(value: Union[int, float]) -> bool:
        """Validate positive."""
        return value > 0

    @staticmethod
    def validate_non_negative(value: Union[int, float]) -> bool:
        """Validate non-negative."""
        return value >= 0


class PatternValidator:
    """Pattern-based validator."""

    def __init__(self):
        self._patterns: Dict[str, str] = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "url": r"^https?://[^\s/$.?#].[^\s]*$",
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "phone": r"^\+?[1-9]\d{1,14}$",
            "ipv4": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
            "date": r"^\d{4}-\d{2}-\d{2}$",
            "datetime": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",
        }

    def register_pattern(self, name: str, pattern: str):
        """Register a custom pattern."""
        self._patterns[name] = pattern

    def validate(self, value: str, pattern: Union[str, re.Pattern]) -> bool:
        """Validate against pattern."""
        if isinstance(pattern, str):
            if pattern in self._patterns:
                pattern = self._patterns[pattern]
            pattern = re.compile(pattern)
        return bool(pattern.match(str(value)))


class CustomValidator:
    """Custom validation rules."""

    def __init__(self):
        self._validators: Dict[str, Callable[[Any], Tuple[bool, str]]] = {}

    def register(self, name: str, validator: Callable[[Any], Tuple[bool, str]]):
        """Register a custom validator.

        Args:
            name: Validator name
            validator: Function that takes value and returns (valid, message)
        """
        self._validators[name] = validator

    def validate(self, name: str, value: Any) -> Tuple[bool, str]:
        """Validate using custom validator."""
        if name not in self._validators:
            return False, f"Validator '{name}' not found"
        return self._validators[name](value)


class DataValidator:
    """Main data validator combining all validators."""

    def __init__(self):
        self.schema_validator = SchemaValidator({})
        self.type_validator = TypeValidator()
        self.range_validator = RangeValidator()
        self.pattern_validator = PatternValidator()
        self.custom_validator = CustomValidator()

    def validate(
        self,
        data: Dict,
        rules: List[ValidationRule],
    ) -> ValidationResult:
        """Validate data against rules."""
        errors = []
        warnings = []

        for rule in rules:
            value = data.get(rule.field)

            if rule.required and value is None:
                errors.append(ValidationError(
                    field=rule.field,
                    message=rule.message or f"Field '{rule.field}' is required",
                    value=value,
                    rule=rule,
                ))
                continue

            if value is None:
                continue

            valid, error_msg = self._validate_rule(rule, value)
            if not valid:
                errors.append(ValidationError(
                    field=rule.field,
                    message=error_msg or f"Validation failed for '{rule.field}'",
                    value=value,
                    rule=rule,
                ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            validated_at=time.time(),
        )

    def _validate_rule(self, rule: ValidationRule, value: Any) -> Tuple[bool, str]:
        """Validate single rule."""
        if rule.validation_type == ValidationType.TYPE:
            if not self.type_validator.is_string(value) if rule.constraint == str else \
               not self.type_validator.is_int(value) if rule.constraint == int else \
               not self.type_validator.is_float(value) if rule.constraint == float else \
               not self.type_validator.is_dict(value) if rule.constraint == dict else \
               not self.type_validator.is_list(value) if rule.constraint == list else True:
                return False, f"Expected type {rule.constraint.__name__}"
            return True, ""

        elif rule.validation_type == ValidationType.RANGE:
            if not self.range_validator.validate_range(value, rule.constraint.get("min", 0), rule.constraint.get("max", float("inf"))):
                return False, f"Value out of range"
            return True, ""

        elif rule.validation_type == ValidationType.PATTERN:
            if not self.pattern_validator.validate(value, rule.constraint):
                return False, f"Pattern mismatch"
            return True, ""

        elif rule.validation_type == ValidationType.LENGTH:
            min_len = rule.constraint.get("min", 0)
            max_len = rule.constraint.get("max", float("inf"))
            length = len(value)
            if length < min_len or length > max_len:
                return False, f"Length must be between {min_len} and {max_len}"
            return True, ""

        elif rule.validation_type == ValidationType.ENUM:
            if value not in rule.constraint:
                return False, f"Value not in allowed values"
            return True, ""

        elif rule.validation_type == ValidationType.CUSTOM:
            valid, msg = self.custom_validator.validate(rule.constraint, value)
            return valid, msg

        return True, ""


class DataValidatorAction(BaseAction):
    """Data validator action."""
    action_type = "data_validator"
    display_name = "数据验证器"
    description = "数据验证和模式检查"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            data = params.get("data", {})

            if operation == "validate":
                return self._validate_data(data, params)
            elif operation == "validate_type":
                return self._validate_type(data, params)
            elif operation == "validate_pattern":
                return self._validate_pattern(data, params)
            elif operation == "register_pattern":
                return self._register_pattern(params)
            elif operation == "register_custom":
                return self._register_custom(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Validation error: {str(e)}")

    def _validate_data(self, data: Dict, params: Dict) -> ActionResult:
        """Validate data with rules."""
        rules_data = params.get("rules", [])

        rules = []
        for r in rules_data:
            try:
                vtype = ValidationType[r.get("type", "REQUIRED").upper()]
                rule = ValidationRule(
                    field=r.get("field", ""),
                    validation_type=vtype,
                    constraint=r.get("constraint"),
                    message=r.get("message", ""),
                    required=r.get("required", True),
                )
                rules.append(rule)
            except KeyError:
                continue

        validator = DataValidator()
        result = validator.validate(data, rules)

        return ActionResult(
            success=result.valid,
            message="Valid" if result.valid else f"{len(result.errors)} validation errors",
            data={
                "valid": result.valid,
                "errors": [
                    {"field": e.field, "message": e.message, "value": str(e.value)[:50]}
                    for e in result.errors
                ],
                "validated_at": result.validated_at,
            },
        )

    def _validate_type(self, data: Any, params: Dict) -> ActionResult:
        """Validate data type."""
        type_name = params.get("type", "string").lower()
        validator = TypeValidator()

        checks = {
            "string": validator.is_string,
            "int": validator.is_int,
            "float": validator.is_float,
            "number": validator.is_number,
            "bool": validator.is_bool,
            "list": validator.is_list,
            "dict": validator.is_dict,
            "email": validator.is_email,
            "url": validator.is_url,
            "uuid": validator.is_uuid,
        }

        check_fn = checks.get(type_name)
        if not check_fn:
            return ActionResult(success=False, message=f"Unknown type: {type_name}")

        valid = check_fn(data)
        return ActionResult(
            success=valid,
            message=f"Type check: {'passed' if valid else 'failed'}",
            data={"type": type_name, "valid": valid, "value": str(data)[:50]},
        )

    def _validate_pattern(self, data: str, params: Dict) -> ActionResult:
        """Validate pattern."""
        pattern = params.get("pattern", "")
        validator = PatternValidator()

        valid = validator.validate(data, pattern)
        return ActionResult(
            success=valid,
            message=f"Pattern check: {'passed' if valid else 'failed'}",
            data={"pattern": pattern, "valid": valid},
        )

    def _register_pattern(self, params: Dict) -> ActionResult:
        """Register a pattern."""
        name = params.get("name")
        pattern = params.get("pattern")

        if not name or not pattern:
            return ActionResult(success=False, message="name and pattern are required")

        validator = PatternValidator()
        validator.register_pattern(name, pattern)

        return ActionResult(success=True, message=f"Pattern '{name}' registered")

    def _register_custom(self, params: Dict) -> ActionResult:
        """Register a custom validator."""
        name = params.get("name")
        func_code = params.get("func")

        if not name or not func_code:
            return ActionResult(success=False, message="name and func are required")

        try:
            custom_validator = CustomValidator()
            custom_validator.register(name, eval(f"lambda x: {func_code}"))
            return ActionResult(success=True, message=f"Custom validator '{name}' registered")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to register: {str(e)}")
