"""
Data Schema Validator Action Module.

JSON Schema-inspired validation for structured data with
custom validators, nested path validation, and detailed error reporting.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class ValidationType(Enum):
    """Types of validation rules."""

    TYPE = "type"
    REQUIRED = "required"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    ITEMS = "items"
    PROPERTIES = "properties"
    ONE_OF = "one_of"


@dataclass
class ValidationError:
    """A single validation error."""

    path: str
    rule: str
    message: str
    value: Any = None
    expected: Any = None

    def __str__(self) -> str:
        """Human-readable error string."""
        msg = f"[{self.path}] {self.rule}: {self.message}"
        if self.expected is not None:
            msg += f" (expected: {self.expected})"
        return msg

    def to_dict(self) -> dict[str, Any]:
        """Export as dictionary."""
        return {
            "path": self.path,
            "rule": self.rule,
            "message": self.message,
            "value": str(self.value) if self.value is not None else None,
            "expected": str(self.expected) if self.expected is not None else None,
        }


@dataclass
class ValidationResult:
    """Result of validating a data structure."""

    valid: bool
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        """Number of validation errors."""
        return len(self.errors)

    def add_error(
        self,
        path: str,
        rule: str,
        message: str,
        value: Any = None,
        expected: Any = None,
    ) -> None:
        """Add a validation error."""
        self.errors.append(
            ValidationError(path=path, rule=rule, message=message, value=value, expected=expected)
        )
        self.valid = False

    def merge(self, other: "ValidationResult") -> None:
        """Merge another result into this one."""
        if not other.valid:
            self.valid = False
            self.errors.extend(other.errors)


@dataclass
class SchemaRule:
    """A single validation rule for a field."""

    rule_type: ValidationType
    value: Any = None
    message: str = ""
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class SchemaField:
    """Schema definition for a single field."""

    name: str
    rules: list[SchemaRule] = field(default_factory=list)
    required: bool = False
    default: Any = None
    nullable: bool = True

    def add_rule(
        self,
        rule_type: ValidationType,
        value: Any = None,
        message: str = "",
        validator: Optional[Callable[[Any], bool]] = None,
    ) -> "SchemaField":
        """Add a validation rule to this field."""
        self.rules.append(
            SchemaRule(rule_type=rule_type, value=value, message=message, validator=validator)
        )
        return self


@dataclass
class Schema:
    """Schema definition for a data structure."""

    name: str
    fields: dict[str, SchemaField] = field(default_factory=dict)
    strict: bool = False

    def field(
        self,
        name: str,
        required: bool = False,
        nullable: bool = True,
        default: Any = None,
    ) -> SchemaField:
        """Define a field in this schema."""
        f = SchemaField(name=name, required=required, nullable=nullable, default=default)
        self.fields[name] = f
        return f

    def add_field(self, schema_field: SchemaField) -> None:
        """Add a pre-built SchemaField."""
        self.fields[schema_field.name] = schema_field


class DataSchemaValidator:
    """
    Validates data against defined schemas.

    Supports nested structures, arrays, custom validators,
    and detailed error reporting.
    """

    TYPE_MAP: dict[str, type] = {
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": float,
        "bool": bool,
        "boolean": bool,
        "list": list,
        "array": list,
        "dict": dict,
        "object": dict,
        "none": type(None),
    }

    def __init__(self) -> None:
        """Initialize the validator."""
        self._schemas: dict[str, Schema] = {}

    def define_schema(self, name: str, strict: bool = False) -> Schema:
        """
        Define a new schema.

        Args:
            name: Schema name for later reference.
            strict: Fail on unknown fields if True.

        Returns:
            The Schema object for building.
        """
        schema = Schema(name=name, strict=strict)
        self._schemas[name] = schema
        return schema

    def get_schema(self, name: str) -> Optional[Schema]:
        """Retrieve a defined schema by name."""
        return self._schemas.get(name)

    def _get_type_name(self, value: Any) -> str:
        """Get type name for a value."""
        if value is None:
            return "none"
        for name, t in self.TYPE_MAP.items():
            if isinstance(value, t) and not isinstance(value, bool):
                return name
        if isinstance(value, bool):
            return "bool"
        return type(value).__name__

    def _validate_field(
        self,
        data: dict[str, Any],
        field_def: SchemaField,
        path: str,
        result: ValidationResult,
    ) -> Any:
        """
        Validate a single field against its schema.

        Args:
            data: Parent data dictionary.
            field_def: Field schema definition.
            path: Current path in the data structure.
            result: ValidationResult to accumulate errors.

        Returns:
            The validated/coerced value or None.
        """
        field_name = field_def.name
        value = data.get(field_name)
        current_path = f"{path}.{field_name}" if path else field_name

        if value is None:
            if field_def.required:
                result.add_error(
                    current_path,
                    "required",
                    f"Field '{field_name}' is required",
                    value=value,
                )
            if field_def.default is not None:
                return field_def.default
            return None

        for rule in field_def.rules:
            rule_passed = True
            error_msg = ""

            if rule.rule_type == ValidationType.TYPE:
                expected_type = self.TYPE_MAP.get(rule.value, rule.value)
                if not isinstance(value, expected_type):  # type: ignore
                    rule_passed = False
                    error_msg = f"Expected type '{rule.value}' but got '{self._get_type_name(value)}'"
                    result.add_error(current_path, "type", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.MIN_LENGTH:
                if hasattr(value, "__len__") and len(value) < rule.value:
                    rule_passed = False
                    error_msg = f"Length must be at least {rule.value}"
                    result.add_error(current_path, "min_length", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.MAX_LENGTH:
                if hasattr(value, "__len__") and len(value) > rule.value:
                    rule_passed = False
                    error_msg = f"Length must be at most {rule.value}"
                    result.add_error(current_path, "max_length", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.MIN_VALUE:
                if isinstance(value, (int, float)) and value < rule.value:
                    rule_passed = False
                    error_msg = f"Value must be at least {rule.value}"
                    result.add_error(current_path, "min_value", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.MAX_VALUE:
                if isinstance(value, (int, float)) and value > rule.value:
                    rule_passed = False
                    error_msg = f"Value must be at most {rule.value}"
                    result.add_error(current_path, "max_value", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.PATTERN:
                if isinstance(value, str) and not re.match(rule.value, value):
                    rule_passed = False
                    error_msg = f"Value does not match pattern '{rule.value}'"
                    result.add_error(current_path, "pattern", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.ENUM:
                if value not in rule.value:
                    rule_passed = False
                    error_msg = f"Value must be one of {rule.value}"
                    result.add_error(current_path, "enum", error_msg, value=value, expected=rule.value)

            elif rule.rule_type == ValidationType.CUSTOM and rule.validator:
                try:
                    rule_passed = rule.validator(value)
                    if not rule_passed:
                        error_msg = rule.message or "Custom validation failed"
                        result.add_error(current_path, "custom", error_msg, value=value)
                except Exception as e:
                    rule_passed = False
                    result.add_error(current_path, "custom", str(e), value=value)

        return value

    def validate(
        self,
        data: dict[str, Any],
        schema: Schema,
    ) -> ValidationResult:
        """
        Validate data against a schema.

        Args:
            data: Data dictionary to validate.
            schema: Schema to validate against.

        Returns:
            ValidationResult with success status and errors.
        """
        result = ValidationResult(valid=True)

        for field_name, field_def in schema.fields.items():
            path = ""
            self._validate_field(data, field_def, path, result)

        if schema.strict:
            known_fields = set(schema.fields.keys())
            extra_fields = set(data.keys()) - known_fields
            for extra in extra_fields:
                result.add_error(
                    extra,
                    "unknown_field",
                    f"Unknown field '{extra}' in strict mode",
                    value=data.get(extra),
                )

        return result

    def validate_nested(
        self,
        data: dict[str, Any],
        schema: Schema,
        max_depth: int = 10,
    ) -> ValidationResult:
        """
        Validate nested data structures.

        Args:
            data: Data to validate.
            schema: Schema definition.
            max_depth: Maximum recursion depth.

        Returns:
            ValidationResult with nested path errors.
        """
        result = self.validate(data, schema)
        if max_depth <= 0:
            return result

        for field_name, field_def in schema.fields.items():
            if field_name not in data:
                continue
            value = data[field_name]
            if isinstance(value, dict):
                nested_schema = Schema(name=f"{schema.name}.{field_name}")
                for sub_field_name, sub_field_def in field_def.rules:
                    if isinstance(sub_field_def, Schema):
                        nested_schema.fields[sub_field_name] = sub_field_def
                if nested_schema.fields:
                    nested_result = self.validate_nested(value, nested_schema, max_depth - 1)
                    result.merge(nested_result)

        return result


def create_validator() -> DataSchemaValidator:
    """
    Factory function to create a data schema validator.

    Returns:
        Configured DataSchemaValidator instance.
    """
    return DataSchemaValidator()
