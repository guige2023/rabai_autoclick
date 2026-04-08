"""
Structured data utilities: data class helpers and validation.

Provides decorators, validators, and converters for
working with structured Python data.

Example:
    >>> from utils.struct_utils import validate, field_validator
    >>> @validate
    ... class User:
    ...     name: str
    ...     age: int
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union, get_type_hints

T = TypeVar("T")


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class FieldValidator:
    """Validator for a single field."""

    def __init__(
        self,
        name: str,
        validators: Optional[List[Callable]] = None,
        required: bool = True,
        default: Any = None,
    ) -> None:
        """Initialize field validator."""
        self.name = name
        self.validators = validators or []
        self.required = required
        self.default = default


class Validator:
    """
    Schema validator for structured data.

    Supports field-level validation, type checking,
    and custom validators.
    """

    def __init__(self, schema: Type) -> None:
        """
        Initialize the validator.

        Args:
            schema: Data class or type with type hints.
        """
        self.schema = schema
        self._field_validators: Dict[str, FieldValidator] = {}
        self._build_field_map()

    def _build_field_map(self) -> None:
        """Build field validator map from schema."""
        if dataclasses.is_dataclass(self.schema):
            hints = get_type_hints(self.schema)
            for f in dataclasses.fields(self.schema):
                if f.name in hints:
                    self._field_validators[f.name] = FieldValidator(
                        name=f.name,
                        required=f.default is dataclasses.MISSING,
                        default=f.default,
                    )

    def add_validator(
        self,
        field_name: str,
        validator: Callable,
    ) -> None:
        """
        Add a validator function to a field.

        Args:
            field_name: Name of the field.
            validator: Function that takes (field_name, value) and raises on error.
        """
        if field_name not in self._field_validators:
            self._field_validators[field_name] = FieldValidator(name=field_name)

        self._field_validators[field_name].validators.append(validator)

    def validate(self, data: Any) -> Any:
        """
        Validate data against the schema.

        Args:
            data: Data to validate (dict or dataclass).

        Returns:
            Validated data.

        Raises:
            ValidationError: If validation fails.
        """
        if dataclasses.is_dataclass(data):
            obj = data
            data_dict = dataclasses.asdict(data)
        else:
            obj = None
            data_dict = dict(data) if isinstance(data, dict) else data

        errors: List[str] = []

        for field_name, field_validator in self._field_validators.items():
            value = data_dict.get(field_name) if isinstance(data_dict, dict) else getattr(data_dict, field_name, None)

            if value is None:
                if field_validator.required and field_validator.default is None:
                    errors.append(f"Missing required field: {field_name}")
                continue

            for validator in field_validator.validators:
                try:
                    validator(field_name, value)
                except ValidationError as e:
                    errors.append(str(e))

        if errors:
            raise ValidationError("; ".join(errors))

        return data if obj else data_dict


def validate_dataclass(cls: Type[T]) -> Type[T]:
    """
    Decorator to add validation to a dataclass.

    Args:
        cls: Dataclass to decorate.

    Returns:
        Decorated dataclass.
    """
    original_init = cls.__init__ if hasattr(cls, "__init__") else None

    def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
        if original_init:
            original_init(self, *args, **kwargs)
        validator = Validator(type(self))
        validator.validate(self)

    cls.__init__ = new_init
    return cls


def field_validator(*validators: Callable) -> Callable:
    """
    Decorator to define field validators.

    Args:
        *validators: Validator functions.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        func._field_validators = list(validators)
        return func
    return decorator


def required(value: Any) -> None:
    """Validator that checks value is not None."""
    if value is None:
        raise ValidationError("Value is required")


def not_empty(value: Any) -> None:
    """Validator that checks value is not empty."""
    if value is None or (isinstance(value, (str, list, dict)) and len(value) == 0):
        raise ValidationError("Value cannot be empty")


def min_length(min_len: int) -> Callable:
    """Create a validator for minimum length."""
    def validator(field_name: str, value: Any) -> None:
        if value is not None and len(value) < min_len:
            raise ValidationError(f"{field_name} must be at least {min_len} characters")
    return validator


def max_length(max_len: int) -> Callable:
    """Create a validator for maximum length."""
    def validator(field_name: str, value: Any) -> None:
        if value is not None and len(value) > max_len:
            raise ValidationError(f"{field_name} must be at most {max_len} characters")
    return validator


def min_value(min_val: float) -> Callable:
    """Create a validator for minimum numeric value."""
    def validator(field_name: str, value: Any) -> None:
        if value is not None and value < min_val:
            raise ValidationError(f"{field_name} must be at least {min_val}")
    return validator


def max_value(max_val: float) -> Callable:
    """Create a validator for maximum numeric value."""
    def validator(field_name: str, value: Any) -> None:
        if value is not None and value > max_val:
            raise ValidationError(f"{field_name} must be at most {max_val}")
    return validator


def pattern(regex: str) -> Callable:
    """Create a validator for regex pattern match."""
    import re
    compiled = re.compile(regex)

    def validator(field_name: str, value: Any) -> None:
        if value is not None and not compiled.match(str(value)):
            raise ValidationError(f"{field_name} does not match pattern {regex}")
    return validator


def one_of(choices: List[Any]) -> Callable:
    """Create a validator for one-of choices."""
    def validator(field_name: str, value: Any) -> None:
        if value is not None and value not in choices:
            raise ValidationError(f"{field_name} must be one of {choices}")
    return validator


def is_email(value: Any) -> None:
    """Validator that checks value is a valid email."""
    import re
    pattern = r"^[\w.+-]+@[\w-]+\.[\w.-]+$"
    if value is not None and not re.match(pattern, str(value)):
        raise ValidationError("Invalid email address")


def is_url(value: Any) -> None:
    """Validator that checks value is a valid URL."""
    import re
    pattern = r"^https?://[\w\-._~:/?#\[\]@!$&'()*+,;=%]+$"
    if value is not None and not re.match(pattern, str(value)):
        raise ValidationError("Invalid URL")


def is_uuid(value: Any) -> None:
    """Validator that checks value is a valid UUID."""
    import re
    pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    if value is not None and not re.match(pattern, str(value), re.IGNORECASE):
        raise ValidationError("Invalid UUID")


class SchemaBuilder:
    """Fluent builder for creating validation schemas."""

    def __init__(self, schema_class: Type) -> None:
        """Initialize the schema builder."""
        self.schema_class = schema_class
        self._validators: Dict[str, List[Callable]] = defaultdict(list)

    def add_validation(
        self,
        field_name: str,
        validator: Callable,
    ) -> "SchemaBuilder":
        """Add a validation to a field."""
        self._validators[field_name].append(validator)
        return self

    def build(self) -> Validator:
        """Build the validator."""
        validator = Validator(self.schema_class)
        for field_name, validators in self._validators.items():
            for v in validators:
                validator.add_validator(field_name, v)
        return validator


def merge_dicts(
    *dicts: Dict[str, Any],
    strategy: str = "override",
) -> Dict[str, Any]:
    """
    Merge multiple dictionaries.

    Args:
        *dicts: Dictionaries to merge.
        strategy: Merge strategy ('override', 'keep', 'merge').

    Returns:
        Merged dictionary.
    """
    if not dicts:
        return {}

    result: Dict[str, Any] = {}

    for d in dicts:
        if not d:
            continue

        for key, value in d.items():
            if key not in result:
                result[key] = value
            elif strategy == "override":
                result[key] = value
            elif strategy == "keep":
                pass
            elif strategy == "merge" and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_dicts(result[key], value, strategy)

    return result


def flatten_dict(
    d: Dict[str, Any],
    parent_key: str = "",
    sep: str = ".",
) -> Dict[str, Any]:
    """
    Flatten a nested dictionary.

    Args:
        d: Dictionary to flatten.
        parent_key: Parent key prefix.
        sep: Key separator.

    Returns:
        Flattened dictionary.
    """
    items: Dict[str, Any] = {}

    for key, value in d.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep))
        else:
            items[new_key] = value

    return items
