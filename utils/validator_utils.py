"""Validation utilities for automation action parameters.

Provides schema-based and rule-based validation for
UI elements, action parameters, and configuration data.

Example:
    >>> from utils.validator_utils import Validator, Schema, Required, Range
    >>> schema = Schema({"x": Range(0, 1000), "y": Range(0, 1000)})
    >>> validator = Validator(schema)
    >>> validator.validate({"x": 100, "y": 200})  # raises ValidationError
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Pattern,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, message: str, field: Optional[str] = None) -> None:
        self.field = field
        super().__init__(message)


class Validator:
    """Schema-based validator for dictionaries.

    Example:
        >>> schema = Schema({
        ...     "name": Required(str),
        ...     "age": Range(0, 150),
        ... })
        >>> Validator(schema).validate({"name": "Alice", "age": 30})
    """

    def __init__(
        self,
        schema: Dict[str, Any],
        *,
        allow_extra: bool = True,
        allow_missing: bool = False,
    ) -> None:
        self.schema = schema
        self.allow_extra = allow_extra
        self.allow_missing = allow_missing

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against schema.

        Args:
            data: Data to validate.

        Returns:
            Validated data (possibly transformed).

        Raises:
            ValidationError: If validation fails.
        """
        errors: List[Tuple[Optional[str], str]] = []

        for field, rule in self.schema.items():
            if field not in data:
                if isinstance(rule, Required):
                    errors.append((field, f"Required field missing: {field}"))
                elif not self.allow_missing:
                    errors.append((field, f"Missing field: {field}"))
                continue

            try:
                rule.validate(data[field])
            except ValidationError as e:
                errors.append((field, str(e)))

        extra_fields = set(data.keys()) - set(self.schema.keys())
        if extra_fields and not self.allow_extra:
            for field in extra_fields:
                errors.append((field, f"Extra field not allowed: {field}"))

        if errors:
            raise ValidationError(
                f"Validation failed: {errors[0][1]}",
                field=errors[0][0],
            )

        return data

    def is_valid(self, data: Dict[str, Any]) -> bool:
        """Check if data is valid without raising.

        Args:
            data: Data to validate.

        Returns:
            True if valid.
        """
        try:
            self.validate(data)
            return True
        except ValidationError:
            return False


class Required:
    """Required field rule."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def validate(self, value: Any) -> None:
        if value is None:
            raise ValidationError("Value is required")
        if hasattr(self.inner, "validate"):
            self.inner.validate(value)


class Range:
    """Numeric range rule.

    Args:
        min_val: Minimum value (inclusive).
        max_val: Maximum value (inclusive).
    """

    def __init__(
        self,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> None:
        self.min_val = min_val
        self.max_val = max_val

    def validate(self, value: Any) -> None:
        if not isinstance(value, (int, float)):
            raise ValidationError(f"Expected number, got {type(value).__name__}")
        if self.min_val is not None and value < self.min_val:
            raise ValidationError(f"Value {value} below minimum {self.min_val}")
        if self.max_val is not None and value > self.max_val:
            raise ValidationError(f"Value {value} above maximum {self.max_val}")


class Length:
    """String/collection length rule."""

    def __init__(
        self,
        min_len: Optional[int] = None,
        max_len: Optional[int] = None,
    ) -> None:
        self.min_len = min_len
        self.max_len = max_len

    def validate(self, value: Any) -> None:
        if not hasattr(value, "__len__"):
            raise ValidationError(f"Value has no length")
        length = len(value)
        if self.min_len is not None and length < self.min_len:
            raise ValidationError(f"Length {length} below minimum {self.min_len}")
        if self.max_len is not None and length > self.max_len:
            raise ValidationError(f"Length {length} above maximum {self.max_len}")


class PatternRule:
    """Regex pattern rule."""

    def __init__(self, pattern: Union[str, Pattern[str]]) -> None:
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern)
        else:
            self.pattern = pattern

    def validate(self, value: Any) -> None:
        if not isinstance(value, str):
            raise ValidationError(f"Expected string, got {type(value).__name__}")
        if not self.pattern.match(value):
            raise ValidationError(f"Pattern '{self.pattern.pattern}' did not match")


class OneOf:
    """Value must be one of the options."""

    def __init__(self, *options: Any) -> None:
        self.options = set(options)

    def validate(self, value: Any) -> None:
        if value not in self.options:
            raise ValidationError(f"Value '{value}' not in {self.options}")


class AnyOf:
    """Value must match at least one of the rules."""

    def __init__(self, *rules: Any) -> None:
        self.rules = rules

    def validate(self, value: Any) -> None:
        errors: List[str] = []
        for rule in self.rules:
            try:
                if hasattr(rule, "validate"):
                    rule.validate(value)
                    return
                elif callable(rule) and not isinstance(rule, type):
                    rule(value)
                    return
                elif isinstance(value, rule):
                    return
            except ValidationError as e:
                errors.append(str(e))
        raise ValidationError(f"Value did not match any rule: {errors}")


class Schema:
    """Nested schema for dict validation."""

    def __init__(self, fields: Dict[str, Any]) -> None:
        self.fields = fields

    def validate(self, value: Any) -> None:
        if not isinstance(value, dict):
            raise ValidationError(f"Expected dict, got {type(value).__name__}")
        for field, rule in self.fields.items():
            if hasattr(rule, "validate"):
                if field in value:
                    rule.validate(value[field])


def validate_coordinates(
    x: Any,
    y: Any,
    *,
    width: Optional[int] = None,
    height: Optional[int] = None,
    screen_width: Optional[int] = None,
    screen_height: Optional[int] = None,
) -> Tuple[int, int]:
    """Validate screen coordinates.

    Args:
        x: X coordinate.
        y: Y coordinate.
        width: Optional element width.
        height: Optional element height.
        screen_width: Screen width bound.
        screen_height: Screen height bound.

    Returns:
        Validated (x, y) as integers.

    Raises:
        ValidationError: If coordinates are invalid.
    """
    try:
        x = int(x)
        y = int(y)
    except (TypeError, ValueError) as e:
        raise ValidationError(f"Invalid coordinate type: {e}")

    if x < 0 or y < 0:
        raise ValidationError(f"Coordinates must be non-negative: ({x}, {y})")

    if screen_width is not None and x >= screen_width:
        raise ValidationError(f"X coordinate {x} exceeds screen width {screen_width}")

    if screen_height is not None and y >= screen_height:
        raise ValidationError(f"Y coordinate {y} exceeds screen height {screen_height}")

    if width is not None and x + width > (screen_width or 999999):
        raise ValidationError(f"Element extends beyond screen width")

    if height is not None and y + height > (screen_height or 999999):
        raise ValidationError(f"Element extends beyond screen height")

    return x, y


def validate_action_params(
    action: str,
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Validate common action parameters.

    Args:
        action: Action name (e.g., "click", "type").
        params: Action parameters dict.

    Returns:
        Validated parameters.

    Raises:
        ValidationError: If validation fails.
    """
    if action == "click":
        if "x" not in params and "element" not in params:
            raise ValidationError("click requires 'x' or 'element'")
        if "x" in params:
            x = int(params["x"])
            y = int(params["y"])
            validate_coordinates(x, y)

    elif action == "type":
        if "text" not in params:
            raise ValidationError("type requires 'text'")
        if not isinstance(params["text"], str):
            raise ValidationError("type 'text' must be string")

    elif action == "drag":
        for coord in ["x1", "y1", "x2", "y2"]:
            if coord not in params:
                raise ValidationError(f"drag requires '{coord}'")

    return params
