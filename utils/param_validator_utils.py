"""Parameter validation utilities.

Provides composable validation rules for function parameters
and input sanitization in automation workflows.
"""

from typing import Any, Callable, List, Optional, Tuple, TypeVar


T = TypeVar("T")


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, message: str, field: Optional[str] = None) -> None:
        super().__init__(message)
        self.field = field
        self.message = message


class Validator(Generic[T]):
    """Composable parameter validator.

    Example:
        v = Validator[int]("age") \
            .must(lambda x: x > 0, "must be positive") \
            .must(lambda x: x < 150, "must be reasonable")
        v.validate(25)  # passes
        v.validate(-1)  # raises ValidationError
    """

    def __init__(self, field_name: str = "value") -> None:
        self._field_name = field_name
        self._rules: List[Tuple[Callable[[T], bool], str]] = []

    def must(self, predicate: Callable[[T], bool], message: str) -> "Validator[T]":
        """Add a validation rule.

        Args:
            predicate: Function that returns True if valid.
            message: Error message if invalid.

        Returns:
            Self for chaining.
        """
        self._rules.append((predicate, message))
        return self

    def validate(self, value: T) -> T:
        """Validate a value.

        Args:
            value: Value to validate.

        Returns:
            The value if valid.

        Raises:
            ValidationError: If validation fails.
        """
        for predicate, message in self._rules:
            if not predicate(value):
                raise ValidationError(message, self._field_name)
        return value

    def test(self, value: T) -> bool:
        """Test if value is valid without raising.

        Args:
            value: Value to test.

        Returns:
            True if valid.
        """
        for predicate, _ in self._rules:
            if not predicate(value):
                return False
        return True


def validate_int(
    value: Any,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
    field: str = "value",
) -> int:
    """Validate and convert to integer.

    Args:
        value: Value to validate.
        min_val: Minimum allowed value.
        max_val: Maximum allowed value.
        field: Field name for error messages.

    Returns:
        Validated integer.

    Raises:
        ValidationError: If not a valid integer or out of range.
    """
    try:
        result = int(value)
    except (TypeError, ValueError) as e:
        raise ValidationError(f"must be an integer: {e}", field)

    if min_val is not None and result < min_val:
        raise ValidationError(f"must be >= {min_val}", field)
    if max_val is not None and result > max_val:
        raise ValidationError(f"must be <= {max_val}", field)
    return result


def validate_float(
    value: Any,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    field: str = "value",
) -> float:
    """Validate and convert to float.

    Args:
        value: Value to validate.
        min_val: Minimum allowed value.
        max_val: Maximum allowed value.
        field: Field name for error messages.

    Returns:
        Validated float.

    Raises:
        ValidationError: If not a valid float or out of range.
    """
    try:
        result = float(value)
    except (TypeError, ValueError) as e:
        raise ValidationError(f"must be a number: {e}", field)

    if min_val is not None and result < min_val:
        raise ValidationError(f"must be >= {min_val}", field)
    if max_val is not None and result > max_val:
        raise ValidationError(f"must be <= {max_val}", field)
    return result


def validate_string(
    value: Any,
    min_len: int = 0,
    max_len: Optional[int] = None,
    pattern: Optional[str] = None,
    field: str = "value",
) -> str:
    """Validate string value.

    Args:
        value: Value to validate.
        min_len: Minimum length.
        max_len: Maximum length.
        pattern: Regex pattern to match.
        field: Field name for error messages.

    Returns:
        Validated string.

    Raises:
        ValidationError: If validation fails.
    """
    if not isinstance(value, str):
        raise ValidationError("must be a string", field)

    if len(value) < min_len:
        raise ValidationError(f"must be at least {min_len} characters", field)
    if max_len is not None and len(value) > max_len:
        raise ValidationError(f"must be at most {max_len} characters", field)

    if pattern is not None:
        import re
        if not re.match(pattern, value):
            raise ValidationError(f"must match pattern: {pattern}", field)

    return value


def validate_in(
    value: Any,
    choices: List[Any],
    field: str = "value",
) -> Any:
    """Validate value is in allowed choices.

    Args:
        value: Value to validate.
        choices: Allowed values.
        field: Field name for error messages.

    Returns:
        Value if valid.

    Raises:
        ValidationError: If value not in choices.
    """
    if value not in choices:
        raise ValidationError(f"must be one of: {choices}", field)
    return value
