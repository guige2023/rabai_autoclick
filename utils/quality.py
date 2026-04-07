"""Quality assurance utilities for RabAI AutoClick.

Provides:
- Type guards
- Assert helpers
- Contract checking
"""

from typing import Any, Callable, List, Optional, Tuple, Type, TypeVar


T = TypeVar("T")


def type_check(value: Any, expected_type: Type[T]) -> T:
    """Type-check a value and return it if valid.

    Args:
        value: Value to check.
        expected_type: Expected type.

    Returns:
        The value if type matches.

    Raises:
        TypeError: If type doesn't match.
    """
    if not isinstance(value, expected_type):
        raise TypeError(
            f"Expected {expected_type.__name__}, got {type(value).__name__}"
        )
    return value


def isinstance_check(value: Any, types: Tuple[Type, ...]) -> Any:
    """Type-check a value against multiple types.

    Args:
        value: Value to check.
        types: Tuple of acceptable types.

    Returns:
        The value if type matches.

    Raises:
        TypeError: If type doesn't match.
    """
    if not isinstance(value, types):
        type_names = " | ".join(t.__name__ for t in types)
        raise TypeError(
            f"Expected {type_names}, got {type(value).__name__}"
        )
    return value


def assert_not_none(value: Any, message: Optional[str] = None) -> Any:
    """Assert value is not None.

    Args:
        value: Value to check.
        message: Optional error message.

    Returns:
        The value if not None.

    Raises:
        ValueError: If value is None.
    """
    if value is None:
        raise ValueError(message or "Value cannot be None")
    return value


def assert_positive(value: Any, name: str = "value") -> Any:
    """Assert value is positive.

    Args:
        value: Value to check.
        name: Name for error message.

    Returns:
        The value if positive.

    Raises:
        ValueError: If value is not positive.
    """
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def assert_non_empty(value: Any, name: str = "value") -> Any:
    """Assert value is non-empty.

    Args:
        value: Value to check.
        name: Name for error message.

    Returns:
        The value if non-empty.

    Raises:
        ValueError: If value is empty.
    """
    if not value:
        raise ValueError(f"{name} cannot be empty")
    return value


def assert_in_range(
    value: Any,
    min_val: Any,
    max_val: Any,
    name: str = "value",
) -> Any:
    """Assert value is within range.

    Args:
        value: Value to check.
        min_val: Minimum value (inclusive).
        max_val: Maximum value (inclusive).
        name: Name for error message.

    Returns:
        The value if in range.

    Raises:
        ValueError: If value is out of range.
    """
    if not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    if value < min_val or value > max_val:
        raise ValueError(f"{name} must be between {min_val} and {max_val}, got {value}")
    return value


def invariant(
    condition: bool,
    message: str = "Invariant violated",
) -> None:
    """Assert an invariant.

    Args:
        condition: Condition that must be True.
        message: Error message if violated.

    Raises:
        AssertionError: If condition is False.
    """
    if not condition:
        raise AssertionError(message)


def requires(
    condition: bool,
    message: str = "Precondition violated",
) -> None:
    """Assert a precondition.

    Args:
        condition: Condition that must be True.
        message: Error message if violated.

    Raises:
        AssertionError: If condition is False.
    """
    if not condition:
        raise AssertionError(message)


def ensures(
    condition: bool,
    message: str = "Postcondition violated",
) -> None:
    """Assert a postcondition.

    Args:
        condition: Condition that must be True.
        message: Error message if violated.

    Raises:
        AssertionError: If condition is False.
    """
    if not condition:
        raise AssertionError(message)


class Contract:
    """Contract for enforcing pre/post conditions.

    Usage:
        @Contract.pre(lambda x: x > 0)
        @Contract.post(lambda r: r >= 0)
        def sqrt(x):
            return x ** 0.5
    """

    @staticmethod
    def pre(condition: Callable[[Any], bool]) -> Callable:
        """Decorator for precondition."""
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args: Any, **kwargs: Any) -> T:
                if not condition(*args, **kwargs):
                    raise AssertionError(f"Precondition failed for {func.__name__}")
                return func(*args, **kwargs)
            return wrapper
        return decorator

    @staticmethod
    def post(condition: Callable[..., bool]) -> Callable:
        """Decorator for postcondition."""
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            def wrapper(*args: Any, **kwargs: Any) -> T:
                result = func(*args, **kwargs)
                if not condition(result):
                    raise AssertionError(f"Postcondition failed for {func.__name__}")
                return result
            return wrapper
        return decorator


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, errors: List[str]) -> None:
        self.errors = errors
        super().__init__(f"Validation failed: {', '.join(errors)}")


class Validator:
    """Chainable validator.

    Usage:
        result = (
            Validator(value, "field")
            .is_not_none()
            .is_positive()
            .is_string()
            .validate()
        )
    """

    def __init__(self, value: Any, name: str = "value") -> None:
        self._value = value
        self._name = name
        self._errors: List[str] = []

    def is_not_none(self) -> 'Validator':
        """Check value is not None."""
        if self._value is None:
            self._errors.append(f"{self._name} cannot be None")
        return self

    def is_type(self, expected: Type) -> 'Validator':
        """Check value is of expected type."""
        if not isinstance(self._value, expected):
            self._errors.append(
                f"{self._name} must be {expected.__name__}, "
                f"got {type(self._value).__name__}"
            )
        return self

    def is_positive(self) -> 'Validator':
        """Check value is positive."""
        if isinstance(self._value, (int, float)) and self._value <= 0:
            self._errors.append(f"{self._name} must be positive")
        return self

    def is_in_range(self, min_val: Any, max_val: Any) -> 'Validator':
        """Check value is in range."""
        if isinstance(self._value, (int, float)):
            if self._value < min_val or self._value > max_val:
                self._errors.append(
                    f"{self._name} must be between {min_val} and {max_val}"
                )
        return self

    def is_non_empty(self) -> 'Validator':
        """Check value is non-empty."""
        if not self._value:
            self._errors.append(f"{self._name} cannot be empty")
        return self

    def matches(self, pattern: str) -> 'Validator':
        """Check value matches regex pattern."""
        import re
        if not re.match(pattern, str(self._value)):
            self._errors.append(f"{self._name} does not match pattern {pattern}")
        return self

    def validate(self) -> Any:
        """Validate and return value or raise.

        Returns:
            The validated value.

        Raises:
            ValidationError: If any validations failed.
        """
        if self._errors:
            raise ValidationError(self._errors)
        return self._value


def validate(
    value: Any,
    *checks: Callable[[Any], bool],
) -> Any:
    """Validate value with checks.

    Args:
        value: Value to validate.
        *checks: Boolean functions to apply.

    Returns:
        Value if all checks pass.

    Raises:
        ValueError: If any check fails.
    """
    for check in checks:
        if not check(value):
            raise ValueError(f"Validation failed: {check.__name__}")
    return value