"""Input validation utilities for automation parameters."""

from typing import Any, List, Tuple, Callable, Optional, Dict
import re


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


def validate_int_range(
    value: Any,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
    name: str = "value"
) -> int:
    """Validate integer is within range.
    
    Args:
        value: Value to validate.
        min_val: Minimum allowed value.
        max_val: Maximum allowed value.
        name: Name for error messages.
    
    Returns:
        Validated integer.
    
    Raises:
        ValidationError: If validation fails.
    """
    try:
        int_val = int(value)
    except (TypeError, ValueError):
        raise ValidationError(f"{name} must be an integer, got {type(value).__name__}")
    if min_val is not None and int_val < min_val:
        raise ValidationError(f"{name} must be >= {min_val}, got {int_val}")
    if max_val is not None and int_val > max_val:
        raise ValidationError(f"{name} must be <= {max_val}, got {int_val}")
    return int_val


def validate_float_range(
    value: Any,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
    name: str = "value"
) -> float:
    """Validate float is within range.
    
    Args:
        value: Value to validate.
        min_val: Minimum allowed value.
        max_val: Maximum allowed value.
        name: Name for error messages.
    
    Returns:
        Validated float.
    
    Raises:
        ValidationError: If validation fails.
    """
    try:
        float_val = float(value)
    except (TypeError, ValueError):
        raise ValidationError(f"{name} must be a number, got {type(value).__name__}")
    if min_val is not None and float_val < min_val:
        raise ValidationError(f"{name} must be >= {min_val}, got {float_val}")
    if max_val is not None and float_val > max_val:
        raise ValidationError(f"{name} must be <= {max_val}, got {float_val}")
    return float_val


def validate_in_choices(
    value: Any,
    choices: List[Any],
    name: str = "value"
) -> Any:
    """Validate value is in allowed choices.
    
    Args:
        value: Value to validate.
        choices: Allowed values.
        name: Name for error messages.
    
    Returns:
        Validated value.
    
    Raises:
        ValidationError: If validation fails.
    """
    if value not in choices:
        raise ValidationError(f"{name} must be one of {choices}, got {value}")
    return value


def validate_non_empty(
    value: Any,
    name: str = "value"
) -> Any:
    """Validate value is not empty.
    
    Args:
        value: Value to validate.
        name: Name for error messages.
    
    Returns:
        Validated value.
    
    Raises:
        ValidationError: If validation fails.
    """
    if value is None:
        raise ValidationError(f"{name} cannot be None")
    if isinstance(value, (list, tuple, str)) and len(value) == 0:
        raise ValidationError(f"{name} cannot be empty")
    return value


def validate_regex(
    value: str,
    pattern: str,
    name: str = "value"
) -> str:
    """Validate string matches regex pattern.
    
    Args:
        value: String to validate.
        pattern: Regex pattern.
        name: Name for error messages.
    
    Returns:
        Validated string.
    
    Raises:
        ValidationError: If validation fails.
    """
    if not isinstance(value, str):
        raise ValidationError(f"{name} must be a string, got {type(value).__name__}")
    if not re.match(pattern, value):
        raise ValidationError(f"{name} does not match pattern {pattern}")
    return value


def validate_coordinates(
    x: Any,
    y: Any,
    width: Optional[int] = None,
    height: Optional[int] = None
) -> Tuple[int, int]:
    """Validate coordinates.
    
    Args:
        x, y: Coordinate values.
        width, height: Optional bounds.
    
    Returns:
        Tuple of validated (x, y).
    
    Raises:
        ValidationError: If validation fails.
    """
    x_val = validate_int_range(x, min_val=0, name="x")
    y_val = validate_int_range(y, min_val=0, name="y")
    if width is not None and x_val >= width:
        raise ValidationError(f"x ({x_val}) must be less than width ({width})")
    if height is not None and y_val >= height:
        raise ValidationError(f"y ({y_val}) must be less than height ({height})")
    return (x_val, y_val)


class Validator:
    """Composable validation helper."""

    def __init__(self, name: str = "value"):
        """Initialize validator.
        
        Args:
            name: Name for error messages.
        """
        self.name = name
        self._rules: List[Callable[[Any], Any]] = []

    def add_rule(self, rule: Callable[[Any], Any]) -> "Validator":
        """Add a validation rule.
        
        Args:
            rule: Function that validates and returns value.
        
        Returns:
            Self for chaining.
        """
        self._rules.append(rule)
        return self

    def int_range(self, min_val: Optional[int] = None, max_val: Optional[int] = None) -> "Validator":
        """Add integer range rule."""
        def rule(v):
            return validate_int_range(v, min_val, max_val, self.name)
        return self.add_rule(rule)

    def float_range(self, min_val: Optional[float] = None, max_val: Optional[float] = None) -> "Validator":
        """Add float range rule."""
        def rule(v):
            return validate_float_range(v, min_val, max_val, self.name)
        return self.add_rule(rule)

    def in_choices(self, choices: List[Any]) -> "Validator":
        """Add choices rule."""
        def rule(v):
            return validate_in_choices(v, choices, self.name)
        return self.add_rule(rule)

    def non_empty(self) -> "Validator":
        """Add non-empty rule."""
        def rule(v):
            return validate_non_empty(v, self.name)
        return self.add_rule(rule)

    def validate(self, value: Any) -> Any:
        """Run all validation rules.
        
        Args:
            value: Value to validate.
        
        Returns:
            Validated value.
        
        Raises:
            ValidationError: If any rule fails.
        """
        result = value
        for rule in self._rules:
            result = rule(result)
        return result
