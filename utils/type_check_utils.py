"""Type checking and casting utilities.

Provides runtime type checking and safe type conversion
for input validation in automation workflows.
"""

from typing import Any, List, Optional, Type, TypeVar, Union, get_origin, get_args


T = TypeVar("T")


def is_instance(value: Any, type_hint: Any) -> bool:
    """Check if value is instance of type hint.

    Args:
        value: Value to check.
        type_hint: Type hint to check against.

    Returns:
        True if value matches type hint.
    """
    if type_hint is Any:
        return True
    if type_hint is type(None):
        return value is None
    if isinstance(type_hint, TypeVar):
        return True
    if isinstance(type_hint, Union):
        return any(is_instance(value, arg) for arg in get_args(type_hint))
    if get_origin(type_hint) is list:
        return isinstance(value, list)
    if get_origin(type_hint) is dict:
        return isinstance(value, dict)
    if get_origin(type_hint) is tuple:
        return isinstance(value, tuple)
    if get_origin(type_hint) is set:
        return isinstance(value, set)
    try:
        return isinstance(value, type_hint)
    except TypeError:
        return False


def cast(value: Any, target_type: Type[T]) -> T:
    """Cast value to target type.

    Args:
        value: Value to cast.
        target_type: Target type.

    Returns:
        Cast value.

    Raises:
        TypeError: If cast is not possible.
    """
    if is_instance(value, target_type):
        return value
    raise TypeError(f"Cannot cast {type(value).__name__} to {target_type.__name__}")


def safe_cast(value: Any, target_type: Type[T], default: Optional[T] = None) -> Optional[T]:
    """Safely cast value to target type with default.

    Args:
        value: Value to cast.
        target_type: Target type.
        default: Default value if cast fails.

    Returns:
        Cast value or default.
    """
    try:
        return cast(value, target_type)
    except TypeError:
        return default


def coerce_int(value: Any, default: int = 0) -> int:
    """Coerce value to integer.

    Args:
        value: Value to coerce.
        default: Default if coercion fails.

    Returns:
        Integer value.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def coerce_float(value: Any, default: float = 0.0) -> float:
    """Coerce value to float.

    Args:
        value: Value to coerce.
        default: Default if coercion fails.

    Returns:
        Float value.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_str(value: Any, default: str = "") -> str:
    """Coerce value to string.

    Args:
        value: Value to coerce.
        default: Default if coercion fails.

    Returns:
        String value.
    """
    try:
        return str(value)
    except Exception:
        return default


def coerce_bool(value: Any, default: bool = False) -> bool:
    """Coerce value to boolean.

    Args:
        value: Value to coerce.
        default: Default if coercion fails.

    Returns:
        Boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    try:
        return bool(value)
    except Exception:
        return default


def get_type_name(type_hint: Any) -> str:
    """Get human-readable type name.

    Args:
        type_hint: Type hint.

    Returns:
        Type name string.
    """
    if type_hint is Any:
        return "Any"
    if type_hint is None:
        return "None"
    if isinstance(type_hint, TypeVar):
        return type_hint.__name__
    if get_origin(type_hint) is Union:
        args = get_args(type_hint)
        return f"Union[{', '.join(get_type_name(a) for a in args)}]"
    if get_origin(type_hint) is list:
        return f"List[{get_type_name(get_args(type_hint)[0])}]"
    if get_origin(type_hint) is dict:
        return f"Dict[{get_type_name(get_args(type_hint)[0])}]"
    if get_origin(type_hint) is tuple:
        return f"Tuple[{', '.join(get_type_name(a) for a in get_args(type_hint))}]"
    try:
        return type_hint.__name__
    except AttributeError:
        return str(type_hint)


def check_fields(
    obj: Any,
    expected_types: List[tuple],
) -> List[str]:
    """Check object fields against expected types.

    Args:
        obj: Object to check.
        expected_types: List of (field_name, type_hint) tuples.

    Returns:
        List of field names that failed type check.
    """
    failures = []
    for field_name, type_hint in expected_types:
        if not hasattr(obj, field_name):
            failures.append(field_name)
        elif not is_instance(getattr(obj, field_name), type_hint):
            failures.append(field_name)
    return failures
