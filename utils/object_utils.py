"""Object utilities for RabAI AutoClick.

Provides:
- Object introspection and inspection
- Attribute access helpers
- Object copying and comparison
- Type checking utilities
"""

from __future__ import annotations

import copy
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
)


T = TypeVar("T")


def get_attrs(
    obj: Any,
    exclude_private: bool = True,
) -> Dict[str, Any]:
    """Get all instance attributes of an object.

    Args:
        obj: Object to inspect.
        exclude_private: Exclude attributes starting with _.

    Returns:
        Dict of attribute names to values.
    """
    result = {}
    for k, v in obj.__dict__.items():
        if exclude_private and k.startswith("_"):
            continue
        result[k] = v
    return result


def has_attr(obj: Any, name: str) -> bool:
    """Check if an object has a specific attribute.

    Args:
        obj: Object to check.
        name: Attribute name.

    Returns:
        True if attribute exists.
    """
    return hasattr(obj, name)


def getattr_safe(
    obj: Any,
    name: str,
    default: Optional[Any] = None,
) -> Any:
    """Safely get an attribute with a default.

    Args:
        obj: Object to inspect.
        name: Attribute name.
        default: Default value if not found.

    Returns:
        Attribute value or default.
    """
    return getattr(obj, name, default)


def setattr_if_exists(
    obj: Any,
    name: str,
    value: Any,
) -> bool:
    """Set an attribute if it exists.

    Args:
        obj: Object to modify.
        name: Attribute name.
        value: Value to set.

    Returns:
        True if attribute was set.
    """
    if has_attr(obj, name):
        setattr(obj, name, value)
        return True
    return False


def copy_object(obj: Any, deep: bool = True) -> Any:
    """Create a copy of an object.

    Args:
        obj: Object to copy.
        deep: If True, deep copy; otherwise shallow.

    Returns:
        Copy of the object.
    """
    if deep:
        return copy.deepcopy(obj)
    return copy.copy(obj)


def objects_equal(a: Any, b: Any, deep: bool = False) -> bool:
    """Check if two objects are equal.

    Args:
        a: First object.
        b: Second object.
        deep: Use deep comparison.

    Returns:
        True if objects are equal.
    """
    if deep:
        return copy.deepcopy(a) == copy.deepcopy(b)
    return a == b


def get_type_name(obj: Any) -> str:
    """Get the type name of an object.

    Args:
        obj: Object to inspect.

    Returns:
        Type name string.
    """
    return type(obj).__name__


def get_qualified_name(cls: Type[Any]) -> str:
    """Get the fully qualified name of a class.

    Args:
        cls: Class to inspect.

    Returns:
        Qualified name (module.ClassName).
    """
    module = cls.__module__
    qualname = cls.__qualname__
    if module == "builtins":
        return qualname
    return f"{module}.{qualname}"


def isinstance_check(
    obj: Any,
    types: TypeOrTuple,
) -> bool:
    """Check if object is an instance of type(s).

    Args:
        obj: Object to check.
        types: Type or tuple of types.

    Returns:
        True if obj is instance.
    """
    return isinstance(obj, types)


def is_none(value: Any) -> bool:
    """Check if value is None."""
    return value is None


def is_not_none(value: Any) -> bool:
    """Check if value is not None."""
    return value is not None


def is_collection(value: Any) -> bool:
    """Check if value is a collection (list, tuple, set, dict)."""
    return isinstance(value, (list, tuple, set, dict))


def is_mapping(value: Any) -> bool:
    """Check if value is a mapping (dict-like)."""
    return isinstance(value, dict)


def deep_get(
    obj: Any,
    path: str,
    default: Optional[Any] = None,
    sep: str = ".",
) -> Any:
    """Get a nested value from an object using a path.

    Args:
        obj: Nested object/dict.
        path: Dot-separated path (e.g., 'a.b.c').
        default: Default if path not found.
        sep: Path separator.

    Returns:
        Value at path or default.
    """
    keys = path.split(sep)
    current = obj
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            current = getattr(current, key, None)
        if current is None:
            return default
    return current


def deep_set(
    obj: Any,
    path: str,
    value: Any,
    sep: str = ".",
) -> None:
    """Set a nested value in an object using a path.

    Args:
        obj: Nested object/dict.
        path: Dot-separated path.
        value: Value to set.
        sep: Path separator.
    """
    keys = path.split(sep)
    current = obj
    for key in keys[:-1]:
        if isinstance(current, dict):
            current = current.setdefault(key, {})
        else:
            current = getattr(current, key)
    if isinstance(current, dict):
        current[keys[-1]] = value
    else:
        setattr(current, keys[-1], value)


__all__ = [
    "get_attrs",
    "has_attr",
    "getattr_safe",
    "setattr_if_exists",
    "copy_object",
    "objects_equal",
    "get_type_name",
    "get_qualified_name",
    "isinstance_check",
    "is_none",
    "is_not_none",
    "is_collection",
    "is_mapping",
    "deep_get",
    "deep_set",
]


from typing import Union  # noqa: E402

TypeOrTuple = Union[type, tuple]
