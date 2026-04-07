"""Types utilities v3 - advanced type operations.

Advanced type utilities including type guards,
 coercion chains, and generic builders.
"""

from __future__ import annotations

from typing import Any, Callable, TypeVar, Union, get_origin, get_args

__all__ = [
    "is_primitive",
    "is_composite",
    "is_numeric",
    "is_sequence",
    "is_mapping",
    "coerce_to",
    "coerce_chain",
    "TypeGuard",
    "safe_cast",
    "TypeBuilder",
    "GenericBuilder",
]


T = TypeVar("T")


def is_primitive(value: Any) -> bool:
    """Check if value is a primitive type."""
    return isinstance(value, (bool, int, float, str, bytes, type(None)))


def is_composite(value: Any) -> bool:
    """Check if value is a composite type."""
    return isinstance(value, (list, tuple, set, dict, frozenset))


def is_numeric(value: Any) -> bool:
    """Check if value is numeric."""
    return isinstance(value, (int, float))


def is_sequence(value: Any) -> bool:
    """Check if value is a sequence type."""
    return isinstance(value, (list, tuple, str, bytes, bytearray))


def is_mapping(value: Any) -> bool:
    """Check if value is a mapping type."""
    return isinstance(value, dict)


def coerce_to(value: Any, target_type: type) -> Any:
    """Coerce value to target type.

    Args:
        value: Value to coerce.
        target_type: Target type.

    Returns:
        Coerced value.

    Raises:
        ValueError: If coercion fails.
    """
    if isinstance(value, target_type):
        return value
    if target_type is bool:
        return bool(value)
    if target_type is int:
        return int(value)
    if target_type is float:
        return float(value)
    if target_type is str:
        return str(value)
    if target_type is list:
        return list(value)
    if target_type is dict:
        return dict(value)
    if target_type is tuple:
        return tuple(value)
    if target_type is set:
        return set(value)
    return target_type(value)


def coerce_chain(value: Any, *types: type) -> Any:
    """Try to coerce through a chain of types.

    Args:
        value: Value to coerce.
        *types: Types to try in order.

    Returns:
        First successful coercion.

    Raises:
        ValueError: If all coercions fail.
    """
    for target_type in types:
        try:
            return coerce_to(value, target_type)
        except (ValueError, TypeError):
            continue
    raise ValueError(f"Cannot coerce {type(value).__name__} to any of {types}")


class TypeGuard:
    """Type guard functions."""

    @staticmethod
    def is_list(value: Any) -> bool:
        return isinstance(value, list)

    @staticmethod
    def is_dict(value: Any) -> bool:
        return isinstance(value, dict)

    @staticmethod
    def is_tuple(value: Any) -> bool:
        return isinstance(value, tuple)

    @staticmethod
    def is_set(value: Any) -> bool:
        return isinstance(value, (set, frozenset))

    @staticmethod
    def is_str(value: Any) -> bool:
        return isinstance(value, str)

    @staticmethod
    def is_int(value: Any) -> bool:
        return isinstance(value, int)

    @staticmethod
    def is_float(value: Any) -> bool:
        return isinstance(value, float)

    @staticmethod
    def is_bool(value: Any) -> bool:
        return isinstance(value, bool)

    @staticmethod
    def is_none(value: Any) -> bool:
        return value is None

    @staticmethod
    def is_bytes(value: Any) -> bool:
        return isinstance(value, bytes)


def safe_cast(value: Any, target_type: type[T]) -> T | None:
    """Safely cast to target type.

    Args:
        value: Value to cast.
        target_type: Target type.

    Returns:
        Cast value or None.
    """
    try:
        return coerce_to(value, target_type)
    except (ValueError, TypeError):
        return None


class TypeBuilder:
    """Build complex types."""

    def __init__(self, base: type | None = None) -> None:
        self._base = base
        self._args: list[type] = []

    def with_args(self, *args: type) -> TypeBuilder:
        """Add type arguments."""
        self._args.extend(args)
        return self

    def build(self) -> type:
        """Build the type."""
        if not self._args:
            return self._base
        return self._base[tuple(self._args)]


class GenericBuilder:
    """Build generic types."""

    @staticmethod
    def list_of(tp: type) -> type:
        """Build List[type]."""
        return list[tp]

    @staticmethod
    def dict_of(k: type, v: type) -> type:
        """Build Dict[type, type]."""
        return dict[k, v]

    @staticmethod
    def set_of(tp: type) -> type:
        """Build Set[type]."""
        return set[tp]

    @staticmethod
    def tuple_of(*types: type) -> type:
        """Build Tuple[type, ...]."""
        return tuple[types]

    @staticmethod
    def optional_of(tp: type) -> type:
        """Build Optional[type]."""
        return Union[tp, None]
