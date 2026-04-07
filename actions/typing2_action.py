"""Typing utilities v2 - advanced type operations.

Extended typing utilities including generics,
 protocol checking, and type guards.
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    get_type_hints,
    get_origin,
    get_args,
)
from dataclasses import dataclass, fields, is_dataclass
import inspect

__all__ = [
    "is_generic",
    "is_union_type",
    "is_optional",
    "unwrap_optional",
    "get_origin_type",
    "get_parameters",
    "is_subtype",
    "type_args",
    "TypeChecker",
    "TypeGuard",
    "type_cache",
    "resolve_type",
    "deep_type",
    "type_equals",
    "type_name",
    "type_variables",
    "has_type_hints",
    "CallableInfo",
    "TypeBuilder",
    "ProtocolChecker",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def is_generic(tp: Any) -> bool:
    """Check if type is a generic type.

    Args:
        tp: Type to check.

    Returns:
        True if generic.
    """
    return get_origin(tp) is not None


def is_union_type(tp: Any) -> bool:
    """Check if type is Union.

    Args:
        tp: Type to check.

    Returns:
        True if Union type.
    """
    return get_origin(tp) is Union


def is_optional(tp: Any) -> bool:
    """Check if type is Optional (Union with None).

    Args:
        tp: Type to check.

    Returns:
        True if Optional.
    """
    if get_origin(tp) is Union:
        args = get_args(tp)
        return len(args) == 2 and type(None) in args
    return False


def unwrap_optional(tp: Any) -> Any:
    """Unwrap Optional type to get inner type.

    Args:
        tp: Optional type.

    Returns:
        Inner type or original if not Optional.

    Raises:
        ValueError: If not Optional type.
    """
    if is_optional(tp):
        args = get_args(tp)
        return next(arg for arg in args if arg is not type(None))
    return tp


def get_origin_type(tp: Any) -> Any | None:
    """Get the origin type of a generic.

    Args:
        tp: Generic type.

    Returns:
        Origin type or None.
    """
    return get_origin(tp)


def get_parameters(tp: Any) -> tuple[type, ...]:
    """Get type parameters from generic.

    Args:
        tp: Generic type.

    Returns:
        Tuple of type parameters.
    """
    return getattr(tp, "__parameters__", ())


def is_subtype(tp1: Any, tp2: Any) -> bool:
    """Check if tp1 is subtype of tp2.

    Args:
        tp1: Potential subtype.
        tp2: Potential supertype.

    Returns:
        True if tp1 is subtype of tp2.
    """
    try:
        return tp1 in (tp2, *get_args(tp2)) if is_union_type(tp2) else tp1 == tp2 or issubclass(tp1, tp2)
    except TypeError:
        return False


def type_args(tp: Any) -> tuple[Any, ...]:
    """Get type arguments from generic.

    Args:
        tp: Generic type.

    Returns:
        Tuple of type arguments.
    """
    return get_args(tp)


class TypeChecker:
    """Runtime type checking."""

    def __init__(self, strict: bool = False) -> None:
        self._strict = strict

    def check(self, value: Any, expected_type: Any) -> bool:
        """Check if value matches expected type.

        Args:
            value: Value to check.
            expected_type: Type or generic to check against.

        Returns:
            True if value matches type.
        """
        if expected_type is Any:
            return True
        origin = get_origin(expected_type)
        if origin is Union:
            return any(self.check(value, arg) for arg in get_args(expected_type))
        if origin in (list, tuple, set, frozenset):
            return self._check_collection(value, expected_type)
        if origin is dict:
            return self._check_dict(value, expected_type)
        try:
            return isinstance(value, expected_type)
        except TypeError:
            return False

    def _check_collection(self, value: Any, expected_type: Any) -> bool:
        """Check collection type."""
        if not isinstance(value, (list, tuple, set, frozenset)):
            return False
        args = get_args(expected_type)
        if not args:
            return True
        elem_type = args[0]
        return all(self.check(item, elem_type) for item in value)

    def _check_dict(self, value: Any, expected_type: Any) -> bool:
        """Check dict type."""
        if not isinstance(value, dict):
            return False
        args = get_args(expected_type)
        if not args:
            return True
        key_type, val_type = args[0], args[1]
        return all(self.check(k, key_type) and self.check(v, val_type) for k, v in value.items())


class TypeGuard:
    """Type guard utilities."""

    @staticmethod
    def is_list(value: Any) -> bool:
        """Type guard for list."""
        return isinstance(value, list)

    @staticmethod
    def is_dict(value: Any) -> bool:
        """Type guard for dict."""
        return isinstance(value, dict)

    @staticmethod
    def is_tuple(value: Any) -> bool:
        """Type guard for tuple."""
        return isinstance(value, tuple)

    @staticmethod
    def is_set(value: Any) -> bool:
        """Type guard for set."""
        return isinstance(value, (set, frozenset))

    @staticmethod
    def is_string(value: Any) -> bool:
        """Type guard for string."""
        return isinstance(value, str)

    @staticmethod
    def is_number(value: Any) -> bool:
        """Type guard for number."""
        return isinstance(value, (int, float))

    @staticmethod
    def is_bool(value: Any) -> bool:
        """Type guard for bool."""
        return isinstance(value, bool)

    @staticmethod
    def is_none(value: Any) -> bool:
        """Type guard for None."""
        return value is None

    @staticmethod
    def is_bytes(value: Any) -> bool:
        """Type guard for bytes."""
        return isinstance(value, bytes)


_type_cache: dict[Any, type] = {}


def type_cache(tp: Any) -> type:
    """Get or create cached type."""
    if tp not in _type_cache:
        _type_cache[tp] = tp
    return _type_cache[tp]


def resolve_type(tp: Any, globals: dict | None = None) -> Any:
    """Resolve string type annotation to actual type.

    Args:
        tp: Type or string.
        globals: Global namespace.

    Returns:
        Resolved type.
    """
    if isinstance(tp, str):
        import typing
        if globals:
            return eval(tp, globals)
        return eval(tp, {"typing": typing})
    return tp


def deep_type(obj: Any) -> Any:
    """Get deep type of object.

    Args:
        obj: Object to type.

    Returns:
        Type including generic arguments.
    """
    if isinstance(obj, dict):
        key_type = deep_type(next(iter(obj.keys()))) if obj else Any
        val_type = deep_type(next(iter(obj.values()))) if obj else Any
        return dict[key_type, val_type]
    if isinstance(obj, (list, tuple, set)):
        elem_types = set(deep_type(item) for item in obj) if obj else {Any}
        if len(elem_types) == 1:
            elem_type = next(iter(elem_types))
        else:
            elem_type = Union[tuple(elem_types)]
        if isinstance(obj, list):
            return list[elem_type]
        if isinstance(obj, tuple):
            return tuple[elem_type, ...]
        return set[elem_type]
    return type(obj)


def type_equals(tp1: Any, tp2: Any) -> bool:
    """Check if two types are equal.

    Args:
        tp1: First type.
        tp2: Second type.

    Returns:
        True if equal.
    """
    if tp1 == tp2:
        return True
    if get_origin(tp1) != get_origin(tp2):
        return False
    args1 = get_args(tp1)
    args2 = get_args(tp2)
    if len(args1) != len(args2):
        return False
    return all(type_equals(a, b) for a, b in zip(args1, args2))


def type_name(tp: Any) -> str:
    """Get human-readable type name.

    Args:
        tp: Type.

    Returns:
        String name.
    """
    origin = get_origin(tp)
    if origin is None:
        return getattr(tp, "__name__", str(tp))
    args = get_args(tp)
    name = getattr(origin, "__name__", str(origin))
    if args:
        return f"{name}[{', '.join(type_name(a) for a in args)}]"
    return name


def type_variables(tp: Any) -> set[TypeVar]:
    """Extract TypeVars from type.

    Args:
        tp: Type to extract from.

    Returns:
        Set of TypeVars.
    """
    variables: set[TypeVar] = set()
    if isinstance(tp, TypeVar):
        return {tp}
    if hasattr(tp, "__parameters__"):
        variables.update(tp.__parameters__)
    for arg in get_args(tp):
        variables.update(type_variables(arg))
    return variables


def has_type_hints(func: Callable) -> bool:
    """Check if function has type hints.

    Args:
        func: Function to check.

    Returns:
        True if has hints.
    """
    try:
        hints = get_type_hints(func)
        return bool(hints)
    except Exception:
        return False


class CallableInfo:
    """Inspect callable signatures."""

    def __init__(self, func: Callable) -> None:
        self._func = func
        self._sig = None
        try:
            self._sig = inspect.signature(func)
        except (ValueError, TypeError):
            pass

    @property
    def signature(self):
        """Get signature."""
        return self._sig

    def parameters(self) -> list[dict]:
        """Get parameter info."""
        if not self._sig:
            return []
        result = []
        for name, param in self._sig.parameters.items():
            result.append({
                "name": name,
                "kind": param.kind.name,
                "default": param.default,
                "annotation": param.annotation,
            })
        return result

    def return_type(self) -> Any:
        """Get return type annotation."""
        if not self._sig:
            return Any
        return self._sig.return_annotation


class TypeBuilder(Generic[T]):
    """Build complex types programmatically."""

    def __init__(self, base: type) -> None:
        self._base = base
        self._args: list[type] = []

    def with_args(self, *args: type) -> TypeBuilder[T]:
        """Add type arguments."""
        self._args.extend(args)
        return self

    def build(self) -> Any:
        """Build the type."""
        if not self._args:
            return self._base
        return self._base[tuple(self._args)]

    def list_type(self) -> type:
        """Build as list type."""
        if self._args:
            return list[self._args[0]]
        return list

    def dict_type(self) -> type:
        """Build as dict type."""
        if len(self._args) >= 2:
            return dict[self._args[0], self._args[1]]
        return dict

    def optional_type(self) -> type:
        """Build as Optional."""
        if self._args:
            return Union[self._args[0], type(None)]
        return Union[Any, None]


class ProtocolChecker:
    """Check if objects satisfy protocols."""

    def __init__(self) -> None:
        from typing import Protocol
        self._Protocol = Protocol

    def has_method(self, obj: Any, method_name: str) -> bool:
        """Check if object has method.

        Args:
            obj: Object to check.
            method_name: Name of method.

        Returns:
            True if has callable method.
        """
        return hasattr(obj, method_name) and callable(getattr(obj, method_name))

    def supports_protocol(self, obj: Any, protocol: type) -> bool:
        """Check if object supports protocol.

        Args:
            obj: Object to check.
            protocol: Protocol type.

        Returns:
            True if supports.
        """
        if not isinstance(protocol, type) or not issubclass(protocol, self._Protocol):
            return False
        for attr in dir(protocol):
            if not attr.startswith("_") and not self.has_method(obj, attr):
                return False
        return True

    def required_methods(self, protocol: type) -> list[str]:
        """Get required methods from protocol."""
        if not isinstance(protocol, type) or not issubclass(protocol, self._Protocol):
            return []
        return [attr for attr in dir(protocol) if not attr.startswith("_")]
