"""typing action extensions for rabai_autoclick.

Provides utilities for type checking, type hints manipulation,
runtime type validation, and generic type operations.
"""

from __future__ import annotations

import sys
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Literal,
    Optional,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

__all__ = [
    "is_type",
    "is_subtype",
    "is_union",
    "is_optional",
    "is_list",
    "is_dict",
    "is_tuple",
    "is_callable",
    "is_literal",
    "is_generic",
    "get_type_vars",
    "resolve_type",
    "type_name",
    "type_args",
    "cast",
    "safe_cast",
    "NoneType",
    "AnyType",
    "IntType",
    "StrType",
    "FloatType",
    "BoolType",
    "ListType",
    "DictType",
    "TupleType",
    "UnionType",
    "OptionalType",
    "CallableType",
    "TypedDict",
    "NamedTuple",
    "Protocol",
    "runtime_type",
    "validate_type",
    "type_guard",
    "overload",
    "TypeChecker",
    "TypeBuilder",
    "TypeCache",
]


NoneType = type(None)
AnyType = object
IntType = int
StrType = str
FloatType = float
BoolType = bool


ListType = TypeVar("ListType")
DictType = TypeVar("DictType")
TupleType = TypeVar("TupleType")
UnionType = TypeVar("UnionType")
OptionalType = TypeVar("OptionalType")
CallableType = TypeVar("CallableType")


def type_name(tp: Any) -> str:
    """Get a human-readable name for a type.

    Args:
        tp: Type to get name for.

    Returns:
        String name of the type.
    """
    origin = get_origin(tp)
    if origin is None:
        name = getattr(tp, "__name__", str(tp))
        return name

    args = get_args(tp)
    if args:
        arg_names = ", ".join(type_name(a) for a in args)
        return f"{origin.__name__}[{arg_names}]"

    return origin.__name__


def is_type(obj: Any, tp: Any) -> bool:
    """Check if object is an instance of type or generic type.

    Args:
        obj: Object to check.
        tp: Type to check against.

    Returns:
        True if obj is instance of tp.
    """
    if tp is AnyType:
        return True

    origin = get_origin(tp)
    if origin is None:
        return isinstance(obj, tp)

    return isinstance(obj, origin)


def is_subtype(tp1: Any, tp2: Any) -> bool:
    """Check if tp1 is a subtype of tp2.

    Args:
        tp1: Potential subtype.
        tp2: Potential supertype.

    Returns:
        True if tp1 is subtype of tp2.
    """
    try:
        from typing import get_origin

        o1, o2 = get_origin(tp1), get_origin(tp2)
        if o1 is None and o2 is None:
            return issubclass(tp1, tp2)
        if o1 is not None and o2 is not None:
            return issubclass(o1, o2)
        return False
    except Exception:
        return False


def is_union(tp: Any) -> bool:
    """Check if type is a Union type.

    Args:
        tp: Type to check.

    Returns:
        True if tp is Union.
    """
    return get_origin(tp) is Union


def is_optional(tp: Any) -> bool:
    """Check if type is Optional (Union with None).

    Args:
        tp: Type to check.

    Returns:
        True if tp is Optional.
    """
    if get_origin(tp) is Union:
        args = get_args(tp)
        return NoneType in args
    return False


def is_list(tp: Any) -> bool:
    """Check if type is List or list.

    Args:
        tp: Type to check.

    Returns:
        True if tp is a list type.
    """
    origin = get_origin(tp)
    return origin in (list, List) or tp is list


def is_dict(tp: Any) -> bool:
    """Check if type is Dict or dict.

    Args:
        tp: Type to check.

    Returns:
        True if tp is a dict type.
    """
    origin = get_origin(tp)
    return origin in (dict, Dict) or tp is dict


def is_tuple(tp: Any) -> bool:
    """Check if type is Tuple or tuple.

    Args:
        tp: Type to check.

    Returns:
        True if tp is a tuple type.
    """
    origin = get_origin(tp)
    return origin in (tuple, Tuple) or tp is tuple


def is_callable(tp: Any) -> bool:
    """Check if type is Callable.

    Args:
        tp: Type to check.

    Returns:
        True if tp is callable.
    """
    return callable(tp) or get_origin(tp) is Callable


def is_literal(tp: Any) -> bool:
    """Check if type is Literal.

    Args:
        tp: Type to check.

    Returns:
        True if tp is Literal.
    """
    return get_origin(tp) is Literal


def is_generic(tp: Any) -> bool:
    """Check if type is a generic type with parameters.

    Args:
        tp: Type to check.

    Returns:
        True if tp has type parameters.
    """
    try:
        from typing import _GenericAlias

        return isinstance(tp, _GenericAlias) and bool(get_args(tp))
    except Exception:
        return hasattr(tp, "__parameters__") and len(getattr(tp, "__parameters__", [])) > 0


def get_type_vars(tp: Any) -> tuple:
    """Get type variables from a generic type.

    Args:
        tp: Generic type.

    Returns:
        Tuple of type variables.
    """
    try:
        return getattr(tp, "__parameters__", ())
    except Exception:
        return ()


def resolve_type(tp: Any, type_vars: dict[Any, Any]) -> Any:
    """Resolve a generic type by substituting type variables.

    Args:
        tp: Generic type to resolve.
        type_vars: Mapping of type variables to concrete types.

    Returns:
        Resolved type.
    """
    args = get_args(tp)
    if not args:
        return tp

    resolved_args = tuple(type_vars.get(arg, arg) for arg in args)
    origin = get_origin(tp)
    if origin is not None:
        return origin[resolved_args]
    return tp


def type_args(tp: Any) -> tuple:
    """Get type arguments from a generic type.

    Args:
        tp: Generic type.

    Returns:
        Tuple of type arguments.
    """
    return get_args(tp)


def safe_cast(tp: Any, value: Any, default: Any = None) -> Any:
    """Safely cast a value to a type, returning default on failure.

    Args:
        tp: Target type.
        value: Value to cast.
        default: Default value if cast fails.

    Returns:
        Cast value or default.
    """
    try:
        if not is_type(value, tp):
            return default
        return value
    except Exception:
        return default


def runtime_type(obj: Any) -> str:
    """Get runtime type name of an object.

    Args:
        obj: Object to inspect.

    Returns:
        Type name string.
    """
    return type(obj).__name__


def validate_type(value: Any, expected_type: Any) -> tuple[bool, str | None]:
    """Validate a value against an expected type.

    Args:
        value: Value to validate.
        expected_type: Type to validate against.

    Returns:
        Tuple of (is_valid, error_message).
    """
    try:
        if expected_type is AnyType:
            return True, None

        origin = get_origin(expected_type)
        if origin is None:
            if not isinstance(value, expected_type):
                return False, f"Expected {type_name(expected_type)}, got {runtime_type(value)}"
            return True, None

        if origin in (list, List):
            if not isinstance(value, list):
                return False, f"Expected list, got {runtime_type(value)}"
            args = get_args(expected_type)
            if args:
                for i, item in enumerate(value):
                    valid, err = validate_type(item, args[0])
                    if not valid:
                        return False, f"Item[{i}]: {err}"
            return True, None

        if origin in (dict, Dict):
            if not isinstance(value, dict):
                return False, f"Expected dict, got {runtime_type(value)}"
            args = get_args(expected_type)
            if args and len(args) == 2:
                for k, v in value.items():
                    valid_k, err_k = validate_type(k, args[0])
                    if not valid_k:
                        return False, f"Key {k}: {err_k}"
                    valid_v, err_v = validate_type(v, args[1])
                    if not valid_v:
                        return False, f"Value[{k}]: {err_v}"
            return True, None

        if not isinstance(value, origin):
            return False, f"Expected {type_name(expected_type)}, got {runtime_type(value)}"

        return True, None

    except Exception as e:
        return False, f"Validation error: {e}"


def type_guard(tp: Any) -> Callable[[Callable], Callable]:
    """Decorator to add runtime type checking to a function.

    Args:
        tp: Expected return type.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            valid, err = validate_type(result, tp)
            if not valid:
                raise TypeError(f"{func.__name__} return type error: {err}")
            return result
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator


class TypedDict(dict):
    """A dict with type checking on keys and values.

    Example:
        class MyDict(TypedDict):
            __key_types__ = {"name": str, "age": int}
    """

    __key_types__: dict[str, type] = {}

    def __init__(self, data: dict | None = None, **kwargs: Any) -> None:
        super().__init__()
        if data:
            self._validate_and_set(data)
        self._validate_and_set(kwargs)

    def _validate_and_set(self, data: dict) -> None:
        for key, value in data.items():
            expected_type = self.__key_types__.get(key, Any)
            if not isinstance(value, expected_type) and expected_type is not Any:
                raise TypeError(f"Key '{key}' expects {expected_type}, got {type(value)}")
            super().__setitem__(key, value)

    def __setitem__(self, key: str, value: Any) -> None:
        expected_type = self.__key_types__.get(key, Any)
        if not isinstance(value, expected_type) and expected_type is not Any:
            raise TypeError(f"Key '{key}' expects {expected_type}, got {type(value)}")
        super().__setitem__(key, value)


class TypeChecker:
    """Runtime type checker with caching."""

    def __init__(self) -> None:
        self._cache: dict[tuple, bool] = {}

    def check(self, value: Any, tp: Any) -> bool:
        """Check if value matches type.

        Args:
            value: Value to check.
            tp: Type to check against.

        Returns:
            True if valid.
        """
        cache_key = (id(value), id(tp))
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = is_type(value, tp)
        self._cache[cache_key] = result
        return result

    def clear_cache(self) -> None:
        """Clear the type cache."""
        self._cache.clear()


class TypeBuilder(Generic[T]):
    """Builder for constructing complex types programmatically."""

    def __init__(self, base_type: type[T]) -> None:
        self._base = base_type
        self._type_args: list[type] = []

    def add_arg(self, tp: type) -> TypeBuilder[T]:
        """Add a type argument.

        Args:
            tp: Type argument to add.

        Returns:
            Self for chaining.
        """
        self._type_args.append(tp)
        return self

    def build(self) -> Any:
        """Build the final type.

        Returns:
            Constructed type.
        """
        if not self._type_args:
            return self._base
        return self._base[tuple(self._type_args)]


class TypeCache:
    """Cache for type-related computations."""

    def __init__(self) -> None:
        self._type_names: dict[Any, str] = {}
        self._type_hints: dict[Callable, dict[str, type]] = {}

    def get_type_name(self, tp: Any) -> str:
        """Get cached type name.

        Args:
            tp: Type to name.

        Returns:
            Cached name.
        """
        if tp not in self._type_names:
            self._type_names[tp] = type_name(tp)
        return self._type_names[tp]

    def get_type_hints(self, func: Callable) -> dict[str, type]:
        """Get cached type hints for a function.

        Args:
            func: Function to get hints for.

        Returns:
            Dict of parameter names to types.
        """
        if func not in self._type_hints:
            try:
                self._type_hints[func] = get_type_hints(func)
            except Exception:
                self._type_hints[func] = {}
        return self._type_hints[func]
