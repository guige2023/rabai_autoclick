"""Types utilities v2 - extended built-in type operations.

Extended type utilities including unions,
 optionals, and type coercion.
"""

from __future__ import annotations

from typing import Any, Union, get_origin, get_args, TypeVar, Generic
import types as builtins_types

__all__ = [
    "is_type",
    "is_subclass",
    "is_instance",
    "coerce_to",
    "coerce_many",
    "safe_cast",
    "union_types",
    "intersect_types",
    "is_none_type",
    "is_bool_type",
    "is_int_type",
    "is_float_type",
    "is_str_type",
    "is_bytes_type",
    "is_list_type",
    "is_dict_type",
    "is_tuple_type",
    "is_set_type",
    "is_callable_type",
    "is_generator_type",
    "type_hash",
    "type_name",
    "type_args",
    "resolve_typevar",
    "TypeBuilder",
    "TypeMatcher",
    "TypeCoercer",
    "TypeRegistry",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def is_type(obj: Any) -> bool:
    """Check if object is a type.

    Args:
        obj: Object to check.

    Returns:
        True if obj is a type.
    """
    return isinstance(obj, builtins_types.TypeType)


def is_subclass(cls1: Any, cls2: Any) -> bool:
    """Check if cls1 is subclass of cls2.

    Args:
        cls1: Potential subclass.
        cls2: Potential superclass.

    Returns:
        True if cls1 is subclass of cls2.
    """
    try:
        return issubclass(cls1, cls2)
    except TypeError:
        return False


def is_instance(obj: Any, cls: Any) -> bool:
    """Check if obj is instance of cls.

    Args:
        obj: Object to check.
        cls: Type to check against.

    Returns:
        True if obj is instance.
    """
    try:
        return isinstance(obj, cls)
    except TypeError:
        return False


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
    if value is None and target_type is not type(None):
        raise ValueError(f"Cannot coerce None to {target_type}")
    if isinstance(value, target_type):
        return value
    try:
        if target_type is bool:
            return bool(value)
        if target_type is int:
            if isinstance(value, float):
                if value.is_integer():
                    return int(value)
                raise ValueError(f"Cannot coerce {value} to int")
            return int(value)
        if target_type is float:
            return float(value)
        if target_type is str:
            return str(value)
        if target_type is bytes:
            return bytes(value)
        if target_type is list:
            return list(value)
        if target_type is dict:
            return dict(value)
        if target_type is tuple:
            return tuple(value)
        if target_type is set:
            return set(value)
        return target_type(value)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Cannot coerce {type(value).__name__} to {target_type.__name__}") from e


def coerce_many(values: list[Any], target_type: type) -> list[Any]:
    """Coerce multiple values to type.

    Args:
        values: Values to coerce.
        target_type: Target type.

    Returns:
        List of coerced values.
    """
    return [coerce_to(v, target_type) for v in values]


def safe_cast(value: Any, target_type: type[T]) -> T | None:
    """Safely cast value to type.

    Args:
        value: Value to cast.
        target_type: Target type.

    Returns:
        Cast value or None if fails.
    """
    try:
        return coerce_to(value, target_type)
    except ValueError:
        return None


def union_types(*types: type) -> type:
    """Create union type.

    Args:
        *types: Types to union.

    Returns:
        Union type.
    """
    return Union[tuple(types)]


def intersect_types(cls1: type, cls2: type) -> type | None:
    """Find intersection of two types.

    Args:
        cls1: First type.
        cls2: Second type.

    Returns:
        Intersection type or None.
    """
    if issubclass(cls1, cls2):
        return cls1
    if issubclass(cls2, cls1):
        return cls2
    return None


def is_none_type(tp: Any) -> bool:
    """Check if type is NoneType.

    Args:
        tp: Type to check.

    Returns:
        True if NoneType.
    """
    return tp is type(None)


def is_bool_type(tp: Any) -> bool:
    """Check if type is bool."""
    return tp is bool


def is_int_type(tp: Any) -> bool:
    """Check if type is int."""
    return tp is int


def is_float_type(tp: Any) -> bool:
    """Check if type is float."""
    return tp is float


def is_str_type(tp: Any) -> bool:
    """Check if type is str."""
    return tp is str


def is_bytes_type(tp: Any) -> bool:
    """Check if type is bytes."""
    return tp is bytes


def is_list_type(tp: Any) -> bool:
    """Check if type is list or list[T]."""
    origin = get_origin(tp)
    return origin is list or tp is list


def is_dict_type(tp: Any) -> bool:
    """Check if type is dict or dict[K,V]."""
    origin = get_origin(tp)
    return origin is dict or tp is dict


def is_tuple_type(tp: Any) -> bool:
    """Check if type is tuple or tuple[T,...]."""
    origin = get_origin(tp)
    return origin is tuple or tp is tuple


def is_set_type(tp: Any) -> bool:
    """Check if type is set or set[T]."""
    origin = get_origin(tp)
    return origin is set or tp is set


def is_callable_type(tp: Any) -> bool:
    """Check if type is callable."""
    return callable(tp) or (hasattr(tp, "__call__"))


def is_generator_type(obj: Any) -> bool:
    """Check if object is a generator.

    Args:
        obj: Object to check.

    Returns:
        True if generator.
    """
    return hasattr(obj, "__next__") and hasattr(obj, "send") and hasattr(obj, "throw")


def type_hash(tp: type) -> int:
    """Get hash of type.

    Args:
        tp: Type to hash.

    Returns:
        Hash value.
    """
    return hash(tp.__name__)


def type_name(tp: Any) -> str:
    """Get name of type.

    Args:
        tp: Type.

    Returns:
        Type name.
    """
    return getattr(tp, "__name__", str(tp))


def type_args(tp: Any) -> tuple[Any, ...]:
    """Get type arguments from generic.

    Args:
        tp: Generic type.

    Returns:
        Tuple of type arguments.
    """
    return get_args(tp)


def resolve_typevar(tp: TypeVar) -> type | None:
    """Resolve TypeVar to actual type.

    Args:
        tp: TypeVar.

    Returns:
        Resolved type or None.
    """
    if hasattr(tp, "__bound__"):
        return tp.__bound__
    if hasattr(tp, "__constraints__") and tp.__constraints__:
        return tp.__constraints__[0]
    return None


class TypeBuilder:
    """Build complex types programmatically."""

    def __init__(self, base: type | None = None) -> None:
        self._base = base
        self._args: list[type] = []

    def with_args(self, *args: type) -> TypeBuilder:
        """Add type arguments.

        Args:
            *args: Type arguments.

        Returns:
            Self.
        """
        self._args.extend(args)
        return self

    def list_type(self) -> type:
        """Build list type."""
        if self._args:
            return list[self._args[0]]
        return list

    def dict_type(self) -> type:
        """Build dict type."""
        if len(self._args) >= 2:
            return dict[self._args[0], self._args[1]]
        return dict

    def set_type(self) -> type:
        """Build set type."""
        if self._args:
            return set[self._args[0]]
        return set

    def tuple_type(self) -> type:
        """Build tuple type."""
        if self._args:
            return tuple[self._args]
        return tuple

    def optional_type(self) -> type:
        """Build Optional type."""
        if self._args:
            return Union[self._args[0], type(None)]
        return Union[Any, None]

    def union_type(self) -> type:
        """Build Union type."""
        return Union[self._args]


class TypeMatcher:
    """Pattern matching for types."""

    def __init__(self, value: Any) -> None:
        self._value = value
        self._matched = False
        self._result: Any = None

    def case(self, tp: type, handler: Callable[[Any], Any]) -> TypeMatcher:
        """Match type and apply handler.

        Args:
            tp: Type to match.
            handler: Function to apply if matched.

        Returns:
            Self.
        """
        if not self._matched and isinstance(self._value, tp):
            self._matched = True
            self._result = handler(self._value)
        return self

    def default(self, handler: Callable[[Any], Any]) -> Any:
        """Default case if no match.

        Args:
            handler: Default handler.

        Returns:
            Result or None.
        """
        if not self._matched:
            return handler(self._value)
        return self._result


class TypeCoercer:
    """Coercion utilities."""

    def __init__(self, strict: bool = True) -> None:
        self._strict = strict
        self._coercions: dict[type, Callable[[Any], Any]] = {}

    def register(self, from_type: type, to_type: type, coercion: Callable[[Any], Any]) -> TypeCoercer:
        """Register coercion function.

        Args:
            from_type: Source type.
            to_type: Target type.
            coercion: Coercion function.

        Returns:
            Self.
        """
        self._coercions[(from_type, to_type)] = coercion
        return self

    def coerce(self, value: Any, target_type: type) -> Any:
        """Coerce value to target type.

        Args:
            value: Value to coerce.
            target_type: Target type.

        Returns:
            Coerced value.

        Raises:
            ValueError: If coercion not possible.
        """
        if isinstance(value, target_type):
            return value
        key = (type(value), target_type)
        if key in self._coercions:
            return self._coercions[key](value)
        if self._strict:
            raise ValueError(f"No coercion from {type(value).__name__} to {target_type.__name__}")
        return value

    def can_coerce(self, from_type: type, to_type: type) -> bool:
        """Check if coercion is possible.

        Args:
            from_type: Source type.
            to_type: Target type.

        Returns:
            True if coercion registered.
        """
        return (from_type, to_type) in self._coercions


class TypeRegistry(Generic[T]):
    """Registry for type-based objects."""

    def __init__(self) -> None:
        self._types: dict[type, T] = {}

    def register(self, tp: type, obj: T) -> None:
        """Register object for type.

        Args:
            tp: Type to register for.
            obj: Object to register.
        """
        self._types[tp] = obj

    def get(self, tp: type) -> T | None:
        """Get registered object for type.

        Args:
            tp: Type to lookup.

        Returns:
            Registered object or None.
        """
        for registered_type, obj in self._types.items():
            if issubclass(tp, registered_type):
                return obj
        return None

    def register_many(self, mappings: dict[type, T]) -> None:
        """Register multiple type mappings.

        Args:
            mappings: Dict of type -> object.
        """
        self._types.update(mappings)

    def types(self) -> list[type]:
        """List registered types."""
        return list(self._types.keys())
