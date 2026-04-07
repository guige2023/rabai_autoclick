"""Operator utilities v2 - extended operator operations.

Extended operator module utilities including
 attribute access, item access, and comparison.
"""

from __future__ import annotations

import operator
from operator import (
    add, sub, mul, truediv, floordiv, mod, pow,
    and_, or_, xor, not_, neg, pos, abs_,
    lt, le, eq, ne, ge, gt,
    getitem, setitem, delitem,
    attrgetter, itemgetter, methodcaller,
)
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "apply_binary",
    "apply_unary",
    "apply_compare",
    "safe_get",
    "safe_set",
    "safe_del",
    "attr_get",
    "attr_set",
    "item_get",
    "item_set",
    "method_call",
    "OpFactory",
    "ComparisonChain",
    "BinaryOps",
    "UnaryOps",
    "is_truthy",
    "is_falsy",
    "to_bool",
    "negate",
    "invert",
    "length_hint",
    "index_of",
    "count_of",
    "contains",
    "not_contains",
]


T = TypeVar("T")


def apply_binary(op: Callable[[Any, Any], Any], a: Any, b: Any) -> Any:
    """Apply binary operator.

    Args:
        op: Operator function.
        a: Left operand.
        b: Right operand.

    Returns:
        Result of operation.
    """
    return op(a, b)


def apply_unary(op: Callable[[Any], Any], a: Any) -> Any:
    """Apply unary operator.

    Args:
        op: Operator function.
        a: Operand.

    Returns:
        Result.
    """
    return op(a)


def apply_compare(op: Callable[[Any, Any], bool], a: Any, b: Any) -> bool:
    """Apply comparison operator.

    Args:
        op: Comparison operator.
        a: Left operand.
        b: Right operand.

    Returns:
        True or False.
    """
    return op(a, b)


def safe_get(obj: Any, key: Any, default: Any = None) -> Any:
    """Safely get item/attribute with default.

    Args:
        obj: Object to get from.
        key: Key or attribute name.
        default: Default value.

    Returns:
        Value or default.
    """
    try:
        return obj[key] if isinstance(key, (int, slice)) else getattr(obj, key)
    except (KeyError, IndexError, AttributeError, TypeError):
        return default


def safe_set(obj: Any, key: Any, value: Any) -> bool:
    """Safely set item/attribute.

    Args:
        obj: Object to set on.
        key: Key or attribute name.
        value: Value to set.

    Returns:
        True if successful.
    """
    try:
        if isinstance(key, (int, slice)):
            obj[key] = value
        else:
            setattr(obj, key, value)
        return True
    except (KeyError, IndexError, AttributeError, TypeError, ValueError):
        return False


def safe_del(obj: Any, key: Any) -> bool:
    """Safely delete item/attribute.

    Args:
        obj: Object to delete from.
        key: Key or attribute name.

    Returns:
        True if successful.
    """
    try:
        if isinstance(key, (int, slice)):
            del obj[key]
        else:
            delattr(obj, key)
        return True
    except (KeyError, IndexError, AttributeError, TypeError):
        return False


def attr_get(name: str) -> Callable[[Any], Any]:
    """Create attribute getter.

    Args:
        name: Attribute name.

    Returns:
        Getter function.
    """
    return attrgetter(name)


def attr_set(name: str, value: Any) -> Callable[[Any], None]:
    """Create attribute setter.

    Args:
        name: Attribute name.
        value: Value to set.

    Returns:
        Setter function.
    """
    def setter(obj: Any) -> None:
        setattr(obj, name, value)
    return setter


def item_get(key: Any) -> Callable[[Any], Any]:
    """Create item getter.

    Args:
        key: Item key.

    Returns:
        Getter function.
    """
    return itemgetter(key)


def item_set(key: Any, value: Any) -> Callable[[Any], None]:
    """Create item setter.

    Args:
        key: Item key.
        value: Value to set.

    Returns:
        Setter function.
    """
    def setter(obj: Any) -> None:
        obj[key] = value
    return setter


def method_call(name: str, *args: Any, **kwargs: Any) -> Callable[[Any], Any]:
    """Create method caller.

    Args:
        name: Method name.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Caller function.
    """
    return methodcaller(name, *args, **kwargs)


class OpFactory(Generic[T]):
    """Factory for creating operator functions."""

    _BINARY_OPS = {
        "+": add,
        "-": sub,
        "*": mul,
        "/": truediv,
        "//": floordiv,
        "%": mod,
        "**": pow,
        "&": and_,
        "|": or_,
        "^": xor,
        "==": eq,
        "!=": ne,
        "<": lt,
        "<=": le,
        ">": gt,
        ">=": ge,
    }

    _UNARY_OPS = {
        "-": neg,
        "+": pos,
        "abs": abs_,
        "not": not_,
    }

    @classmethod
    def binary(cls, op: str) -> Callable[[Any, Any], Any]:
        """Get binary operator.

        Args:
            op: Operator symbol.

        Returns:
            Operator function.

        Raises:
            ValueError: If unknown operator.
        """
        if op not in cls._BINARY_OPS:
            raise ValueError(f"Unknown operator: {op}")
        return cls._BINARY_OPS[op]

    @classmethod
    def unary(cls, op: str) -> Callable[[Any], Any]:
        """Get unary operator.

        Args:
            op: Operator symbol.

        Returns:
            Operator function.

        Raises:
            ValueError: If unknown operator.
        """
        if op not in cls._UNARY_OPS:
            raise ValueError(f"Unknown operator: {op}")
        return cls._UNARY_OPS[op]

    @classmethod
    def create_comparison(cls, op: str) -> Callable[[Any, Any], bool]:
        """Create comparison operator.

        Args:
            op: Comparison symbol.

        Returns:
            Comparison function.
        """
        return cls.binary(op)


class ComparisonChain:
    """Chain comparison operations."""

    def __init__(self, value: Any) -> None:
        self._value = value
        self._result = True

    def eq(self, other: Any) -> ComparisonChain:
        """Check equality."""
        if self._result:
            self._result = self._value == other
        return self

    def ne(self, other: Any) -> ComparisonChain:
        """Check inequality."""
        if self._result:
            self._result = self._value != other
        return self

    def lt(self, other: Any) -> ComparisonChain:
        """Check less than."""
        if self._result:
            self._result = self._value < other
        return self

    def le(self, other: Any) -> ComparisonChain:
        """Check less or equal."""
        if self._result:
            self._result = self._value <= other
        return self

    def gt(self, other: Any) -> ComparisonChain:
        """Check greater than."""
        if self._result:
            self._result = self._value > other
        return self

    def ge(self, other: Any) -> ComparisonChain:
        """Check greater or equal."""
        if self._result:
            self._result = self._value >= other
        return self

    def result(self) -> bool:
        """Get chain result."""
        return self._result

    def __bool__(self) -> bool:
        return self._result


class BinaryOps:
    """Binary operation utilities."""

    @staticmethod
    def add(a: Any, b: Any) -> Any:
        return operator.add(a, b)

    @staticmethod
    def sub(a: Any, b: Any) -> Any:
        return operator.sub(a, b)

    @staticmethod
    def mul(a: Any, b: Any) -> Any:
        return operator.mul(a, b)

    @staticmethod
    def div(a: Any, b: Any) -> Any:
        return operator.truediv(a, b)

    @staticmethod
    def floordiv(a: Any, b: Any) -> Any:
        return operator.floordiv(a, b)

    @staticmethod
    def mod(a: Any, b: Any) -> Any:
        return operator.mod(a, b)

    @staticmethod
    def pow(a: Any, b: Any) -> Any:
        return operator.pow(a, b)

    @staticmethod
    def and_(a: Any, b: Any) -> Any:
        return operator.and_(a, b)

    @staticmethod
    def or_(a: Any, b: Any) -> Any:
        return operator.or_(a, b)

    @staticmethod
    def xor(a: Any, b: Any) -> Any:
        return operator.xor(a, b)


class UnaryOps:
    """Unary operation utilities."""

    @staticmethod
    def neg(a: Any) -> Any:
        return operator.neg(a)

    @staticmethod
    def pos(a: Any) -> Any:
        return operator.pos(a)

    @staticmethod
    def abs(a: Any) -> Any:
        return operator.abs(a)

    @staticmethod
    def invert(a: Any) -> Any:
        return operator.invert(a)

    @staticmethod
    def not_(a: Any) -> bool:
        return operator.not_(a)


def is_truthy(value: Any) -> bool:
    """Check if value is truthy.

    Args:
        value: Value to check.

    Returns:
        True if truthy.
    """
    return bool(value)


def is_falsy(value: Any) -> bool:
    """Check if value is falsy.

    Args:
        value: Value to check.

    Returns:
        True if falsy.
    """
    return not bool(value)


def to_bool(value: Any) -> bool:
    """Convert value to bool.

    Args:
        value: Value to convert.

    Returns:
        Boolean value.
    """
    return bool(value)


def negate(value: Any) -> Any:
    """Negate value.

    Args:
        value: Value to negate.

    Returns:
        Negated value.
    """
    return operator.neg(value)


def invert(value: Any) -> Any:
    """Invert value (bitwise not).

    Args:
        value: Value to invert.

    Returns:
        Inverted value.
    """
    return operator.invert(value)


def length_hint(obj: Any, default: int = 0) -> int:
    """Get length hint of object.

    Args:
        obj: Object to hint.
        default: Default value.

    Returns:
        Length hint.
    """
    return operator.length_hint(obj, default)


def index_of(container: Any, value: Any) -> int:
    """Get index of value in container.

    Args:
        container: Container to search.
        value: Value to find.

    Returns:
        Index or -1.
    """
    try:
        return container.index(value)
    except (ValueError, AttributeError):
        return -1


def count_of(container: Any, value: Any) -> int:
    """Count occurrences of value.

    Args:
        container: Container to search.
        value: Value to count.

    Returns:
        Count.
    """
    return sum(1 for x in container if x == value)


def contains(container: Any, value: Any) -> bool:
    """Check if container contains value.

    Args:
        container: Container to check.
        value: Value to find.

    Returns:
        True if contained.
    """
    return value in container


def not_contains(container: Any, value: Any) -> bool:
    """Check if container does not contain value.

    Args:
        container: Container to check.
        value: Value to find.

    Returns:
        True if not contained.
    """
    return value not in container
