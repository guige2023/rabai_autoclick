"""Operator utilities v3 - comparison and accessor patterns.

Extended operator utilities for comparison chains,
 attribute/item accessors, and functional operators.
"""

from __future__ import annotations

import operator
from typing import Any, Callable

__all__ = [
    "ComparisonChain",
    "Accessor",
    "attr_accessor",
    "item_accessor",
    "method_caller",
    "binary_op",
    "unary_op",
    "truthy",
    "falsy",
    "length",
    "contains",
    "not_contains",
]


class ComparisonChain:
    """Chain comparison operations."""

    def __init__(self, value: Any) -> None:
        self._value = value
        self._result = True

    def lt(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value < other
        return self

    def le(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value <= other
        return self

    def gt(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value > other
        return self

    def ge(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value >= other
        return self

    def eq(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value == other
        return self

    def ne(self, other: Any) -> ComparisonChain:
        if self._result:
            self._result = self._value != other
        return self

    def result(self) -> bool:
        return self._result

    def __bool__(self) -> bool:
        return self._result


class Accessor:
    """Generic accessor factory."""

    @staticmethod
    def attr(name: str) -> Callable[[Any], Any]:
        """Create attribute accessor."""
        return operator.attrgetter(name)

    @staticmethod
    def item(key: Any) -> Callable[[Any], Any]:
        """Create item accessor."""
        return operator.itemgetter(key)

    @staticmethod
    def method(name: str, *args: Any, **kwargs: Any) -> Callable[[Any], Any]:
        """Create method caller."""
        return operator.methodcaller(name, *args, **kwargs)


attr_accessor = Accessor.attr
item_accessor = Accessor.item
method_caller = Accessor.method


def binary_op(op: str) -> Callable[[Any, Any], Any]:
    """Get binary operator function.

    Args:
        op: Operator name.

    Returns:
        Operator function.
    """
    ops = {
        "add": operator.add,
        "sub": operator.sub,
        "mul": operator.mul,
        "truediv": operator.truediv,
        "floordiv": operator.floordiv,
        "mod": operator.mod,
        "pow": operator.pow,
        "and": operator.and_,
        "or": operator.or_,
        "xor": operator.xor,
        "eq": operator.eq,
        "ne": operator.ne,
        "lt": operator.lt,
        "le": operator.le,
        "gt": operator.gt,
        "ge": operator.ge,
    }
    return ops.get(op, operator.eq)


def unary_op(op: str) -> Callable[[Any], Any]:
    """Get unary operator function.

    Args:
        op: Operator name.

    Returns:
        Operator function.
    """
    ops = {
        "neg": operator.neg,
        "pos": operator.pos,
        "abs": operator.abs,
        "invert": operator.invert,
        "not": operator.not_,
    }
    return ops.get(op, operator.neg)


def truthy(value: Any) -> bool:
    """Check if value is truthy."""
    return bool(value)


def falsy(value: Any) -> bool:
    """Check if value is falsy."""
    return not bool(value)


def length(value: Any) -> int:
    """Get length of value."""
    return len(value)


def contains(container: Any, value: Any) -> bool:
    """Check if container contains value."""
    return value in container


def not_contains(container: Any, value: Any) -> bool:
    """Check if container does not contain value."""
    return value not in container
