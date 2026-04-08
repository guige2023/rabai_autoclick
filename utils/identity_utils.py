"""Identity and equality utilities.

Provides identity comparison, equality checking,
and object comparison helpers for automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Type


def identity(x: T) -> T:
    """Return argument unchanged.

    Example:
        identity(5)  # 5
    """
    return x


def constantly(x: T) -> Callable[..., T]:
    """Return function that always returns x.

    Example:
        f = constantly(42)
        f(anything)  # 42
    """
    return lambda *args, **kwargs: x


def eq(x: Any) -> Callable[[Any], bool]:
    """Create equality predicate.

    Example:
        is_zero = eq(0)
        is_zero(0)  # True
        is_zero(1)  # False
    """
    return lambda y: x == y


def ne(x: Any) -> Callable[[Any], bool]:
    """Create inequality predicate.

    Example:
        not_none = ne(None)
        not_none(5)  # True
        not_none(None)  # False
    """
    return lambda y: x != y


def lt(x: Any) -> Callable[[Any], bool]:
    """Create less-than predicate.

    Example:
        is_negative = lt(0)
        is_negative(-1)  # True
        is_negative(1)  # False
    """
    return lambda y: y < x


def le(x: Any) -> Callable[[Any], bool]:
    """Create less-than-or-equal predicate."""
    return lambda y: y <= x


def gt(x: Any) -> Callable[[Any], bool]:
    """Create greater-than predicate.

    Example:
        is_positive = gt(0)
        is_positive(1)  # True
        is_positive(-1)  # False
    """
    return lambda y: y > x


def ge(x: Any) -> Callable[[Any], bool]:
    """Create greater-than-or-equal predicate."""
    return lambda y: y >= x


def is_none(x: Any) -> bool:
    """Check if value is None.

    Example:
        is_none(None)  # True
        is_none(0)  # False
    """
    return x is None


def is_not_none(x: Any) -> bool:
    """Check if value is not None."""
    return x is not None


def is_truthy(x: Any) -> bool:
    """Check if value is truthy."""
    return bool(x)


def is_falsy(x: Any) -> bool:
    """Check if value is falsy."""
    return not x


def is_instance_of(*types: Type) -> Callable[[Any], bool]:
    """Create isinstance predicate.

    Example:
        is_num = is_instance_of(int, float)
        is_num(5)  # True
        is_num("5")  # False
    """
    return lambda x: isinstance(x, types)


def is_type(t: Type) -> Callable[[Any], bool]:
    """Create type equality predicate.

    Example:
        is_str = is_type(str)
        is_str("hello")  # True
        is_str(123)  # False
    """
    return lambda x: type(x) is t


def compare_by(*funcs: Callable[[Any], Any]) -> Callable[[Any, Any], int]:
    """Create comparator using key functions.

    Example:
        cmp_by_length = compare_by(len)
        cmp_by_length("ab", "abc")  # -1
        cmp_by_length("abc", "ab")  # 1
    """
    def comparator(a: Any, b: Any) -> int:
        for func in funcs:
            va, vb = func(a), func(b)
            if va < vb:
                return -1
            if va > vb:
                return 1
        return 0
    return comparator


def deep_eq(a: Any, b: Any) -> bool:
    """Deep equality comparison.

    Example:
        deep_eq([1, [2, 3]], [1, [2, 3]])  # True
        deep_eq({"a": [1, 2]}, {"a": [1, 2]})  # True
    """
    if type(a) != type(b):
        return False
    if isinstance(a, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(deep_eq(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(deep_eq(av, bv) for av, bv in zip(a, b))
    return a == b


def equals(a: Any, b: Any, deep: bool = False) -> bool:
    """Compare two values for equality.

    Args:
        a: First value.
        b: Second value.
        deep: Use deep comparison.

    Returns:
        True if equal.
    """
    if deep:
        return deep_eq(a, b)
    return a == b


class Eq:
    """Value equality wrapper.

    Example:
        items = [Eq([1, 2]), Eq([1, 2]), Eq([1, 3])]
        len(set(items))  # 2 (third item is unique)
    """

    def __init__(self, value: Any) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Eq):
            return False
        return deep_eq(self.value, other.value)

    def __hash__(self) -> int:
        return hash(self._make_hashable(self.value))

    def _make_hashable(self, value: Any) -> Any:
        if isinstance(value, dict):
            return tuple(sorted((k, self._make_hashable(v)) for k, v in value.items()))
        if isinstance(value, list):
            return tuple(self._make_hashable(v) for v in value)
        return value


class HashCache:
    """Cache for expensive hash computations."""

    def __init__(self) -> None:
        self._cache: Dict[int, Any] = {}

    def memoized_hash(self, obj: Any) -> int:
        """Get memoized hash of object."""
        h = hash(obj)
        if h not in self._cache:
            self._cache[h] = obj
        return h

    def clear(self) -> None:
        """Clear cache."""
        self._cache.clear()


def same(x: Any, y: Any) -> bool:
    """Check if two references are the same object.

    Example:
        a = [1, 2]
        b = a
        c = [1, 2]
        same(a, b)  # True
        same(a, c)  # False
    """
    return x is y


def not_same(x: Any, y: Any) -> bool:
    """Check if two references are not the same object."""
    return x is not y
