"""filter_action module for rabai_autoclick.

Provides advanced filtering operations: predicate combinators,
set-based filtering, pattern matching, and conditional filtering utilities.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, List, Optional, Pattern, Sequence, Set, TypeVar, Union

__all__ = [
    "Predicate",
    "pred",
    "pred_and",
    "pred_or",
    "pred_not",
    "pred_xor",
    "filter_by",
    "filter_in",
    "filter_out",
    "filter_by_key",
    "filter_pattern",
    "filter_regex",
    "filter_range",
    "filter_any",
    "filter_all",
    "filter_none",
    "filter_distinct_by",
    "filter_indices",
    "Partition",
    "partition_pred",
    "select_keys",
    "exclude_keys",
    "include_where",
    "exclude_where",
    "filter_by_type",
    "filter_by_attr",
    "FilterChain",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class Predicate(Generic[T]):
    """Composable predicate wrapper."""

    def __init__(self, func: Callable[[T], bool]) -> None:
        self._func = func

    def __call__(self, item: T) -> bool:
        return self._func(item)

    def __and__(self, other: "Predicate[T]") -> "Predicate[T]":
        return Predicate(lambda x: self(x) and other(x))

    def __or__(self, other: "Predicate[T]") -> "Predicate[T]":
        return Predicate(lambda x: self(x) or other(x))

    def __invert__(self) -> "Predicate[T]":
        return Predicate(lambda x: not self(x))

    def __xor__(self, other: "Predicate[T]") -> "Predicate[T]":
        return Predicate(lambda x: self(x) != other(x))


def pred(func: Callable[[T], bool]) -> Predicate[T]:
    """Create a Predicate from a function."""
    return Predicate(func)


def pred_and(*preds: Predicate[T]) -> Predicate[T]:
    """AND multiple predicates."""
    return Predicate(lambda x: all(p(x) for p in preds))


def pred_or(*preds: Predicate[T]) -> Predicate[T]:
    """OR multiple predicates."""
    return Predicate(lambda x: any(p(x) for p in preds))


def pred_not(p: Predicate[T]) -> Predicate[T]:
    """Negate a predicate."""
    return Predicate(lambda x: not p(x))


def pred_xor(p1: Predicate[T], p2: Predicate[T]) -> Predicate[T]:
    """XOR two predicates."""
    return Predicate(lambda x: p1(x) != p2(x))


def filter_by(items: Iterable[T], pred: Callable[[T], bool]) -> Iterator[T]:
    """Filter items by predicate.

    Args:
        items: Input items.
        pred: Filter predicate.

    Yields:
        Items where pred returns True.
    """
    return (item for item in items if pred(item))


def filter_in(items: Iterable[T], allowed: Iterable[Any]) -> Iterator[T]:
    """Filter items to only those in allowed set.

    Args:
        items: Input items.
        allowed: Set/list of allowed values.

    Yields:
        Items that are in allowed.
    """
    allowed_set = set(allowed)
    return (item for item in items if item in allowed_set)


def filter_out(items: Iterable[T], excluded: Iterable[Any]) -> Iterator[T]:
    """Filter items to exclude those in excluded set.

    Args:
        items: Input items.
        excluded: Set/list of values to exclude.

    Yields:
        Items not in excluded.
    """
    excluded_set = set(excluded)
    return (item for item in items if item not in excluded_set)


def filter_by_key(
    items: Iterable[Dict[K, V]],
    key: K,
    pred: Optional[Callable[[V], bool]] = None,
) -> Iterator[Dict[K, V]]:
    """Filter dicts by key existence or value.

    Args:
        items: Input dicts.
        key: Key to filter on.
        pred: Optional value predicate.

    Yields:
        Dicts matching criteria.
    """
    for item in items:
        if key not in item:
            continue
        if pred is None or pred(item[key]):
            yield item


def filter_pattern(items: Iterable[str], pattern: str, case_sensitive: bool = False) -> Iterator[str]:
    """Filter strings by glob pattern.

    Args:
        items: Input strings.
        pattern: Glob pattern (e.g., "*.txt").
        case_sensitive: Whether to match case.

    Yields:
        Strings matching pattern.
    """
    flags = 0 if case_sensitive else re.IGNORECASE
    for item in items:
        if fnmatch.fnmatch(item, pattern, flags):
            yield item


def filter_regex(items: Iterable[str], pattern: str, flags: int = 0) -> Iterator[str]:
    """Filter strings by regex pattern.

    Args:
        items: Input strings.
        pattern: Regex pattern.
        flags: Regex flags.

    Yields:
        Strings matching pattern.
    """
    compiled: Pattern = re.compile(pattern, flags)
    for item in items:
        if compiled.search(item):
            yield item


def filter_range(
    items: Iterable[Union[int, float]],
    min_val: Optional[Union[int, float]] = None,
    max_val: Optional[Union[int, float]] = None,
    inclusive: bool = True,
) -> Iterator[Union[int, float]]:
    """Filter numbers within a range.

    Args:
        items: Input numbers.
        min_val: Minimum value (None = no minimum).
        max_val: Maximum value (None = no maximum).
        inclusive: Include boundary values.

    Yields:
        Numbers within range.
    """
    for item in items:
        if min_val is not None:
            if inclusive and item < min_val:
                continue
            if not inclusive and item <= min_val:
                continue
        if max_val is not None:
            if inclusive and item > max_val:
                continue
            if not inclusive and item >= max_val:
                continue
        yield item


def filter_any(
    items: Iterable[T],
    predicates: Sequence[Callable[[T], bool]],
) -> Iterator[T]:
    """Filter items where ANY predicate returns True.

    Args:
        items: Input items.
        predicates: List of predicates.

    Yields:
        Items matching at least one predicate.
    """
    for item in items:
        if any(p(item) for p in predicates):
            yield item


def filter_all(
    items: Iterable[T],
    predicates: Sequence[Callable[[T], bool]],
) -> Iterator[T]:
    """Filter items where ALL predicates return True.

    Args:
        items: Input items.
        predicates: List of predicates.

    Yields:
        Items matching all predicates.
    """
    for item in items:
        if all(p(item) for p in predicates):
            yield item


def filter_none(items: Iterable[Optional[T]]) -> Iterator[T]:
    """Filter out None values.

    Args:
        items: Input items that may be None.

    Yields:
        Non-None items.
    """
    return (item for item in items if item is not None)


def filter_distinct_by(
    items: Iterable[T],
    key_fn: Callable[[T], K],
) -> Iterator[T]:
    """Filter to distinct items by key function.

    Args:
        items: Input items.
        key_fn: Function to extract distinctness key.

    Yields:
        First occurrence of each distinct key.
    """
    seen: set = set()
    for item in items:
        key = key_fn(item)
        if key not in seen:
            seen.add(key)
            yield item


def filter_indices(
    items: Sequence[T],
    indices: Iterable[int],
) -> Iterator[T]:
    """Filter by specific indices.

    Args:
        items: Input sequence.
        indices: Indices to keep.

    Yields:
        Items at specified indices.
    """
    index_set = set(indices)
    for i, item in enumerate(items):
        if i in index_set:
            yield item


class Partition(Generic[T]):
    """Result of partitioning items."""

    def __init__(self, matching: List[T], non_matching: List[T]) -> None:
        self.matching = matching
        self.non_matching = non_matching

    @property
    def matched(self) -> List[T]:
        return self.matching

    @property
    def unmatched(self) -> List[T]:
        return self.non_matching

    def __len__(self) -> int:
        return len(self.matching) + len(self.non_matching)


def partition_pred(
    items: Iterable[T],
    pred: Callable[[T], bool],
) -> Partition[T]:
    """Partition items by predicate.

    Args:
        items: Input items.
        pred: Partition predicate.

    Returns:
        Partition with matching and non-matching items.
    """
    matching: List[T] = []
    non_matching: List[T] = []
    for item in items:
        if pred(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return Partition(matching, non_matching)


def select_keys(items: Iterable[Dict[K, V]], keys: Iterable[K]) -> Iterator[Dict[K, V]]:
    """Select only specified keys from dicts.

    Args:
        items: Input dicts.
        keys: Keys to keep.

    Yields:
        Dicts with only specified keys.
    """
    key_set = set(keys)
    for item in items:
        yield {k: item[k] for k in item if k in key_set}


def exclude_keys(items: Iterable[Dict[K, V]], keys: Iterable[K]) -> Iterator[Dict[K, V]]:
    """Exclude specified keys from dicts.

    Args:
        items: Input dicts.
        keys: Keys to remove.

    Yields:
        Dicts without specified keys.
    """
    key_set = set(keys)
    for item in items:
        yield {k: v for k, v in item.items() if k not in key_set}


def include_where(
    items: Iterable[Dict[K, V]],
    condition: Callable[[Dict[K, V]], bool],
) -> Iterator[Dict[K, V]]:
    """Include dicts where condition is True.

    Args:
        items: Input dicts.
        condition: Function taking dict, returning bool.

    Yields:
        Dicts where condition is True.
    """
    for item in items:
        if condition(item):
            yield item


def exclude_where(
    items: Iterable[Dict[K, V]],
    condition: Callable[[Dict[K, V]], bool],
) -> Iterator[Dict[K, V]]:
    """Exclude dicts where condition is True.

    Args:
        items: Input dicts.
        condition: Function taking dict, returning bool.

    Yields:
        Dicts where condition is False.
    """
    for item in items:
        if not condition(item):
            yield item


def filter_by_type(items: Iterable[Any], *types: type) -> Iterator[Any]:
    """Filter items by type.

    Args:
        items: Input items.
        *types: Allowed types.

    Yields:
        Items that are instances of any of the types.
    """
    for item in items:
        if isinstance(item, types):
            yield item


def filter_by_attr(
    items: Iterable[Any],
    attr: str,
    value: Any = True,
) -> Iterator[Any]:
    """Filter objects by attribute value.

    Args:
        items: Input objects.
        attr: Attribute name.
        value: Required attribute value (True = any truthy value).

    Yields:
        Objects with matching attribute.
    """
    for item in items:
        if not hasattr(item, attr):
            continue
        attr_val = getattr(item, attr)
        if value is True:
            if attr_val:
                yield item
        elif attr_val == value:
            yield item


class FilterChain(Generic[T]):
    """Chainable filter builder."""

    def __init__(self, items: Iterable[T]) -> None:
        self._items = list(items)
        self._filters: List[Callable[[List[T]], List[T]]] = []

    def where(self, pred: Callable[[T], bool]) -> "FilterChain[T]":
        """Add a where filter."""
        self._filters.append(lambda items: [x for x in items if pred(x)])
        return self

    def where_in(self, key: Callable[[T], Any], allowed: Iterable[Any]) -> "FilterChain[T]":
        """Filter where key is in allowed values."""
        allowed_set = set(allowed)
        self._filters.append(lambda items: [x for x in items if key(x) in allowed_set])
        return self

    def where_not_in(self, key: Callable[[T], Any], excluded: Iterable[Any]) -> "FilterChain[T]":
        """Filter where key is not in excluded values."""
        excluded_set = set(excluded)
        self._filters.append(lambda items: [x for x in items if key(x) not in excluded_set])
        return self

    def where_type(self, *types: type) -> "FilterChain[T]":
        """Filter by type."""
        self._filters.append(lambda items: [x for x in items if isinstance(x, types)])
        return self

    def take(self, n: int) -> "FilterChain[T]":
        """Take first n items."""
        self._filters.append(lambda items: items[:n])
        return self

    def skip(self, n: int) -> "FilterChain[T]":
        """Skip first n items."""
        self._filters.append(lambda items: items[n:])
        return self

    def distinct(self) -> "FilterChain[T]":
        """Remove duplicates."""
        seen: set = set()
        self._filters.append(lambda items: [x for x in items if id(x) not in seen and not seen.add(id(x))])
        return self

    def distinct_by(self, key_fn: Callable[[T], Any]) -> "FilterChain[T]":
        """Remove duplicates by key."""
        seen: set = set()
        self._filters.append(lambda items: [x for x in items if key_fn(x) not in seen and not seen.add(key_fn(x))])
        return self

    def order_by(self, key_fn: Callable[[T], Any], reverse: bool = False) -> "FilterChain[T]":
        """Sort items."""
        self._filters.append(lambda items: sorted(items, key=key_fn, reverse=reverse))
        return self

    def execute(self) -> List[T]:
        """Execute all filters and return result."""
        result = self._items
        for f in self._filters:
            result = f(result)
        return result

    def __iter__(self) -> Iterator[T]:
        return iter(self.execute())

    def to_list(self) -> List[T]:
        """Execute and return as list."""
        return self.execute()

    def first(self) -> Optional[T]:
        """Execute and return first item."""
        result = self.execute()
        return result[0] if result else None

    def count(self) -> int:
        """Execute and return count."""
        return len(self.execute())
