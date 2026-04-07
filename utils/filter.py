"""Filter utilities for RabAI AutoClick.

Provides:
- Data filtering
- Query filtering
- Filter chains
"""

import fnmatch
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar


T = TypeVar("T")


class Filter:
    """Base filter class."""

    def matches(self, item: T) -> bool:
        """Check if item matches filter.

        Args:
            item: Item to check.

        Returns:
            True if matches.
        """
        raise NotImplementedError


class PredicateFilter(Filter[T]):
    """Filter using predicate function."""

    def __init__(self, predicate: Callable[[T], bool]) -> None:
        """Initialize filter.

        Args:
            predicate: Function that returns True if matches.
        """
        self._predicate = predicate

    def matches(self, item: T) -> bool:
        """Check if item matches."""
        return self._predicate(item)


class EqFilter(Filter[T]):
    """Filter for equality check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Value to match.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item equals value."""
        return item == self._value


class NeFilter(Filter[T]):
    """Filter for not equal check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Value to not match.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item does not equal value."""
        return item != self._value


class LtFilter(Filter[T]):
    """Filter for less than check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Threshold value.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item is less than value."""
        return item < self._value


class LteFilter(Filter[T]):
    """Filter for less than or equal check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Threshold value.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item is less than or equal to value."""
        return item <= self._value


class GtFilter(Filter[T]):
    """Filter for greater than check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Threshold value.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item is greater than value."""
        return item > self._value


class GteFilter(Filter[T]):
    """Filter for greater than or equal check."""

    def __init__(self, value: Any) -> None:
        """Initialize filter.

        Args:
            value: Threshold value.
        """
        self._value = value

    def matches(self, item: T) -> bool:
        """Check if item is greater than or equal to value."""
        return item >= self._value


class InFilter(Filter[T]):
    """Filter for membership check."""

    def __init__(self, values: List[T]) -> None:
        """Initialize filter.

        Args:
            values: List of acceptable values.
        """
        self._values = values

    def matches(self, item: T) -> bool:
        """Check if item is in values."""
        return item in self._values


class ContainsFilter(Filter[T]):
    """Filter for substring containing check."""

    def __init__(self, substring: str) -> None:
        """Initialize filter.

        Args:
            substring: Substring to look for.
        """
        self._substring = substring

    def matches(self, item: T) -> bool:
        """Check if item contains substring."""
        return self._substring in str(item)


class RegexFilter(Filter[T]):
    """Filter using regex pattern."""

    def __init__(self, pattern: str) -> None:
        """Initialize filter.

        Args:
            pattern: Regex pattern.
        """
        self._pattern = re.compile(pattern)

    def matches(self, item: T) -> bool:
        """Check if item matches pattern."""
        return bool(self._pattern.search(str(item)))


class WildcardFilter(Filter[T]):
    """Filter using wildcard matching."""

    def __init__(self, pattern: str) -> None:
        """Initialize filter.

        Args:
            pattern: Wildcard pattern (e.g., "*.txt").
        """
        self._pattern = pattern

    def matches(self, item: T) -> bool:
        """Check if item matches wildcard pattern."""
        return fnmatch.fnmatch(str(item), self._pattern)


class KeyFilter(Filter[Dict]):
    """Filter dictionary by key value."""

    def __init__(
        self,
        key: str,
        filter_impl: Filter,
    ) -> None:
        """Initialize filter.

        Args:
            key: Dictionary key to check.
            filter_impl: Filter for key value.
        """
        self._key = key
        self._filter = filter_impl

    def matches(self, item: Dict) -> bool:
        """Check if item's key value matches."""
        if self._key not in item:
            return False
        return self._filter.matches(item[self._key])


class FilterChain(Filter[T]):
    """Chain multiple filters."""

    def __init__(self, filters: Optional[List[Filter[T]]] = None) -> None:
        """Initialize chain.

        Args:
            filters: List of filters to chain.
        """
        self._filters = filters or []

    def add(self, filter_impl: Filter[T]) -> "FilterChain[T]":
        """Add filter to chain.

        Args:
            filter_impl: Filter to add.

        Returns:
            Self for chaining.
        """
        self._filters.append(filter_impl)
        return self

    def matches(self, item: T) -> bool:
        """Check if item matches all filters (AND)."""
        for f in self._filters:
            if not f.matches(item):
                return False
        return True


class OrFilter(Filter[T]):
    """Filter that matches if any filter matches (OR)."""

    def __init__(self, filters: Optional[List[Filter[T]]] = None) -> None:
        """Initialize OR filter.

        Args:
            filters: List of filters.
        """
        self._filters = filters or []

    def add(self, filter_impl: Filter[T]) -> "OrFilter[T]":
        """Add filter to OR group.

        Args:
            filter_impl: Filter to add.

        Returns:
            Self for chaining.
        """
        self._filters.append(filter_impl)
        return self

    def matches(self, item: T) -> bool:
        """Check if item matches any filter."""
        for f in self._filters:
            if f.matches(item):
                return True
        return False


class NotFilter(Filter[T]):
    """Filter that inverts another filter."""

    def __init__(self, filter_impl: Filter[T]) -> None:
        """Initialize NOT filter.

        Args:
            filter_impl: Filter to invert.
        """
        self._filter = filter_impl

    def matches(self, item: T) -> bool:
        """Check if item does not match filter."""
        return not self._filter.matches(item)


def filter_list(items: Iterable[T], filter_impl: Filter[T]) -> List[T]:
    """Filter list of items.

    Args:
        items: Items to filter.
        filter_impl: Filter to apply.

    Returns:
        Filtered list.
    """
    return [item for item in items if filter_impl.matches(item)]


def filter_dict(data: Dict[str, T], filter_impl: Filter[T]) -> Dict[str, T]:
    """Filter dictionary.

    Args:
        data: Dictionary to filter.
        filter_impl: Filter to apply to values.

    Returns:
        Filtered dictionary.
    """
    return {k: v for k, v in data.items() if filter_impl.matches(v)}


def partition(items: Iterable[T], filter_impl: Filter[T]) -> tuple:
    """Partition items into matching and non-matching.

    Args:
        items: Items to partition.
        filter_impl: Filter to apply.

    Returns:
        Tuple of (matching, non_matching).
    """
    matching = []
    non_matching = []

    for item in items:
        if filter_impl.matches(item):
            matching.append(item)
        else:
            non_matching.append(item)

    return matching, non_matching
