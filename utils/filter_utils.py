"""Filter utilities for RabAI AutoClick.

Provides:
- Data filtering
- Predicate composition
- Query-style filtering
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T")


def filter_by(
    data: List[Dict[str, Any]],
    **conditions: Any,
) -> List[Dict[str, Any]]:
    """Filter list of dictionaries by conditions.

    Args:
        data: List of dictionaries.
        **conditions: Key-value pairs to filter by.

    Returns:
        Filtered list.
    """
    result = []
    for item in data:
        match = True
        for key, value in conditions.items():
            if key not in item or item[key] != value:
                match = False
                break
        if match:
            result.append(item)
    return result


def filter_in(
    data: List[Dict[str, Any]],
    key: str,
    values: List[Any],
) -> List[Dict[str, Any]]:
    """Filter where key value is in list of values.

    Args:
        data: List of dictionaries.
        key: Dictionary key.
        values: Allowed values.

    Returns:
        Filtered list.
    """
    return [item for item in data if item.get(key) in values]


def filter_range(
    data: List[Dict[str, Any]],
    key: str,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Filter by numeric range.

    Args:
        data: List of dictionaries.
        key: Dictionary key.
        min_val: Minimum value (inclusive).
        max_val: Maximum value (inclusive).

    Returns:
        Filtered list.
    """
    result = []
    for item in data:
        if key not in item:
            continue
        val = item[key]
        if min_val is not None and val < min_val:
            continue
        if max_val is not None and val > max_val:
            continue
        result.append(item)
    return result


def filter_predicate(
    data: Iterator[T],
    predicate: Callable[[T], bool],
) -> List[T]:
    """Filter by predicate function.

    Args:
        data: Input data.
        predicate: Filter function.

    Returns:
        Filtered list.
    """
    return [item for item in data if predicate(item)]


def exclude(
    data: List[Dict[str, Any]],
    **conditions: Any,
) -> List[Dict[str, Any]]:
    """Exclude items matching conditions.

    Args:
        data: List of dictionaries.
        **conditions: Key-value pairs to exclude.

    Returns:
        Filtered list.
    """
    result = []
    for item in data:
        match = False
        for key, value in conditions.items():
            if key in item and item[key] == value:
                match = True
                break
        if not match:
            result.append(item)
    return result


def filter_by_pattern(
    data: List[Dict[str, Any]],
    key: str,
    pattern: str,
) -> List[Dict[str, Any]]:
    """Filter by string pattern (substring match).

    Args:
        data: List of dictionaries.
        key: Dictionary key containing string.
        pattern: Substring to match.

    Returns:
        Filtered list.
    """
    return [
        item for item in data
        if key in item and pattern in str(item[key])
    ]


def filter_none(data: List[Optional[T]]) -> List[T]:
    """Remove None values from list.

    Args:
        data: Input list.

    Returns:
        List with None values removed.
    """
    return [item for item in data if item is not None]


def filter_falsy(data: List[T]) -> List[T]:
    """Remove falsy values from list.

    Args:
        data: Input list.

    Returns:
        List with falsy values removed.
    """
    return [item for item in data if item]


def dedup(data: List[T], key: Optional[Callable[[T], Any]] = None) -> List[T]:
    """Remove duplicates preserving order.

    Args:
        data: Input list.
        key: Optional function to extract comparison key.

    Returns:
        Deduplicated list.
    """
    seen: set = set()
    result: List[T] = []
    for item in data:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result


class QueryFilter:
    """Fluent filter builder.

    Example:
        results = (
            QueryFilter(data)
            .where(status="active")
            .where(age__gte=18)
            .where(name__contains="John")
            .execute()
        )
    """

    def __init__(self, data: List[Dict[str, Any]]) -> None:
        self._data = data
        self._filters: List[Callable[[Dict[str, Any]], bool]] = []

    def where(self, **conditions) -> QueryFilter:
        """Add equality conditions."""
        def filter_fn(item: Dict[str, Any]) -> bool:
            for key, value in conditions.items():
                if key.endswith("__gte"):
                    actual_key = key[:-4]
                    if item.get(actual_key) < value:
                        return False
                elif key.endswith("__lte"):
                    actual_key = key[:-4]
                    if item.get(actual_key) > value:
                        return False
                elif key.endswith("__gt"):
                    actual_key = key[:-3]
                    if item.get(actual_key) <= value:
                        return False
                elif key.endswith("__lt"):
                    actual_key = key[:-3]
                    if item.get(actual_key) >= value:
                        return False
                elif key.endswith("__contains"):
                    actual_key = key[:-10]
                    if value not in str(item.get(actual_key, "")):
                        return False
                else:
                    if item.get(key) != value:
                        return False
            return True

        self._filters.append(filter_fn)
        return self

    def execute(self) -> List[Dict[str, Any]]:
        """Apply all filters and return results."""
        result = self._data
        for filter_fn in self._filters:
            result = [item for item in result if filter_fn(item)]
        return result

    def first(self) -> Optional[Dict[str, Any]]:
        """Get first matching item."""
        results = self.execute()
        return results[0] if results else None

    def count(self) -> int:
        """Get count of matching items."""
        return len(self.execute())
