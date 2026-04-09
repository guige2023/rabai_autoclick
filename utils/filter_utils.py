"""Filter and predicate utilities.

Provides functional filter operations, predicates,
and data transformation utilities.
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, Union


T = TypeVar("T")
U = TypeVar("U")


class Predicate:
    """Composable predicate for filtering.

    Example:
        is_valid = Predicate(lambda x: x > 0) & Predicate(lambda x: x < 100)
        filtered = is_valid.filter([5, -1, 50, 200])
    """

    def __init__(self, func: Callable[[Any], bool]) -> None:
        self._func = func

    def __call__(self, value: Any) -> bool:
        """Evaluate predicate."""
        return self._func(value)

    def __and__(self, other: "Predicate") -> "Predicate":
        """Combine with AND."""
        return Predicate(lambda x: self(x) and other(x))

    def __or__(self, other: "Predicate") -> "Predicate":
        """Combine with OR."""
        return Predicate(lambda x: self(x) or other(x))

    def __invert__(self) -> "Predicate":
        """Negate predicate."""
        return Predicate(lambda x: not self(x))

    def filter(self, iterable: List[T]) -> List[T]:
        """Filter iterable using predicate."""
        return [x for x in iterable if self(x)]

    def exclude(self, iterable: List[T]) -> List[T]:
        """Exclude items matching predicate."""
        return [x for x in iterable if not self(x)]


def is_none(value: Any) -> bool:
    """Check if value is None."""
    return value is None


def is_not_none(value: Any) -> bool:
    """Check if value is not None."""
    return value is not None


def is_empty(value: Any) -> bool:
    """Check if value is empty (None, empty string, empty container)."""
    if value is None:
        return True
    if isinstance(value, (str, list, dict, tuple, set)):
        return len(value) == 0
    return False


def is_truthy(value: Any) -> bool:
    """Check if value is truthy."""
    return bool(value)


def is_falsy(value: Any) -> bool:
    """Check if value is falsy."""
    return not value


def equals(target: Any) -> Callable[[Any], bool]:
    """Create predicate that checks equality."""
    return Predicate(lambda x: x == target)


def contains(item: Any) -> Callable[[Any], bool]:
    """Create predicate that checks containment."""
    return Predicate(lambda x: item in x if hasattr(x, "__contains__") else False)


def matches(pattern: str) -> Callable[[str], bool]:
    """Create predicate that checks regex match."""
    import re
    compiled = re.compile(pattern)
    return Predicate(lambda x: bool(compiled.search(str(x))))


def greater_than(threshold: T) -> Callable[[T], bool]:
    """Create predicate for greater than comparison."""
    return Predicate(lambda x: x > threshold)


def less_than(threshold: T) -> Callable[[T], bool]:
    """Create predicate for less than comparison."""
    return Predicate(lambda x: x < threshold)


def in_range(min_val: T, max_val: T) -> Callable[[T], bool]:
    """Create predicate for range check."""
    return Predicate(lambda x: min_val <= x <= max_val)


def has_key(key: str) -> Callable[[Dict, Any], bool]:
    """Create predicate that checks dict key existence."""
    return Predicate(lambda x: key in x if isinstance(x, dict) else False)


def has_property(prop: str) -> Callable[[Any], bool]:
    """Create predicate that checks attribute existence."""
    return Predicate(lambda x: hasattr(x, prop))


def instance_of(cls: type) -> Callable[[Any], bool]:
    """Create predicate that checks instance type."""
    return Predicate(lambda x: isinstance(x, cls))


def filter_by(
    iterable: List[T],
    **conditions: Any,
) -> List[T]:
    """Filter iterable by keyword conditions.

    Example:
        users = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        adults = filter_by(users, age__gte=18)
    """
    result = iterable
    for key, value in conditions.items():
        if "__" in key:
            field, op = key.rsplit("__", 1)
            result = _apply_filter(result, field, op, value)
        else:
            result = [x for x in result if getattr(x, key, None) == value]
    return result


def _apply_filter(
    iterable: List[Dict],
    field: str,
    op: str,
    value: Any,
) -> List[Dict]:
    """Apply filter operation to list of dicts."""
    ops = {
        "eq": lambda v, x: v == x,
        "ne": lambda v, x: v != x,
        "gt": lambda v, x: v > x,
        "gte": lambda v, x: v >= x,
        "lt": lambda v, x: v < x,
        "lte": lambda v, x: v <= x,
        "in": lambda v, x: v in x,
        "contains": lambda v, x: x in v,
    }

    op_func = ops.get(op, ops["eq"])

    filtered = []
    for item in iterable:
        val = item.get(field) if isinstance(item, dict) else getattr(item, field, None)
        if val is not None and op_func(val, value):
            filtered.append(item)

    return filtered


def partition(
    iterable: List[T],
    predicate: Callable[[T], bool],
) -> tuple[List[T], List[T]]:
    """Split iterable into two lists based on predicate.

    Example:
        evens, odds = partition([1, 2, 3, 4], lambda x: x % 2 == 0)
    """
    matched = []
    not_matched = []
    for item in iterable:
        if predicate(item):
            matched.append(item)
        else:
            not_matched.append(item)
    return matched, not_matched


def unique(
    iterable: List[T],
    key: Optional[Callable[[T], Any]] = None,
) -> List[T]:
    """Return unique items preserving order.

    Example:
        unique([1, 2, 2, 3, 1])  # [1, 2, 3]
        unique(users, key=lambda u: u["id"])  # unique by id
    """
    seen: set = set()
    result: List[T] = []
    for item in iterable:
        k = key(item) if key else item
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result


def group_by(
    iterable: List[Dict],
    key: str,
) -> Dict[Any, List[Dict]]:
    """Group items by key field.

    Example:
        by_dept = group_by(employees, "department")
    """
    groups: Dict[Any, List[Dict]] = {}
    for item in iterable:
        k = item.get(key) if isinstance(item, dict) else getattr(item, key, None)
        if k not in groups:
            groups[k] = []
        groups[k].append(item)
    return groups


def sort_by(
    iterable: List[T],
    key: Callable[[T], Any],
    reverse: bool = False,
) -> List[T]:
    """Sort iterable by key function."""
    return sorted(iterable, key=key, reverse=reverse)


def chunk(iterable: List[T], size: int) -> Iterator[List[T]]:
    """Split iterable into chunks of specified size.

    Example:
        for batch in chunk(items, 10):
            process_batch(batch)
    """
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def flatten(nested: List[List[T]]) -> List[T]:
    """Flatten nested list."""
    return [item for sublist in nested for item in sublist]


def map_values(
    dict_list: List[Dict[str, Any]],
    key: str,
    transform: Optional[Callable[[Any], Any]] = None,
) -> List[Any]:
    """Extract and optionally transform values from list of dicts.

    Example:
        names = map_values(users, "name", str.title)
    """
    values = [d.get(key) for d in dict_list if key in d]
    if transform:
        return [transform(v) for v in values]
    return values
