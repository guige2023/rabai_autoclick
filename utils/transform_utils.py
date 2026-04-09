"""Data transformation utilities for automation workflows.

Provides common transformation functions for lists, dicts,
strings, and structured data used in automation pipelines.

Example:
    >>> from utils.transform_utils import flatten, group_by, pivot
    >>> flatten([[1, 2], [3, [4]]])
    [1, 2, 3, 4]
    >>> group_by([{"type": "a", "v": 1}, {"type": "b", "v": 2}], "type")
    {"a": [{"type": "a", "v": 1}], "b": [{"type": "b", "v": 2}]}
"""

from __future__ import annotations

from collections import defaultdict
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from uuid import uuid4

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def flatten(items: Iterable[Iterable[T]], *, depth: int = 1) -> List[T]:
    """Flatten a nested iterable.

    Args:
        items: Nested iterable to flatten.
        depth: Maximum nesting depth (1 = one level, 0 = no flatten).

    Returns:
        Flattened list.

    Example:
        >>> flatten([[1, 2], [3, [4, [5]]]], depth=2)
        [1, 2, 3, 4, [5]]
    """
    result: List[T] = []

    def _flatten(src: Iterable[Any], current_depth: int) -> None:
        for item in src:
            if isinstance(item, (list, tuple)) and current_depth < depth:
                _flatten(item, current_depth + 1)
            else:
                result.append(item)

    _flatten(items, 0)
    return result


def group_by(
    items: List[Dict[str, Any]],
    key: str,
    *,
    aggr: Optional[Callable[[List[Any]], Any]] = None,
) -> Dict[Any, List[Dict[str, Any]]]:
    """Group items by a key field.

    Args:
        items: List of dicts to group.
        key: Dict key to group by.
        aggr: Optional aggregation function for values.

    Returns:
        Dict mapping key value -> list of matching items.

    Example:
        >>> data = [{"type": "x", "v": 1}, {"type": "y", "v": 2}, {"type": "x", "v": 3}]
        >>> group_by(data, "type")
        {"x": [{"type": "x", "v": 1}, {"type": "x", "v": 3}], "y": [{"type": "y", "v": 2}]}
    """
    groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if key in item:
            groups[item[key]].append(item)
    return dict(groups)


def pivot(
    items: List[Dict[str, Any]],
    row_key: str,
    col_key: str,
    value_key: str,
    *,
    aggr: Callable[[List[Any]], Any] = sum,
) -> Dict[Any, Dict[Any, Any]]:
    """Pivot a list of dicts into a matrix.

    Args:
        items: List of dicts to pivot.
        row_key: Key for row labels.
        col_key: Key for column labels.
        value_key: Key for cell values.
        aggr: Aggregation function when multiple values match.

    Returns:
        Nested dict matrix[row][col] = value.

    Example:
        >>> data = [{"r": "a", "c": "x", "v": 1}, {"r": "a", "c": "y", "v": 2}]
        >>> pivot(data, "r", "c", "v")
        {"a": {"x": 1, "y": 2}}
    """
    matrix: Dict[Any, Dict[Any, Any]] = defaultdict(dict)
    for item in items:
        r, c, v = item[row_key], item[col_key], item[value_key]
        if c in matrix[r] and aggr:
            matrix[r][c] = aggr([matrix[r][c], v])
        else:
            matrix[r][c] = v
    return dict(matrix)


def transpose_matrix(
    rows: List[List[T]],
) -> List[List[T]]:
    """Transpose a matrix (swap rows and columns).

    Args:
        rows: List of rows, each a list of cells.

    Returns:
        Transposed matrix.

    Example:
        >>> transpose_matrix([[1, 2, 3], [4, 5, 6]])
        [[1, 4], [2, 5], [3, 6]]
    """
    if not rows:
        return []
    return [list(col) for col in zip(*rows)]


def dedupe(
    items: List[T],
    *,
    key: Optional[Callable[[T], Any]] = None,
    order_preserving: bool = True,
) -> List[T]:
    """Remove duplicates from a list.

    Args:
        items: List to deduplicate.
        key: Optional key function for identity.
        order_preserving: Keep first occurrence ordering.

    Returns:
        Deduplicated list.

    Example:
        >>> dedupe([1, 2, 2, 3, 1])
        [1, 2, 3]
    """
    if not order_preserving:
        seen: set = set()
        if key:
            return [x for x in items if (k := key(x)) not in seen and not seen.add(k)]
        else:
            return list(set(items))

    seen: List[Any] = []
    result: List[T] = []
    for item in items:
        k = key(item) if key else item
        if k not in seen:
            seen.append(k)
            result.append(item)
    return result


def deep_get(
    obj: Dict[str, Any],
    path: str,
    default: Any = None,
    separator: str = ".",
) -> Any:
    """Get a nested value using dot-notation path.

    Args:
        obj: Dict to traverse.
        path: Dot-separated key path.
        default: Default if path not found.
        separator: Path separator.

    Returns:
        Value at path or default.

    Example:
        >>> deep_get({"a": {"b": {"c": 42}}}, "a.b.c")
        42
    """
    keys = path.split(separator)
    current: Any = obj
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def deep_set(
    obj: Dict[str, Any],
    path: str,
    value: Any,
    separator: str = ".",
    create_missing: bool = True,
) -> bool:
    """Set a nested value using dot-notation path.

    Args:
        obj: Dict to modify.
        path: Dot-separated key path.
        value: Value to set.
        separator: Path separator.
        create_missing: Create intermediate dicts if missing.

    Returns:
        True if successful, False otherwise.
    """
    keys = path.split(separator)
    current = obj
    for key in keys[:-1]:
        if key not in current:
            if not create_missing:
                return False
            current[key] = {}
        elif not isinstance(current[key], dict):
            return False
        current = current[key]
    current[keys[-1]] = value
    return True


def merge_dicts(
    *dicts: Dict[str, Any],
    strategy: str = "overwrite",
) -> Dict[str, Any]:
    """Merge multiple dicts with configurable conflict strategy.

    Args:
        *dicts: Dicts to merge.
        strategy: "overwrite" (last wins), "keep" (first wins), "combine" (merge lists).

    Returns:
        Merged dict.
    """
    if not dicts:
        return {}

    result: Dict[str, Any] = {}
    for d in dicts:
        for key, value in d.items():
            if key not in result:
                result[key] = value
            elif strategy == "overwrite":
                result[key] = value
            elif strategy == "keep":
                pass
            elif strategy == "combine":
                existing = result[key]
                if isinstance(existing, list) and isinstance(value, list):
                    result[key] = existing + value
                else:
                    result[key] = [existing, value]
    return result


def chunks(items: List[T], size: int) -> Iterator[List[T]]:
    """Split items into fixed-size chunks.

    Args:
        items: Items to chunk.
        size: Chunk size.

    Yields:
        Chunks of up to size items.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def window(
    items: List[T],
    size: int,
    step: int = 1,
    fill: Optional[T] = None,
) -> Iterator[List[T]]:
    """Create sliding windows over items.

    Args:
        items: Items to window.
        size: Window size.
        step: Step between windows.
        fill: Value to fill when not enough items.

    Yields:
        Windows of size items.
    """
    if size <= 0 or step <= 0:
        return

    for i in range(0, len(items), step):
        window_items = items[i : i + size]
        if fill is not None:
            while len(window_items) < size:
                window_items.append(fill)
        yield window_items


def interleave(*lists: List[T]) -> List[T]:
    """Interleave multiple lists.

    Args:
        *lists: Lists to interleave.

    Returns:
        Interleaved list.

    Example:
        >>> interleave([1, 2], ["a", "b"])
        [1, "a", 2, "b"]
    """
    result: List[T] = []
    iterators = [iter(lst) for lst in lists]
    while iterators:
        new_iterators = []
        for it in iterators:
            try:
                result.append(next(it))
                new_iterators.append(it)
            except StopIteration:
                pass
        iterators = new_iterators
    return result
