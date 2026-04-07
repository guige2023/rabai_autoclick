"""Collection utilities for RabAI AutoClick.

Provides:
- Collection operations and helpers
- Advanced data structure manipulations
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Set, TypeVar, Tuple


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def unique(items: List[T]) -> List[T]:
    """Get unique items preserving order.

    Args:
        items: List of items.

    Returns:
        List with duplicates removed.
    """
    seen: Set[T] = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def unique_by(items: List[T], key_func: Callable[[T], K]) -> List[T]:
    """Get unique items by key function.

    Args:
        items: List of items.
        key_func: Function to extract key.

    Returns:
        List with duplicates removed.
    """
    seen: Set[K] = set()
    result = []
    for item in items:
        key = key_func(item)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def partition(
    items: List[T],
    predicate: Callable[[T], bool],
) -> Tuple[List[T], List[T]]:
    """Partition items by predicate.

    Args:
        items: List of items.
        predicate: Function to partition by.

    Returns:
        Tuple of (matching, non-matching).
    """
    matching = []
    non_matching = []
    for item in items:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def first(items: List[T], default: T = None) -> Optional[T]:
    """Get first item from list.

    Args:
        items: List of items.
        default: Default if empty.

    Returns:
        First item or default.
    """
    return items[0] if items else default


def last(items: List[T], default: T = None) -> Optional[T]:
    """Get last item from list.

    Args:
        items: List of items.
        default: Default if empty.

    Returns:
        Last item or default.
    """
    return items[-1] if items else default


def sample(items: List[T], count: int) -> List[T]:
    """Sample items from list.

    Args:
        items: List of items.
        count: Number of items to sample.

    Returns:
        List of sampled items.
    """
    if count >= len(items):
        return items.copy()
    result = []
    remaining = items.copy()
    for _ in range(count):
        idx = hash(remaining[0]) % len(remaining)
        result.append(remaining.pop(idx))
    return result


def transpose(matrix: List[List[T]]) -> List[List[T]]:
    """Transpose a matrix (list of lists).

    Args:
        matrix: 2D list.

    Returns:
        Transposed matrix.
    """
    if not matrix:
        return []
    return list(map(list, zip(*matrix)))


def zip_with(func: Callable[[T, T], V], a: List[T], b: List[T]) -> List[V]:
    """Zip two lists with function.

    Args:
        func: Function to combine elements.
        a: First list.
        b: Second list.

    Returns:
        List of combined elements.
    """
    min_len = min(len(a), len(b))
    return [func(a[i], b[i]) for i in range(min_len)]


def batch(items: List[T], size: int) -> Iterator[List[T]]:
    """Batch items into chunks.

    Args:
        items: List of items.
        size: Batch size.

    Yields:
        Batches of items.
    """
    for i in range(0, len(items), size):
        yield items[i:i + size]


def sliding_window(items: List[T], size: int) -> Iterator[List[T]]:
    """Create sliding window over items.

    Args:
        items: List of items.
        size: Window size.

    Yields:
        Windows of items.
    """
    for i in range(len(items) - size + 1):
        yield items[i:i + size]


def count_by(items: List[T], key_func: Callable[[T], K]) -> Dict[K, int]:
    """Count items by key function.

    Args:
        items: List of items.
        key_func: Function to extract key.

    Returns:
        Dictionary mapping keys to counts.
    """
    result: Dict[K, int] = {}
    for item in items:
        key = key_func(item)
        result[key] = result.get(key, 0) + 1
    return result


def group_by_to_dict(
    items: List[T],
    key_func: Callable[[T], K],
) -> Dict[K, List[T]]:
    """Group items by key function.

    Args:
        items: List of items.
        key_func: Function to extract key.

    Returns:
        Dictionary mapping keys to item lists.
    """
    result: Dict[K, List[T]] = {}
    for item in items:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def intersection(*lists: List[T]) -> List[T]:
    """Get intersection of lists.

    Args:
        *lists: Lists to intersect.

    Returns:
        List of items in all lists.
    """
    if not lists:
        return []
    result = set(lists[0])
    for lst in lists[1:]:
        result &= set(lst)
    return list(result)


def union(*lists: List[T]) -> List[T]:
    """Get union of lists.

    Args:
        *lists: Lists to union.

    Returns:
        List of unique items from all lists.
    """
    result: Set[T] = set()
    for lst in lists:
        result |= set(lst)
    return list(result)


def difference(a: List[T], b: List[T]) -> List[T]:
    """Get difference of two lists.

    Args:
        a: First list.
        b: Second list.

    Returns:
        Items in a but not in b.
    """
    b_set = set(b)
    return [item for item in a if item not in b_set]


def symmetric_difference(a: List[T], b: List[T]) -> List[T]:
    """Get symmetric difference of two lists.

    Args:
        a: First list.
        b: Second list.

    Returns:
        Items in either list but not both.
    """
    a_set = set(a)
    b_set = set(b)
    return list((a_set | b_set) - (a_set & b_set))


def update_in(
    data: Dict[K, V],
    key: K,
    func: Callable[[V], V],
    default: V = None,
) -> None:
    """Update dict value using function.

    Args:
        data: Dictionary to update.
        key: Key to update.
        func: Update function.
        default: Default value if key not present.
    """
    if key in data:
        data[key] = func(data[key])
    elif default is not None:
        data[key] = func(default)


def map_keys(data: Dict[K, V], func: Callable[[K], K]) -> Dict[K, V]:
    """Map dictionary keys.

    Args:
        data: Dictionary to map.
        func: Function to transform keys.

    Returns:
        New dictionary with transformed keys.
    """
    return {func(k): v for k, v in data.items()}


def map_items(
    data: Dict[K, V],
    key_func: Callable[[K, V], K],
    value_func: Callable[[K, V], V],
) -> Dict[K, V]:
    """Map dictionary keys and values.

    Args:
        data: Dictionary to map.
        key_func: Function to transform keys.
        value_func: Function to transform values.

    Returns:
        New dictionary with transformed keys and values.
    """
    return {key_func(k, v): value_func(k, v) for k, v in data.items()}


def filter_dict(
    data: Dict[K, V],
    predicate: Callable[[K, V], bool],
) -> Dict[K, V]:
    """Filter dictionary by predicate.

    Args:
        data: Dictionary to filter.
        predicate: Function to filter by.

    Returns:
        Filtered dictionary.
    """
    return {k: v for k, v in data.items() if predicate(k, v)}


def key_by(items: List[T], key_func: Callable[[T], K]) -> Dict[K, T]:
    """Index items by key function.

    Args:
        items: List of items.
        key_func: Function to extract key.

    Returns:
        Dictionary mapping keys to items.
    """
    return {key_func(item): item for item in items}


def key_by_list(items: List[T], key_func: Callable[[T], K]) -> Dict[K, List[T]]:
    """Index items by key function, allowing duplicates.

    Args:
        items: List of items.
        key_func: Function to extract key.

    Returns:
        Dictionary mapping keys to item lists.
    """
    result: Dict[K, List[T]] = {}
    for item in items:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def find(items: List[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Find item in list.

    Args:
        items: List of items.
        predicate: Function to find by.

    Returns:
        First matching item or None.
    """
    for item in items:
        if predicate(item):
            return item
    return None


def find_index(items: List[T], predicate: Callable[[T], bool]) -> int:
    """Find index of item in list.

    Args:
        items: List of items.
        predicate: Function to find by.

    Returns:
        Index of first match or -1.
    """
    for i, item in enumerate(items):
        if predicate(item):
            return i
    return -1


def contains(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if list contains item matching predicate.

    Args:
        items: List of items.
        predicate: Function to check by.

    Returns:
        True if found.
    """
    return any(predicate(item) for item in items)


def all_match(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if all items match predicate.

    Args:
        items: List of items.
        predicate: Function to check by.

    Returns:
        True if all match.
    """
    return all(predicate(item) for item in items)


def none_match(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if no items match predicate.

    Args:
        items: List of items.
        predicate: Function to check by.

    Returns:
        True if none match.
    """
    return not any(predicate(item) for item in items)


def sort_by(items: List[T], key_func: Callable[[T], Any], reverse: bool = False) -> List[T]:
    """Sort items by key function.

    Args:
        items: List of items.
        key_func: Function to extract sort key.
        reverse: If True, sort descending.

    Returns:
        Sorted list.
    """
    return sorted(items, key=key_func, reverse=reverse)


def chunk_list(items: List[T], size: int) -> List[List[T]]:
    """Split list into chunks.

    Args:
        items: List to chunk.
        size: Chunk size.

    Returns:
        List of chunks.
    """
    return [items[i:i + size] for i in range(0, len(items), size)]


def deduplicate(items: List[T]) -> List[T]:
    """Remove duplicates from list.

    Args:
        items: List to deduplicate.

    Returns:
        Deduplicated list.
    """
    return list(dict.fromkeys(items))
