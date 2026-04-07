"""List utilities for RabAI AutoClick.

Provides:
- List manipulation helpers
- List transformations
- List searching and filtering
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

T = TypeVar('T')


def first(items: List[T]) -> Optional[T]:
    """Get first item from list.

    Args:
        items: List to query.

    Returns:
        First item or None if empty.
    """
    return items[0] if items else None


def last(items: List[T]) -> Optional[T]:
    """Get last item from list.

    Args:
        items: List to query.

    Returns:
        Last item or None if empty.
    """
    return items[-1] if items else None


def get_at(items: List[T], index: int) -> Optional[T]:
    """Get item at index.

    Args:
        items: List to query.
        index: Index to get.

    Returns:
        Item at index or None if out of bounds.
    """
    if -len(items) <= index < len(items):
        return items[index]
    return None


def set_at(items: List[T], index: int, value: T) -> bool:
    """Set item at index.

    Args:
        items: List to modify.
        index: Index to set.
        value: Value to set.

    Returns:
        True if set, False if out of bounds.
    """
    if -len(items) <= index < len(items):
        items[index] = value
        return True
    return False


def insert_at(items: List[T], index: int, value: T) -> None:
    """Insert item at index.

    Args:
        items: List to modify.
        index: Index to insert at.
        value: Value to insert.
    """
    items.insert(index, value)


def delete_at(items: List[T], index: int) -> Optional[T]:
    """Delete item at index.

    Args:
        items: List to modify.
        index: Index to delete.

    Returns:
        Deleted item or None if out of bounds.
    """
    if -len(items) <= index < len(items):
        return items.pop(index)
    return None


def append(items: List[T], value: T) -> None:
    """Append item to list.

    Args:
        items: List to modify.
        value: Value to append.
    """
    items.append(value)


def extend(items: List[T], values: List[T]) -> None:
    """Extend list with values.

    Args:
        items: List to modify.
        values: Values to add.
    """
    items.extend(values)


def prepend(items: List[T], value: T) -> None:
    """Prepend item to list.

    Args:
        items: List to modify.
        value: Value to prepend.
    """
    items.insert(0, value)


def unique(items: List[T]) -> List[T]:
    """Get unique items preserving order.

    Args:
        items: List to deduplicate.

    Returns:
        List with duplicates removed.
    """
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def unique_by(items: List[T], key: Callable[[T], Any]) -> List[T]:
    """Get unique items by key function.

    Args:
        items: List to deduplicate.
        key: Function to extract key for comparison.

    Returns:
        List with duplicates removed.
    """
    seen = set()
    result = []
    for item in items:
        k = key(item)
        if k not in seen:
            seen.add(k)
            result.append(item)
    return result


def filter_list(items: List[T], predicate: Callable[[T], bool]) -> List[T]:
    """Filter list by predicate.

    Args:
        items: List to filter.
        predicate: Function that returns True to keep item.

    Returns:
        Filtered list.
    """
    return [item for item in items if predicate(item)]


def reject_list(items: List[T], predicate: Callable[[T], bool]) -> List[T]:
    """Reject items from list by predicate.

    Args:
        items: List to filter.
        predicate: Function that returns True to reject item.

    Returns:
        Filtered list.
    """
    return [item for item in items if not predicate(item)]


def map_list(items: List[T], transformer: Callable[[T], Any]) -> List[Any]:
    """Transform list items.

    Args:
        items: List to transform.
        transformer: Function to transform each item.

    Returns:
        Transformed list.
    """
    return [transformer(item) for item in items]


def flat_map(items: List[T], mapper: Callable[[T], List[Any]]) -> List[Any]:
    """Map and flatten result.

    Args:
        items: List to transform.
        mapper: Function that returns list for each item.

    Returns:
        Flattened list.
    """
    result = []
    for item in items:
        result.extend(mapper(item))
    return result


def reduce_list(items: List[T], reducer: Callable[[Any, T], Any], initial: Any = None) -> Any:
    """Reduce list to single value.

    Args:
        items: List to reduce.
        reducer: Function that combines accumulator with item.
        initial: Initial accumulator value.

    Returns:
        Reduced value.
    """
    result = initial
    for item in items:
        result = reducer(result, item)
    return result


def find_item(items: List[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Find item in list.

    Args:
        items: List to search.
        predicate: Function to match item.

    Returns:
        Found item or None.
    """
    for item in items:
        if predicate(item):
            return item
    return None


def find_index(items: List[T], predicate: Callable[[T], bool]) -> int:
    """Find index of item in list.

    Args:
        items: List to search.
        predicate: Function to match item.

    Returns:
        Index of found item or -1.
    """
    for i, item in enumerate(items):
        if predicate(item):
            return i
    return -1


def contains_item(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if list contains item.

    Args:
        items: List to search.
        predicate: Function to match item.

    Returns:
        True if found.
    """
    return find_item(items, predicate) is not None


def all_match(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if all items match predicate.

    Args:
        items: List to check.
        predicate: Function to match.

    Returns:
        True if all match.
    """
    return all(predicate(item) for item in items)


def none_match(items: List[T], predicate: Callable[[T], bool]) -> bool:
    """Check if no items match predicate.

    Args:
        items: List to check.
        predicate: Function to match.

    Returns:
        True if none match.
    """
    return not any(predicate(item) for item in items)


def partition_list(items: List[T], predicate: Callable[[T], bool]) -> Tuple[List[T], List[T]]:
    """Partition list into two lists by predicate.

    Args:
        items: List to partition.
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


def chunk(items: List[T], size: int) -> List[List[T]]:
    """Split list into chunks.

    Args:
        items: List to chunk.
        size: Size of each chunk.

    Returns:
        List of chunks.
    """
    return [items[i:i + size] for i in range(0, len(items), size)]


def window(items: List[T], size: int, step: int = 1) -> List[List[T]]:
    """Create sliding window over list.

    Args:
        items: List to window.
        size: Window size.
        step: Step between windows.

    Returns:
        List of windows.
    """
    if size <= 0 or step <= 0:
        return []
    windows = []
    for i in range(0, len(items) - size + 1, step):
        windows.append(items[i:i + size])
    return windows


def flatten(items: List[Any]) -> List[Any]:
    """Flatten nested lists.

    Args:
        items: List to flatten.

    Returns:
        Flattened list.
    """
    result = []
    for item in items:
        if isinstance(item, list):
            result.extend(flatten(item))
        else:
            result.append(item)
    return result


def group_by(items: List[Dict[str, Any]], key: str) -> Dict[Any, List[Dict[str, Any]]]:
    """Group list of dicts by key.

    Args:
        items: List to group.
        key: Key to group by.

    Returns:
        Dictionary mapping key value to list of items.
    """
    result: Dict[Any, List[Dict[str, Any]]] = {}
    for item in items:
        if key in item:
            value = item[key]
            if value not in result:
                result[value] = []
            result[value].append(item)
    return result


def sort_by(items: List[T], key: Callable[[T], Any], reverse: bool = False) -> List[T]:
    """Sort list by key function.

    Args:
        items: List to sort.
        key: Function to extract sort key.
        reverse: Whether to reverse sort.

    Returns:
        Sorted list.
    """
    return sorted(items, key=key, reverse=reverse)


def reverse_list(items: List[T]) -> List[T]:
    """Reverse list.

    Args:
        items: List to reverse.

    Returns:
        Reversed list.
    """
    return items[::-1]


def shuffle(items: List[T]) -> List[T]:
    """Shuffle list (returns new list).

    Args:
        items: List to shuffle.

    Returns:
        Shuffled list.
    """
    import random
    result = items[:]
    random.shuffle(result)
    return result


def sample(items: List[T], n: int) -> List[T]:
    """Sample n items from list.

    Args:
        items: List to sample from.
        n: Number of items to sample.

    Returns:
        List of sampled items.
    """
    import random
    if n >= len(items):
        return items[:]
    return random.sample(items, n)


def take(items: List[T], n: int) -> List[T]:
    """Take first n items.

    Args:
        items: List to take from.
        n: Number of items to take.

    Returns:
        First n items.
    """
    return items[:n]


def drop(items: List[T], n: int) -> List[T]:
    """Drop first n items.

    Args:
        items: List to drop from.
        n: Number of items to drop.

    Returns:
        List without first n items.
    """
    return items[n:]


def take_while(items: List[T], predicate: Callable[[T], bool]) -> List[T]:
    """Take items while predicate is true.

    Args:
        items: List to take from.
        predicate: Function to check.

    Returns:
        Items taken while predicate is true.
    """
    result = []
    for item in items:
        if predicate(item):
            result.append(item)
        else:
            break
    return result


def drop_while(items: List[T], predicate: Callable[[T], bool]) -> List[T]:
    """Drop items while predicate is true.

    Args:
        items: List to drop from.
        predicate: Function to check.

    Returns:
        List after dropping leading items.
    """
    i = 0
    while i < len(items) and predicate(items[i]):
        i += 1
    return items[i:]


def zip_with_index(items: List[T]) -> List[Tuple[int, T]]:
    """Zip list with indices.

    Args:
        items: List to zip.

    Returns:
        List of (index, item) tuples.
    """
    return list(enumerate(items))


def unzip_pairs(pairs: List[Tuple[T, Any]]) -> Tuple[List[T], List[Any]]:
    """Unzip list of pairs.

    Args:
        pairs: List of tuples.

    Returns:
        Tuple of two lists.
    """
    unzipped = tuple(zip(*pairs))
    return (list(unzipped[0]), list(unzipped[1])) if unzipped else ([], [])


def interleave(*lists: List[T]) -> List[T]:
    """Interleave multiple lists.

    Args:
        *lists: Lists to interleave.

    Returns:
        Interleaved list.
    """
    result = []
    max_len = max(len(lst) for lst in lists)
    for i in range(max_len):
        for lst in lists:
            if i < len(lst):
                result.append(lst[i])
    return result


def intersperse(items: List[T], value: T) -> List[T]:
    """Intersperse value between items.

    Args:
        items: List to intersperse.
        value: Value to insert.

    Returns:
        Interspersed list.
    """
    if not items:
        return []
    result = [items[0]]
    for item in items[1:]:
        result.append(value)
        result.append(item)
    return result


def count_items(items: List[T], predicate: Callable[[T], bool]) -> int:
    """Count items matching predicate.

    Args:
        items: List to count.
        predicate: Function to match.

    Returns:
        Count of matching items.
    """
    return sum(1 for item in items if predicate(item))


def sum_list(items: List[Union[int, float]]) -> Union[int, float]:
    """Sum list of numbers.

    Args:
        items: List to sum.

    Returns:
        Sum of items.
    """
    return sum(items)


def product_list(items: List[Union[int, float]]) -> Union[int, float]:
    """Product of list of numbers.

    Args:
        items: List to multiply.

    Returns:
        Product of items.
    """
    result = 1
    for item in items:
        result *= item
    return result


def average_list(items: List[Union[int, float]]) -> float:
    """Average of list of numbers.

    Args:
        items: List to average.

    Returns:
        Average of items.
    """
    if not items:
        return 0.0
    return sum(items) / len(items)


def min_item(items: List[T], key: Callable[[T], Any] = None) -> Optional[T]:
    """Get minimum item.

    Args:
        items: List to search.
        key: Optional key function.

    Returns:
        Minimum item or None.
    """
    if not items:
        return None
    return min(items, key=key) if key else min(items)


def max_item(items: List[T], key: Callable[[T], Any] = None) -> Optional[T]:
    """Get maximum item.

    Args:
        items: List to search.
        key: Optional key function.

    Returns:
        Maximum item or None.
    """
    if not items:
        return None
    return max(items, key=key) if key else max(items)
