"""
Collections extensions - Counter, defaultdict, OrderedDict, deque operations.
"""

from __future__ import annotations

import collections
from collections import (
    Counter,
    OrderedDict,
    defaultdict,
    deque,
)
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


def counter_most_common(
    counter: Counter,
    n: Optional[int] = None,
    reverse: bool = False,
) -> List[Tuple[Any, int]]:
    """
    Get most common elements from a Counter.
    
    Args:
        counter: Counter object
        n: Number of elements to return (None for all)
        reverse: If True, return least common instead
    
    Returns:
        List of (element, count) tuples
    
    Example:
        >>> counter_most_common(Counter(['a', 'b', 'a', 'c', 'a']), 2)
        [('a', 3), ('b', 1)]
    """
    if reverse:
        return counter.most_common()[:-n-1:-1] if n else counter.most_common()[::-1]
    return counter.most_common(n)


def counter_update(
    counter: Counter,
    data: Any,
    increment: int = 1,
) -> Counter:
    """
    Update counter with data (list, dict, or single element).
    
    Args:
        counter: Counter to update
        data: Data to add (list, dict, or single element)
        increment: Increment value for single elements
    
    Returns:
        Updated counter
    
    Example:
        >>> c = Counter(); c.update(['a', 'b', 'a'])
        >>> c['a']
        2
    """
    counter.update(data)
    return counter


def counter_subtract_from(
    counter: Counter,
    data: Any,
) -> Counter:
    """
    Subtract counts from counter.
    
    Args:
        counter: Counter to update
        data: Data to subtract (list, dict, or single element)
    
    Returns:
        Updated counter
    
    Example:
        >>> c = Counter(['a', 'b', 'a', 'c'])
        >>> c.subtract(['a'])
        >>> c['a']
        1
    """
    counter.subtract(data)
    return counter


def counter_intersection(
    counter1: Counter,
    counter2: Counter,
) -> Counter:
    """
    Get intersection (min of counts) of two counters.
    
    Args:
        counter1: First counter
        counter2: Second counter
    
    Returns:
        New counter with minimum counts
    
    Example:
        >>> c1 = Counter(['a', 'b', 'a'])
        >>> c2 = Counter(['a', 'b', 'b', 'c'])
        >>> counter_intersection(c1, c2)
        Counter({'a': 1, 'b': 1})
    """
    return counter1 & counter2


def counter_union(
    counter1: Counter,
    counter2: Counter,
) -> Counter:
    """
    Get union (max of counts) of two counters.
    
    Args:
        counter1: First counter
        counter2: Second counter
    
    Returns:
        New counter with maximum counts
    
    Example:
        >>> c1 = Counter(['a', 'b', 'a'])
        >>> c2 = Counter(['a', 'b', 'b', 'c'])
        >>> counter_union(c1, c2)
        Counter({'a': 2, 'b': 2, 'c': 1})
    """
    return counter1 | counter2


def counter_elements(
    counter: Counter,
) -> Iterator[Any]:
    """
    Iterate over all elements in counter (repeating per count).
    
    Args:
        counter: Counter object
    
    Yields:
        Elements repeated by their count
    
    Example:
        >>> list(counter_elements(Counter(['a', 'b', 'a'])))
        ['a', 'a', 'b']
    """
    for elem, count in counter.elements():
        yield elem


def defaultdict_factory(
    default_factory: Callable[[], Any],
    initial: Optional[Dict[K, V]] = None,
) -> defaultdict:
    """
    Create a defaultdict with specified default factory.
    
    Args:
        default_factory: Factory function for default values (list, int, dict, etc.)
        initial: Optional initial dictionary
    
    Returns:
        Configured defaultdict
    
    Example:
        >>> d = defaultdict_factory(list)
        >>> d['key'].append('value')
        >>> d['key']
        ['value']
    """
    d = defaultdict(default_factory)
    if initial:
        d.update(initial)
    return d


def defaultdict_set_default(
    d: defaultdict,
    key: K,
    default: Any,
) -> Any:
    """
    Set default value for key if not exists, return value.
    
    Args:
        d: defaultdict
        key: Key to check/set
        default: Default value if key doesn't exist
    
    Returns:
        The value (existing or newly set)
    
    Example:
        >>> d = defaultdict_factory(int)
        >>> defaultdict_set_default(d, 'count', 0)
        0
    """
    if key not in d:
        d[key] = default
    return d[key]


def ordered_dict_move_to_end(
    od: OrderedDict,
    key: K,
    last: bool = True,
) -> OrderedDict:
    """
    Move a key to the beginning or end of OrderedDict.
    
    Args:
        od: OrderedDict
        key: Key to move
        last: If True, move to end; else to beginning
    
    Returns:
        Updated OrderedDict
    
    Raises:
        KeyError: If key not in dict
    """
    od.move_to_end(key, last=last)
    return od


def ordered_dict_remove_last(
    od: OrderedDict,
    n: int = 1,
) -> List[Tuple[K, V]]:
    """
    Remove and return the last n items from OrderedDict.
    
    Args:
        od: OrderedDict
        n: Number of items to remove
    
    Returns:
        List of (key, value) tuples removed
    
    Example:
        >>> od = OrderedDict([('a', 1), ('b', 2), ('c', 3)])
        >>> ordered_dict_remove_last(od, 2)
        [('b', 2), ('c', 3)]
    """
    results = []
    for _ in range(min(n, len(od))):
        key = next(iter(od))
        results.append((key, od.pop(key)))
    return results


def ordered_dict_remove_first(
    od: OrderedDict,
    n: int = 1,
) -> List[Tuple[K, V]]:
    """
    Remove and return the first n items from OrderedDict.
    
    Args:
        od: OrderedDict
        n: Number of items to remove
    
    Returns:
        List of (key, value) tuples removed
    """
    results = ordered_dict_remove_last(od, n)
    return results[::-1]


def deque_appendleft_many(
    dq: deque,
    items: List[T],
) -> deque:
    """
    Extend deque on the left side.
    
    Args:
        dq: deque to extend
        items: Items to add
    
    Returns:
        Updated deque
    
    Example:
        >>> d = deque([3, 4])
        >>> deque_appendleft_many(d, [1, 2])
        deque([1, 2, 3, 4])
    """
    dq.extendleft(items)
    return dq


def deque_remove_all(
    dq: deque,
    value: Any,
) -> int:
    """
    Remove all occurrences of value from deque.
    
    Args:
        dq: deque to modify
        value: Value to remove
    
    Returns:
        Number of items removed
    
    Example:
        >>> d = deque([1, 2, 1, 3, 1])
        >>> deque_remove_all(d, 1)
        3
        >>> d
        deque([2, 3])
    """
    original_len = len(dq)
    while value in dq:
        dq.remove(value)
    return original_len - len(dq)


def deque_rotate_n(
    dq: deque,
    n: int,
) -> deque:
    """
    Rotate deque by n positions.
    
    Args:
        dq: deque to rotate
        n: Number of positions (positive = right, negative = left)
    
    Returns:
        Updated deque
    
    Example:
        >>> d = deque([1, 2, 3, 4, 5])
        >>> deque_rotate_n(d, 2)
        deque([4, 5, 1, 2, 3])
    """
    dq.rotate(n)
    return dq


def deque_slice(
    dq: deque,
    start: int,
    end: int,
) -> deque:
    """
    Get a slice of deque as new deque.
    
    Args:
        dq: Source deque
        start: Start index
        end: End index
    
    Returns:
        New deque with sliced elements
    
    Example:
        >>> d = deque([1, 2, 3, 4, 5])
        >>> deque_slice(d, 1, 4)
        deque([2, 3, 4])
    """
    return deque(list(dq)[start:end])


def deque_filter(
    dq: deque,
    predicate: Callable[[Any], bool],
) -> deque:
    """
    Filter deque elements by predicate.
    
    Args:
        dq: deque to filter
        predicate: Function that returns True to keep element
    
    Returns:
        New deque with filtered elements
    
    Example:
        >>> d = deque([1, 2, 3, 4, 5])
        >>> deque_filter(d, lambda x: x > 2)
        deque([3, 4, 5])
    """
    return deque(x for x in dq if predicate(x))


def chain_maps(
    *maps: Dict[K, V],
) -> Iterator[Tuple[K, V]]:
    """
    Chain multiple dictionaries together.
    
    Args:
        *maps: Variable number of dictionaries
    
    Yields:
        (key, value) tuples from all maps in order
    
    Example:
        >>> dict(chain_maps({'a': 1}, {'b': 2}, {'a': 3}))
        {'a': 3, 'b': 2}
    """
    for m in maps:
        yield from m.items()


def merge_counters(
    *counters: Counter,
) -> Counter:
    """
    Merge multiple counters (sum all counts).
    
    Args:
        *counters: Variable number of Counter objects
    
    Returns:
        Merged Counter with summed counts
    
    Example:
        >>> c1 = Counter(['a', 'b'])
        >>> c2 = Counter(['b', 'c'])
        >>> merge_counters(c1, c2)
        Counter({'b': 2, 'a': 1, 'c': 1})
    """
    result = Counter()
    for c in counters:
        result.update(c)
    return result


def counter_difference(
    counter1: Counter,
    counter2: Counter,
) -> Counter:
    """
    Get elements in counter1 that are not in counter2.
    
    Args:
        counter1: First counter
        counter2: Second counter (subtracted)
    
    Returns:
        New counter with counts from counter1 minus counter2
    
    Example:
        >>> c1 = Counter(['a', 'b', 'a', 'c'])
        >>> c2 = Counter(['a', 'b'])
        >>> counter_difference(c1, c2)
        Counter({'c': 1})
    """
    return counter1 - counter2


def flatten_dict_values(
    d: Dict[K, List[V]],
) -> Iterator[V]:
    """
    Flatten dictionary of lists/iterables.
    
    Args:
        d: Dictionary with list/tuple values
    
    Yields:
        Individual values from all lists
    
    Example:
        >>> list(flatten_dict_values({'a': [1, 2], 'b': [3]}))
        [1, 2, 3]
    """
    for values in d.values():
        yield from values


def group_by_key(
    items: List[Dict[K, V]],
    key: K,
) -> Dict[K, List[Dict[K, V]]]:
    """
    Group list of dicts by a common key.
    
    Args:
        items: List of dictionaries
        key: Key to group by
    
    Returns:
        Dictionary mapping key values to lists of items
    
    Example:
        >>> items = [{'type': 'a', 'v': 1}, {'type': 'b'}, {'type': 'a', 'v': 2}]
        >>> group_by_key(items, 'type')
        {'a': [{'type': 'a', 'v': 1}, {'type': 'a', 'v': 2}], 'b': [{'type': 'b'}]}
    """
    result: Dict[K, List[Dict[K, V]]] = defaultdict(list)
    for item in items:
        if key in item:
            result[item[key]].append(item)
    return dict(result)


def count_by(
    items: List[T],
    key: Callable[[T], K],
) -> Dict[K, int]:
    """
    Count items by a key function.
    
    Args:
        items: List of items
        key: Function to extract grouping key
    
    Returns:
        Dictionary mapping keys to counts
    
    Example:
        >>> count_by([1, 2, 3, 4, 5], lambda x: x % 2)
        {1: 3, 0: 2}
    """
    result: Dict[K, int] = defaultdict(int)
    for item in items:
        result[key(item)] += 1
    return dict(result)


def invert_dict(
    d: Dict[K, V],
) -> Dict[V, List[K]]:
    """
    Invert dictionary (values become keys).
    
    Args:
        d: Dictionary to invert
    
    Returns:
        Dictionary mapping values to lists of original keys
    
    Example:
        >>> invert_dict({'a': 1, 'b': 2, 'c': 1})
        {1: ['a', 'c'], 2: ['b']}
    """
    result: Dict[V, List[K]] = defaultdict(list)
    for k, v in d.items():
        result[v].append(k)
    return dict(result)


def partition_by(
    items: List[T],
    predicate: Callable[[T], bool],
) -> Tuple[List[T], List[T]]:
    """
    Partition items into two lists based on predicate.
    
    Args:
        items: List of items to partition
        predicate: Function returning True for first group
    
    Returns:
        Tuple of (matching, non-matching) lists
    
    Example:
        >>> partition_by([1, 2, 3, 4, 5], lambda x: x > 2)
        ([3, 4, 5], [1, 2])
    """
    true_list: List[T] = []
    false_list: List[T] = []
    for item in items:
        if predicate(item):
            true_list.append(item)
        else:
            false_list.append(item)
    return true_list, false_list
