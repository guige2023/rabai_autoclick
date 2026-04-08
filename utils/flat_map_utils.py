"""Flat map and functional utilities.

Provides functional programming utilities for collections
and data transformation in automation workflows.
"""

from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, TypeVar, Union


T = TypeVar("T")
U = TypeVar("U")


def flat_map(func: Callable[[T], Iterable[U]], iterable: Iterable[T]) -> List[U]:
    """Map function over iterable and flatten results.

    Example:
        result = flat_map(lambda x: [x, x*2], [1, 2, 3])
        # [1, 2, 2, 4, 3, 6]
    """
    result: List[U] = []
    for item in iterable:
        for sub_item in func(item):
            result.append(sub_item)
    return result


def group_by(key_func: Callable[[T], K], iterable: Iterable[T]) -> dict:
    """Group items by key function.

    Example:
        grouped = group_by(lambda x: x % 2, [1, 2, 3, 4, 5])
        # {1: [1, 3, 5], 0: [2, 4]}
    """
    result: dict = {}
    for item in iterable:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def partition(predicate: Callable[[T], bool], iterable: Iterable[T]) -> tuple:
    """Partition iterable by predicate.

    Example:
        positives, negatives = partition(lambda x: x > 0, [-1, 0, 1, 2])
        # ([1, 2], [-1, 0])
    """
    truthy: List[T] = []
    falsy: List[T] = []
    for item in iterable:
        if predicate(item):
            truthy.append(item)
        else:
            falsy.append(item)
    return (truthy, falsy)


def chunk(size: int, iterable: Iterable[T]) -> Iterator[List[T]]:
    """Split iterable into chunks of size.

    Example:
        chunks = list(chunk(3, [1, 2, 3, 4, 5, 6, 7]))
        # [[1, 2, 3], [4, 5, 6], [7]]
    """
    chunk: List[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def window(size: int, iterable: Iterable[T]) -> Iterator[List[T]]:
    """Create sliding windows over iterable.

    Example:
        windows = list(window(3, [1, 2, 3, 4, 5]))
        # [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    """
    window: List[T] = []
    for item in iterable:
        window.append(item)
        if len(window) == size:
            yield window
            window = window[1:]


def flatten(iterable: Iterable, depth: int = 1) -> Iterator:
    """Flatten nested iterable.

    Example:
        result = list(flatten([[1, 2], [3, [4]]], depth=2))
        # [1, 2, 3, 4]
    """
    for item in iterable:
        if depth > 0 and isinstance(item, (list, tuple)):
            for sub in flatten(item, depth - 1):
                yield sub
        else:
            yield item


def pluck(key: str, iterable: Iterable[dict]) -> List[Any]:
    """Extract values by key from list of dicts.

    Example:
        result = pluck("name", [{"name": "a", "v": 1}, {"name": "b"}])
        # ["a", "b"]
    """
    return [item.get(key) if isinstance(item, dict) else getattr(item, key, None) for item in iterable]


def pluck_many(keys: List[str], iterable: Iterable[dict]) -> List[dict]:
    """Extract multiple keys from list of dicts.

    Example:
        result = pluck_many(["a", "b"], [{"a": 1, "b": 2, "c": 3}])
        # [{"a": 1, "b": 2}]
    """
    return [{k: item.get(k) if isinstance(item, dict) else getattr(item, k, None) for k in keys} for item in iterable]


def union(*iterables: Iterable[T]) -> List[T]:
    """Union of multiple iterables (no duplicates).

    Example:
        result = union([1, 2], [2, 3], [3, 4])
        # [1, 2, 3, 4]
    """
    seen = set()
    result: List[T] = []
    for iterable in iterables:
        for item in iterable:
            if item not in seen:
                seen.add(item)
                result.append(item)
    return result


def intersection(*iterables: Iterable[T]) -> List[T]:
    """Intersection of multiple iterables.

    Example:
        result = intersection([1, 2, 3], [2, 3, 4], [3, 4, 5])
        # [3]
    """
    if not iterables:
        return []
    common = set(iterables[0])
    for iterable in iterables[1:]:
        common &= set(iterable)
    return list(common)


def difference(base: Iterable[T], *others: Iterable[T]) -> List[T]:
    """Elements in base not in others.

    Example:
        result = difference([1, 2, 3, 4], [2, 3], [4])
        # [1]
    """
    others_set = set()
    for other in others:
        others_set |= set(other)
    return [item for item in base if item not in others_set]


def zip_longest(*iterables: Any, fillvalue: Any = None) -> Iterator[tuple]:
    """Zip iterables of different lengths.

    Example:
        result = list(zip_longest([1, 2], [3, 4, 5], fillvalue=0))
        # [(1, 3), (2, 4), (0, 5)]
    """
    iterators = [iter(it) for it in iterables]
    while True:
        values = []
        done = True
        for it in iterators:
            try:
                value = next(it)
                done = False
            except StopIteration:
                value = fillvalue
            values.append(value)
        if done:
            break
        yield tuple(values)


class Composable(Generic[T]):
    """Composable function wrapper.

    Example:
        f = Composable(lambda x: x * 2)
        g = Composable(lambda x: x + 1)
        h = f.compose(g)  # h(x) = (x + 1) * 2
        result = h.apply(5)  # (5 + 1) * 2 = 12
    """

    def __init__(self, func: Callable[..., T]) -> None:
        self._func = func

    def apply(self, value: T) -> Any:
        """Apply function to value."""
        return self._func(value)

    def compose(self, other: "Composable") -> "Composable":
        """Compose two functions: self.after(other).

        Args:
            other: Function to apply first.

        Returns:
            New composed function.
        """
        return Composable(lambda x: self._func(other._func(x)))

    def then(self, other: "Composable") -> "Composable":
        """Chain functions: self.then(other) = self.after(other).

        Args:
            other: Function to apply after.

        Returns:
            New composed function.
        """
        return self.compose(other)

    def __call__(self, value: T) -> Any:
        return self.apply(value)
