"""Functional programming utilities for RabAI AutoClick.

Provides:
- Function composition
- Currying
- Memoization
- Lazy evaluation
"""

import functools
import itertools
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TypeVar


T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")


def compose(*functions: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *functions: Functions to compose.

    Returns:
        Composed function.

    Usage:
        f = compose(f1, f2, f3)
        result = f(x)  # equivalent to f1(f2(f3(x)))
    """
    if not functions:
        raise ValueError("At least one function required")

    if len(functions) == 1:
        return functions[0]

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(functions):
            result = func(result)
        return result

    return composed


def pipe(*functions: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Pipe functions left-to-right.

    Args:
        *functions: Functions to pipe.

    Returns:
        Piped function.

    Usage:
        f = pipe(f1, f2, f3)
        result = f(x)  # equivalent to f3(f2(f1(x)))
    """
    if not functions:
        raise ValueError("At least one function required")

    if len(functions) == 1:
        return functions[0]

    def piped(x: Any) -> Any:
        result = x
        for func in functions:
            result = func(result)
        return result

    return piped


def curry(func: Callable[..., R]) -> Callable[..., R]:
    """Curry a function.

    Args:
        func: Function to curry.

    Returns:
        Curried function.

    Usage:
        @curry
        def add(a, b):
            return a + b

        add(1)(2)  # Returns 3
        add(1, 2)  # Also returns 3
    """
    @functools.wraps(func)
    def curried(*args: Any, **kwargs: Any) -> Any:
        if len(args) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **{**kwargs, **more_kwargs})
    return curried


def memoize(func: Optional[Callable[..., T]] = None, *, max_size: int = 128) -> Callable[..., T]:
    """Memoize a function with bounded cache.

    Args:
        func: Function to memoize.
        max_size: Maximum cache size.

    Returns:
        Memoized function or decorator.

    Usage:
        @memoize
        def expensive_func(x):
            return x ** 2
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        cache: Dict[Tuple, T] = {}
        cache_order: List[Tuple] = []

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                return cache[key]

            result = f(*args, **kwargs)

            if len(cache) >= max_size:
                oldest = cache_order.pop(0)
                del cache[oldest]

            cache[key] = result
            cache_order.append(key)
            return result

        wrapper.cache_clear = lambda: (cache.clear(), cache_order.clear())
        wrapper.cache_info = lambda: {"size": len(cache), "max_size": max_size}
        return wrapper

    if func is None:
        return decorator
    return decorator(func)


def lazy(func: Callable[..., T]) -> Callable[..., lambda: T]:
    """Make a function return its result lazily.

    Args:
        func: Function to make lazy.

    Returns:
        Function that returns a lambda which evaluates to result.

    Usage:
        @lazy
        def expensive_computation():
            return compute()

        result_getter = expensive_computation()
        # ... later ...
        actual_result = result_getter()
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Callable[[], T]:
        result: List[T] = [None]
        computed = [False]

        def getter() -> T:
            if not computed[0]:
                result[0] = func(*args, **kwargs)
                computed[0] = True
            return result[0]

        return getter

    return wrapper


def retry_call(
    func: Callable[..., T],
    *args: Any,
    retries: int = 3,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
    **kwargs: Any,
) -> T:
    """Call function with retry on failure.

    Args:
        func: Function to call.
        *args: Positional arguments.
        retries: Number of retries.
        exceptions: Exceptions to catch.
        on_retry: Optional callback on retry.
        **kwargs: Keyword arguments.

    Returns:
        Function result.

    Raises:
        Last exception if all retries fail.
    """
    last_exception: Exception
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt < retries - 1 and on_retry:
                on_retry(e, attempt + 1)
    raise last_exception


def chunk(iterable: List[T], size: int) -> Generator[List[T], None, None]:
    """Split iterable into chunks of specified size.

    Args:
        iterable: Input iterable.
        size: Chunk size.

    Yields:
        Chunks of the input.
    """
    it = iter(iterable)
    while True:
        chunk_result = list(itertools.islice(it, size))
        if not chunk_result:
            return
        yield chunk_result


def flatten(nested: List[Any]) -> Generator[Any, None, None]:
    """Flatten a nested iterable.

    Args:
        nested: Nested iterable to flatten.

    Yields:
        Flattened elements.
    """
    for item in nested:
        if isinstance(item, (list, tuple)):
            yield from flatten(item)
        else:
            yield item


def group_by(
    items: List[T],
    key_func: Callable[[T], U],
) -> Dict[U, List[T]]:
    """Group items by a key function.

    Args:
        items: Items to group.
        key_func: Function to extract grouping key.

    Returns:
        Dict mapping keys to lists of items.
    """
    result: Dict[U, List[T]] = {}
    for item in items:
        key = key_func(item)
        if key not in result:
            result[key] = []
        result[key].append(item)
    return result


def partition(
    items: List[T],
    predicate: Callable[[T], bool],
) -> Tuple[List[T], List[T]]:
    """Partition items into two lists based on predicate.

    Args:
        items: Items to partition.
        predicate: Function that returns True for items in first list.

    Returns:
        Tuple of (matching, non-matching) lists.
    """
    matching = []
    non_matching = []
    for item in items:
        if predicate(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


def pluck(items: List[Dict[str, Any]], key: str, default: Any = None) -> List[Any]:
    """Extract values for a key from list of dicts.

    Args:
        items: List of dictionaries.
        key: Key to extract.
        default: Default value if key not found.

    Returns:
        List of values.
    """
    return [item.get(key, default) for item in items]


def sort_by(items: List[T], key_func: Callable[[T], Any], reverse: bool = False) -> List[T]:
    """Sort items by a key function.

    Args:
        items: Items to sort.
        key_func: Function to extract sort key.
        reverse: If True, sort in descending order.

    Returns:
        Sorted list.
    """
    return sorted(items, key=key_func, reverse=reverse)


def unique(items: List[T], key_func: Optional[Callable[[T], Any]] = None) -> List[T]:
    """Get unique items preserving order.

    Args:
        items: Items to deduplicate.
        key_func: Optional function to extract key for uniqueness check.

    Returns:
        List with duplicates removed.
    """
    seen: set = set()
    result: List[T] = []

    for item in items:
        key = key_func(item) if key_func else item
        if key not in seen:
            seen.add(key)
            result.append(item)

    return result


def batch(items: List[T], batch_size: int) -> Generator[List[T], None, None]:
    """Yield successive batches from items.

    Args:
        items: Items to batch.
        batch_size: Size of each batch.

    Yields:
        Batches of items.
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]