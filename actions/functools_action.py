"""
Functools Action Module

Provides functools utilities including partial functions, caching,
comparison functions, and function wrappers for the automation framework.

Author: AI Assistant
Version: 1.0.0
"""

from __future__ import annotations

import functools
import operator
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

# Type variables
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class FunctoolsAction:
    """
    Main functools action handler providing utility functions.
    
    This class wraps and extends Python's functools module with
    additional utilities for common functional programming tasks.
    
    Attributes:
        None (all methods are static or class methods)
    """
    
    @staticmethod
    def partial(
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., T]:
        """
        Create a partial function with some arguments pre-filled.
        
        Args:
            func: Function to wrap
            *args: Positional arguments to bind
            **kwargs: Keyword arguments to bind
        
        Returns:
            Partial function
        
        Example:
            >>> def power(base, exponent):
            ...     return base ** exponent
            >>> square = FunctoolsAction.partial(power, exponent=2)
            >>> square(5)
            25
        """
        return functools.partial(func, *args, **kwargs)
    
    @staticmethod
    def cache(func: Callable[..., T]) -> Callable[..., T]:
        """
        Decorator to cache function results (unbounded cache).
        
        Results are cached based on arguments. Use this for expensive
        computations with frequently repeated inputs.
        
        Args:
            func: Function to wrap
        
        Returns:
            Cached function
        
        Example:
            >>> @FunctoolsAction.cache
            ... def fib(n):
            ...     return n if n < 2 else fib(n-1) + fib(n-2)
        """
        return functools.cache(func)
    
    @staticmethod
    def lru_cache(
        maxsize: Optional[int] = 128,
        *,
        typed: bool = False,
    ) -> Callable[[F], F]:
        """
        Decorator factory for LRU (Least Recently Used) caching.
        
        Args:
            maxsize: Maximum cache size (None for unlimited, 0 to disable)
            typed: If True, cache different types separately
        
        Returns:
            Decorator function
        
        Example:
            >>> @FunctoolsAction.lru_cache(maxsize=100)
            ... def expensive(x):
            ...     return x * x
        """
        return functools.lru_cache(maxsize=maxsize, typed=typed)
    
    @staticmethod
    def cached_property(
        func: Optional[Callable[[Any], T]] = None,
        *,
        name: Optional[str] = None,
    ) -> Any:
        """
        Decorator that converts a method into a cached property.
        
        Args:
            func: Function to wrap
            name: Optional custom name for the attribute
        
        Returns:
            Cached property descriptor
        
        Example:
            >>> class MyClass:
            ...     @FunctoolsAction.cached_property
            ...     def expensive_result(self):
            ...         return compute_something()
        """
        return functools.cached_property(func, name=name)  # type: ignore
    
    @staticmethod
    def reduce(
        func: Callable[[T, Any], T],
        iterable: Union[List[Any], Tuple[Any, ...]],
        initial: Optional[T] = None,
    ) -> T:
        """
        Reduce a sequence to a single value using a function.
        
        Args:
            func: Binary function (takes accumulated value and item)
            iterable: Sequence to reduce
            initial: Initial value for accumulator
        
        Returns:
            Reduced value
        
        Example:
            >>> FunctoolsAction.reduce(operator.add, [1, 2, 3, 4], 0)
            10
        """
        if initial is not None:
            return functools.reduce(func, iterable, initial)
        return functools.reduce(func, iterable)
    
    @staticmethod
    def wraps(
        wrapped: Callable[..., Any],
        *,
        assigned: Optional[Tuple[str, ...]] = None,
        updated: Optional[Tuple[str, ...]] = None,
    ) -> Callable[[F], F]:
        """
        Decorator factory to copy function metadata from one function to another.
        
        Args:
            wrapped: Function whose metadata to copy
            assigned: Tuple of attributes to copy (default: __module__, __name__, __qualname__, __annotations__, __doc__)
            updated: Tuple of attributes to update from wrapped function
        
        Returns:
            Decorator function
        
        Example:
            >>> def my_decorator(func):
            ...     @FunctoolsAction.wraps(func)
            ...     def wrapper(*args, **kwargs):
            ...         return func(*args, **kwargs)
            ...     return wrapper
        """
        return functools.wraps(wrapped, assigned=assigned, updated=updated)
    
    @staticmethod
    def cmp_to_key(
        func: Callable[[Any, Any], int],
    ) -> Callable[[Any], Any]:
        """
        Convert a comparison function to a key function.
        
        Args:
            func: Comparison function (returns negative, zero, positive)
        
        Returns:
            Key function for use with sorted(), min(), max(), etc.
        
        Example:
            >>> sorted(["banana", "apple", "cherry"], key=FunctoolsAction.cmp_to_key(str.compare))
        """
        return functools.cmp_to_key(func)
    
    @staticmethod
    def total_ordering(
        cls: Type[T],
    ) -> Type[T]:
        """
        Class decorator that fills in rich comparison methods.
        
        Args:
            cls: Class to decorate
        
        Returns:
            Decorated class with all comparison methods defined
        
        Example:
            >>> @FunctoolsAction.total_ordering
            ... class Ordered:
            ...     def __init__(self, value):
            ...         self.value = value
            ...     def __eq__(self, other):
            ...         return self.value == other.value
            ...     def __lt__(self, other):
            ...         return self.value < other.value
        """
        return functools.total_ordering(cls)
    
    @staticmethod
    def singledispatch(
        func: Optional[Callable[..., T]] = None,
        *,
        doc: Optional[str] = None,
    ) -> Callable[..., T]:
        """
        Decorator to create a function with multiple implementations
        based on the type of the first argument.
        
        Args:
            func: Function to decorate
            doc: Optional docstring for the function
        
        Returns:
            Dispatched function
        
        Example:
            >>> @FunctoolsAction.singledispatch
            ... def process(data):
            ...     return str(data)
            >>> @process.register(list)
            ... def _process_list(data):
            ...     return sum(data)
        """
        return functools.singledispatch(func, doc=doc)  # type: ignore
    
    @staticmethod
    def compose(
        *funcs: Callable[[Any], Any],
    ) -> Callable[[Any], Any]:
        """
        Compose multiple functions into a single function.
        
        The composed function applies funcs from right to left.
        
        Args:
            *funcs: Functions to compose
        
        Returns:
            Composed function
        
        Example:
            >>> add_one = lambda x: x + 1
            >>> double = lambda x: x * 2
            >>> f = FunctoolsAction.compose(add_one, double)
            >>> f(5)  # (5 * 2) + 1 = 11
            11
        """
        if not funcs:
            raise ValueError("At least one function must be provided")
        
        def composed(x: Any) -> Any:
            result = x
            for func in reversed(funcs):
                result = func(result)
            return result
        
        return composed
    
    @staticmethod
    def pipe(
        *funcs: Callable[[Any], Any],
    ) -> Callable[[Any], Any]:
        """
        Pipe functions left to right.
        
        Each function's output is passed to the next function.
        
        Args:
            *funcs: Functions to pipe
        
        Returns:
            Piped function
        
        Example:
            >>> add_one = lambda x: x + 1
            >>> double = lambda x: x * 2
            >>> f = FunctoolsAction.pipe(add_one, double)
            >>> f(5)  # (5 + 1) * 2 = 12
            12
        """
        if not funcs:
            raise ValueError("At least one function must be provided")
        
        def piped(x: Any) -> Any:
            result = x
            for func in funcs:
                result = func(result)
            return result
        
        return piped
    
    @staticmethod
    def flip(func: Callable[..., T]) -> Callable[..., T]:
        """
        Flip (reverse) the argument order of a binary function.
        
        Args:
            func: Binary function
        
        Returns:
            Function with reversed arguments
        
        Example:
            >>> def divide(a, b):
            ...     return a / b
            >>> flipped = FunctoolsAction.flip(divide)
            >>> flipped(2, 4)  # 4 / 2 = 2.0
            2.0
        """
        @functools.wraps(func)
        def flipped(*args: Any, **kwargs: Any) -> T:
            return func(*reversed(args), **kwargs)
        return flipped
    
    @staticmethod
    def retry(
        max_attempts: int = 3,
        delay: float = 0,
        backoff: float = 1,
        exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ) -> Callable[[F], F]:
        """
        Decorator to retry a function on failure.
        
        Args:
            max_attempts: Maximum number of attempts
            delay: Initial delay between retries (seconds)
            backoff: Multiplier for delay after each retry
            exceptions: Tuple of exceptions to catch
        
        Returns:
            Decorator function
        
        Example:
            >>> @FunctoolsAction.retry(max_attempts=3, delay=1, backoff=2)
            ... def unstable_function():
            ...     return might_fail()
        """
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                current_delay = delay
                last_exception: Optional[Exception] = None
                
                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt < max_attempts - 1:
                            import time
                            time.sleep(current_delay)
                            current_delay *= backoff
                
                if last_exception:
                    raise last_exception
                raise RuntimeError(f"Function failed after {max_attempts} attempts")
            
            return wrapper  # type: ignore
        return decorator
    
    @staticmethod
    def memoize_with_ttl(
        ttl: float,
        maxsize: Optional[int] = 128,
    ) -> Callable[[F], F]:
        """
        Memoization decorator with time-to-live expiration.
        
        Args:
            ttl: Time-to-live in seconds
            maxsize: Maximum cache size
        
        Returns:
            Decorator function
        
        Example:
            >>> @FunctoolsAction.memoize_with_ttl(ttl=60)
            ... def get_data():
            ...     return fetch_from_api()
        """
        cache: Dict[Tuple[Any, ...], Tuple[float, Any]] = {}
        cache_order: List[Tuple[Any, ...]] = []
        
        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                import time
                key = (args, tuple(sorted(kwargs.items())))
                current_time = time.time()
                
                # Check cache
                if key in cache:
                    cached_time, cached_value = cache[key]
                    if current_time - cached_time < ttl:
                        return cached_value
                
                # Compute and cache
                result = func(*args, **kwargs)
                cache[key] = (current_time, result)
                cache_order.append(key)
                
                # Evict if over maxsize
                if maxsize and len(cache) > maxsize:
                    oldest = cache_order.pop(0)
                    cache.pop(oldest, None)
                
                return result
            
            return wrapper  # type: ignore
        return decorator
    
    @staticmethod
    def curry(func: Callable[..., T]) -> Callable[..., T]:
        """
        Curry a function (transform to single-argument form).
        
        Args:
            func: Function to curry
        
        Returns:
            Curried function
        
        Example:
            >>> def add(a, b):
            ...     return a + b
            >>> curried = FunctoolsAction.curry(add)
            >>> curried(1)(2)
            3
        """
        @functools.wraps(func)
        def curried(*args: Any, **kwargs: Any) -> Any:
            if len(args) + len(kwargs) >= func.__code__.co_argcount:
                return func(*args, **kwargs)
            return lambda x: curried(*args, x, **kwargs)
        return curried  # type: ignore
    
    @staticmethod
    def uncurry(func: Callable[[Any], Callable[[Any], T]]) -> Callable[..., T]:
        """
        Uncurry a curried function.
        
        Args:
            func: Curried function
        
        Returns:
            Uncurried function
        
        Example:
            >>> def curried_add(x):
            ...     return lambda y: x + y
            >>> add = FunctoolsAction.uncurry(curried_add)
            >>> add(1, 2)
            3
        """
        @functools.wraps(func)
        def uncurried(*args: Any, **kwargs: Any) -> T:
            result = func(*args, **kwargs)
            while callable(result):
                result = result()
            return result
        return uncurried  # type: ignore
    
    @staticmethod
    def apply_when(
        condition: bool,
        func: Callable[..., T],
    ) -> Callable[..., Optional[T]]:
        """
        Conditionally apply a function.
        
        Args:
            condition: Whether to apply the function
            func: Function to apply conditionally
        
        Returns:
            Wrapper function
        
        Example:
            >>> process = FunctoolsAction.apply_when(
            ...     config.debug,
            ...     log_request
            ... )
            >>> process(request)  # Only calls log_request if config.debug is True
        """
        if condition:
            return func
        return lambda *args, **kwargs: None  # type: ignore
    
    @staticmethod
    def call_once(func: Callable[..., T]) -> Callable[..., T]:
        """
        Decorator to ensure a function is only called once.
        
        Args:
            func: Function to wrap
        
        Returns:
            Wrapped function that only executes once
        
        Example:
            >>> @FunctoolsAction.call_once
            ... def initialize():
            ...     print("Initialized!")
            >>> initialize()
            Initialized!
            >>> initialize()  # No output
        """
        called = False
        result: Optional[T] = None
        lock = __import__('threading').Lock()
        
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            nonlocal called, result
            with lock:
                if not called:
                    called = True
                    result = func(*args, **kwargs)
            return result  # type: ignore
        
        return wrapper


# Module-level convenience functions
def partial(func: Callable[..., T], *args: Any, **kwargs: Any) -> Callable[..., T]:
    """Create a partial function."""
    return FunctoolsAction.partial(func, *args, **kwargs)


def cache(func: Callable[..., T]) -> Callable[..., T]:
    """Cache function results."""
    return FunctoolsAction.cache(func)


def lru_cache(maxsize: Optional[int] = 128) -> Callable[[F], F]:
    """LRU cache decorator."""
    return FunctoolsAction.lru_cache(maxsize)


def reduce(func: Callable[[T, Any], T], iterable: Union[List[Any], Tuple[Any, ...]], initial: Optional[T] = None) -> T:
    """Reduce a sequence to a single value."""
    return FunctoolsAction.reduce(func, iterable, initial)


def compose(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions."""
    return FunctoolsAction.compose(*funcs)


def retry(max_attempts: int = 3, delay: float = 0) -> Callable[[F], F]:
    """Retry decorator."""
    return FunctoolsAction.retry(max_attempts, delay)


# Module metadata
__author__ = "AI Assistant"
__version__ = "1.0.0"
__all__ = [
    "FunctoolsAction",
    "partial",
    "cache",
    "lru_cache",
    "reduce",
    "compose",
    "retry",
]
