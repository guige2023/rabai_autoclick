"""
Function composition and higher-order utilities for UI automation.

Provides function composition, partial application, memoization,
and other functional programming utilities.
"""

from __future__ import annotations

import time
import functools
import inspect
from typing import (
    TypeVar, Callable, Optional, Any, Union, 
    ParamSpec, Generic, Iterator, overload
)
from collections import OrderedDict
from dataclasses import dataclass


P = ParamSpec('P')
T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
R = TypeVar('R')


def compose(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions right-to-left.
    
    Args:
        *funcs: Functions to compose
    
    Returns:
        Composed function
    
    Example:
        f = compose(str, lambda x: x * 2, lambda x: x + 1)
        f(5)  # str((5 + 1) * 2) = "12"
    """
    if not funcs:
        return lambda x: x
    
    def composed(x: Any) -> Any:
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result
    
    return composed


def pipe(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Pipe functions left-to-right.
    
    Args:
        *funcs: Functions to pipe
    
    Returns:
        Piped function
    
    Example:
        f = pipe(lambda x: x + 1, lambda x: x * 2, str)
        f(5)  # str(5 + 1 * 2) = "12"
    """
    if not funcs:
        return lambda x: x
    
    return compose(*reversed(funcs))


def partial(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Callable[[], T]:
    """Create partial application of function.
    
    Args:
        func: Function to partially apply
        *args: Positional arguments to bind
        **kwargs: Keyword arguments to bind
    
    Returns:
        Function with bound arguments
    """
    @functools.wraps(func)
    def wrapper(*more_args: P.args, **more_kwargs: P.kwargs) -> T:
        return func(*args, *more_args, **kwargs, **more_kwargs)
    
    return wrapper


def curry(func: Callable[P, T]) -> Callable[..., T]:
    """Curry a function.
    
    Args:
        func: Function to curry
    
    Returns:
        Curried function
    
    Example:
        def add(a, b): return a + b
        curried_add = curry(add)
        curried_add(1)(2)  # 3
    """
    sig = inspect.signature(func)
    
    @functools.wraps(func)
    def curried(*args: P.args, **kwargs: P.kwargs) -> Any:
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        
        if len(bound.arguments) == len(sig.parameters):
            return func(*bound.args, **bound.kwargs)
        
        def next_curry(*more_args: P.args, **more_kwargs: P.kwargs) -> Any:
            new_args = {**bound.arguments, **more_kwargs}
            new_positional = list(bound.args) + list(more_args)
            return curried(*new_positional, **new_args)
        
        return next_curry
    
    return curried


def flip(func: Callable[[T, U], R]) -> Callable[[U, T], R]:
    """Flip argument order of a two-argument function.
    
    Args:
        func: Function to flip
    
    Returns:
        Function with flipped arguments
    """
    @functools.wraps(func)
    def flipped(a: U, b: T) -> R:
        return func(b, a)
    
    return flipped


def identity(x: T) -> T:
    """Identity function.
    
    Args:
        x: Any value
    
    Returns:
        The same value
    """
    return x


def constant(x: T) -> Callable[[Any], T]:
    """Create constant function.
    
    Args:
        x: Value to always return
    
    Returns:
        Function that always returns x
    """
    return lambda _: x


def memoize(
    func: Optional[Callable[P, T]] = None,
    *,
    max_size: int = 128,
    ttl: Optional[float] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Memoize function with optional TTL and size limit.
    
    Args:
        func: Function to memoize
        max_size: Maximum cache size
        ttl: Time-to-live in seconds
    
    Returns:
        Memoized function decorator
    """
    def decorator(f: Callable[P, T]) -> Callable[P, T]:
        cache: OrderedDict[tuple, tuple[Any, float]] = OrderedDict()
        
        @functools.wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            
            if key in cache:
                result, timestamp = cache[key]
                if ttl is None or time.time() - timestamp < ttl:
                    cache.move_to_end(key)
                    return result
            
            result = f(*args, **kwargs)
            cache[key] = (result, time.time())
            
            if len(cache) > max_size:
                cache.popitem(last=False)
            
            return result
        
        def cache_clear() -> None:
            cache.clear()
        
        wrapper.cache_clear = cache_clear  # type: ignore
        wrapper.cache_info = lambda: {'size': len(cache), 'max_size': max_size}  # type: ignore
        
        return wrapper
    
    if func is not None:
        return decorator(func)
    return decorator


def retry(
    max_attempts: int = 3,
    delay: float = 0.0,
    backoff: float = 1.0,
    exceptions: tuple = (Exception,),
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum retry attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier
        exceptions: Exceptions to catch
    
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exception: Optional[Exception] = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
            
            raise last_exception  # type: ignore
        
        return wrapper
    
    return decorator


def timeout(seconds: float) -> Callable[[Callable[P, T]], Callable[P, Optional[T]]]:
    """Add timeout to function execution.
    
    Args:
        seconds: Timeout in seconds
    
    Returns:
        Decorated function with timeout
    """
    def decorator(func: Callable[P, T]) -> Callable[P, Optional[T]]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            result: Any = None
            exception: Optional[Exception] = None
            
            def target() -> None:
                nonlocal result, exception
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    exception = e
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(seconds)
            
            if thread.is_alive():
                return None
            
            if exception is not None:
                raise exception
            
            return result
        
        return wrapper
    
    return decorator


import threading


def debounce(wait: float) -> Callable[[Callable[P, T]], Callable[P, Optional[T]]]:
    """Debounce function calls.
    
    Args:
        wait: Seconds to wait after last call
    
    Returns:
        Decorated debounced function
    """
    def decorator(func: Callable[P, T]) -> Callable[P, Optional[T]]:
        timer: Optional[threading.Timer] = None
        
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            nonlocal timer
            
            if timer is not None:
                timer.cancel()
            
            result: T = None  # type: ignore
            
            def call() -> None:
                nonlocal result
                result = func(*args, **kwargs)
            
            timer = threading.Timer(wait, call)
            timer.start()
            
            return None
        
        return wrapper
    
    return decorator


def throttle(rate: float) -> Callable[[Callable[P, T]], Callable[P, Optional[T]]]:
    """Throttle function calls.
    
    Args:
        rate: Minimum seconds between calls
    
    Returns:
        Decorated throttled function
    """
    last_call = [0.0]
    lock = threading.Lock()
    
    def decorator(func: Callable[P, T]) -> Callable[P, Optional[T]]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
            with lock:
                now = time.time()
                if now - last_call[0] < rate:
                    return None
                last_call[0] = now
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


def once(func: Callable[P, T]) -> Callable[P, Optional[T]]:
    """Call function only once, cache result.
    
    Args:
        func: Function to call once
    
    Returns:
        Function that calls func only on first call
    """
    called = [False]
    result = [None]
    
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Optional[T]:
        if not called[0]:
            called[0] = True
            result[0] = func(*args, **kwargs)
        return result[0]
    
    return wrapper


def tap(func: Callable[[T], Any]) -> Callable[[T], T]:
    """Tap into a pipeline, applying side effect.
    
    Args:
        func: Side effect function
    
    Returns:
        Function that applies func and returns original value
    
    Example:
        result = tap(lambda x: print(f"Value: {x}"))(5)  # prints "Value: 5", returns 5
    """
    def decorator(x: T) -> T:
        func(x)
        return x
    
    return decorator


def juxt(*funcs: Callable[[T], U]) -> Callable[[T], list[U]]:
    """Apply multiple functions to same argument.
    
    Args:
        *funcs: Functions to apply
    
    Returns:
        Function returning list of results
    
    Example:
        f = juxt(len, str.upper, lambda x: x * 2)
        f("hi")  # [2, 'HI', 'hihi']
    """
    def decorator(x: T) -> list[U]:
        return [f(x) for f in funcs]
    
    return decorator


def complement(func: Callable[[T], bool]) -> Callable[[T], bool]:
    """Return complement of predicate function.
    
    Args:
        func: Predicate function
    
    Returns:
        Function returning not func(x)
    """
    return lambda x: not func(x)


def iterate(func: Callable[[T], T], n: int) -> Callable[[T], Iterator[T]]:
    """Create iterator that applies func n times.
    
    Args:
        func: Function to apply
        n: Number of iterations
    
    Returns:
        Function yielding iterated values
    
    Example:
        f = iterate(lambda x: x * 2, 3)
        list(f(1))  # [2, 4, 8]
    """
    def decorator(start: T) -> Iterator[T]:
        current = start
        for _ in range(n):
            current = func(current)
            yield current
    
    return decorator


def unfold(
    func: Callable[[T], Optional[tuple[U, T]]],
    seed: T,
) -> Iterator[U]:
    """Unfold value from seed using function.
    
    Args:
        func: Unfold function returning (value, next_seed) or None
        seed: Initial seed value
    
    Yields:
        Unfolded values
    
    Example:
        f = lambda x: (x, x + 1) if x < 5 else None
        list(unfold(f, 0))  # [0, 1, 2, 3, 4]
    """
    current = seed
    while True:
        result = func(current)
        if result is None:
            break
        value, current = result
        yield value


@dataclass
class LazyValue:
    """Lazy evaluated value."""
    _value: Any = None
    _evaluated: bool = False
    
    def get(self, func: Callable[[], T]) -> T:
        """Get value, evaluating if needed."""
        if not self._evaluated:
            self._value = func()
            self._evaluated = True
        return self._value
    
    def reset(self) -> None:
        """Reset lazy value."""
        self._value = None
        self._evaluated = False
