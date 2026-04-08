"""Function wrapper and decorator utilities.

Provides common function wrappers and decorators for
aspect-oriented programming in automation workflows.
"""

import functools
import time
import traceback
from typing import Any, Callable, List, Optional, TypeVar


T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to retry function on failure.

    Args:
        max_attempts: Maximum retry attempts.
        delay: Initial delay between retries.
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exceptions to catch.

    Example:
        @retry(max_attempts=3, delay=1.0)
        def unstable_operation():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
            raise last_exception
        return wrapper
    return decorator


def timeout(seconds: float) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to add timeout to function.

    Args:
        seconds: Timeout in seconds.

    Example:
        @timeout(5.0)
        def long_operation():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            import signal
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Function {func.__name__} timed out")
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(seconds))
            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        return wrapper
    return decorator


def memoized(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to memoize function results.

    Example:
        @memoized
        def expensive_computation(x):
            return x * x
    """
    cache: dict = {}

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    wrapper.cache_clear = lambda: cache.clear()
    return wrapper


def cached_property(func: Callable[..., T]) -> property:
    """Decorator for cached property (computed once per instance).

    Example:
        class MyClass:
            @cached_property
            def heavy_value(self):
                return compute_heavy_value()
    """
    attr_name = f"_cached_{func.__name__}"

    @property
    def wrapper(self: Any) -> T:
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return wrapper


def log_calls(logger: Any = None, level: str = "debug") -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to log function calls.

    Args:
        logger: Logger instance (uses print if None).
        level: Log level name.

    Example:
        @log_calls()
        def my_function(x):
            return x * 2
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            log = logger if logger else print
            log_msg = getattr(log, level, log.debug)
            log_msg(f"Calling {func.__name__}(args={args}, kwargs={kwargs})")
            try:
                result = func(*args, **kwargs)
                log_msg(f"{func.__name__} returned: {result}")
                return result
            except Exception as e:
                log_msg(f"{func.__name__} raised: {type(e).__name__}: {e}")
                raise
        return wrapper
    return decorator


def benchmark(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to benchmark function execution time.

    Example:
        @benchmark
        def slow_function():
            time.sleep(1)
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f"{func.__name__}: {end - start:.4f}s")
        return result
    return wrapper


def before(before_func: Callable) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to run before function.

    Example:
        @before(lambda: print("Before"))
        def my_function():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            before_func()
            return func(*args, **kwargs)
        return wrapper
    return decorator


def after(after_func: Callable) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to run after function.

    Example:
        @after(lambda: print("After"))
        def my_function():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            result = func(*args, **kwargs)
            after_func()
            return result
        return wrapper
    return decorator


def ignore_exceptions(*exceptions: type) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to ignore specific exceptions.

    Example:
        @ignore_exceptions(ValueError, TypeError)
        def risky_operation():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            try:
                return func(*args, **kwargs)
            except exceptions:
                return None
        return wrapper
    return decorator


def validate_args(**validators: Any) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to validate function arguments.

    Example:
        @validate_args(x=int, y=int)
        def add(x, y):
            return x + y
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            sig = functools.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            for name, validator in validators.items():
                if name in bound.arguments:
                    value = bound.arguments[name]
                    if not isinstance(value, validator):
                        raise TypeError(
                            f"Argument {name} must be {validator.__name__}, "
                            f"got {type(value).__name__}"
                        )
            return func(*args, **kwargs)
        return wrapper
    return decorator


def trace_errors(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to trace and log errors.

    Example:
        @trace_errors
        def risky_function():
            pass
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        try:
            return func(*args, **kwargs)
        except Exception:
            print(f"Error in {func.__name__}:")
            traceback.print_exc()
            raise
    return wrapper


class Partial:
    """Create partial function with pre-filled arguments.

    Example:
        add_ten = Partial(add, y=10)
        result = add_ten(5)  # 15
    """

    def __init__(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *args: Any, **kwargs: Any) -> T:
        return self._func(*self._args, *args, **{**self._kwargs, **kwargs})

    def partial(self, *args: Any, **kwargs: Any) -> "Partial":
        """Create partial of partial."""
        return Partial(self._func, *self._args, *args, **{**self._kwargs, **kwargs})
