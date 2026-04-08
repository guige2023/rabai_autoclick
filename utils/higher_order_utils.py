"""Higher-order function utilities.

Provides functional programming helpers for composing,
currying, and partial application in automation workflows.
"""

import functools
from typing import Any, Callable, TypeVar


T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


def compose(*funcs: Callable) -> Callable:
    """Compose functions right-to-left.

    Example:
        f = compose(f1, f2, f3)  # f(x) = f1(f2(f3(x)))
    """
    if not funcs:
        return lambda x: x

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result

    return composed


def pipe(*funcs: Callable) -> Callable:
    """Pipe functions left-to-right.

    Example:
        f = pipe(f1, f2, f3)  # f(x) = f3(f2(f1(x)))
    """
    if not funcs:
        return lambda x: x

    def piped(x: Any) -> Any:
        result = x
        for func in funcs:
            result = func(result)
        return result

    return piped


def curry(func: Callable[..., T]) -> Callable[..., T]:
    """Curry a function.

    Example:
        @curry
        def add(x, y):
            return x + y
        add(1)(2)  # 3
    """
    @functools.wraps(func)
    def curried(*args: Any, **kwargs: Any) -> Any:
        if len(args) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: curried(*(args + more_args), **{**kwargs, **more_kwargs})
    return curried


def rcurry(func: Callable[..., T]) -> Callable[..., T]:
    """Right-curry a function.

    Example:
        @rcurry
        def sub(x, y):
            return x - y
        sub(10)(2)  # 8 (10 - 2)
    """
    @functools.wraps(func)
    def rcurried(*args: Any, **kwargs: Any) -> Any:
        if len(args) >= func.__code__.co_argcount:
            return func(*args, **kwargs)
        return lambda *more_args, **more_kwargs: rcurried(*(args + more_args), **{**kwargs, **more_kwargs})
    return rcurried


def flip(func: Callable[[A, B], C]) -> Callable[[B, A], C]:
    """Flip argument order.

    Example:
        div = flip(lambda a, b: a / b)
        div(2, 4)  # 2.0 (4 / 2)
    """
    @functools.wraps(func)
    def flipped(b: B, a: A) -> C:
        return func(a, b)
    return flipped


def juxt(*funcs: Callable[[A], B]) -> Callable[[A], tuple]:
    """Juxtapose - apply functions and return tuple of results.

    Example:
        f = juxt(lambda x: x * 2, lambda x: x + 1)
        f(5)  # (10, 6)
    """
    @functools.wraps(funcs[0] if funcs else lambda x: x)
    def juxtaposed(a: A) -> tuple:
        return tuple(f(a) for f in funcs)
    return juxtaposed


def complement(func: Callable[..., bool]) -> Callable[..., bool]:
    """Return complement of predicate.

    Example:
        is_even = complement(lambda x: x % 2)
        is_even(2)  # True
        is_even(3)  # False
    """
    @functools.wraps(func)
    def complemented(*args: Any, **kwargs: Any) -> bool:
        return not func(*args, **kwargs)
    return complemented


def constantly(value: T) -> Callable[..., T]:
    """Return function that always returns the same value.

    Example:
        f = constantly(42)
        f()  # 42
        f(anything)  # 42
    """
    return lambda *args, **kwargs: value


def tap(func: Callable[[T], Any]) -> Callable[[T], T]:
    """Tap into a pipeline to perform side-effect.

    Example:
        result = tap(lambda x: print(f"Value: {x}"))(5)  # prints "Value: 5", returns 5
    """
    @functools.wraps(func)
    def tapped(x: T) -> T:
        func(x)
        return x
    return tapped


def memoize(func: Callable[..., T]) -> Callable[..., T]:
    """Memoize function with unbounded cache.

    Example:
        @memoize
        def fib(n):
            return n if n < 2 else fib(n-1) + fib(n-2)
    """
    cache: dict = {}

    @functools.wraps(func)
    def memoized(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    memoized.cache_clear = lambda: cache.clear()
    return memoized


def unfold(func: Callable[[A], tuple], seed: A) -> "Generator":
    """Unfold a value through a function.

    Example:
        def next_pair(n):
            return n + 1, (n * 2, n + 1)
        list(unfold(next_pair, 0))[:5]  # [0, 1, 2, 4, 8]
    """
    current = seed
    while True:
        result, current = func(current)
        yield result


class Func:
    """Function composition helper.

    Example:
        result = (Func(lambda x: x + 1)
                   .pipe(lambda x: x * 2)
                   .call(5))  # 12
    """

    def __init__(self, func: Callable) -> None:
        self._func = func

    def pipe(self, other: Callable) -> "Func":
        """Add function to pipeline (left-to-right)."""
        return Func(lambda x: other(self._func(x)))

    def compose(self, other: Callable) -> "Func":
        """Add function to composition (right-to-left)."""
        return Func(lambda x: self._func(other(x)))

    def call(self, *args: Any, **kwargs: Any) -> Any:
        """Call the function with arguments."""
        return self._func(*args, **kwargs)

    def then(self, func: Callable) -> "Func":
        """Alias for pipe."""
        return self.pipe(func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.call(*args, **kwargs)


def partial_right(func: Callable[..., T], *right_args: Any, **right_kwargs: Any) -> Callable[..., T]:
    """Partial application from the right.

    Example:
        div = lambda a, b: a / b
        half = partial_right(div, 2)
        half(10)  # 5.0 (10 / 2)
    """
    @functools.wraps(func)
    def partial(*args: Any, **kwargs: Any) -> T:
        return func(*args, *right_args, **{**kwargs, **right_kwargs})
    return partial


def wrap(wrapper: Callable[[Callable], Callable]) -> Callable[[Callable], Callable]:
    """Decorator to wrap a function with a wrapper.

    Example:
        @wrap(lambda f: lambda x: f(x + 1))
        def add_two(x):
            return x + 2
        # add_two(5) now returns 8
    """
    def decorator(func: Callable) -> Callable:
        return wrapper(func)
    return decorator
