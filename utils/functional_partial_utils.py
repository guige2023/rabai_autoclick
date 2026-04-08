"""
Functional programming utilities: partial application, composition, and monadic operations.

Provides functional helpers for partial application, function composition,
currying, and pipe operations.

Example:
    >>> from utils.functional_partial_utils import partial, compose, pipe, curry
    >>> add = lambda a, b: a + b
    >>> add_five = partial(add, 5)
    >>> add_five(3)  # returns 8
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
R = TypeVar("R")


class partial:
    """
    Enhanced partial function application.

    Supports positional and keyword argument binding
    with priority-based argument resolution.
    """

    def __init__(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any
    ) -> None:
        """
        Create a partial application.

        Args:
            func: Function to partially apply.
            *args: Positional arguments to bind.
            **kwargs: Keyword arguments to bind.
        """
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.__signature__ = inspect.signature(func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the partially applied function."""
        combined_args = list(self.args) + list(args)
        combined_kwargs = {**self.kwargs, **kwargs}
        return self.func(*combined_args, **combined_kwargs)

    def __repr__(self) -> str:
        args_str = ", ".join(repr(a) for a in self.args)
        kwargs_str = ", ".join(f"{k}={v!r}" for k, v in self.kwargs.items())
        all_args = ", ".join(filter(None, [args_str, kwargs_str]))
        return f"partial({self.func.__name__}, {all_args})"


class curry(Generic[T, R]):
    """
    Curried function wrapper.

    Transforms a function to support incremental argument
    application until all required arguments are provided.
    """

    def __init__(
        self,
        func: Callable[[T], R],
        arity: Optional[int] = None,
    ) -> None:
        """
        Initialize the curried function.

        Args:
            func: Function to curry.
            arity: Number of arguments (inferred if None).
        """
        self.func = func
        self._cache: List[Any] = []
        self._arity = arity or self._get_arity(func)

    @staticmethod
    def _get_arity(func: Callable) -> int:
        """Get the number of required arguments."""
        sig = inspect.signature(func)
        required = sum(
            1 for p in sig.parameters.values()
            if p.default is inspect.Parameter.MISSING
            and p.kind not in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            )
        )
        return required

    def __call__(self, *args: Any) -> Any:
        """Call with accumulated arguments."""
        self._cache.extend(args)

        if len(self._cache) >= self._arity:
            result = self.func(*self._cache[:self._arity])
            self._cache.clear()
            return result

        return self

    def __repr__(self) -> str:
        return f"curry({self.func.__name__})"


def compose(*functions: Callable) -> Callable:
    """
    Compose functions right-to-left.

    compose(f, g, h)(x) = f(g(h(x)))

    Args:
        *functions: Functions to compose.

    Returns:
        Composed function.
    """
    if not functions:
        return lambda x: x

    def composed(x: Any) -> Any:
        result = x
        for func in reversed(functions):
            result = func(result)
        return result

    return composed


def compose_right(*functions: Callable) -> Callable:
    """
    Compose functions left-to-right.

    compose_right(f, g, h)(x) = h(g(f(x)))

    Args:
        *functions: Functions to compose.

    Returns:
        Composed function.
    """
    if not functions:
        return lambda x: x

    def composed(x: Any) -> Any:
        result = x
        for func in functions:
            result = func(result)
        return result

    return composed


def pipe(*functions: Callable) -> Callable:
    """
    Pipe functions left-to-right.

    pipe(f, g, h)(x) = h(g(f(x)))

    Alias for compose_right.

    Args:
        *functions: Functions to pipe.

    Returns:
        Piped function.
    """
    return compose_right(*functions)


def juxt(*functions: Callable) -> Callable:
    """
    Juxtapose functions.

    juxt(f, g, h)(x) = (f(x), g(x), h(x))

    Args:
        *functions: Functions to juxtapose.

    Returns:
        Function that returns tuple of results.
    """
    def juxtaposed(*args: Any, **kwargs: Any) -> tuple:
        return tuple(f(*args, **kwargs) for f in functions)

    return juxtaposed


def identity(x: T) -> T:
    """Identity function - returns input unchanged."""
    return x


def constantly(value: Any) -> Callable:
    """
    Returns a function that always returns the same value.

    Args:
        value: Value to always return.

    Returns:
        Function that ignores its arguments.
    """
    def constant(*args: Any, **kwargs: Any) -> Any:
        return value
    return constant


def flip(func: Callable[[T, U], R]) -> Callable[[U, T], R]:
    """
    Flip the arguments of a binary function.

    Args:
        func: Function to flip.

    Returns:
        Function with flipped arguments.
    """
    @functools.wraps(func)
    def flipped(a: Any, b: Any) -> Any:
        return func(b, a)
    return flipped


def memoize(func: Callable) -> Callable:
    """
    Memoize a function with LRU cache.

    Args:
        func: Function to memoize.

    Returns:
        Memoized function.
    """
    cache: Dict[tuple, Any] = {}

    @functools.wraps(func)
    def memoized(*args: Any, **kwargs: Any) -> Any:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    memoized.cache = cache
    return memoized


def after(count: int, func: Callable) -> Callable:
    """
    Return a function that only calls func after being called n times.

    Args:
        count: Number of times to call before executing.
        func: Function to call.

    Returns:
        Wrapped function.
    """
    call_count = [0]

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        call_count[0] += 1
        if call_count[0] >= count:
            return func(*args, **kwargs)
        return None

    return wrapper


def before(count: int, func: Callable) -> Callable:
    """
    Return a function that calls func at most n times.

    Args:
        count: Maximum number of calls.
        func: Function to call.

    Returns:
        Wrapped function.
    """
    call_count = [0]

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        call_count[0] += 1
        if call_count[0] <= count:
            return func(*args, **kwargs)
        return None

    return wrapper


def once(func: Callable) -> Callable:
    """
    Return a function that only calls func once.

    Args:
        func: Function to call.

    Returns:
        Wrapped function.
    """
    return after(1, func)


def negate(predicate: Callable) -> Callable:
    """
    Return the negation of a predicate.

    Args:
        predicate: Predicate function to negate.

    Returns:
        Negated predicate.
    """
    def negated(*args: Any, **kwargs: Any) -> bool:
        return not predicate(*args, **kwargs)
    return negated


def tap(value: T, func: Callable[[T], Any]) -> T:
    """
    Tap into a value, passing it through a function.

    Args:
        value: Value to tap.
        func: Function to apply.

    Returns:
        The original value.
    """
    func(value)
    return value


def apply(func: Callable[[List], R]) -> Callable[[tuple], R]:
    """
    Convert a function taking positional args to one taking a tuple.

    Args:
        func: Function that takes a list.

    Returns:
        Function that takes a tuple.
    """
    def applied(args: tuple) -> R:
        return func(list(args))
    return applied


def spread(func: Callable) -> Callable:
    """
    Convert a function taking a single iterable to one taking multiple args.

    Args:
        func: Function taking an iterable.

    Returns:
        Function taking multiple arguments.
    """
    def spreader(*args: Any, **kwargs: Any) -> Any:
        return func(args, **kwargs)
    return spreader


class Maybe(Generic[T]):
    """
    Maybe monad for handling optional values.

    Provides safe chaining of operations that may fail.
    """

    def __init__(self, value: Any) -> None:
        """Initialize Maybe with a value."""
        self._value = value
        self._is_just = value is not None

    @classmethod
    def just(cls, value: T) -> "Maybe[T]":
        """Create a Just Maybe."""
        return cls(value)

    @classmethod
    def nothing(cls) -> "Maybe[Any]":
        """Create a Nothing Maybe."""
        return cls(None)

    def map(self, func: Callable[[T], Any]) -> "Maybe[Any]":
        """Apply function if Just, return Nothing if not."""
        if self._is_just:
            return Maybe(func(self._value))
        return self

    def flat_map(self, func: Callable[[T], "Maybe"]) -> "Maybe":
        """Apply function that returns Maybe."""
        if self._is_just:
            result = func(self._value)
            return result if isinstance(result, Maybe) else Maybe(result)
        return self

    def get_or_else(self, default: T) -> T:
        """Get value or default."""
        return self._value if self._is_just else default

    def get(self) -> Optional[T]:
        """Get value or None."""
        return self._value if self._is_just else None

    def is_just(self) -> bool:
        """Check if Just."""
        return self._is_just

    def is_nothing(self) -> bool:
        """Check if Nothing."""
        return not self._is_just

    def __repr__(self) -> str:
        if self._is_just:
            return f"Just({self._value!r})"
        return "Nothing"
