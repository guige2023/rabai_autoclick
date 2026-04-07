"""Functools utilities v2 - advanced functional programming.

Extended functools utilities including composition,
 monadic operations, and function trees.
"""

from __future__ import annotations

import functools
from functools import (
    lru_cache,
    cache,
    cached_property,
    partial,
    reduce,
    singledispatch,
    wraps,
    total_ordering,
)
from typing import Any, Callable, Generic, TypeVar, Awaitable

__all__ = [
    "compose",
    "pipe",
    "rcompose",
    "rpipe",
    "curry",
    "rcurry",
    "maybe",
    "Either",
    "Left",
    "Right",
    "Option",
    "Some",
    "Nothing",
    "monad_bind",
    "monad_map",
    "monad_flatmap",
    "lazy",
    "thunk",
    "memoize_async",
    "retry_async",
    "debounce_async",
    "rate_limit_async",
    "FunctionTree",
    "FunctionGraph",
    "PartialBuilder",
    "Pipeline",
]


T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
R = TypeVar("R")


def compose(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Compose functions right-to-left.

    Args:
        *funcs: Functions to compose.

    Returns:
        Composed function.

    Example:
        compose(f, g, h)(x) == f(g(h(x)))
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
        *funcs: Functions to pipe.

    Returns:
        Piped function.

    Example:
        pipe(f, g, h)(x) == h(g(f(x)))
    """
    return compose(*reversed(funcs))


def rcompose(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Right compose (alias for compose)."""
    return compose(*funcs)


def rpipe(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Right pipe (alias for pipe)."""
    return pipe(*funcs)


def curry(func: Callable[..., R], arity: int | None = None) -> Callable[..., R]:
    """Curry a function.

    Args:
        func: Function to curry.
        arity: Number of arguments.

    Returns:
        Curried function.
    """
    if arity is None:
        arity = func.__code__.co_argcount
    @wraps(func)
    def curried(*args: Any, **kwargs: Any) -> R:
        if len(args) >= arity:
            return func(*args[:arity], **kwargs)
        def next_curry(arg: Any) -> Callable[..., R]:
            return curry(func(*args, arg), arity - len(args) - 1)
        return next_curry
    return curried


def rcurry(func: Callable[..., R], arity: int | None = None) -> Callable[..., R]:
    """Right curry (collect args from right)."""
    if arity is None:
        arity = func.__code__.co_argcount
    @wraps(func)
    def curried(*args: Any, **kwargs: Any) -> R:
        if len(args) >= arity:
            return func(*args[-arity:], **kwargs)
        def next_curry(arg: Any) -> Callable[..., R]:
            return rcurry(func(arg, *args), arity - 1)
        return next_curry
    return curried


class Either(Generic[T, U]):
    """Either monad - Left or Right."""
    pass


class Left(Either[T, U]):
    """Left either (represents failure/error)."""

    def __init__(self, value: T) -> None:
        self._value = value

    @property
    def is_left(self) -> bool:
        return True

    @property
    def is_right(self) -> bool:
        return False

    def get(self) -> T:
        return self._value

    def map(self, fn: Callable[[U], Any]) -> Either[T, Any]:
        return self

    def flatmap(self, fn: Callable[[U], Either[T, Any]]) -> Either[T, Any]:
        return self


class Right(Either[T, U]):
    """Right either (represents success)."""

    def __init__(self, value: U) -> None:
        self._value = value

    @property
    def is_left(self) -> bool:
        return False

    @property
    def is_right(self) -> bool:
        return True

    def get(self) -> U:
        return self._value

    def map(self, fn: Callable[[U], Any]) -> Either[T, Any]:
        return Right(fn(self._value))

    def flatmap(self, fn: Callable[[U], Either[T, Any]]) -> Either[T, Any]:
        return fn(self._value)


class Option(Generic[T]):
    """Option monad - Some or Nothing."""
    pass


class Some(Option[T]):
    """Some option (has value)."""

    def __init__(self, value: T) -> None:
        self._value = value

    @property
    def is_some(self) -> bool:
        return True

    @property
    def is_nothing(self) -> bool:
        return False

    def get(self) -> T:
        return self._value

    def get_or_else(self, default: T) -> T:
        return self._value

    def map(self, fn: Callable[[T], Any]) -> Option[Any]:
        return Some(fn(self._value))

    def flatmap(self, fn: Callable[[T], Option[Any]]) -> Option[Any]:
        return fn(self._value)

    def filter(self, pred: Callable[[T], bool]) -> Option[T]:
        if pred(self._value):
            return self
        return Nothing()


class Nothing(Option[T]):
    """Nothing option (no value)."""

    @property
    def is_some(self) -> bool:
        return False

    @property
    def is_nothing(self) -> bool:
        return True

    def get(self) -> T:
        raise ValueError("Nothing has no value")

    def get_or_else(self, default: T) -> T:
        return default

    def map(self, fn: Callable[[T], Any]) -> Option[Any]:
        return self

    def flatmap(self, fn: Callable[[T], Option[Any]]) -> Option[Any]:
        return self

    def filter(self, pred: Callable[[T], bool]) -> Option[T]:
        return self


def maybe(value: T | None) -> Option[T]:
    """Create Option from value.

    Args:
        value: Value or None.

    Returns:
        Some(value) or Nothing.
    """
    if value is None:
        return Nothing()
    return Some(value)


def monad_bind(m: Option[T], fn: Callable[[T], Option[Any]]) -> Option[Any]:
    """Bind monadic operation.

    Args:
        m: Monad.
        fn: Function to apply.

    Returns:
        Result monad.
    """
    return m.flatmap(fn)


def monad_map(m: Option[T], fn: Callable[[T], Any]) -> Option[Any]:
    """Map monadic operation.

    Args:
        m: Monad.
        fn: Function to apply.

    Returns:
        Result monad.
    """
    return m.map(fn)


def monad_flatmap(m: Option[T], fn: Callable[[T], Option[Any]]) -> Option[Any]:
    """FlatMap monadic operation."""
    return m.flatmap(fn)


def lazy(func: Callable[..., T]) -> Callable[..., T]:
    """Create lazy evaluator.

    Args:
        func: Function to make lazy.

    Returns:
        Lazy wrapper.
    """
    cache: dict[None, T] = {}
    sentinel = object()
    def lazy_eval(*args: Any, **kwargs: Any) -> T:
        if not cache:
            cache[None] = func(*args, **kwargs)
        return cache[None]
    return lazy_eval


def thunk(func: Callable[..., T]) -> Callable[[], T]:
    """Create thunk (delayed evaluation).

    Args:
        func: Function to delay.

    Returns:
        Thunk returning result.
    """
    @wraps(func)
    def thunk_fn(*args: Any, **kwargs: Any) -> T:
        return func(*args, **kwargs)
    return thunk_fn


async def memoize_async(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Memoize async function.

    Args:
        func: Async function to memoize.

    Returns:
        Memoized async function.
    """
    cache: dict[Any, T] = {}
    @wraps(func)
    async def memoized(*args: Any, **kwargs: Any) -> T:
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = await func(*args, **kwargs)
        return cache[key]
    return memoized


async def retry_async(func: Callable[..., Awaitable[T]], max_retries: int = 3, delay: float = 1.0) -> Callable[..., Awaitable[T]]:
    """Retry async function on failure.

    Args:
        func: Async function.
        max_retries: Max retry count.
        delay: Delay between retries.

    Returns:
        Wrapped function.
    """
    @wraps(func)
    async def retrying(*args: Any, **kwargs: Any) -> T:
        import asyncio
        last_error: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    await asyncio.sleep(delay * (2 ** attempt))
        if last_error:
            raise last_error
        raise RuntimeError("retry_async failed")
    return retrying


async def debounce_async(func: Callable[..., Awaitable[T]], wait: float) -> Callable[..., Awaitable[T]]:
    """Debounce async function.

    Args:
        func: Async function.
        wait: Debounce window in seconds.

    Returns:
        Debounced function.
    """
    import asyncio
    task: asyncio.Task | None = None
    @wraps(func)
    async def debounced(*args: Any, **kwargs: Any) -> T:
        nonlocal task
        if task:
            task.cancel()
        async def run():
            await asyncio.sleep(wait)
            return await func(*args, **kwargs)
        task = asyncio.create_task(run())
        return await task
    return debounced


async def rate_limit_async(func: Callable[..., Awaitable[T]], rate: float, burst: int = 1) -> Callable[..., Awaitable[T]]:
    """Rate limit async function.

    Args:
        func: Async function.
        rate: Calls per second.
        burst: Burst allowance.

    Returns:
        Rate limited function.
    """
    import asyncio
    sem = asyncio.Semaphore(burst)
    min_interval = 1.0 / rate
    last_call = 0.0
    @wraps(func)
    async def limited(*args: Any, **kwargs: Any) -> T:
        nonlocal last_call
        async with sem:
            import asyncio
            now = asyncio.get_event_loop().time()
            elapsed = now - last_call
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            last_call = asyncio.get_event_loop().time()
            return await func(*args, **kwargs)
    return limited


class FunctionTree(Generic[T]):
    """Tree of composed functions."""

    def __init__(self, func: Callable[..., T] | None = None, left: FunctionTree | None = None, right: FunctionTree | None = None) -> None:
        self._func = func
        self._left = left
        self._right = right

    def set_left(self, tree: FunctionTree) -> FunctionTree:
        """Set left subtree."""
        self._left = tree
        return self

    def set_right(self, tree: FunctionTree) -> FunctionTree:
        """Set right subtree."""
        self._right = tree
        return self

    def evaluate(self, input: Any) -> T:
        """Evaluate function tree."""
        if self._func:
            return self._func(input)
        if self._left and self._right:
            return self._right.evaluate(self._left.evaluate(input))
        if self._left:
            return self._left.evaluate(input)
        if self._right:
            return self._right.evaluate(input)
        return input


class FunctionGraph(Generic[T]):
    """Graph of functions with dependencies."""

    def __init__(self) -> None:
        self._nodes: dict[str, Callable] = {}
        self._edges: dict[str, list[str]] = {}

    def add_node(self, name: str, func: Callable[..., T]) -> None:
        """Add function node.

        Args:
            name: Node name.
            func: Function.
        """
        self._nodes[name] = func
        if name not in self._edges:
            self._edges[name] = []

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add dependency edge.

        Args:
            from_node: Source node.
            to_node: Target node.
        """
        if from_node not in self._nodes:
            raise KeyError(f"Node {from_node} not found")
        if to_node not in self._nodes:
            raise KeyError(f"Node {to_node} not found")
        self._edges[from_node].append(to_node)

    def evaluate(self, name: str, input: Any) -> T:
        """Evaluate node with dependencies."""
        return self._nodes[name](input)


class PartialBuilder(Generic[T]):
    """Builder for partial applications."""

    def __init__(self, func: Callable[..., T]) -> None:
        self._func = func
        self._args: list[Any] = []
        self._kwargs: dict[str, Any] = {}

    def arg(self, *args: Any) -> PartialBuilder[T]:
        """Add positional argument."""
        self._args.extend(args)
        return self

    def kwarg(self, **kwargs: Any) -> PartialBuilder[T]:
        """Add keyword argument."""
        self._kwargs.update(kwargs)
        return self

    def build(self) -> Callable[..., T]:
        """Build partial function."""
        return partial(self._func, *self._args, **self._kwargs)


class Pipeline(Generic[T, R]):
    """Function pipeline builder."""

    def __init__(self, initial: Callable[[T], Any] | None = None) -> None:
        self._steps: list[Callable[[Any], Any]] = []
        if initial:
            self._steps.append(initial)

    def then(self, func: Callable[[Any], Any]) -> Pipeline[T, R]:
        """Add step to pipeline.

        Args:
            func: Step function.

        Returns:
            Self.
        """
        self._steps.append(func)
        return self

    def execute(self, input: T) -> R:
        """Execute pipeline.

        Args:
            input: Input value.

        Returns:
            Final result.
        """
        result = input
        for step in self._steps:
            result = step(result)
        return result

    def __call__(self, input: T) -> R:
        return self.execute(input)
