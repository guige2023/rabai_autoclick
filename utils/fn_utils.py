"""Functional utilities for RabAI AutoClick.

Provides:
- Function composition
- Currying and partial application
- Higher-order function helpers
- Monadic operations
"""

from typing import Any, Callable, List, Optional, TypeVar, Generic, Dict


T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
R = TypeVar("R")


def compose(*funcs: Callable) -> Callable:
    """Compose functions right to left.

    Args:
        *funcs: Functions to compose.

    Returns:
        Composed function.
    """
    if not funcs:
        return lambda x: x

    def composed(x):
        result = x
        for func in reversed(funcs):
            result = func(result)
        return result

    return composed


def pipe(*funcs: Callable) -> Callable:
    """Pipe functions left to right.

    Args:
        *funcs: Functions to pipe.

    Returns:
        Piped function.
    """
    if not funcs:
        return lambda x: x

    def piped(x):
        result = x
        for func in funcs:
            result = func(result)
        return result

    return piped


def curry(func: Callable, arity: int = None) -> Callable:
    """Curry a function.

    Args:
        func: Function to curry.
        arity: Number of arguments (defaults to function signature).

    Returns:
        Curried function.
    """
    if arity is None:
        import inspect
        sig = inspect.signature(func)
        arity = len(sig.parameters)

    def curried(*args, **kwargs):
        if len(args) >= arity:
            return func(*args[:arity], **kwargs)
        return curry(lambda *a, **kw: func(*(args + a), **kw), arity - len(args))

    return curried


def partial(func: Callable, *args, **kwargs) -> Callable:
    """Create partial application.

    Args:
        func: Function to partially apply.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Partially applied function.
    """
    def partial_func(*remaining_args, **remaining_kwargs):
        return func(*args, *remaining_args, **kwargs, **remaining_kwargs)
    return partial_func


def memoize(func: Callable) -> Callable:
    """Memoize a function.

    Args:
        func: Function to memoize.

    Returns:
        Memoized function.
    """
    cache: Dict[Any, Any] = {}

    def memoized(*args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        if key not in cache:
            cache[key] = func(*args, **kwargs)
        return cache[key]

    memoized.cache = cache
    memoized.cache_clear = lambda: cache.clear()
    return memoized


def once(func: Callable) -> Callable:
    """Ensure function is called only once.

    Args:
        func: Function to wrap.

    Returns:
        Wrapper function.
    """
    called = False
    result = None

    def wrapper(*args, **kwargs):
        nonlocal called, result
        if not called:
            called = True
            result = func(*args, **kwargs)
        return result

    wrapper.called = called
    return wrapper


def flip(func: Callable) -> Callable:
    """Flip argument order.

    Args:
        func: Function to flip.

    Returns:
        Flipped function.
    """
    def flipped(*args, **kwargs):
        return func(*reversed(args), **kwargs)
    return flipped


def juxt(*funcs: Callable) -> Callable:
    """Juxtapose functions - return list of results.

    Args:
        *funcs: Functions to apply.

    Returns:
        Function returning list of results.
    """
    def juxtaposed(x):
        return [f(x) for f in funcs]
    return juxtaposed


def apply(func: Callable) -> Callable:
    """Apply function to argument.

    Args:
        func: Function to apply.

    Returns:
        Applied function.
    """
    return func()


def identity(x: T) -> T:
    """Identity function.

    Args:
        x: Value to return.

    Returns:
        Same value.
    """
    return x


def constant(x: T) -> Callable[[Any], T]:
    """Create constant function.

    Args:
        x: Value to return.

    Returns:
        Constant function.
    """
    return lambda _: x


def thunk(func: Callable, *args, **kwargs) -> Callable:
    """Create lazy evaluation thunk.

    Args:
        func: Function to defer.
        *args: Arguments.
        **kwargs: Keyword arguments.

    Returns:
        Thunk function.
    """
    result = []
    called = [False]

    def thunk_func():
        if not called[0]:
            called[0] = True
            result.append(func(*args, **kwargs))
        return result[0]

    return thunk_func


def lift(func: Callable[[T], R]) -> Callable[[Optional[T]], Optional[R]]:
    """Lift function to work with Optional values.

    Args:
        func: Function to lift.

    Returns:
        Lifted function.
    """
    def lifted(x: Optional[T]) -> Optional[R]:
        if x is None:
            return None
        return func(x)
    return lifted


def lift2(func: Callable[[T, U], R]) -> Callable[[Optional[T], Optional[U]], Optional[R]]:
    """Lift binary function to work with Optional values.

    Args:
        func: Function to lift.

    Returns:
        Lifted function.
    """
    def lifted(x: Optional[T], y: Optional[U]) -> Optional[R]:
        if x is None or y is None:
            return None
        return func(x, y)
    return lifted


def try_catch(func: Callable, *catch_handlers: Callable) -> Callable:
    """Try-catch wrapper.

    Args:
        func: Function to wrap.
        *catch_handlers: Exception handlers.

    Returns:
        Wrapped function.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            for handler in catch_handlers:
                result = handler(e)
                if result is not None:
                    return result
            raise
    return wrapper


def tap(func: Callable[[T], Any]) -> Callable[[T], T]:
    """Tap into a pipeline - apply function for side effects.

    Args:
        func: Side-effect function.

    Returns:
        Function that returns input unchanged.
    """
    def tapped(x: T) -> T:
        func(x)
        return x
    return tapped


def property_(func: Callable[[T], Any]) -> property:
    """Convert getter function to property.

    Args:
        func: Getter function.

    Returns:
        Property object.
    """
    return property(func)


def set_property(func: Callable[[T, Any], None]) -> property:
    """Convert setter function to property.

    Args:
        func: Setter function.

    Returns:
        Property object.
    """
    return property(None, func)


def getter(func: Callable[[T], Any]) -> Callable[[T], Any]:
    """Mark function as getter.

    Args:
        func: Getter function.

    Returns:
        Same function.
    """
    return func


def setter(func: Callable[[T, Any], None]) -> Callable[[T, Any], None]:
    """Mark function as setter.

    Args:
        func: Setter function.

    Returns:
        Same function.
    """
    return func


def swap(func: Callable[[T, U], R]) -> Callable[[U, T], R]:
    """Swap function arguments.

    Args:
        func: Function to swap.

    Returns:
        Swapped function.
    """
    return lambda x, y: func(y, x)


def unfold(func: Callable[[T], Optional[tuple]] ) -> Callable[[T], List]:
    """Unfold a value using a generator function.

    Args:
        func: Function that returns (value, state) or None.

    Returns:
        Unfolded list.
    """
    def unfolded(start):
        result = []
        state = start
        while True:
            try:
                item, state = func(state)
                result.append(item)
            except (TypeError, ValueError):
                break
        return result
    return unfolded


def iterate(func: Callable[[T], T], count: int) -> Callable[[T], List[T]]:
    """Iterate function n times.

    Args:
        func: Function to iterate.
        count: Number of iterations.

    Returns:
        Function that returns list of iterations.
    """
    def iterated(start):
        result = [start]
        current = start
        for _ in range(count):
            current = func(current)
            result.append(current)
        return result
    return iterated


def always() -> Callable[..., bool]:
    """Always return True.

    Returns:
        Always True function.
    """
    return lambda *args, **kwargs: True


def never() -> Callable[..., bool]:
    """Always return False.

    Returns:
        Always False function.
    """
    return lambda *args, **kwargs: False


def getter_attr(attr: str) -> Callable[[Any], Any]:
    """Create attribute getter.

    Args:
        attr: Attribute name.

    Returns:
        Getter function.
    """
    return lambda obj: getattr(obj, attr)


def setter_attr(attr: str, value: Any) -> Callable[[Any], None]:
    """Create attribute setter.

    Args:
        attr: Attribute name.
        value: Value to set.

    Returns:
        Setter function.
    """
    return lambda obj: setattr(obj, attr, value)


def method_caller(method: str, *args, **kwargs) -> Callable[[Any], Any]:
    """Create method caller.

    Args:
        method: Method name.
        *args: Arguments.
        **kwargs: Keyword arguments.

    Returns:
        Caller function.
    """
    return lambda obj: getattr(obj, method)(*args, **kwargs)


def accessor(item: int) -> Callable[[List], Any]:
    """Create list/item accessor.

    Args:
        item: Item index.

    Returns:
        Accessor function.
    """
    return lambda seq: seq[item]


def itemgetter(key: Any) -> Callable[[Dict], Any]:
    """Create dict item getter.

    Args:
        key: Dictionary key.

    Returns:
        Item getter function.
    """
    return lambda d: d[key]


def truthy() -> Callable[[Any], bool]:
    """Check if value is truthy.

    Returns:
        Truthy check function.
    """
    return bool


def falsy() -> Callable[[Any], bool]:
    """Check if value is falsy.

    Returns:
        Falsy check function.
    """
    return lambda x: not bool(x)


def is_none() -> Callable[[Any], bool]:
    """Check if value is None.

    Returns:
        None check function.
    """
    return lambda x: x is None


def is_not_none() -> Callable[[Any], bool]:
    """Check if value is not None.

    Returns:
        Not None check function.
    """
    return lambda x: x is not None