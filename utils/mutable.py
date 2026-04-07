"""Mutable values and references for Python.

Provides:
- Mutable primitives
- By-reference values
"""

from typing import Any, Callable, Generic, TypeVar


T = TypeVar("T")


class Mutable(Generic[T]):
    """Mutable container for value types.

    Allows passing values by reference where needed.

    Usage:
        x = Mutable(5)
        x.value = 10
        modify(x)  # x.value is now 10
    """

    def __init__(self, value: T) -> None:
        """Initialize mutable.

        Args:
            value: Initial value.
        """
        self._value = value

    @property
    def value(self) -> T:
        """Get current value."""
        return self._value

    @value.setter
    def value(self, new_value: T) -> None:
        """Set new value."""
        self._value = new_value

    def get(self) -> T:
        """Get current value."""
        return self._value

    def set(self, value: T) -> None:
        """Set new value."""
        self._value = value

    def mutate(self, func: Callable[[T], T]) -> None:
        """Mutate value with function.

        Args:
            func: Function to apply to current value.
        """
        self._value = func(self._value)

    def __repr__(self) -> str:
        return f"Mutable({self._value!r})"

    def __str__(self) -> str:
        return str(self._value)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Mutable):
            return self._value == other._value
        return self._value == other

    def __hash__(self) -> int:
        return hash(self._value)


class MutableList(Generic[T]):
    """Mutable list that notifies on changes."""

    def __init__(self, initial: list = None) -> None:
        """Initialize mutable list.

        Args:
            initial: Initial list items.
        """
        self._items = list(initial) if initial else []
        self._on_change: list = []

    def append(self, item: T) -> None:
        """Append item."""
        self._items.append(item)
        self._notify()

    def remove(self, item: T) -> None:
        """Remove item."""
        self._items.remove(item)
        self._notify()

    def on_change(self, callback: Callable) -> None:
        """Register change callback."""
        self._on_change.append(callback)

    def _notify(self) -> None:
        """Notify all callbacks."""
        for cb in self._on_change:
            cb(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> T:
        return self._items[index]

    def __iter__(self):
        return iter(self._items)


class MutableDict(dict):
    """Dictionary that notifies on changes."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._on_change: list = []

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(key, value)
        self._notify()

    def __delitem__(self, key: Any) -> None:
        super().__delitem__(key)
        self._notify()

    def update(self, *args, **kwargs) -> None:
        super().update(*args, **kwargs)
        self._notify()

    def on_change(self, callback: Callable) -> None:
        """Register change callback."""
        self._on_change.append(callback)

    def _notify(self) -> None:
        """Notify all callbacks."""
        for cb in self._on_change:
            cb(dict(self))


class Lazy(Generic[T]):
    """Lazy value that is computed on first access.

    Usage:
        expensive = Lazy(lambda: compute_something())
        # ... later ...
        result = expensive.value  # Computation happens here
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        """Initialize lazy value.

        Args:
            factory: Function to compute value.
        """
        self._factory = factory
        self._value: T = None
        self._computed = False

    @property
    def value(self) -> T:
        """Get value, computing if needed."""
        if not self._computed:
            self._value = self._factory()
            self._computed = True
        return self._value

    def reset(self) -> None:
        """Reset cached value."""
        self._computed = False
        self._value = None


class Ref(Generic[T]):
    """Reference to a value.

    Similar to Mutable but operations are immutable-style.
    """

    def __init__(self, value: T) -> None:
        """Initialize reference.

        Args:
            value: Initial value.
        """
        self._value = value

    @property
    def get(self) -> T:
        """Get value."""
        return self._value

    def map(self, func: Callable[[T], T]) -> 'Ref[T]':
        """Map function over value.

        Args:
            func: Function to apply.

        Returns:
            New Ref with mapped value.
        """
        return Ref(func(self._value))

    def flat_map(self, func: Callable[[T], 'Ref']) -> 'Ref':
        """FlatMap operation.

        Args:
            func: Function returning Ref.

        Returns:
            New Ref from flatMap.
        """
        return func(self._value)