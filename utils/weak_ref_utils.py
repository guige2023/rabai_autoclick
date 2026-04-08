"""Weak reference utilities.

Provides weak reference and weak dictionary/collection
implementations for memory-efficient caching and callbacks.
"""

import gc
import weakref
from typing import Any, Callable, Dict, Iterator, Optional, Set


class WeakCallback:
    """Weak reference to a callable that auto-cleans when target is GC'd.

    Example:
        def on_close():
            print("closed")

        callback = WeakCallback(on_close)
        callback()  # calls on_close
        del on_close
        gc.collect()
        callback()  # no-op after target is GC'd
    """

    def __init__(self, func: Callable[..., Any]) -> None:
        self._ref = weakref.ref(func)
        self._alive = True

        def finalizer(wr: weakref.ref) -> None:
            self._alive = False

        self._finalizer_ref = weakref.finalize(func, finalizer, self._ref)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not self._alive:
            return None
        func = self._ref()
        if func is not None:
            return func(*args, **kwargs)
        self._alive = False
        return None

    @property
    def is_alive(self) -> bool:
        """Check if target callable is still alive."""
        return self._alive and self._ref() is not None


class WeakValueDict(Dict):
    """Dictionary that stores weak references to values.

    When values are garbage collected, they are automatically removed.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._wrapping: Dict[str, weakref.ref] = {}
        self._data: Dict[str, Any] = {}

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value
        self._wrapping[key] = weakref.ref(value, self._on_value_deleted)
        super().__setitem__(key, None)

    def _on_value_deleted(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
        if key in super():
            del super()[key]
        if key in self._wrapping:
            del self._wrapping[key]

    def __getitem__(self, key: str) -> Any:
        if key in self._wrapping:
            ref = self._wrapping[key]
            value = ref()
            if value is not None:
                return value
            del self._wrapping[key]
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: object) -> bool:
        if key in self._wrapping:
            return self._wrapping[key]() is not None
        return key in self._data

    def __delitem__(self, key: str) -> None:
        if key in self._data:
            del self._data[key]
        if key in self._wrapping:
            del self._wrapping[key]
        if key in super():
            del super()[key]

    def __iter__(self) -> Iterator[str]:
        to_remove: Set[str] = set()
        for key in self._data:
            if key in self._wrapping and self._wrapping[key]() is None:
                to_remove.add(key)
            else:
                yield key
        for key in to_remove:
            del self._data[key]
            if key in self._wrapping:
                del self._wrapping[key]

    def __len__(self) -> int:
        return len([k for k in self._data if k in self._wrapping and self._wrapping[k]() is not None])


class WeakSet(set):
    """Set that holds weak references to objects.

    Objects are automatically removed when garbage collected.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._refs: Dict[Any, weakref.ref] = {}

    def add(self, item: Any) -> None:
        ref = weakref.ref(item, self._on_item_deleted)
        self._refs[item] = ref
        super().add(item)

    def _on_item_deleted(self, item: Any) -> None:
        if item in self._refs:
            del self._refs[item]
        super().discard(item)

    def remove(self, item: Any) -> None:
        if item in self._refs:
            del self._refs[item]
        super().remove(item)

    def discard(self, item: Any) -> None:
        if item in self._refs:
            del self._refs[item]
        super().discard(item)

    def __contains__(self, item: Any) -> bool:
        if item in self._refs:
            return self._refs[item]() is not None
        return super().__contains__(item)

    def __iter__(self) -> Iterator[Any]:
        to_remove: Set[Any] = set()
        for item in super().__iter__():
            if item in self._refs and self._refs[item]() is None:
                to_remove.add(item)
            else:
                yield item
        for item in to_remove:
            if item in self._refs:
                del self._refs[item]
            super().discard(item)


class ObserverSet:
    """Set that notifies when items are added or removed.

    Useful for managing listeners or callbacks.
    """

    def __init__(self) -> None:
        self._items: Set[Any] = set()
        self._on_add: Optional[Callable[[Any], None]] = None
        self._on_remove: Optional[Callable[[Any], None]] = None

    def set_handlers(
        self,
        on_add: Optional[Callable[[Any], None]] = None,
        on_remove: Optional[Callable[[Any], None]] = None,
    ) -> None:
        """Set callbacks for add/remove operations."""
        self._on_add = on_add
        self._on_remove = on_remove

    def add(self, item: Any) -> bool:
        """Add item and return True if new."""
        if item not in self._items:
            self._items.add(item)
            if self._on_add:
                self._on_add(item)
            return True
        return False

    def remove(self, item: Any) -> bool:
        """Remove item and return True if existed."""
        if item in self._items:
            self._items.remove(item)
            if self._on_remove:
                self._on_remove(item)
            return True
        return False

    def __contains__(self, item: Any) -> bool:
        return item in self._items

    def __iter__(self) -> Iterator[Any]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)
