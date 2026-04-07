"""reaper action module for rabai_autoclick.

Provides resource cleanup utilities: finalizers, garbage collection
helpers, context managers for resource cleanup, and cleanup schedulers.
"""

from __future__ import annotations

import atexit
import gc
import sys
import threading
import weakref
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Deque, Dict, List, Optional, Set

__all__ = [
    "Reaper",
    "Finalizer",
    "CleanupRegistry",
    "ResourceGuard",
    "finalize",
    "weakref_cache",
    "clear_weakref_cache",
    "gc_collect",
    "gc_stats",
    "gc_tune",
    "auto_cleanup",
    "scope",
    "ResourceType",
]


class ResourceType(Enum):
    """Types of resources that can be cleaned up."""
    FILE = auto()
    SOCKET = auto()
    THREAD = auto()
    PROCESS = auto()
    LOCK = auto()
    MEMORY = auto()
    CUSTOM = auto()


@dataclass
class Finalizer:
    """A finalizer that runs cleanup when object is garbage collected."""
    callback: Callable[[], None]
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    registered: bool = False

    def __del__(self) -> None:
        """Run finalizer when object is garbage collected."""
        try:
            self.callback(*self.args, **self.kwargs)
        except Exception:
            pass


class CleanupRegistry:
    """Global registry for cleanup functions."""

    def __init__(self) -> None:
        self._handlers: Dict[str, Callable] = {}
        self._lock = threading.Lock()
        self._atexit_registered = False

    def register(self, key: str, handler: Callable) -> None:
        """Register a cleanup handler.

        Args:
            key: Unique identifier for the handler.
            handler: Cleanup function to call.
        """
        with self._lock:
            self._handlers[key] = handler
            if not self._atexit_registered:
                atexit.register(self._run_all)
                self._atexit_registered = True

    def unregister(self, key: str) -> bool:
        """Unregister a cleanup handler.

        Returns:
            True if handler was found and removed.
        """
        with self._lock:
            if key in self._handlers:
                del self._handlers[key]
                return True
        return False

    def run(self, key: str) -> bool:
        """Run a specific cleanup handler.

        Returns:
            True if handler was found and executed.
        """
        with self._lock:
            handler = self._handlers.pop(key, None)
        if handler:
            try:
                handler()
                return True
            except Exception:
                pass
        return False

    def _run_all(self) -> None:
        """Run all cleanup handlers on shutdown."""
        with self._lock:
            handlers = dict(self._handlers)
            self._handlers.clear()
        for key, handler in handlers.items():
            try:
                handler()
            except Exception:
                pass


class Reaper:
    """Resource reaper that cleans up objects and handles."""

    def __init__(self, name: str = "reaper") -> None:
        self.name = name
        self._finalizers: List[Finalizer] = []
        self._weak_refs: Dict[int, Any] = {}
        self._lock = threading.Lock()
        self._closed = False

    def register_finalizer(
        self,
        callback: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Finalizer:
        """Register a finalizer callback.

        Args:
            callback: Function to call on cleanup.
            *args: Positional args for callback.
            **kwargs: Keyword args for callback.

        Returns:
            Finalizer instance.
        """
        finalizer = Finalizer(callback=callback, args=args, kwargs=kwargs)
        with self._lock:
            self._finalizers.append(finalizer)
        return finalizer

    def add_weakref(self, obj: Any, callback: Optional[Callable] = None) -> weakref.ref:
        """Add object as weak reference.

        Args:
            obj: Object to reference weakly.
            callback: Optional callback when object is GC'd.

        Returns:
            Weak reference to the object.
        """
        def on_delete(ref: weakref.ref) -> None:
            if callback:
                try:
                    callback()
                except Exception:
                    pass
            with self._lock:
                for k, v in list(self._weak_refs.items()):
                    if v() is None:
                        del self._weak_refs[k]

        ref = weakref.ref(obj, on_delete)
        with self._lock:
            self._weak_refs[id(obj)] = ref
        return ref

    def collect(self) -> int:
        """Run garbage collection and return objects collected."""
        before = len(gc.get_objects())
        gc.collect()
        after = len(gc.get_objects())
        return after - before

    def reap(self) -> int:
        """Run all finalizers and cleanup.

        Returns:
            Number of finalizers run.
        """
        if self._closed:
            return 0
        self._closed = True
        with self._lock:
            count = len(self._finalizers)
            for finalizer in self._finalizers:
                try:
                    finalizer.callback(*finalizer.args, **finalizer.kwargs)
                except Exception:
                    pass
            self._finalizers.clear()
            self._weak_refs.clear()
        return count

    def close(self) -> None:
        """Alias for reap()."""
        self.reap()

    def __enter__(self) -> "Reaper":
        return self

    def __exit__(self, *args: Any) -> None:
        self.reap()


class ResourceGuard:
    """Context manager for automatic resource cleanup."""

    def __init__(
        self,
        creator: Callable[[], Any],
        cleaner: Callable[[Any], None],
    ) -> None:
        self._creator = creator
        self._cleaner = cleaner
        self._resource: Optional[Any] = None

    def __enter__(self) -> Any:
        self._resource = self._creator()
        return self._resource

    def __exit__(self, *args: Any) -> None:
        if self._resource is not None:
            try:
                self._cleaner(self._resource)
            except Exception:
                pass
            self._resource = None


_global_registry = CleanupRegistry()


def finalize(callback: Callable, *args: Any, **kwargs: Any) -> Finalizer:
    """Register a finalizer using global registry.

    Args:
        callback: Function to call on cleanup.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Finalizer instance.
    """
    return Finalizer(callback=callback, args=args, kwargs=kwargs)


_weakref_cache: Dict[int, Any] = {}
_cache_lock = threading.Lock()


def weakref_cache(obj: Any) -> weakref.ref:
    """Add object to weakref cache.

    Returns weakref that persists as long as object is alive.
    """
    with _cache_lock:
        obj_id = id(obj)
        if obj_id in _weakref_cache:
            ref = _weakref_cache[obj_id]
            if ref() is not None:
                return ref
        ref = weakref.ref(obj)
        _weakref_cache[obj_id] = ref
        return ref


def clear_weakref_cache() -> int:
    """Clear dead references from cache.

    Returns:
        Number of entries removed.
    """
    with _cache_lock:
        before = len(_weakref_cache)
        _weakref_cache = {k: v for k, v in _weakref_cache.items() if v() is not None}
        return before - len(_weakref_cache)


def gc_collect() -> int:
    """Force garbage collection.

    Returns:
        Number of unreachable objects collected.
    """
    return gc.collect()


def gc_stats() -> Dict[str, Any]:
    """Get garbage collection statistics."""
    return {
        "collections": gc.get_count(),
        "garbage": len(gc.garbage),
        "callbacks": len(gc.callbacks),
        "thresholds": gc.get_threshold(),
    }


def gc_tune(
    generation: int = 2,
    threshold: Optional[int] = None,
) -> None:
    """Tune garbage collection thresholds.

    Args:
        generation: Which generation to tune (0, 1, or 2).
        threshold: New threshold value.
    """
    if threshold is not None:
        current = gc.get_threshold()
        new_thresholds = list(current)
        if 0 <= generation < 3:
            new_thresholds[generation] = threshold
        gc.set_threshold(*new_thresholds)


@contextmanager
def auto_cleanup():
    """Context manager that runs cleanup on exit."""
    cleaners: Deque[Callable] = deque()
    try:
        yield cleaners
    finally:
        while cleaners:
            try:
                cleaners.pop()()
            except Exception:
                pass


@contextmanager
def scope(**kwargs: Any):
    """Generic context manager for resource scoping.

    Usage:
        with scope(file=open("foo.txt")) as resources:
            resources.file.write("data")

    Exit automatically closes all registered resources.
    """
    resources: List[Any] = []
    finalizers: List[Callable] = []
    try:
        yield {"resources": resources, "finalizers": finalizers}
    finally:
        for cleanup in reversed(finalizers):
            try:
                cleanup()
            except Exception:
                pass
        for resource in reversed(resources):
            try:
                if hasattr(resource, "close"):
                    resource.close()
            except Exception:
                pass
