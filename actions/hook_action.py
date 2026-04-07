"""hook_action module for rabai_autoclick.

Provides hook/callback utilities: event hooks, lifecycle callbacks,
plugin system with hook registration, and signal handling.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = [
    "Hook",
    "HookRegistry",
    "LifecycleHook",
    "Signal",
    "HookManager",
    "register_hook",
    "unregister_hook",
    "emit_signal",
    "HookError",
]


class HookError(Exception):
    """Raised when hook operations fail."""
    pass


@dataclass
class Hook:
    """A single hook/callback."""
    name: str
    callback: Callable
    priority: int = 0
    once: bool = False
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._called = False

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the hook callback."""
        if self.once and self._called:
            return None
        self._called = True
        return self.callback(*args, **kwargs)


class HookRegistry:
    """Registry for managing hooks."""

    def __init__(self) -> None:
        self._hooks: Dict[str, List[Hook]] = defaultdict(list)
        self._lock = threading.RLock()

    def register(
        self,
        event: str,
        callback: Callable,
        priority: int = 0,
        once: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Hook:
        """Register a hook for an event.

        Args:
            event: Event name.
            callback: Function to call.
            priority: Execution priority (higher first).
            once: Execute only once.
            metadata: Optional metadata.

        Returns:
            Created Hook object.
        """
        hook = Hook(
            name=event,
            callback=callback,
            priority=priority,
            once=once,
            metadata=metadata or {},
        )
        with self._lock:
            self._hooks[event].append(hook)
            self._hooks[event].sort(key=lambda h: h.priority, reverse=True)
        return hook

    def unregister(self, event: str, hook: Hook) -> bool:
        """Unregister a hook.

        Args:
            event: Event name.
            hook: Hook to remove.

        Returns:
            True if removed.
        """
        with self._lock:
            if event in self._hooks:
                try:
                    self._hooks[event].remove(hook)
                    return True
                except ValueError:
                    pass
        return False

    def emit(self, event: str, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit event and call all registered hooks.

        Args:
            event: Event name.
            *args: Positional args for hooks.
            **kwargs: Keyword args for hooks.

        Returns:
            List of results from hooks.
        """
        with self._lock:
            hooks = list(self._hooks.get(event, []))

        results = []
        for hook in hooks:
            if not hook.enabled:
                continue
            try:
                result = hook(*args, **kwargs)
                results.append(result)
            except Exception as e:
                results.append(e)
        return results

    def has_hooks(self, event: str) -> bool:
        """Check if event has registered hooks."""
        with self._lock:
            return len(self._hooks.get(event, [])) > 0

    def clear(self, event: Optional[str] = None) -> None:
        """Clear hooks for event or all events.

        Args:
            event: Event to clear (all if None).
        """
        with self._lock:
            if event is None:
                self._hooks.clear()
            elif event in self._hooks:
                del self._hooks[event]

    def get_hooks(self, event: str) -> List[Hook]:
        """Get all hooks for an event."""
        with self._lock:
            return list(self._hooks.get(event, []))


class LifecycleHook:
    """Lifecycle hook manager for application phases.

    Supports: init, start, stop, cleanup phases.
    """

    def __init__(self) -> None:
        self._registry = HookRegistry()
        self._started = False
        self._stopped = False
        self._lock = threading.Lock()

    def on_init(self, callback: Callable, **kwargs: Any) -> Hook:
        """Register init hook."""
        return self._registry.register("lifecycle.init", callback, **kwargs)

    def on_start(self, callback: Callable, **kwargs: Any) -> Hook:
        """Register start hook."""
        return self._registry.register("lifecycle.start", callback, **kwargs)

    def on_stop(self, callback: Callable, **kwargs: Any) -> Hook:
        """Register stop hook."""
        return self._registry.register("lifecycle.stop", callback, **kwargs)

    def on_cleanup(self, callback: Callable, **kwargs: Any) -> Hook:
        """Register cleanup hook."""
        return self._registry.register("lifecycle.cleanup", callback, **kwargs)

    def emit_init(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit init event."""
        return self._registry.emit("lifecycle.init", *args, **kwargs)

    def emit_start(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit start event."""
        with self._lock:
            if self._started:
                raise HookError("Lifecycle already started")
            self._started = True
        return self._registry.emit("lifecycle.start", *args, **kwargs)

    def emit_stop(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit stop event."""
        with self._lock:
            if self._stopped:
                return []
            self._stopped = True
        return self._registry.emit("lifecycle.stop", *args, **kwargs)

    def emit_cleanup(self, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit cleanup event."""
        return self._registry.emit("lifecycle.cleanup", *args, **kwargs)


class Signal:
    """Signal/slot pattern implementation."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._slots: List[Callable] = []
        self._lock = threading.Lock()

    def connect(self, slot: Callable) -> None:
        """Connect a slot to the signal."""
        with self._lock:
            if slot not in self._slots:
                self._slots.append(slot)

    def disconnect(self, slot: Callable) -> bool:
        """Disconnect a slot from the signal."""
        with self._lock:
            try:
                self._slots.remove(slot)
                return True
            except ValueError:
                return False

    def emit(self, *args: Any, **kwargs: Any) -> None:
        """Emit signal to all connected slots."""
        with self._lock:
            slots = list(self._slots)
        for slot in slots:
            try:
                slot(*args, **kwargs)
            except Exception:
                pass

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        """Emit signal."""
        self.emit(*args, **kwargs)

    def clear(self) -> None:
        """Disconnect all slots."""
        with self._lock:
            self._slots.clear()


class HookManager:
    """Global hook manager for application-wide hooks."""

    _instance: Optional["HookManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "HookManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._registry = HookRegistry()
        self._lifecycle = LifecycleHook()
        self._signals: Dict[str, Signal] = {}
        self._initialized = True

    def hook(self, event: str) -> Callable:
        """Decorator to register a hook.

        Args:
            event: Event name.

        Returns:
            Decorator function.
        """
        def decorator(func: Callable) -> Callable:
            self._registry.register(event, func)
            return func
        return decorator

    def signal(self, name: str) -> Signal:
        """Get or create a signal.

        Args:
            name: Signal name.

        Returns:
            Signal object.
        """
        with self._lock:
            if name not in self._signals:
                self._signals[name] = Signal(name)
            return self._signals[name]

    def emit(self, event: str, *args: Any, **kwargs: Any) -> List[Any]:
        """Emit event."""
        return self._registry.emit(event, *args, **kwargs)

    @property
    def lifecycle(self) -> LifecycleHook:
        """Get lifecycle hook manager."""
        return self._lifecycle


_global_manager: Optional[HookManager] = None


def get_hook_manager() -> HookManager:
    """Get global hook manager instance."""
    global _global_manager
    if _global_manager is None:
        _global_manager = HookManager()
    return _global_manager


def register_hook(event: str, callback: Callable, **kwargs: Any) -> Hook:
    """Register a global hook."""
    return get_hook_manager()._registry.register(event, callback, **kwargs)


def unregister_hook(event: str, hook: Hook) -> bool:
    """Unregister a global hook."""
    return get_hook_manager()._registry.unregister(event, hook)


def emit_signal(signal_name: str, *args: Any, **kwargs: Any) -> None:
    """Emit a global signal."""
    signal = get_hook_manager().signal(signal_name)
    signal.emit(*args, **kwargs)
