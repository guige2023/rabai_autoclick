"""Hook system utilities for extensible automation plugins.

Provides a hook/callback registration system that allows
plugins to inject behavior at specific points in automation
workflows without tight coupling.

Example:
    >>> from utils.hook_utils import HookManager, hook
    >>> manager = HookManager()
    >>> @manager.register("before_click")
    ... def log_click(event):
    ...     print(f"Click at {event['coords']}")
    >>> manager.trigger("before_click", {"coords": (100, 200)})
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum, auto


class HookPhase(Enum):
    """Phases of hook execution."""
    BEFORE = auto()
    AROUND = auto()
    INSTEAD = auto()
    AFTER = auto()


class HookManager:
    """Centralized hook registration and trigger system.

    Hooks are organized by name and support:
    - Multiple handlers per hook
    - Phase ordering (before/around/instead/after)
    - Synchronous and asynchronous execution
    - Handler priority

    Example:
        >>> mgr = HookManager()
        >>> mgr.register("action:start", pre_handler, phase=HookPhase.BEFORE)
        >>> mgr.register("action:start", main_handler, phase=HookPhase.INSTEAD)
        >>> mgr.register("action:start", post_handler, phase=HookPhase.AFTER)
    """

    def __init__(self) -> None:
        self._hooks: Dict[
            str,
            Dict[HookPhase, List[Tuple[int, Callable[..., Any]]]]
        ] = {}
        self._lock = threading.RLock()
        self._enabled = True

    def register(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        phase: HookPhase = HookPhase.BEFORE,
        priority: int = 0,
    ) -> Callable[[], None]:
        """Register a hook handler.

        Args:
            name: Hook name.
            handler: Callback function.
            phase: Execution phase.
            priority: Higher priority runs first within phase.

        Returns:
            Unregister function.
        """
        with self._lock:
            if name not in self._hooks:
                self._hooks[name] = {phase: [] for phase in HookPhase}
            if phase not in self._hooks[name]:
                self._hooks[name][phase] = []
            self._hooks[name][phase].append((priority, handler))
            self._hooks[name][phase].sort(key=lambda x: -x[0])

        def unregister() -> None:
            self.unregister(name, handler, phase=phase)

        return unregister

    def unregister(
        self,
        name: str,
        handler: Callable[..., Any],
        *,
        phase: Optional[HookPhase] = None,
    ) -> bool:
        """Unregister a hook handler.

        Args:
            name: Hook name.
            handler: Handler to remove.
            phase: If provided, only remove from this phase.

        Returns:
            True if found and removed.
        """
        with self._lock:
            if name not in self._hooks:
                return False

            removed = False
            phases = [phase] if phase else list(HookPhase)
            for p in phases:
                if p in self._hooks[name]:
                    self._hooks[name][p] = [
                        (pri, h) for pri, h in self._hooks[name][p]
                        if h != handler
                    ]
                    if self._hooks[name][p]:
                        removed = True

            return removed

    def trigger(
        self,
        name: str,
        event: Optional[Dict[str, Any]] = None,
        *,
        phases: Optional[List[HookPhase]] = None,
    ) -> List[Any]:
        """Trigger all handlers for a hook.

        Args:
            name: Hook name.
            event: Event data passed to handlers.
            phases: Specific phases to trigger (default all).

        Returns:
            List of handler results.
        """
        if not self._enabled:
            return []

        event = event or {}
        event["_hook"] = name
        results: List[Any] = []

        with self._lock:
            if name not in self._hooks:
                return []
            hook_data = dict(self._hooks[name])
            phases = phases or list(HookPhase)

        for phase in phases:
            if phase not in hook_data:
                continue
            handlers = list(hook_data[phase])
            for _, handler in handlers:
                try:
                    if phase == HookPhase.INSTEAD:
                        result = handler(event)
                        if result is not None:
                            results.append(result)
                        return results
                    else:
                        result = handler(event)
                        if result is not None:
                            results.append(result)
                except Exception:
                    pass

        return results

    def has_hooks(self, name: str) -> bool:
        """Check if a hook has any handlers."""
        with self._lock:
            if name not in self._hooks:
                return False
            return any(self._hooks[name].values())

    def enable(self) -> None:
        """Enable all hooks."""
        self._enabled = True

    def disable(self) -> None:
        """Disable all hooks (triggers become no-ops)."""
        self._enabled = False

    def list_hooks(self) -> List[str]:
        """List all registered hook names."""
        with self._lock:
            return list(self._hooks.keys())


_global_hook_manager: Optional[HookManager] = None
_hm_lock = threading.Lock()


def get_hook_manager() -> HookManager:
    """Get the global hook manager singleton."""
    global _global_hook_manager
    with _hm_lock:
        if _global_hook_manager is None:
            _global_hook_manager = HookManager()
        return _global_hook_manager


def hook(name: str, *, phase: HookPhase = HookPhase.BEFORE) -> Callable:
    """Decorator to register a function as a hook handler.

    Args:
        name: Hook name.
        phase: Execution phase.

    Example:
        >>> @hook("before_action", phase=HookPhase.BEFORE)
        ... def my_handler(event):
        ...     pass
    """
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        manager = get_hook_manager()
        manager.register(name, fn, phase=phase)
        return fn
    return decorator
