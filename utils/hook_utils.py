"""Hook/callback system utilities.

Event-driven hooks for extending and customizing application behavior.
Supports sync/async hooks, ordering, filtering, and error handling.

Example:
    hooks = HookSystem()
    hooks.register("on_user_created", lambda user: send_welcome_email(user))
    hooks.trigger("on_user_created", user={"id": 1, "name": "Alice"})
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, ParamSpec
from uuid import uuid4

logger = logging.getLogger(__name__)

P = ParamSpec("P")


@dataclass
class Hook:
    """A registered hook callback."""
    id: str
    name: str
    callback: Callable[..., Any]
    priority: int = 100
    once: bool = False
    filter: Callable[..., bool] | None = None
    enabled: bool = True


@dataclass
class HookResult:
    """Result of a triggered hook."""
    hook_name: str
    called: int
    results: list[Any]
    errors: list[Exception]


class HookSystem:
    """Event hook system for decoupled plugin-style callbacks.

    Supports priority ordering, one-time hooks, filtering, and
    async execution.
    """

    def __init__(self) -> None:
        self._hooks: dict[str, list[Hook]] = defaultdict(list)
        self._once_fired: set[str] = set()

    def register(
        self,
        name: str,
        callback: Callable[..., Any],
        *,
        priority: int = 100,
        once: bool = False,
        filter: Callable[..., bool] | None = None,
        hook_id: str | None = None,
    ) -> str:
        """Register a hook callback.

        Args:
            name: Hook event name (e.g., "on_user_created").
            callback: Callable to invoke when hook triggers.
            priority: Lower = called first (0-999, default 100).
            once: If True, hook fires at most once then auto-removes.
            filter: Optional filter function - hook only fires if returns True.
            hook_id: Optional custom ID (auto-generated if not provided).

        Returns:
            The hook ID.
        """
        hook_id = hook_id or str(uuid4())
        hook = Hook(
            id=hook_id,
            name=name,
            callback=callback,
            priority=priority,
            once=once,
            filter=filter,
        )
        self._hooks[name].append(hook)
        self._hooks[name].sort(key=lambda h: h.priority)
        logger.debug("Registered hook %s (id=%s, priority=%d)", name, hook_id, priority)
        return hook_id

    def unregister(self, name: str, hook_id: str) -> bool:
        """Unregister a specific hook by ID.

        Args:
            name: Hook event name.
            hook_id: Hook ID to remove.

        Returns:
            True if hook was found and removed.
        """
        hooks = self._hooks.get(name, [])
        for i, hook in enumerate(hooks):
            if hook.id == hook_id:
                hooks.pop(i)
                logger.debug("Unregistered hook %s (id=%s)", name, hook_id)
                return True
        return False

    def trigger(
        self,
        name: str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> HookResult:
        """Trigger all hooks for an event.

        Args:
            name: Hook event name.
            *args: Positional args passed to each hook.
            **kwargs: Keyword args passed to each hook.

        Returns:
            HookResult with all return values and any errors.
        """
        hooks = self._hooks.get(name, [])
        results: list[Any] = []
        errors: list[Exception] = []

        for hook in hooks:
            if not hook.enabled:
                continue

            once_key = f"{name}:{hook.id}"
            if hook.once and once_key in self._once_fired:
                continue

            if hook.filter and not hook.filter(*args, **kwargs):
                continue

            try:
                result = hook.callback(*args, **kwargs)
                results.append(result)

                if hook.once:
                    self._once_fired.add(once_key)
                    self.unregister(name, hook.id)

            except Exception as e:
                logger.error("Hook %s (id=%s) raised: %s", name, hook.id, e)
                errors.append(e)

        return HookResult(
            hook_name=name,
            called=len(results),
            results=results,
            errors=errors,
        )

    async def trigger_async(
        self,
        name: str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> HookResult:
        """Async version of trigger - runs all hooks concurrently.

        Args:
            name: Hook event name.
            *args: Positional args passed to each hook.
            **kwargs: Keyword args passed to each hook.

        Returns:
            HookResult with all return values and any errors.
        """
        hooks = self._hooks.get(name, [])
        tasks: list[asyncio.Task] = []

        async def run_hook(hook: Hook) -> Any:
            once_key = f"{name}:{hook.id}"
            if hook.once and once_key in self._once_fired:
                return None
            if hook.filter and not hook.filter(*args, **kwargs):
                return None
            result = hook.callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            if hook.once:
                self._once_fired.add(once_key)
                self.unregister(name, hook.id)
            return result

        for hook in hooks:
            if not hook.enabled:
                continue
            task = asyncio.create_task(run_hook(hook))
            tasks.append(task)

        results: list[Any] = []
        errors: list[Exception] = []

        for task in tasks:
            try:
                result = await task
                results.append(result)
            except Exception as e:
                errors.append(e)

        return HookResult(
            hook_name=name,
            called=len(results),
            results=results,
            errors=errors,
        )

    def has_hooks(self, name: str) -> bool:
        """Check if any hooks are registered for an event."""
        return name in self._hooks and len(self._hooks[name]) > 0

    def list_hooks(self, name: str | None = None) -> list[Hook]:
        """List registered hooks, optionally filtered by name.

        Args:
            name: If provided, only list hooks for this event.

        Returns:
            List of Hook objects.
        """
        if name:
            return list(self._hooks.get(name, []))
        return [h for hooks in self._hooks.values() for h in hooks]

    def clear(self, name: str | None = None) -> None:
        """Clear hooks for a specific event or all events.

        Args:
            name: Event name to clear. If None, clears all hooks.
        """
        if name:
            self._hooks.pop(name, None)
        else:
            self._hooks.clear()
            self._once_fired.clear()

    def enable(self, name: str, hook_id: str | None = None) -> None:
        """Enable a hook or all hooks for an event."""
        if hook_id:
            for hook in self._hooks.get(name, []):
                if hook.id == hook_id:
                    hook.enabled = True
        else:
            for hook in self._hooks.get(name, []):
                hook.enabled = True

    def disable(self, name: str, hook_id: str | None = None) -> None:
        """Disable a hook or all hooks for an event."""
        if hook_id:
            for hook in self._hooks.get(name, []):
                if hook.id == hook_id:
                    hook.enabled = False
        else:
            for hook in self._hooks.get(name, []):
                hook.enabled = False


def filter_hook(data: dict[str, Any], **conditions: Any) -> bool:
    """Standard filter function for hooks checking data conditions.

    Args:
        data: Data dict to check.
        **conditions: Key-value pairs that must match in data.

    Returns:
        True if all conditions match.
    """
    for key, value in conditions.items():
        if data.get(key) != value:
            return False
    return True
