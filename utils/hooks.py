"""Hook system for RabAI AutoClick.

Provides:
- Hook manager for extensibility
- Pre/post execution hooks
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass


@dataclass
class Hook:
    """Represents a registered hook."""
    name: str
    callback: Callable
    priority: int = 0
    once: bool = False


class HookManager:
    """Manages hooks for extensibility.

    Usage:
        hooks = HookManager()

        @hooks.register("before_action")
        def my_hook(data):
            print("Before action!")

        hooks.trigger("before_action", some_data)
    """

    def __init__(self) -> None:
        self._hooks: Dict[str, List[Hook]] = {}
        self._executed_onces: set = set()

    def register(
        self,
        name: str,
        callback: Callable,
        priority: int = 0,
        once: bool = False,
    ) -> Callable:
        """Register a hook.

        Args:
            name: Hook name.
            callback: Function to call.
            priority: Execution priority (higher = earlier).
            once: If True, execute only once.

        Returns:
            The callback (for use as decorator).
        """
        hook = Hook(name, callback, priority, once)

        if name not in self._hooks:
            self._hooks[name] = []

        self._hooks[name].append(hook)
        self._hooks[name].sort(key=lambda h: -h.priority)

        return callback

    def unregister(self, name: str, callback: Callable) -> bool:
        """Unregister a hook.

        Args:
            name: Hook name.
            callback: Callback to remove.

        Returns:
            True if removed.
        """
        if name not in self._hooks:
            return False

        self._hooks[name] = [
            h for h in self._hooks[name] if h.callback != callback
        ]
        return True

    def trigger(self, name: str, *args: Any, **kwargs: Any) -> List[Any]:
        """Trigger all hooks for an event.

        Args:
            name: Hook name.
            *args: Positional arguments for callbacks.
            **kwargs: Keyword arguments for callbacks.

        Returns:
            List of callback results.
        """
        if name not in self._hooks:
            return []

        results = []
        to_remove = []

        for hook in self._hooks[name]:
            # Check once flag
            if hook.once:
                key = (name, id(hook.callback))
                if key in self._executed_onces:
                    continue
                self._executed_onces.add(key)
                to_remove.append(hook)

            try:
                result = hook.callback(*args, **kwargs)
                results.append(result)
            except Exception as e:
                results.append(e)

        # Remove executed once hooks
        for hook in to_remove:
            self._hooks[name].remove(hook)

        return results

    def has_hooks(self, name: str) -> bool:
        """Check if hooks exist for name.

        Args:
            name: Hook name.

        Returns:
            True if hooks exist.
        """
        return name in self._hooks and len(self._hooks[name]) > 0

    def clear(self, name: Optional[str] = None) -> None:
        """Clear hooks.

        Args:
            name: Optional specific hook name to clear.
        """
        if name:
            self._hooks.pop(name, None)
        else:
            self._hooks.clear()
            self._executed_onces.clear()

    def list_hooks(self) -> List[str]:
        """List all registered hook names."""
        return list(self._hooks.keys())


# Global hook manager
_hooks = HookManager()


def get_hooks() -> HookManager:
    """Get global hook manager."""
    return _hooks


def hook(name: str, **kwargs: Any) -> Callable:
    """Decorator to register function as hook.

    Args:
        name: Hook name.
        **kwargs: Additional hook options.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        _hooks.register(name, func, **kwargs)
        return func
    return decorator


# Predefined hook names
class Hooks:
    """Predefined hook names for RabAI AutoClick."""
    # Workflow hooks
    WORKFLOW_START = "workflow.start"
    WORKFLOW_END = "workflow.end"
    WORKFLOW_ERROR = "workflow.error"

    # Action hooks
    ACTION_BEFORE = "action.before"
    ACTION_AFTER = "action.after"
    ACTION_ERROR = "action.error"

    # System hooks
    SYSTEM_INIT = "system.init"
    SYSTEM_SHUTDOWN = "system.shutdown"

    # UI hooks
    UI_SHOW = "ui.show"
    UI_HIDE = "ui.hide"