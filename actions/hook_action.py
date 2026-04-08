"""Hook action module for RabAI AutoClick.

Provides hook/callback utilities:
- HookRegistry: Register and manage hooks
- HookExecutor: Execute hooks
- FilterChain: Chain filter hooks
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Hook:
    """Hook definition."""
    hook_id: str
    name: str
    func: Callable
    priority: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = None


class HookRegistry:
    """Registry for hooks."""

    def __init__(self):
        self._hooks: Dict[str, List[Hook]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        func: Callable,
        hook_id: Optional[str] = None,
        priority: int = 0,
    ) -> str:
        """Register a hook."""
        with self._lock:
            hid = hook_id or str(uuid.uuid4())

            hook = Hook(
                hook_id=hid,
                name=name,
                func=func,
                priority=priority,
            )

            if name not in self._hooks:
                self._hooks[name] = []

            self._hooks[name].append(hook)
            self._hooks[name].sort(key=lambda h: h.priority, reverse=True)

            return hid

    def unregister(self, name: str, hook_id: str) -> bool:
        """Unregister a hook."""
        with self._lock:
            if name not in self._hooks:
                return False

            for i, hook in enumerate(self._hooks[name]):
                if hook.hook_id == hook_id:
                    self._hooks[name].pop(i)
                    return True

            return False

    def enable(self, name: str, hook_id: str) -> bool:
        """Enable a hook."""
        with self._lock:
            return self._set_enabled(name, hook_id, True)

    def disable(self, name: str, hook_id: str) -> bool:
        """Disable a hook."""
        with self._lock:
            return self._set_enabled(name, hook_id, False)

    def _set_enabled(self, name: str, hook_id: str, enabled: bool) -> bool:
        """Set hook enabled state."""
        if name not in self._hooks:
            return False

        for hook in self._hooks[name]:
            if hook.hook_id == hook_id:
                hook.enabled = enabled
                return True

        return False

    def get_hooks(self, name: str) -> List[Hook]:
        """Get all hooks for a name."""
        with self._lock:
            if name not in self._hooks:
                return []
            return [h for h in self._hooks[name] if h.enabled]

    def list_hooks(self) -> Dict[str, List[str]]:
        """List all registered hooks."""
        with self._lock:
            return {name: [h.hook_id for h in hooks] for name, hooks in self._hooks.items()}


class HookExecutor:
    """Execute hooks."""

    def __init__(self, registry: Optional[HookRegistry] = None):
        self.registry = registry or HookRegistry()

    def execute(self, name: str, *args, **kwargs) -> List[Any]:
        """Execute all hooks for a name."""
        hooks = self.registry.get_hooks(name)
        results = []

        for hook in hooks:
            try:
                result = hook.func(*args, **kwargs)
                results.append({"hook_id": hook.hook_id, "success": True, "result": result})
            except Exception as e:
                results.append({"hook_id": hook.hook_id, "success": False, "error": str(e)})

        return results

    def execute_until_success(self, name: str, *args, **kwargs) -> Optional[Any]:
        """Execute hooks until one succeeds."""
        hooks = self.registry.get_hooks(name)

        for hook in hooks:
            try:
                result = hook.func(*args, **kwargs)
                return result
            except Exception:
                continue

        return None


class FilterChain:
    """Chain of filter hooks."""

    def __init__(self, filters: Optional[List[Callable]] = None):
        self._filters = filters or []

    def add_filter(self, filter_fn: Callable) -> None:
        """Add a filter to chain."""
        self._filters.append(filter_fn)

    def execute(self, data: Any) -> Any:
        """Execute filter chain."""
        result = data
        for filter_fn in self._filters:
            result = filter_fn(result)
        return result


class HookAction(BaseAction):
    """Hook management action."""
    action_type = "hook"
    display_name = "钩子管理"
    description = "钩子注册执行"

    def __init__(self):
        super().__init__()
        self._registry = HookRegistry()
        self._executor = HookExecutor(self._registry)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "unregister":
                return self._unregister(params)
            elif operation == "enable":
                return self._enable(params)
            elif operation == "disable":
                return self._disable(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Hook error: {str(e)}")

    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a hook."""
        name = params.get("name")
        priority = params.get("priority", 0)

        if not name:
            return ActionResult(success=False, message="name is required")

        def dummy_func():
            return {"executed": True}

        hook_id = self._registry.register(name, dummy_func, priority=priority)

        return ActionResult(success=True, message=f"Hook registered: {hook_id}", data={"hook_id": hook_id})

    def _unregister(self, params: Dict[str, Any]) -> ActionResult:
        """Unregister a hook."""
        name = params.get("name")
        hook_id = params.get("hook_id")

        if not name or not hook_id:
            return ActionResult(success=False, message="name and hook_id are required")

        success = self._registry.unregister(name, hook_id)

        return ActionResult(success=success, message="Unregistered" if success else "Hook not found")

    def _enable(self, params: Dict[str, Any]) -> ActionResult:
        """Enable a hook."""
        name = params.get("name")
        hook_id = params.get("hook_id")

        if not name or not hook_id:
            return ActionResult(success=False, message="name and hook_id are required")

        success = self._registry.enable(name, hook_id)

        return ActionResult(success=success, message="Enabled" if success else "Hook not found")

    def _disable(self, params: Dict[str, Any]) -> ActionResult:
        """Disable a hook."""
        name = params.get("name")
        hook_id = params.get("hook_id")

        if not name or not hook_id:
            return ActionResult(success=False, message="name and hook_id are required")

        success = self._registry.disable(name, hook_id)

        return ActionResult(success=success, message="Disabled" if success else "Hook not found")

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute hooks."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        results = self._executor.execute(name)

        successful = sum(1 for r in results if r["success"])

        return ActionResult(
            success=successful > 0,
            message=f"Executed: {successful}/{len(results)} successful",
            data={"results": results, "successful": successful},
        )

    def _list(self) -> ActionResult:
        """List all hooks."""
        hooks = self._registry.list_hooks()

        return ActionResult(success=True, message=f"{len(hooks)} hook types", data={"hooks": hooks})
