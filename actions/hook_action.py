"""Hook system action module for RabAI AutoClick.

Provides hook/callback operations:
- HookRegisterAction: Register hooks for events
- HookTriggerAction: Trigger registered hooks
- HookUnregisterAction: Unregister hooks
- HookChainAction: Chain multiple hooks together
"""

import uuid
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Hook:
    """Represents a hook callback."""
    hook_id: str
    name: str
    event: str
    callback: Callable
    priority: int = 0
    enabled: bool = True
    once: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    call_count: int = 0
    last_called: Optional[datetime] = None


class HookManager:
    """Manages hooks and their execution."""
    def __init__(self):
        self._hooks: Dict[str, List[Hook]] = {}
        self._lock = threading.RLock()

    def register(
        self,
        name: str,
        event: str,
        callback: Callable,
        priority: int = 0,
        once: bool = False
    ) -> str:
        hook_id = str(uuid.uuid4())
        hook = Hook(hook_id=hook_id, name=name, event=event, callback=callback, priority=priority, once=once)
        with self._lock:
            if event not in self._hooks:
                self._hooks[event] = []
            self._hooks[event].append(hook)
            self._hooks[event].sort(key=lambda h: h.priority, reverse=True)
        return hook_id

    def unregister(self, hook_id: str) -> bool:
        with self._lock:
            for event, hooks in self._hooks.items():
                for hook in hooks[:]:
                    if hook.hook_id == hook_id:
                        hooks.remove(hook)
                        return True
        return False

    def trigger(self, event: str, context: Any, data: Any = None) -> List[Any]:
        results = []
        with self._lock:
            hooks = list(self._hooks.get(event, []))
        to_remove = []
        for hook in hooks:
            if not hook.enabled:
                continue
            try:
                result = hook.callback(context, data)
                hook.call_count += 1
                hook.last_called = datetime.utcnow()
                results.append(result)
                if hook.once:
                    to_remove.append(hook.hook_id)
            except Exception:
                pass
        for hid in to_remove:
            self.unregister(hid)
        return results

    def enable(self, hook_id: str) -> bool:
        with self._lock:
            for hooks in self._hooks.values():
                for hook in hooks:
                    if hook.hook_id == hook_id:
                        hook.enabled = True
                        return True
        return False

    def disable(self, hook_id: str) -> bool:
        with self._lock:
            for hooks in self._hooks.values():
                for hook in hooks:
                    if hook.hook_id == hook_id:
                        hook.enabled = False
                        return True
        return False

    def list_hooks(self, event: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            if event:
                hooks = self._hooks.get(event, [])
            else:
                all_hooks = []
                for h_list in self._hooks.values():
                    all_hooks.extend(h_list)
                hooks = all_hooks
            return [
                {
                    "hook_id": h.hook_id,
                    "name": h.name,
                    "event": h.event,
                    "priority": h.priority,
                    "enabled": h.enabled,
                    "once": h.once,
                    "call_count": h.call_count,
                    "last_called": h.last_called.isoformat() if h.last_called else None
                }
                for h in hooks
            ]


_hook_manager = HookManager()


class HookRegisterAction(BaseAction):
    """Register a hook for an event."""
    action_type = "hook_register"
    display_name = "注册Hook"
    description = "为事件注册Hook回调"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            event = params.get("event", "")
            priority = params.get("priority", 0)
            once = params.get("once", False)
            callback_ref = params.get("callback_ref", None)

            if not name:
                return ActionResult(success=False, message="name is required")
            if not event:
                return ActionResult(success=False, message="event is required")

            def default_callback(ctx, data):
                return {"status": "ok", "event": event}

            callback = callback_ref or default_callback
            hook_id = _hook_manager.register(name=name, event=event, callback=callback, priority=priority, once=once)

            return ActionResult(
                success=True,
                message=f"Hook '{name}' registered for event '{event}'",
                data={"hook_id": hook_id, "event": event, "name": name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Hook register failed: {str(e)}")


class HookTriggerAction(BaseAction):
    """Trigger all hooks for an event."""
    action_type = "hook_trigger"
    display_name = "触发Hook"
    description = "触发事件的所有Hook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event = params.get("event", "")
            data = params.get("data", None)
            blocking = params.get("blocking", True)

            if not event:
                return ActionResult(success=False, message="event is required")

            results = _hook_manager.trigger(event, context, data)

            return ActionResult(
                success=True,
                message=f"Triggered {len(results)} hooks for event '{event}'",
                data={"event": event, "results": results, "count": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Hook trigger failed: {str(e)}")


class HookUnregisterAction(BaseAction):
    """Unregister a hook."""
    action_type = "hook_unregister"
    display_name = "注销Hook"
    description = "注销Hook回调"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            hook_id = params.get("hook_id", "")
            name = params.get("name", None)
            event = params.get("event", None)

            if hook_id:
                removed = _hook_manager.unregister(hook_id)
                if removed:
                    return ActionResult(success=True, message=f"Hook {hook_id} unregistered")
                return ActionResult(success=False, message=f"Hook {hook_id} not found")

            if name or event:
                hooks = _hook_manager.list_hooks(event=event)
                matching = [h for h in hooks if (not name) or (name and h["name"] == name)]
                for h in matching:
                    _hook_manager.unregister(h["hook_id"])
                return ActionResult(success=True, message=f"Unregistered {len(matching)} hooks")

            return ActionResult(success=False, message="hook_id or name/event required")

        except Exception as e:
            return ActionResult(success=False, message=f"Hook unregister failed: {str(e)}")


class HookChainAction(BaseAction):
    """Chain multiple hooks together sequentially."""
    action_type = "hook_chain"
    display_name = "Hook链"
    description = "顺序链接多个Hook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event = params.get("event", "")
            data = params.get("data", None)
            stop_on_error = params.get("stop_on_error", True)

            if not event:
                return ActionResult(success=False, message="event is required")

            results = []
            hooks = _hook_manager.list_hooks(event=event)

            for hook_info in hooks:
                hook_id = hook_info["hook_id"]
                hook = None
                with threading.Lock():
                    for h_list in _hook_manager._hooks.values():
                        for h in h_list:
                            if h.hook_id == hook_id:
                                hook = h
                                break
                if not hook or not hook.enabled:
                    continue
                try:
                    result = hook.callback(context, data)
                    results.append({"hook_id": hook_id, "status": "ok", "result": result})
                except Exception as ex:
                    results.append({"hook_id": hook_id, "status": "error", "error": str(ex)})
                    if stop_on_error:
                        break

            errors = [r for r in results if r["status"] == "error"]
            return ActionResult(
                success=len(errors) == 0,
                message=f"Hook chain completed: {len(results) - len(errors)}/{len(results)} succeeded",
                data={"results": results, "count": len(results), "errors": len(errors)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Hook chain failed: {str(e)}")
