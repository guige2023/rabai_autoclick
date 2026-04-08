"""Automation Event Handler Action Module. Handles automation events and triggers."""
import sys, os, time, threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class EventHandler:
    event_type: str; callback: Callable; filter_func: Optional[Callable] = None
    priority: int = 0; once: bool = False

class AutomationEventHandlerAction(BaseAction):
    action_type = "automation_event_handler"; display_name = "自动化事件处理"
    description = "处理自动化事件"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._handlers = {}; self._history = []; self._max_history = 1000
    def register_handler(self, event_type: str, callback: Callable, filter_func: Optional[Callable] = None,
                       priority: int = 0, once: bool = False) -> None:
        handler = EventHandler(event_type, callback, filter_func, priority, once)
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)
            self._handlers[event_type].sort(key=lambda h: -h.priority)
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "emit"); event_type = params.get("event_type", "default")
        if mode == "register":
            callback = lambda ctx, p: ActionResult(success=True, message="Handler executed")
            self.register_handler(event_type, callback, params.get("filter_func"),
                                params.get("priority",0), params.get("once",False))
            return ActionResult(success=True, message=f"Handler registered for '{event_type}'")
        if mode == "history":
            return ActionResult(success=True, message=f"{len(self._history)} events", data={"events": self._history[-50:]})
        event_data = params.get("event_data", {}); timestamp = time.time()
        with self._lock: handlers = list(self._handlers.get(event_type, []) + self._handlers.get("*",[]))
        triggered = []; errors = []
        for handler in handlers:
            try:
                if handler.filter_func and not handler.filter_func(event_data): continue
                result = handler.callback(context, event_data)
                triggered.append({"handler": handler.event_type, "result": str(result) if result else "OK"})
                if handler.once:
                    with self._lock:
                        if handler in self._handlers.get(event_type, []): self._handlers[event_type].remove(handler)
            except Exception as e: errors.append(f"{handler.event_type}: {e}")
        self._history.append({"event_type": event_type, "data": event_data, "timestamp": timestamp,
                              "triggered": len(triggered), "errors": errors})
        if len(self._history) > self._max_history: self._history = self._history[-self._max_history:]
        return ActionResult(success=len(errors)==0, message=f"'{event_type}': {len(triggered)} triggered, {len(errors)} errors",
                          data={"triggered": triggered, "errors": errors})
