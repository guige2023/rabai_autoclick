"""Throttle and debounce action module for RabAI AutoClick.

Provides throttling and debouncing operations:
- ThrottleAction: Throttle function calls
- DebounceAction: Debounce function calls
- ThrottleStatsAction: Get throttle statistics
- ThrottleResetAction: Reset throttle counters
"""

import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class ThrottleEntry:
    """Represents a throttle entry."""
    key: str
    call_count: int = 0
    last_call: Optional[datetime] = None
    blocked_count: int = 0


class ThrottleManager:
    """Manages throttling and debouncing."""
    def __init__(self):
        self._throttles: Dict[str, float] = {}
        self._debounce_timers: Dict[str, threading.Timer] = {}
        self._entries: Dict[str, ThrottleEntry] = {}
        self._lock = threading.Lock()

    def throttle(self, key: str, min_interval: float) -> bool:
        """Returns True if call should proceed, False if throttled."""
        now = time.time()
        with self._lock:
            if key not in self._throttles:
                self._throttles[key] = 0.0
            last_call = self._throttles[key]
            if now - last_call >= min_interval:
                self._throttles[key] = now
                if key not in self._entries:
                    self._entries[key] = ThrottleEntry(key=key)
                self._entries[key].call_count += 1
                self._entries[key].last_call = datetime.utcnow()
                return True
            else:
                if key in self._entries:
                    self._entries[key].blocked_count += 1
                return False

    def debounce(self, key: str, func: Callable, delay: float) -> None:
        """Debounce a function call."""
        with self._lock:
            if key in self._debounce_timers:
                self._debounce_timers[key].cancel()
            self._debounce_timers[key] = threading.Timer(delay, self._execute_debounce, args=[key, func])
            self._debounce_timers[key].start()

    def _execute_debounce(self, key: str, func: Callable) -> None:
        with self._lock:
            if key in self._debounce_timers:
                del self._debounce_timers[key]
        try:
            func()
        except Exception:
            pass

    def get_stats(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [
                {
                    "key": e.key,
                    "call_count": e.call_count,
                    "blocked_count": e.blocked_count,
                    "last_call": e.last_call.isoformat() if e.last_call else None
                }
                for e in self._entries.values()
            ]

    def reset(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key:
                if key in self._throttles:
                    del self._throttles[key]
                if key in self._entries:
                    del self._entries[key]
                if key in self._debounce_timers:
                    self._debounce_timers[key].cancel()
                    del self._debounce_timers[key]
            else:
                self._throttles.clear()
                self._entries.clear()
                for timer in self._debounce_timers.values():
                    timer.cancel()
                self._debounce_timers.clear()


_manager = ThrottleManager()


class ThrottleAction(BaseAction):
    """Throttle function calls."""
    action_type = "throttle"
    display_name = "节流"
    description = "限制函数调用频率"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            min_interval = params.get("min_interval", 1.0)
            func_ref = params.get("func_ref", None)
            args = params.get("args", [])
            kwargs = params.get("kwargs", {})

            allowed = _manager.throttle(key, min_interval)

            if allowed and func_ref:
                try:
                    result = func_ref(*args, **kwargs)
                    return ActionResult(
                        success=True,
                        message=f"Throttled call allowed for '{key}'",
                        data={"allowed": True, "key": key, "result": result}
                    )
                except Exception as e:
                    return ActionResult(success=False, message=f"Call failed: {str(e)}")
            else:
                return ActionResult(
                    success=False,
                    message=f"Call throttled for '{key}', wait {min_interval}s",
                    data={"allowed": False, "key": key, "min_interval": min_interval}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle failed: {str(e)}")


class DebounceAction(BaseAction):
    """Debounce function calls."""
    action_type = "debounce"
    display_name = "防抖"
    description = "防抖函数调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", "default")
            delay = params.get("delay", 0.5)
            func_ref = params.get("func_ref", None)

            if not func_ref:
                return ActionResult(success=False, message="func_ref is required")

            _manager.debounce(key, func_ref)

            return ActionResult(
                success=True,
                message=f"Debounced call scheduled for '{key}' in {delay}s",
                data={"key": key, "delay": delay}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Debounce failed: {str(e)}")


class ThrottleStatsAction(BaseAction):
    """Get throttle statistics."""
    action_type = "throttle_stats"
    display_name = "节流统计"
    description = "获取节流统计数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stats = _manager.get_stats()
            return ActionResult(
                success=True,
                message=f"Throttle stats: {len(stats)} entries",
                data={"stats": stats, "count": len(stats)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle stats failed: {str(e)}")


class ThrottleResetAction(BaseAction):
    """Reset throttle counters."""
    action_type = "throttle_reset"
    display_name = "重置节流"
    description = "重置节流计数器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            key = params.get("key", None)
            _manager.reset(key)
            return ActionResult(
                success=True,
                message=f"Throttle reset for '{key if key else 'all'}'"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Throttle reset failed: {str(e)}")
