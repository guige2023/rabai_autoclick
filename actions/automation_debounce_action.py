"""Automation debounce action module for RabAI AutoClick.

Provides debouncing for automation workflow triggers:
- WorkflowDebouncer: Debounce workflow trigger events
- AutomationThrottler: Throttle rapid automation triggers
- StepCoalescer: Coalesce multiple workflow steps
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DebounceStrategy(Enum):
    """Debounce strategies."""
    LAST = "last"
    FIRST = "first"
    LEADING = "leading"
    TRAILING = "trailing"
    COMBINED = "combined"


@dataclass
class AutomationDebounceConfig:
    """Configuration for automation debouncing."""
    strategy: DebounceStrategy = DebounceStrategy.TRAILING
    delay: float = 0.5
    leading_timeout: float = 0.0
    trailing_timeout: float = 0.5
    max_queued: int = 50
    coalesce_identical: bool = True
    priority_levels: int = 3
    step_grouping: Optional[str] = None
    cancel_on_new: bool = True


class WorkflowDebouncer:
    """Debounce workflow trigger events."""
    
    def __init__(self, config: Optional[AutomationDebounceConfig] = None):
        self.config = config or AutomationDebounceConfig()
        self._timers: Dict[str, Any] = {}
        self._pending: Dict[str, Tuple[Any, ...]] = {}
        self._last_event: Dict[str, Any] = {}
        self._event_count: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        self._stats = {"total_events": 0, "debounced_events": 0, "executed_events": 0, "cancelled_events": 0}
    
    def trigger(self, workflow_id: str, event_data: Any, callback: Callable, *args, **kwargs) -> bool:
        """Trigger a debounced workflow."""
        with self._lock:
            self._stats["total_events"] += 1
            self._event_count[workflow_id] += 1
            
            existing_timer = self._timers.get(workflow_id)
            
            if self.config.cancel_on_new and existing_timer:
                existing_timer.cancel()
                self._stats["cancelled_events"] += 1
            
            if self.config.strategy == DebounceStrategy.FIRST:
                if existing_timer:
                    return False
                return self._schedule(workflow_id, callback, event_data, *args, **kwargs)
            
            if self.config.strategy == DebounceStrategy.LEADING:
                if not existing_timer:
                    self._execute(callback, event_data, *args, **kwargs)
                self._schedule(workflow_id, callback, event_data, *args, **kwargs)
                return True
            
            self._last_event[workflow_id] = event_data
            self._pending[workflow_id] = (callback, event_data, args, kwargs)
            return self._schedule(workflow_id, callback, event_data, *args, **kwargs)
    
    def _schedule(self, workflow_id: str, callback: Callable, event_data: Any, 
                  *args, **kwargs) -> bool:
        """Schedule debounced execution."""
        delay = self.config.delay
        
        if self.config.strategy == DebounceStrategy.COMBINED:
            delay = self.config.trailing_timeout
        
        timer = threading.Timer(delay, self._execute_pending, args=(workflow_id,))
        self._timers[workflow_id] = timer
        timer.start()
        
        self._stats["debounced_events"] += 1
        return True
    
    def _execute_pending(self, workflow_id: str):
        """Execute pending workflow."""
        with self._lock:
            pending = self._pending.pop(workflow_id, None)
            timer = self._timers.pop(workflow_id, None)
        
        if pending:
            callback, event_data, args, kwargs = pending
            self._execute(callback, event_data, *args, **kwargs)
    
    def _execute(self, callback: Callable, event_data: Any, *args, **kwargs):
        """Execute callback."""
        try:
            if callable(callback):
                callback(event_data, *args, **kwargs)
            self._stats["executed_events"] += 1
        except Exception as e:
            logging.error(f"WorkflowDebouncer execution error: {e}")
    
    def cancel(self, workflow_id: Optional[str] = None):
        """Cancel pending workflows."""
        with self._lock:
            if workflow_id:
                timer = self._timers.pop(workflow_id, None)
                if timer:
                    timer.cancel()
                self._pending.pop(workflow_id, None)
            else:
                for timer in self._timers.values():
                    timer.cancel()
                self._timers.clear()
                self._pending.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get debounce statistics."""
        with self._lock:
            return dict(self._stats)


class AutomationDebounceAction(BaseAction):
    """Automation debounce action."""
    action_type = "automation_debounce"
    display_name = "自动化防抖"
    description = "自动化工作流触发防抖"
    
    def __init__(self):
        super().__init__()
        self._debouncers: Dict[str, WorkflowDebouncer] = {}
        self._lock = threading.Lock()
    
    def _get_debouncer(self, name: str, config: Optional[AutomationDebounceConfig] = None) -> WorkflowDebouncer:
        """Get or create debouncer."""
        with self._lock:
            if name not in self._debouncers:
                self._debouncers[name] = WorkflowDebouncer(config)
            return self._debouncers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute debounced automation."""
        try:
            name = params.get("name", "default")
            workflow_id = params.get("workflow_id", "default")
            event_data = params.get("event_data")
            callback = params.get("callback")
            command = params.get("command", "trigger")
            
            config = AutomationDebounceConfig(
                strategy=DebounceStrategy[params.get("strategy", "trailing").upper()],
                delay=params.get("delay", 0.5),
                cancel_on_new=params.get("cancel_on_new", True),
                coalesce_identical=params.get("coalesce_identical", True),
            )
            
            debouncer = self._get_debouncer(name, config)
            
            if command == "trigger" and callback:
                success = debouncer.trigger(workflow_id, event_data, callback)
                return ActionResult(success=success)
            
            elif command == "cancel":
                debouncer.cancel(workflow_id)
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = debouncer.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationDebounceAction error: {str(e)}")
