"""Data debounce action module for RabAI AutoClick.

Provides debouncing for data operations:
- DataDebouncer: Debounce rapid data changes
- DataCoalescer: Coalesce multiple data updates
- DataBatcher: Batch data changes over time windows
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DebounceMode(Enum):
    """Debounce modes."""
    DEBOUNCE = "debounce"
    THROTTLE = "throttle"
    COALESCE = "coalesce"
    LEADING_EDGE = "leading_edge"
    TRAILING_EDGE = "trailing_edge"


@dataclass
class DataDebounceConfig:
    """Configuration for data debouncing."""
    mode: DebounceMode = DebounceMode.DEBOUNCE
    delay: float = 0.3
    max_wait: Optional[float] = None
    leading_timeout: float = 0.0
    trailing_timeout: float = 0.3
    max_burst: int = 100
    coalesce_key: Optional[str] = None
    batch_size: int = 10
    batch_timeout: float = 1.0


class DataDebouncer:
    """Debounce rapid data operations."""
    
    def __init__(self, config: Optional[DataDebounceConfig] = None):
        self.config = config or DataDebounceConfig()
        self._timers: Dict[str, Any] = {}
        self._pending_calls: Dict[str, Tuple[Any, ...]] = {}
        self._last_call_time: Dict[str, float] = {}
        self._call_count: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()
        self._stats = {"total_calls": 0, "debounced_calls": 0, "executed_calls": 0}
    
    def call(self, key: str, func: Callable, *args, **kwargs) -> bool:
        """Schedule a debounced call."""
        with self._lock:
            self._stats["total_calls"] += 1
            now = time.time()
            self._last_call_time[key] = now
            self._call_count[key] += 1
            
            if self.config.mode == DebounceMode.LEADING_EDGE:
                if self._call_count[key] == 1:
                    return self._execute(func, *args, **kwargs)
                self._pending_calls[key] = (func, args, kwargs)
                self._schedule_timer(key)
                return True
            
            if self.config.mode == DebounceMode.THROTTLE:
                last = self._last_call_time.get(key + "_last_exec", 0)
                if now - last < self.config.delay:
                    self._pending_calls[key] = (func, args, kwargs)
                    self._schedule_timer(key)
                    return True
                return self._execute(func, *args, **kwargs)
            
            self._pending_calls[key] = (func, args, kwargs)
            self._schedule_timer(key)
            self._stats["debounced_calls"] += 1
            return True
    
    def _schedule_timer(self, key: str):
        """Schedule debounce timer."""
        existing = self._timers.get(key)
        if existing:
            existing.cancel()
        
        timer = threading.Timer(self.config.delay, self._execute_pending, args=(key,))
        self._timers[key] = timer
        timer.start()
    
    def _execute_pending(self, key: str):
        """Execute pending call for key."""
        with self._lock:
            if key not in self._pending_calls:
                return
            
            func, args, kwargs = self._pending_calls.pop(key)
            self._last_call_time[key + "_last_exec"] = time.time()
        
        self._execute(func, *args, **kwargs)
    
    def _execute(self, func: Callable, *args, **kwargs) -> bool:
        """Execute function."""
        try:
            func(*args, **kwargs)
            self._stats["executed_calls"] += 1
            return True
        except Exception as e:
            logging.error(f"DataDebouncer execution error: {e}")
            return False
    
    def cancel(self, key: Optional[str] = None):
        """Cancel pending calls."""
        with self._lock:
            if key:
                if key in self._timers:
                    self._timers[key].cancel()
                    del self._timers[key]
                self._pending_calls.pop(key, None)
            else:
                for timer in self._timers.values():
                    timer.cancel()
                self._timers.clear()
                self._pending_calls.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get debounce statistics."""
        with self._lock:
            return dict(self._stats)


class DataCoalescer:
    """Coalesce multiple data updates."""
    
    def __init__(self, config: Optional[DataDebounceConfig] = None):
        self.config = config or DataDebounceConfig()
        self._buffers: Dict[str, List[Any]] = defaultdict(list)
        self._timers: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"total_items": 0, "total_batches": 0, "total_coalesced": 0}
    
    def add(self, key: str, item: Any, flush: bool = False) -> Optional[List[Any]]:
        """Add item to coalescing buffer."""
        with self._lock:
            self._stats["total_items"] += 1
            
            if key not in self._buffers:
                self._buffers[key] = []
            
            self._buffers[key].append(item)
            
            if flush or len(self._buffers[key]) >= self.config.batch_size:
                return self._flush_key(key)
            
            if key not in self._timers:
                timer = threading.Timer(self.config.batch_timeout, self._auto_flush, args=(key,))
                self._timers[key] = timer
                timer.start()
            
            return None
    
    def _auto_flush(self, key: str):
        """Auto-flush buffer after timeout."""
        with self._lock:
            self._flush_key(key)
    
    def _flush_key(self, key: str) -> List[Any]:
        """Flush buffer for key."""
        items = self._buffers.pop(key, [])
        timer = self._timers.pop(key, None)
        if timer:
            timer.cancel()
        
        if items:
            self._stats["total_batches"] += 1
            self._stats["total_coalesced"] += len(items)
        
        return items
    
    def flush(self, key: Optional[str] = None) -> Dict[str, List[Any]]:
        """Flush buffers."""
        with self._lock:
            if key:
                return {key: self._flush_key(key)} if key in self._buffers else {}
            
            result = {}
            for k in list(self._buffers.keys()):
                result[k] = self._flush_key(k)
            return result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get coalesce statistics."""
        with self._lock:
            return dict(self._stats)


class DataDebounceAction(BaseAction):
    """Data debounce action."""
    action_type = "data_debounce"
    display_name = "数据防抖"
    description = "数据操作防抖与合并"
    
    def __init__(self):
        super().__init__()
        self._debouncers: Dict[str, DataDebouncer] = {}
        self._coalescers: Dict[str, DataCoalescer] = {}
        self._lock = threading.Lock()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute debounced data operation."""
        try:
            operation = params.get("operation")
            key = params.get("key", "default")
            mode = params.get("mode", "debounce")
            
            mode_enum = DebounceMode[mode.upper().replace("-", "_")]
            
            if mode_enum in [DebounceMode.COALESCE]:
                coalescer_key = f"{key}_{mode}"
                with self._lock:
                    if coalescer_key not in self._coalescers:
                        config = DataDebounceConfig(mode=mode_enum)
                        self._coalescers[coalescer_key] = DataCoalescer(config)
                    coalescer = self._coalescers[coalescer_key]
                
                if operation:
                    result = coalescer.add(key, operation, flush=params.get("flush", False))
                    return ActionResult(success=True, data={"result": result})
                else:
                    flushed = coalescer.flush(key)
                    return ActionResult(success=True, data={"flushed": flushed})
            
            debouncer_key = f"{key}_{mode}"
            with self._lock:
                if debouncer_key not in self._debouncers:
                    config = DataDebounceConfig(
                        mode=mode_enum,
                        delay=params.get("delay", 0.3),
                        max_wait=params.get("max_wait"),
                    )
                    self._debouncers[debouncer_key] = DataDebouncer(config)
                debouncer = self._debouncers[debouncer_key]
            
            if operation:
                success = debouncer.call(key, operation, **(params.get("kwargs", {})))
                return ActionResult(success=success)
            else:
                return ActionResult(success=True, data={"stats": debouncer.get_stats()})
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataDebounceAction error: {str(e)}")
    
    def cancel(self, key: Optional[str] = None) -> ActionResult:
        """Cancel pending operations."""
        try:
            with self._lock:
                for debouncer in self._debouncers.values():
                    debouncer.cancel(key)
                for coalescer in self._coalescers.values():
                    coalescer.flush(key)
            return ActionResult(success=True)
        except Exception as e:
            return ActionResult(success=False, message=str(e))
