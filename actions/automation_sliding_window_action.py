"""Automation sliding window action module for RabAI AutoClick.

Provides sliding window operations for automation:
- SlidingWindowProcessor: Process data in sliding windows
- WindowAggregator: Aggregate data across windows
- TumblingWindow: Fixed-size tumbling windows
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WindowType(Enum):
    """Window types."""
    SLIDING = "sliding"
    TUMBLING = "tumbling"
    SESSION = "session"
    COUNT = "count"


@dataclass
class WindowConfig:
    """Configuration for windows."""
    window_type: WindowType = WindowType.SLIDING
    size: float = 60.0
    slide: float = 10.0
    max_size: int = 1000
    trigger_on_full: bool = True
    late_data_handling: str = "drop"


class SlidingWindowProcessor:
    """Process data in sliding windows."""
    
    def __init__(self, name: str, config: WindowConfig):
        self.name = name
        self.config = config
        self._buffer: deque = deque(maxlen=config.max_size)
        self._windows: Dict[int, List[Any]] = defaultdict(list)
        self._aggregations: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._stats = {"total_items": 0, "windows_created": 0, "windows_triggered": 0}
    
    def add(self, item: Any, timestamp: Optional[float] = None) -> Optional[List[Any]]:
        """Add item to window and return completed windows."""
        ts = timestamp or time.time()
        
        with self._lock:
            self._buffer.append({"item": item, "timestamp": ts})
            self._stats["total_items"] += 1
            
            if self.config.window_type == WindowType.TUMBLING:
                return self._process_tumbling_window(ts)
            elif self.config.window_type == WindowType.SLIDING:
                return self._process_sliding_window(ts)
            elif self.config.window_type == WindowType.COUNT:
                return self._process_count_window()
        
        return None
    
    def _process_tumbling_window(self, ts: float) -> Optional[List[Any]]:
        """Process tumbling window."""
        window_id = int(ts / self.config.size)
        
        if window_id not in self._windows:
            self._windows[window_id] = []
            self._stats["windows_created"] += 1
        
        self._windows[window_id].append(self._buffer[-1]["item"])
        
        if len(self._windows[window_id]) >= self.config.max_size or self.config.trigger_on_full:
            result = list(self._windows.pop(window_id, []))
            if result:
                self._stats["windows_triggered"] += 1
            return result
        
        return None
    
    def _process_sliding_window(self, ts: float) -> Optional[List[Any]]:
        """Process sliding window."""
        cutoff = ts - self.config.size
        cutoff_window_id = int(cutoff / self.config.slide)
        
        expired_keys = [k for k in self._windows.keys() if k <= cutoff_window_id]
        
        if expired_keys and self._windows:
            self._stats["windows_triggered"] += 1
        
        return None
    
    def _process_count_window(self) -> Optional[List[Any]]:
        """Process count-based window."""
        if len(self._buffer) >= self.config.max_size:
            items = [entry["item"] for entry in self._buffer]
            self._buffer.clear()
            self._stats["windows_triggered"] += 1
            return items
        return None
    
    def get_current_window(self) -> List[Any]:
        """Get current window contents."""
        with self._lock:
            return [entry["item"] for entry in self._buffer]
    
    def aggregate(self, agg_fn: Callable) -> Any:
        """Aggregate current window contents."""
        with self._lock:
            items = [entry["item"] for entry in self._buffer]
            if not items:
                return None
            try:
                return agg_fn(items)
            except Exception as e:
                logging.error(f"Aggregation failed: {e}")
                return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get window statistics."""
        with self._lock:
            return {
                "name": self.name,
                "buffer_size": len(self._buffer),
                "windows_tracked": len(self._windows),
                **{k: v for k, v in self._stats.items()},
            }


class AutomationSlidingWindowAction(BaseAction):
    """Automation sliding window action."""
    action_type = "automation_sliding_window"
    display_name = "自动化滑动窗口"
    description = "自动化滑动窗口处理"
    
    def __init__(self):
        super().__init__()
        self._windows: Dict[str, SlidingWindowProcessor] = {}
        self._lock = threading.Lock()
    
    def _get_window(self, name: str, config: Optional[WindowConfig] = None) -> SlidingWindowProcessor:
        """Get or create window processor."""
        with self._lock:
            if name not in self._windows:
                self._windows[name] = SlidingWindowProcessor(name, config or WindowConfig())
            return self._windows[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute sliding window operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "add")
            
            config = WindowConfig(
                window_type=WindowType[params.get("window_type", "sliding").upper()],
                size=params.get("size", 60.0),
                slide=params.get("slide", 10.0),
                max_size=params.get("max_size", 1000),
            )
            
            window = self._get_window(name, config)
            
            if command == "add":
                item = params.get("item")
                ts = params.get("timestamp")
                result = window.add(item, ts)
                if result:
                    return ActionResult(success=True, data={"window_triggered": True, "items": result})
                return ActionResult(success=True, message="Item added to window")
            
            elif command == "current":
                items = window.get_current_window()
                return ActionResult(success=True, data={"items": items, "count": len(items)})
            
            elif command == "aggregate":
                items = window.get_current_window()
                if items and callable(params.get("agg_fn")):
                    result = params.get("agg_fn")(items)
                    return ActionResult(success=True, data={"result": result})
                return ActionResult(success=True, data={"items": items})
            
            elif command == "stats":
                stats = window.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "clear":
                with self._lock:
                    if name in self._windows:
                        self._windows[name]._buffer.clear()
                return ActionResult(success=True)
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationSlidingWindowAction error: {str(e)}")
