"""
Stream Processing Action Module.

Provides stream processing capabilities with windowing,
aggregation, and state management for real-time data flows.
"""

from typing import Optional, Dict, List, Any, Callable, Generic, TypeVar, Iterator
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import logging
import time
import threading
from collections import deque

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class WindowType(Enum):
    """Stream window types."""
    TUMBLING = "tumbling"       # Fixed-size, non-overlapping
    SLIDING = "sliding"         # Overlapping with slide interval
    SESSION = "session"         # Grows during activity, closes on gap
    COUNT = "count"             # Count-based windows


@dataclass
class WindowConfig:
    """Configuration for stream windows."""
    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    slide_seconds: Optional[float] = None
    max_size: Optional[int] = None  # For count windows
    session_gap: float = 30.0        # For session windows


@dataclass
class Window(Generic[T]):
    """Represents a data window with timing information."""
    window_id: str
    start_time: float
    end_time: float
    items: List[T] = field(default_factory=list)
    is_closed: bool = False
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
        
    @property
    def size(self) -> int:
        return len(self.items)
        
    def add(self, item: T) -> None:
        """Add item to window if not closed."""
        if not self.is_closed:
            self.items.append(item)
            
    def close(self) -> None:
        """Close the window for new additions."""
        self.is_closed = True
        
    def get_result(self) -> Any:
        """Get window contents."""
        return self.items.copy()


class StreamProcessor(Generic[T]):
    """
    Stream processor with windowing support.
    
    Example:
        processor = StreamProcessor(window_size=60)
        
        @processor.handler
        def process_item(item):
            return transform(item)
            
        @processor.window_aggregator
        def aggregate_window(window):
            return sum(window.items)
            
        for item in data_stream:
            processor.process(item)
    """
    
    def __init__(
        self,
        config: Optional[WindowConfig] = None,
        buffer_size: int = 10000,
    ):
        self.config = config or WindowConfig()
        self.buffer_size = buffer_size
        self._handlers: List[Callable[[T], Any]] = []
        self._window_aggregators: List[Callable[[Window], Any]] = []
        self._windows: Dict[str, Window] = {}
        self._closed_windows: deque = deque(maxlen=100)
        self._lock = threading.RLock()
        self._last_event_time: float = 0
        self._session_windows: Dict[str, float] = {}  # session_key -> last_access
        
        self._stats = {
            "items_processed": 0,
            "windows_created": 0,
            "windows_closed": 0,
            "handlers_called": 0,
        }
        
    def handler(self, func: Callable[[T], Any]) -> Callable[[T], Any]:
        """Decorator to register an item handler."""
        self._handlers.append(func)
        return func
        
    def window_aggregator(
        self,
        func: Callable[[Window], Any],
    ) -> Callable[[Window], Any]:
        """Decorator to register a window aggregator."""
        self._window_aggregators.append(func)
        return func
        
    def process(self, item: T, key: Optional[str] = None) -> List[Any]:
        """
        Process a single item through the stream.
        
        Args:
            item: Item to process
            key: Optional key for session/sliding windows
            
        Returns:
            List of results from handlers
        """
        with self._lock:
            self._last_event_time = time.time()
            self._stats["items_processed"] += 1
            
            results = []
            for handler in self._handlers:
                try:
                    result = handler(item)
                    results.append(result)
                    self._stats["handlers_called"] += 1
                except Exception as e:
                    logger.error(f"Handler error: {e}")
                    
            self._update_windows(item, key)
            return results
            
    def _update_windows(self, item: T, key: Optional[str]) -> None:
        """Update all active windows with new item."""
        now = time.time()
        
        if self.config.window_type == WindowType.TUMBLING:
            self._update_tumbling_window(item, now)
        elif self.config.window_type == WindowType.SLIDING:
            self._update_sliding_window(item, now)
        elif self.config.window_type == WindowType.SESSION:
            self._update_session_window(item, now, key)
        elif self.config.window_type == WindowType.COUNT:
            self._update_count_window(item, key)
            
    def _update_tumbling_window(self, item: T, now: float) -> None:
        """Update tumbling (fixed) windows."""
        window_id = self._get_tumbling_window_id(now)
        
        if window_id not in self._windows:
            self._close_expired_tumbling_windows(now)
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=self._get_window_start(now),
                end_time=self._get_window_start(now) + self.config.size_seconds,
            )
            self._stats["windows_created"] += 1
            
        self._windows[window_id].add(item)
        
    def _get_tumbling_window_id(self, timestamp: float) -> str:
        """Get window ID for tumbling window."""
        start = self._get_window_start(timestamp)
        return f"tumbling_{int(start)}"
        
    def _get_window_start(self, timestamp: float) -> float:
        """Calculate window start time."""
        return int(timestamp / self.config.size_seconds) * self.config.size_seconds
        
    def _close_expired_tumbling_windows(self, now: float) -> None:
        """Close windows that have passed their end time."""
        to_close = []
        for window_id, window in self._windows.items():
            if now >= window.end_time:
                to_close.append(window_id)
                
        for window_id in to_close:
            self._close_window(window_id)
            
    def _update_sliding_window(self, item: T, now: float) -> None:
        """Update sliding windows with overlap."""
        if self.config.slide_seconds is None:
            slide = self.config.size_seconds / 2
        else:
            slide = self.config.slide_seconds
            
        num_windows = int(self.config.size_seconds / slide)
        
        for i in range(num_windows):
            offset = i * slide
            window_start = self._get_window_start(now - offset)
            window_id = f"sliding_{int(window_start)}_{i}"
            
            if window_id not in self._windows:
                self._windows[window_id] = Window(
                    window_id=window_id,
                    start_time=window_start,
                    end_time=window_start + self.config.size_seconds,
                )
                self._stats["windows_created"] += 1
                
            self._windows[window_id].add(item)
            
        self._close_expired_sliding_windows(now)
        
    def _close_expired_sliding_windows(self, now: float) -> None:
        """Close expired sliding windows."""
        to_close = []
        for window_id, window in self._windows.items():
            if now >= window.end_time:
                to_close.append(window_id)
                
        for window_id in to_close:
            self._close_window(window_id)
            
    def _update_session_window(self, item: T, now: float, key: Optional[str]) -> None:
        """Update session-based windows."""
        if key is None:
            key = "default"
            
        session_key = f"session_{key}"
        
        if session_key not in self._session_windows:
            self._session_windows[session_key] = now
            window_id = f"{session_key}_{int(now)}"
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=now,
                end_time=now + self.config.size_seconds,
            )
            self._stats["windows_created"] += 1
            
        last_time = self._session_windows[session_key]
        if now - last_time <= self.config.session_gap:
            self._session_windows[session_key] = now
            for window_id, window in self._windows.items():
                if window_id.startswith(session_key) and not window.is_closed:
                    window.end_time = now + self.config.size_seconds
                    window.add(item)
        else:
            window_id = f"{session_key}_{int(now)}"
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=now,
                end_time=now + self.config.size_seconds,
            )
            self._windows[window_id].add(item)
            self._session_windows[session_key] = now
            self._stats["windows_created"] += 1
            
        self._close_expired_session_windows(now)
        
    def _close_expired_session_windows(self, now: float) -> None:
        """Close session windows that have timed out."""
        to_close = []
        for window_id, window in self._windows.items():
            if now >= window.end_time:
                to_close.append(window_id)
                
        for window_id in to_close:
            self._close_window(window_id)
            
    def _update_count_window(self, item: T, key: Optional[str]) -> None:
        """Update count-based windows."""
        if key is None:
            key = "default"
            
        window_id = f"count_{key}"
        max_size = self.config.max_size or 100
        
        if window_id not in self._windows:
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=time.time(),
                end_time=time.time() + 86400,  # Dummy end time
            )
            self._stats["windows_created"] += 1
            
        window = self._windows[window_id]
        window.add(item)
        
        if window.size >= max_size:
            self._close_window(window_id)
            
    def _close_window(self, window_id: str) -> None:
        """Close a window and trigger aggregators."""
        if window_id not in self._windows:
            return
            
        window = self._windows[window_id]
        if window.is_closed:
            return
            
        window.close()
        self._closed_windows.append(window)
        self._stats["windows_closed"] += 1
        
        for aggregator in self._window_aggregators:
            try:
                aggregator(window)
            except Exception as e:
                logger.error(f"Window aggregator error: {e}")
                
    def get_active_windows(self) -> List[Window]:
        """Get all currently active windows."""
        with self._lock:
            return [w for w in self._windows.values() if not w.is_closed]
            
    def get_closed_windows(self, limit: int = 100) -> List[Window]:
        """Get recently closed windows."""
        with self._lock:
            return list(self._closed_windows)[-limit:]
            
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_windows": len([w for w in self._windows.values() if not w.is_closed]),
                "closed_windows": len(self._closed_windows),
            }
            
    def flush(self) -> None:
        """Force close all windows and flush pending data."""
        with self._lock:
            for window_id in list(self._windows.keys()):
                self._close_window(window_id)


class StatefulStreamProcessor(StreamProcessor[T]):
    """
    Stream processor with persistent state management.
    
    Example:
        processor = StatefulStreamProcessor(window_size=60)
        
        @processor.handler
        def process(item):
            return item
        processor.set_state("user_123", {"count": 0})
        
        def update_state(state, item):
            state["count"] += 1
            return state
        processor.add_state_transition("counter", update_state)
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._states: Dict[str, Any] = {}
        self._state_transitions: Dict[str, Callable] = {}
        
    def set_state(self, key: str, initial_state: Any) -> None:
        """Set initial state for a key."""
        self._states[key] = initial_state
        
    def get_state(self, key: str, default: Any = None) -> Any:
        """Get current state for a key."""
        return self._states.get(key, default)
        
    def update_state(self, key: str, item: T) -> Any:
        """Update state for a key using registered transitions."""
        current = self._states.get(key)
        
        if key in self._state_transitions:
            new_state = self._state_transitions[key](current, item)
            self._states[key] = new_state
            return new_state
            
        return current
        
    def add_state_transition(
        self,
        name: str,
        transition_fn: Callable[[Any, T], Any],
    ) -> None:
        """Register a state transition function."""
        self._state_transitions[name] = transition_fn
        
    def clear_state(self, key: str) -> None:
        """Clear state for a key."""
        self._states.pop(key, None)
        
    def get_all_states(self) -> Dict[str, Any]:
        """Get all current states."""
        return self._states.copy()
