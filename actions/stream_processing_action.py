"""Stream processing action module for RabAI AutoClick.

Provides stream processing primitives: windowing, aggregation,
filtering, and stateful operations on data streams.
"""

from __future__ import annotations

import sys
import os
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque, defaultdict
from dataclasses import dataclass, field
from enum import Enum
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WindowType(Enum):
    """Stream window types."""
    TUMBLING = "tumbling"      # Non-overlapping fixed windows
    SLIDING = "sliding"         # Overlapping fixed windows
    SESSION = "session"         # Activity-based session windows
    COUNT = "count"             # Fixed count windows


@dataclass
class StreamWindow:
    """A stream window with its elements."""
    window_id: int
    window_type: str
    start_time: float
    end_time: float
    elements: List[Any] = field(default_factory=list)
    is_closed: bool = False


class TumblingWindowAction(BaseAction):
    """Tumbling (non-overlapping) window processor.
    
    Elements are assigned to fixed, non-overlapping windows.
    When a window closes, an aggregate is emitted.
    
    Args:
        window_size_seconds: Window duration in seconds
        window_size_count: Alternative: window by element count
    """

    def __init__(self, window_size_seconds: float = 10.0):
        super().__init__()
        self.window_size_seconds = window_size_seconds
        self._windows: Dict[int, StreamWindow] = {}
        self._current_window_id = 0
        self._closed_windows: List[Dict[str, Any]] = []

    def execute(
        self,
        action: str,
        element: Optional[Any] = None,
        aggregate_func: str = "count",
        field_name: Optional[str] = None,
        window_size_count: Optional[int] = None
    ) -> ActionResult:
        try:
            now = time.time()

            if action == "add":
                if element is None:
                    return ActionResult(success=False, error="element required")

                # Determine window
                if window_size_count:
                    # Count-based window
                    total_elements = sum(w.elements.__len__() for w in self._windows.values()) + 1
                    window_id = total_elements // window_size_count
                else:
                    # Time-based
                    window_id = int(now / self.window_size_seconds)

                if window_id not in self._windows:
                    start = window_id * self.window_size_seconds
                    self._windows[window_id] = StreamWindow(
                        window_id=window_id,
                        window_type="tumbling",
                        start_time=start,
                        end_time=start + self.window_size_seconds
                    )

                self._windows[window_id].elements.append(element)

                # Close expired windows
                for wid, win in list(self._windows.items()):
                    if win.end_time <= now and not win.is_closed:
                        win.is_closed = True

                return ActionResult(success=True, data={
                    "window_id": window_id,
                    "window_size": len(self._windows[window_id].elements),
                    "closed_count": sum(1 for w in self._windows.values() if w.is_closed)
                })

            elif action == "get_closed":
                closed = [w for w in self._windows.values() if w.is_closed]
                if aggregate_func == "count":
                    results = [{"window_id": w.window_id, "count": len(w.elements),
                               "start": w.start_time} for w in closed]
                elif aggregate_func == "sum" and field_name:
                    results = [{"window_id": w.window_id,
                               "sum": sum(float(e.get(field_name, 0)) for e in w.elements if isinstance(e, dict)),
                               "start": w.start_time} for w in closed]
                else:
                    results = [{"window_id": w.window_id, "count": len(w.elements),
                               "start": w.start_time} for w in closed]

                return ActionResult(success=True, data={
                    "closed_windows": results,
                    "count": len(closed)
                })

            elif action == "clear":
                self._windows.clear()
                return ActionResult(success=True, data={"cleared": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class SlidingWindowAction(BaseAction):
    """Sliding (overlapping) window processor.
    
    Maintains a rolling window of the most recent N elements
    or last T seconds worth of elements.
    
    Args:
        size: Window size (elements or seconds)
        slide: Slide step (elements or seconds)
        unit: "count" or "time"
    """

    def __init__(self, size: int = 10, slide: int = 1, unit: str = "count"):
        super().__init__()
        self.size = size
        self.slide = slide
        self.unit = unit
        self._buffer: deque = deque(maxlen=size if unit == "count" else 0)
        self._timestamps: deque = deque(maxlen=size if unit == "count" else 0)
        self._window_count = 0

    def execute(
        self,
        action: str,
        element: Optional[Any] = None,
        aggregate_func: str = "avg",
        field_name: Optional[str] = None
    ) -> ActionResult:
        try:
            if action == "add":
                if element is None:
                    return ActionResult(success=False, error="element required")

                now = time.time()
                if self.unit == "count":
                    self._buffer.append(element)
                else:
                    self._buffer.append(element)
                    self._timestamps.append(now)
                    # Evict old elements
                    cutoff = now - self.size
                    while self._timestamps and self._timestamps[0] < cutoff:
                        self._buffer.popleft()
                        self._timestamps.popleft()

                if len(self._buffer) >= self.size:
                    self._window_count += 1

                return ActionResult(success=True, data={
                    "window_size": len(self._buffer),
                    "window_count": self._window_count,
                    "is_full": len(self._buffer) >= self.size
                })

            elif action == "aggregate":
                if not self._buffer:
                    return ActionResult(success=False, error="Empty window")

                if field_name and isinstance(self._buffer[0], dict):
                    values = [float(e.get(field_name, 0)) for e in self._buffer if isinstance(e, dict)]
                else:
                    values = [float(e) for e in self._buffer if isinstance(e, (int, float))]

                if not values:
                    return ActionResult(success=False, error="No numeric values for aggregation")

                if aggregate_func == "avg":
                    result = sum(values) / len(values)
                elif aggregate_func == "sum":
                    result = sum(values)
                elif aggregate_func == "min":
                    result = min(values)
                elif aggregate_func == "max":
                    result = max(values)
                elif aggregate_func == "count":
                    result = float(len(values))
                elif aggregate_func == "std":
                    mean = sum(values) / len(values)
                    variance = sum((v - mean) ** 2 for v in values) / len(values)
                    result = variance ** 0.5
                else:
                    return ActionResult(success=False, error=f"Unknown aggregate: {aggregate_func}")

                return ActionResult(success=True, data={
                    "aggregate": round(result, 6),
                    "function": aggregate_func,
                    "window_size": len(self._buffer)
                })

            elif action == "clear":
                self._buffer.clear()
                self._timestamps.clear()
                return ActionResult(success=True, data={"cleared": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class StreamJoinAction(BaseAction):
    """Join two streams based on a key and time window.
    
    Joins elements from stream A and stream B that share
    the same key and arrive within a time window of each other.
    
    Args:
        window_seconds: Time window for joining
    """

    def __init__(self, window_seconds: float = 5.0):
        super().__init__()
        self.window_seconds = window_seconds
        self._stream_a: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._stream_b: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._timestamps_a: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._timestamps_b: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

    def execute(
        self,
        action: str,
        stream: Optional[str] = None,
        key: Optional[str] = None,
        element: Optional[Any] = None,
        join_key_field: str = "key",
        timeout_seconds: Optional[float] = None
    ) -> ActionResult:
        try:
            timeout = timeout_seconds or self.window_seconds
            now = time.time()

            if action == "add":
                if not stream or key is None or element is None:
                    return ActionResult(success=False, error="stream, key, element required")
                if stream not in ("a", "b"):
                    return ActionResult(success=False, error="stream must be 'a' or 'b'")

                if stream == "a":
                    self._stream_a[key].append(element)
                    self._timestamps_a[key].append(now)
                else:
                    self._stream_b[key].append(element)
                    self._timestamps_b[key].append(now)

                return ActionResult(success=True, data={
                    "stream": stream, "key": key,
                    "stream_a_size": len(self._stream_a[key]),
                    "stream_b_size": len(self._stream_b[key])
                })

            elif action == "join":
                if not key:
                    return ActionResult(success=False, error="key required")

                joined = []
                cutoff = now - timeout

                # Clean and match
                a_deque = self._stream_a[key]
                b_deque = self._stream_b[key]
                ta = self._timestamps_a[key]
                tb = self._timestamps_b[key]

                # Match elements within window
                for i, (elem_a, ts_a) in enumerate(zip(a_deque, ta)):
                    if ts_a < cutoff:
                        continue
                    for j, (elem_b, ts_b) in enumerate(zip(b_deque, tb)):
                        if abs(ts_a - ts_b) <= timeout:
                            joined.append({"from_a": elem_a, "from_b": elem_b, "time_diff": round(abs(ts_a - ts_b), 4)})

                return ActionResult(success=True, data={
                    "key": key,
                    "joined_count": len(joined),
                    "joins": joined[:50]
                })

            elif action == "clear":
                self._stream_a.clear()
                self._stream_b.clear()
                self._timestamps_a.clear()
                self._timestamps_b.clear()
                return ActionResult(success=True, data={"cleared": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
