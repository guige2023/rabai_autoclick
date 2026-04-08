"""Stream processor action module for RabAI AutoClick.

Provides stream processing:
- StreamProcessor: Process data streams
- WindowedStream: Windowed stream processing
- StreamFilter: Filter stream data
- StreamAggregator: Aggregate stream data
- StreamJoiner: Join multiple streams
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class StreamConfig:
    """Stream configuration."""
    buffer_size: int = 1000
    flush_interval: float = 5.0
    watermark_delay: float = 0.0


@dataclass
class Window:
    """Stream window."""
    window_id: str
    start_time: float
    end_time: float
    data: List[Any]


class StreamProcessor:
    """Stream data processor."""

    def __init__(self, config: Optional[StreamConfig] = None):
        self.config = config or StreamConfig()
        self._buffer: deque = deque(maxlen=self.config.buffer_size)
        self._handlers: List[Callable] = []
        self._lock = threading.RLock()
        self._running = False
        self._stats = {"processed": 0, "dropped": 0, "errors": 0}

    def add_handler(self, handler: Callable[[Any], Any]) -> "StreamProcessor":
        """Add data handler."""
        self._handlers.append(handler)
        return self

    def push(self, data: Any) -> bool:
        """Push data to stream."""
        with self._lock:
            if len(self._buffer) >= self.config.buffer_size:
                self._stats["dropped"] += 1
                return False

            self._buffer.append(data)
            self._process_buffer()
            return True

    def push_batch(self, data: List[Any]) -> int:
        """Push batch of data."""
        pushed = 0
        for item in data:
            if self.push(item):
                pushed += 1
        return pushed

    def _process_buffer(self):
        """Process buffered data."""
        while self._buffer:
            data = self._buffer.popleft()
            try:
                for handler in self._handlers:
                    data = handler(data)
                self._stats["processed"] += 1
            except Exception:
                self._stats["errors"] += 1

    def flush(self) -> int:
        """Flush buffer."""
        with self._lock:
            count = len(self._buffer)
            self._process_buffer()
            return count

    def get_stats(self) -> Dict[str, int]:
        """Get stream statistics."""
        with self._lock:
            return dict(self._stats)


class WindowedStream:
    """Windowed stream processor."""

    def __init__(self, window_size: float, slide_size: Optional[float] = None):
        self.window_size = window_size
        self.slide_size = slide_size or window_size
        self._windows: Dict[str, Window] = {}
        self._lock = threading.RLock()
        self._window_count = 0

    def add(self, data: Any, timestamp: Optional[float] = None) -> List[Window]:
        """Add data to windows."""
        ts = timestamp or time.time()
        triggered_windows = []

        with self._lock:
            window_key = self._get_window_key(ts)
            if window_key not in self._windows:
                self._window_count += 1
                self._windows[window_key] = Window(
                    window_id=f"window_{self._window_count}",
                    start_time=ts - (ts % self.slide_size),
                    end_time=ts - (ts % self.slide_size) + self.window_size,
                    data=[],
                )

            self._windows[window_key].data.append(data)

            for key, window in list(self._windows.items()):
                if window.end_time <= ts:
                    triggered_windows.append(window)
                    del self._windows[key]

        return triggered_windows

    def _get_window_key(self, timestamp: float) -> str:
        """Get window key for timestamp."""
        start = timestamp - (timestamp % self.slide_size)
        return f"{start}_{self.window_size}"

    def get_active_windows(self) -> List[Window]:
        """Get active windows."""
        with self._lock:
            return list(self._windows.values())

    def clear(self):
        """Clear all windows."""
        with self._lock:
            self._windows.clear()


class StreamFilter:
    """Filter stream data."""

    def __init__(self):
        self._filters: List[Callable[[Any], bool]] = []

    def add_filter(self, filter_fn: Callable[[Any], bool]) -> "StreamFilter":
        """Add filter function."""
        self._filters.append(filter_fn)
        return self

    def filter(self, data: List[Any]) -> List[Any]:
        """Filter data."""
        result = data
        for filter_fn in self._filters:
            result = [item for item in result if filter_fn(item)]
        return result


class StreamAggregator:
    """Aggregate stream data."""

    def __init__(self):
        self._aggregations: Dict[str, Callable] = {}

    def add_aggregation(self, name: str, agg_fn: Callable[[List], Any]) -> "StreamAggregator":
        """Add aggregation function."""
        self._aggregations[name] = agg_fn
        return self

    def aggregate(self, data: List[Any]) -> Dict[str, Any]:
        """Aggregate data."""
        result = {}
        for name, agg_fn in self._aggregations.items():
            try:
                result[name] = agg_fn(data)
            except Exception:
                result[name] = None
        return result


class StreamJoiner:
    """Join multiple streams."""

    def __init__(self, key_field: str, window_size: float = 60.0):
        self.key_field = key_field
        self.window_size = window_size
        self._streams: Dict[str, List[Dict]] = {}
        self._lock = threading.RLock()

    def add_stream(self, name: str, data: List[Dict]):
        """Add stream data."""
        with self._lock:
            if name not in self._streams:
                self._streams[name] = []
            self._streams[name].extend(data)
            self._cleanup_streams()

    def join(self) -> List[Dict]:
        """Join all streams."""
        with self._lock:
            if len(self._streams) < 2:
                return []

            keys: set = set()
            for stream_data in self._streams.values():
                for item in stream_data:
                    key = item.get(self.key_field)
                    if key is not None:
                        keys.add(key)

            results = []
            for key in keys:
                joined = {self.key_field: key}
                all_have_key = True

                for name, stream_data in self._streams.items():
                    matching = [item for item in stream_data if item.get(self.key_field) == key]
                    if matching:
                        joined[name] = matching[0]
                    else:
                        all_have_key = False
                        break

                if all_have_key:
                    results.append(joined)

            return results

    def _cleanup_streams(self):
        """Cleanup old stream data."""
        now = time.time()
        for name in self._streams:
            self._streams[name] = [
                item for item in self._streams[name]
                if item.get("timestamp", now) > now - self.window_size
            ]


class StreamProcessorAction(BaseAction):
    """Stream processor action."""
    action_type = "stream_processor"
    display_name = "流处理器"
    description = "数据流实时处理"

    def __init__(self):
        super().__init__()
        self._processors: Dict[str, StreamProcessor] = {}
        self._windowed: Dict[str, WindowedStream] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "push")

            if operation == "create":
                return self._create_processor(params)
            elif operation == "push":
                return self._push(params)
            elif operation == "flush":
                return self._flush(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "window":
                return self._add_window(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream error: {str(e)}")

    def _create_processor(self, params: Dict) -> ActionResult:
        """Create stream processor."""
        name = params.get("name", "default")
        config = StreamConfig(
            buffer_size=params.get("buffer_size", 1000),
            flush_interval=params.get("flush_interval", 5.0),
        )

        processor = StreamProcessor(config)
        self._processors[name] = processor

        return ActionResult(success=True, message=f"Stream processor '{name}' created")

    def _push(self, params: Dict) -> ActionResult:
        """Push data to stream."""
        name = params.get("name", "default")
        data = params.get("data")

        processor = self._processors.get(name)
        if not processor:
            processor = StreamProcessor()
            self._processors[name] = processor

        if isinstance(data, list):
            count = processor.push_batch(data)
            return ActionResult(success=True, message=f"Pushed {count} items")
        else:
            success = processor.push(data)
            return ActionResult(success=success, message="Pushed" if success else "Buffer full")

    def _flush(self, params: Dict) -> ActionResult:
        """Flush stream processor."""
        name = params.get("name", "default")

        processor = self._processors.get(name)
        if not processor:
            return ActionResult(success=False, message=f"Processor '{name}' not found")

        count = processor.flush()
        return ActionResult(success=True, message=f"Flushed {count} items")

    def _get_stats(self, params: Dict) -> ActionResult:
        """Get stream statistics."""
        name = params.get("name", "default")

        processor = self._processors.get(name)
        if not processor:
            return ActionResult(success=False, message=f"Processor '{name}' not found")

        stats = processor.get_stats()
        return ActionResult(success=True, message="Stats retrieved", data=stats)

    def _add_window(self, params: Dict) -> ActionResult:
        """Add data to windowed stream."""
        name = params.get("name", "default")
        data = params.get("data")
        window_size = params.get("window_size", 60.0)
        slide_size = params.get("slide_size")
        timestamp = params.get("timestamp")

        if name not in self._windowed:
            self._windowed[name] = WindowedStream(window_size, slide_size)

        stream = self._windowed[name]
        windows = stream.add(data, timestamp)

        result = {
            "windows_triggered": len(windows),
            "active_windows": len(stream.get_active_windows()),
        }

        if windows:
            result["triggered_data"] = [
                {"window_id": w.window_id, "count": len(w.data)} for w in windows
            ]

        return ActionResult(success=True, message="Data added to window", data=result)
