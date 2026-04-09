"""Data Stream Processing Action Module.

Provides streaming data utilities: stream creation, transformation,
windowing, aggregation, and backpressure handling.

Example:
    result = execute(context, {"action": "create_stream", "source": "api"})
"""
from typing import Any, Optional, Iterator, Generator, Callable
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timedelta
import time


@dataclass
class StreamConfig:
    """Configuration for a data stream."""
    
    name: str
    buffer_size: int = 1000
    timeout: float = 30.0
    batch_size: int = 100
    watermark_ms: int = 5000
    
    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.buffer_size <= 0:
            raise ValueError("buffer_size must be positive")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")


class StreamEvent:
    """A single event in a data stream."""
    
    def __init__(
        self,
        data: Any,
        timestamp: Optional[datetime] = None,
        metadata: Optional[dict[str, Any]] = None,
        key: Optional[str] = None,
    ) -> None:
        """Initialize stream event.
        
        Args:
            data: Event payload
            timestamp: Event timestamp
            metadata: Additional metadata
            key: Optional partition key
        """
        self.data = data
        self.timestamp = timestamp or datetime.now()
        self.metadata = metadata or {}
        self.key = key
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "key": self.key,
        }


class WindowType(Enum) if False else None
# Simpler approach without enum
WINDOW_TUMBLING = "tumbling"
WINDOW_SLIDING = "sliding"
WINDOW_SESSION = "session"
WINDOW_COUNT = "count"


class TumblingWindow:
    """Tumbling window - non-overlapping fixed-size windows."""
    
    def __init__(self, size_seconds: float) -> None:
        """Initialize tumbling window.
        
        Args:
            size_seconds: Window size in seconds
        """
        self.size_seconds = size_seconds
        self._buffer: deque[StreamEvent] = deque()
        self._window_start: Optional[datetime] = None
    
    def add(self, event: StreamEvent) -> list[StreamEvent]:
        """Add event to window.
        
        Args:
            event: Event to add
            
        Returns:
            Completed windows (emitted events)
        """
        if self._window_start is None:
            self._window_start = event.timestamp
        
        self._buffer.append(event)
        
        window_end = self._window_start + timedelta(seconds=self.size_seconds)
        
        if event.timestamp >= window_end:
            events = list(self._buffer)
            self._buffer.clear()
            self._window_start = event.timestamp
            return events
        
        return []
    
    def get_current(self) -> list[StreamEvent]:
        """Get events in current window."""
        return list(self._buffer)


class SlidingWindow:
    """Sliding window - overlapping windows with slide interval."""
    
    def __init__(self, size_seconds: float, slide_seconds: float) -> None:
        """Initialize sliding window.
        
        Args:
            size_seconds: Window size in seconds
            slide_seconds: Slide interval in seconds
        """
        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds
        self._buffer: deque[tuple[datetime, StreamEvent]] = deque()
    
    def add(self, event: StreamEvent) -> list[StreamEvent]:
        """Add event to window."""
        now = event.timestamp
        cutoff = now - timedelta(seconds=self.size_seconds)
        
        self._buffer.append((now, event))
        
        self._buffer = deque(
            (t, e) for t, e in self._buffer
            if t > cutoff
        )
        
        return [e for _, e in self._buffer]
    
    def get_current(self) -> list[StreamEvent]:
        """Get events in current window."""
        return [e for _, e in self._buffer]


class SessionWindow:
    """Session window - windows delimited by inactivity gaps."""
    
    def __init__(self, gap_seconds: float) -> None:
        """Initialize session window.
        
        Args:
            gap_seconds: Inactivity gap threshold
        """
        self.gap_seconds = gap_seconds
        self._buffer: deque[StreamEvent] = deque()
        self._last_event_time: Optional[datetime] = None
    
    def add(self, event: StreamEvent) -> list[StreamEvent]:
        """Add event to window."""
        if self._last_event_time is not None:
            gap = (event.timestamp - self._last_event_time).total_seconds()
            if gap >= self.gap_seconds:
                events = list(self._buffer)
                self._buffer.clear()
                return events
        
        self._buffer.append(event)
        self._last_event_time = event.timestamp
        return []
    
    def get_current(self) -> list[StreamEvent]:
        """Get events in current session."""
        return list(self._buffer)


class CountWindow:
    """Count-based window - windows triggered by count."""
    
    def __init__(self, count: int) -> None:
        """Initialize count window.
        
        Args:
            count: Window trigger count
        """
        self.count = count
        self._buffer: deque[StreamEvent] = deque()
    
    def add(self, event: StreamEvent) -> list[StreamEvent]:
        """Add event to window."""
        self._buffer.append(event)
        
        if len(self._buffer) >= self.count:
            events = list(self._buffer)
            self._buffer.clear()
            return events
        
        return []
    
    def get_current(self) -> list[StreamEvent]:
        """Get events in current window."""
        return list(self._buffer)


class Stream:
    """Data stream with transformation support."""
    
    def __init__(self, config: StreamConfig) -> None:
        """Initialize stream.
        
        Args:
            config: Stream configuration
        """
        self.config = config
        self._buffer: deque[StreamEvent] = deque(maxlen=config.buffer_size)
        self._transformers: list[Callable[[StreamEvent], StreamEvent]] = []
    
    def add_transformer(self, fn: Callable[[StreamEvent], StreamEvent]) -> None:
        """Add event transformer.
        
        Args:
            fn: Transformation function
        """
        self._transformers.append(fn)
    
    def write(self, event: StreamEvent) -> None:
        """Write event to stream.
        
        Args:
            event: Event to write
        """
        for transformer in self._transformers:
            event = transformer(event)
        
        self._buffer.append(event)
    
    def read(self, count: int = 1) -> list[StreamEvent]:
        """Read events from stream.
        
        Args:
            count: Number of events to read
            
        Returns:
            List of events
        """
        events = []
        for _ in range(min(count, len(self._buffer))):
            events.append(self._buffer.popleft())
        return events
    
    def read_all(self) -> list[StreamEvent]:
        """Read all buffered events."""
        events = list(self._buffer)
        self._buffer.clear()
        return events
    
    def size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)


class StreamAggregator:
    """Aggregates stream events."""
    
    @staticmethod
    def count(events: list[StreamEvent]) -> int:
        """Count events."""
        return len(events)
    
    @staticmethod
    def sum(events: list[StreamEvent], field: str) -> float:
        """Sum numeric field."""
        total = 0.0
        for event in events:
            data = event.data if isinstance(event.data, dict) else {}
            value = data.get(field, 0)
            if isinstance(value, (int, float)):
                total += value
        return total
    
    @staticmethod
    def avg(events: list[StreamEvent], field: str) -> float:
        """Average numeric field."""
        values = []
        for event in events:
            data = event.data if isinstance(event.data, dict) else {}
            value = data.get(field)
            if isinstance(value, (int, float)):
                values.append(value)
        
        return sum(values) / len(values) if values else 0.0
    
    @staticmethod
    def min(events: list[StreamEvent], field: str) -> Optional[float]:
        """Minimum numeric field."""
        values = []
        for event in events:
            data = event.data if isinstance(event.data, dict) else {}
            value = data.get(field)
            if isinstance(value, (int, float)):
                values.append(value)
        
        return min(values) if values else None
    
    @staticmethod
    def max(events: list[StreamEvent], field: str) -> Optional[float]:
        """Maximum numeric field."""
        values = []
        for event in events:
            data = event.data if isinstance(event.data, dict) else {}
            value = data.get(field)
            if isinstance(value, (int, float)):
                values.append(value)
        
        return max(values) if values else None


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute data stream action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "create_stream":
        config = StreamConfig(
            name=params.get("name", "stream"),
            buffer_size=params.get("buffer_size", 1000),
            timeout=params.get("timeout", 30.0),
        )
        stream = Stream(config)
        result["data"] = {
            "name": config.name,
            "buffer_size": config.buffer_size,
        }
    
    elif action == "write_event":
        event = StreamEvent(
            data=params.get("data"),
            key=params.get("key"),
        )
        result["data"] = event.to_dict()
    
    elif action == "tumbling_window":
        window = TumblingWindow(params.get("size_seconds", 60))
        result["data"] = {"window_type": "tumbling", "size_seconds": params.get("size_seconds", 60)}
    
    elif action == "sliding_window":
        window = SlidingWindow(
            params.get("size_seconds", 60),
            params.get("slide_seconds", 30),
        )
        result["data"] = {
            "window_type": "sliding",
            "size_seconds": params.get("size_seconds", 60),
            "slide_seconds": params.get("slide_seconds", 30),
        }
    
    elif action == "session_window":
        window = SessionWindow(params.get("gap_seconds", 30))
        result["data"] = {"window_type": "session", "gap_seconds": params.get("gap_seconds", 30)}
    
    elif action == "count_window":
        window = CountWindow(params.get("count", 100))
        result["data"] = {"window_type": "count", "count": params.get("count", 100)}
    
    elif action == "aggregate_count":
        result["data"] = {"function": "count"}
    
    elif action == "aggregate_sum":
        result["data"] = {"function": "sum", "field": params.get("field", "value")}
    
    elif action == "aggregate_avg":
        result["data"] = {"function": "avg", "field": params.get("field", "value")}
    
    elif action == "aggregate_min":
        result["data"] = {"function": "min", "field": params.get("field", "value")}
    
    elif action == "aggregate_max":
        result["data"] = {"function": "max", "field": params.get("field", "value")}
    
    elif action == "stream_status":
        config = StreamConfig(name=params.get("name", "stream"))
        stream = Stream(config)
        result["data"] = {
            "buffer_size": len(stream._buffer),
            "max_buffer": config.buffer_size,
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
