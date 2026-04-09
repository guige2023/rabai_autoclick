"""Data Stream Processing Action Module.

Provides real-time data stream processing capabilities including
windowing, aggregation, filtering, and transformation of event streams.

Example:
    >>> from actions.data.data_stream_action import DataStreamProcessor
    >>> processor = DataStreamProcessor()
    >>> await processor.process_stream(source, sink)
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Generic
import threading


T = TypeVar('T')


class WindowType(Enum):
    """Stream window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"
    GLOBAL = "global"


class StreamStatus(Enum):
    """Status of a stream processor."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class StreamEvent:
    """Represents a stream event.
    
    Attributes:
        event_id: Unique event identifier
        timestamp: Event timestamp
        data: Event payload
        key: Optional partitioning key
        metadata: Additional event metadata
    """
    event_id: str
    timestamp: datetime
    data: Any
    key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Window:
    """Represents a stream processing window.
    
    Attributes:
        window_id: Unique window identifier
        window_type: Type of window
        start_time: Window start timestamp
        end_time: Window end timestamp
        events: Events in this window
        state: Window state data
    """
    window_id: str
    window_type: WindowType
    start_time: datetime
    end_time: datetime
    events: List[StreamEvent] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WindowResult:
    """Result of processing a window.
    
    Attributes:
        window_id: Window identifier
        window_type: Type of window
        event_count: Number of events processed
        result: Aggregation/transformation result
        duration: Processing duration
        errors: Any errors during processing
    """
    window_id: str
    window_type: WindowType
    event_count: int = 0
    result: Any = None
    duration: float = 0.0
    errors: List[str] = field(default_factory=list)


@dataclass
class StreamConfig:
    """Configuration for stream processing.
    
    Attributes:
        window_type: Type of window to use
        window_size: Window size in seconds (or count for COUNT window)
        window_slide: Slide interval for sliding windows
        session_gap: Gap for session windows
        max_window_size: Maximum events in a window (for COUNT type)
        enable_watermarks: Whether to use watermarks for late events
        watermark_threshold: Late event threshold in seconds
        buffer_size: Event buffer size
    """
    window_type: WindowType = WindowType.TUMBLING
    window_size: float = 60.0
    window_slide: float = 30.0
    session_gap: float = 5.0
    max_window_size: int = 1000
    enable_watermarks: bool = True
    watermark_threshold: float = 10.0
    buffer_size: int = 10000


@dataclass
class StreamMetrics:
    """Stream processing metrics.
    
    Attributes:
        total_events: Total events processed
        total_windows: Total windows processed
        windows_by_type: Window counts by type
        avg_latency: Average event processing latency
        throughput: Events per second
        error_count: Number of processing errors
    """
    total_events: int = 0
    total_windows: int = 0
    windows_by_type: Dict[str, int] = field(default_factory=dict)
    avg_latency: float = 0.0
    throughput: float = 0.0
    error_count: int = 0


class DataStreamProcessor:
    """Handles real-time data stream processing.
    
    Provides windowed stream processing with support for
    various window types and aggregation operations.
    
    Attributes:
        config: Stream processing configuration
    
    Example:
        >>> processor = DataStreamProcessor()
        >>> processor.add_aggregation("avg", my_aggregator)
        >>> await processor.process_stream(events, output)
    """
    
    def __init__(self, config: Optional[StreamConfig] = None):
        """Initialize the stream processor.
        
        Args:
            config: Stream configuration. Uses defaults if not provided.
        """
        self.config = config or StreamConfig()
        self._windows: Dict[str, Window] = {}
        self._event_buffer: deque = deque(maxlen=self.config.buffer_size)
        self._aggregations: Dict[str, Callable] = {}
        self._filters: List[Callable[[StreamEvent], bool]] = []
        self._transformers: List[Callable[[StreamEvent], StreamEvent]] = []
        self._status = StreamStatus.IDLE
        self._metrics = StreamMetrics()
        self._lock = threading.RLock()
        self._window_counter = 0
        self._event_counter = 0
        self._processing_task: Optional[asyncio.Task] = None
        self._last_metrics_update = time.time()
        self._latency_sum = 0.0
        self._latency_count = 0
    
    def add_filter(self, filter_fn: Callable[[StreamEvent], bool]) -> None:
        """Add an event filter.
        
        Args:
            filter_fn: Function that returns True to keep the event
        """
        with self._lock:
            self._filters.append(filter_fn)
    
    def add_transformer(self, transformer_fn: Callable[[StreamEvent], StreamEvent]) -> None:
        """Add an event transformer.
        
        Args:
            transformer_fn: Function to transform events
        """
        with self._lock:
            self._transformers.append(transformer_fn)
    
    def add_aggregation(self, name: str, agg_fn: Callable[[List[StreamEvent]], Any]) -> None:
        """Add a named aggregation function.
        
        Args:
            name: Aggregation name
            agg_fn: Function that takes a list of events and returns aggregated result
        """
        with self._lock:
            self._aggregations[name] = agg_fn
    
    async def process_stream(
        self,
        source: Any,  # Async iterable of events
            sink: Callable[[WindowResult], Any],
            window_id: Optional[str] = None
    ) -> None:
        """Process an event stream.
        
        Args:
            source: Async iterable producing StreamEvent objects
            sink: Async function to receive WindowResult objects
            window_id: Optional identifier for this stream
        """
        self._status = StreamStatus.RUNNING
        
        try:
            async for event in self._wrap_source(source):
                processed = self._apply_transformers(event)
                
                if self._should_drop(processed):
                    continue
                
                self._emit_to_window(processed)
                
                # Process completed windows
                completed = self._get_completed_windows()
                for window in completed:
                    result = await self._process_window(window)
                    await sink(result)
                    self._remove_window(window.window_id)
            
            # Process any remaining windows
            remaining = self._get_all_windows()
            for window in remaining:
                result = await self._process_window(window)
                await sink(result)
                self._remove_window(window.window_id)
        
        except Exception as e:
            self._status = StreamStatus.ERROR
            raise
        finally:
            self._status = StreamStatus.STOPPED
    
    async def _wrap_source(self, source: Any) -> AsyncIterator[StreamEvent]:
        """Wrap an async source into an async iterator.
        
        Args:
            source: Async iterable
        
        Yields:
            StreamEvent objects
        """
        if hasattr(source, '__anext__'):
            # Already an async iterator
            async for event in source:
                yield event
        else:
            # Sync iterable - wrap in async
            for event in source:
                if isinstance(event, StreamEvent):
                    yield event
                else:
                    yield self._create_event_from_data(event)
    
    def _create_event_from_data(self, data: Any) -> StreamEvent:
        """Create a StreamEvent from raw data.
        
        Args:
            data: Raw event data
        
        Returns:
            StreamEvent
        """
        self._event_counter += 1
        
        if isinstance(data, dict):
            return StreamEvent(
                event_id=data.get("event_id", f"evt_{self._event_counter}"),
                timestamp=datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat())),
                data=data.get("data", data),
                key=data.get("key"),
                metadata=data.get("metadata", {})
            )
        
        return StreamEvent(
            event_id=f"evt_{self._event_counter}",
            timestamp=datetime.now(),
            data=data
        )
    
    def _should_drop(self, event: StreamEvent) -> bool:
        """Check if an event should be dropped.
        
        Args:
            event: Event to check
        
        Returns:
            True if event should be dropped
        """
        for filter_fn in self._filters:
            if not filter_fn(event):
                return True
        return False
    
    def _apply_transformers(self, event: StreamEvent) -> StreamEvent:
        """Apply all transformers to an event.
        
        Args:
            event: Event to transform
        
        Returns:
            Transformed event
        """
        result = event
        for transformer in self._transformers:
            result = transformer(result)
            if result is None:
                return event  # Transformer dropped the event
        return result
    
    def _emit_to_window(self, event: StreamEvent) -> None:
        """Add an event to the appropriate window.
        
        Args:
            event: Event to add
        """
        with self._lock:
            window_key = event.key or "_global"
            window_id = self._get_or_create_window(window_key)
            window = self._windows[window_id]
            
            window.events.append(event)
            
            # Update watermark
            if self.config.enable_watermarks:
                event_time = event.timestamp.timestamp()
                current_watermark = window.state.get("_watermark", event_time)
                window.state["_watermark"] = min(current_watermark, event_time)
            
            # Check for count-based window close
            if self.config.window_type == WindowType.COUNT:
                if len(window.events) >= self.config.max_window_size:
                    window.end_time = datetime.now()
    
    def _get_or_create_window(self, key: str) -> str:
        """Get or create a window for a key.
        
        Args:
            key: Partitioning key
        
        Returns:
            Window ID
        """
        window_key = f"{key}_{self.config.window_type.value}"
        
        if window_key in self._windows:
            window = self._windows[window_key]
            
            # Check if window should be closed
            if self._should_close_window(window):
                self._window_counter += 1
                new_id = f"window_{self._window_counter}_{int(time.time() * 1000)}"
                window = self._create_window(new_id, key)
                self._windows[new_id] = window
                window_key = new_id
            else:
                return window_key
        else:
            self._window_counter += 1
            window_id = f"window_{self._window_counter}_{int(time.time() * 1000)}"
            window = self._create_window(window_id, key)
            self._windows[window_id] = window
            window_key = window_id
        
        return window_key
    
    def _create_window(self, window_id: str, key: str) -> Window:
        """Create a new window.
        
        Args:
            window_id: Window identifier
            key: Partitioning key
        
        Returns:
            Created window
        """
        now = datetime.now()
        delta = timedelta(seconds=self.config.window_size)
        
        return Window(
            window_id=window_id,
            window_type=self.config.window_type,
            start_time=now,
            end_time=now + delta,
            events=[],
            state={"_key": key, "_watermark": now.timestamp()}
        )
    
    def _should_close_window(self, window: Window) -> bool:
        """Check if a window should be closed.
        
        Args:
            window: Window to check
        
        Returns:
            True if window should be closed
        """
        if window.end_time is not None:
            return datetime.now() >= window.end_time
        
        if self.config.window_type == WindowType.COUNT:
            return len(window.events) >= self.config.max_window_size
        
        return False
    
    def _get_completed_windows(self) -> List[Window]:
        """Get all windows that should be processed.
        
        Returns:
            List of completed windows
        """
        completed = []
        
        for window in self._windows.values():
            if self._should_close_window(window):
                completed.append(window)
        
        return completed
    
    def _get_all_windows(self) -> List[Window]:
        """Get all remaining windows.
        
        Returns:
            List of all windows
        """
        return list(self._windows.values())
    
    def _remove_window(self, window_id: str) -> None:
        """Remove a window.
        
        Args:
            window_id: Window to remove
        """
        with self._lock:
            if window_id in self._windows:
                del self._windows[window_id]
    
    async def _process_window(self, window: Window) -> WindowResult:
        """Process a completed window.
        
        Args:
            window: Window to process
        
        Returns:
            WindowResult
        """
        start_time = time.time()
        result = WindowResult(
            window_id=window.window_id,
            window_type=window.window_type,
            event_count=len(window.events)
        )
        
        try:
            if not window.events:
                return result
            
            # Apply aggregations
            agg_results = {}
            for name, agg_fn in self._aggregations.items():
                try:
                    agg_results[name] = agg_fn(window.events)
                except Exception as e:
                    result.errors.append(f"Aggregation '{name}' error: {str(e)}")
            
            result.result = agg_results if agg_results else window.events[-1].data
        
        except Exception as e:
            result.errors.append(f"Window processing error: {str(e)}")
        
        result.duration = time.time() - start_time
        
        # Update metrics
        with self._lock:
            self._metrics.total_windows += 1
            window_type_key = window.window_type.value
            self._metrics.windows_by_type[window_type_key] = (
                self._metrics.windows_by_type.get(window_type_key, 0) + 1
            )
        
        return result
    
    def _update_latency_metrics(self, latency: float) -> None:
        """Update latency metrics.
        
        Args:
            latency: Event processing latency
        """
        self._latency_sum += latency
        self._latency_count += 1
        self._metrics.avg_latency = self._latency_sum / self._latency_count
        
        # Calculate throughput
        elapsed = time.time() - self._last_metrics_update
        if elapsed >= 1.0:
            self._metrics.throughput = self._metrics.total_events / elapsed
            self._last_metrics_update = time.time()
    
    def pause(self) -> None:
        """Pause stream processing."""
        with self._lock:
            self._status = StreamStatus.PAUSED
    
    def resume(self) -> None:
        """Resume stream processing."""
        with self._lock:
            self._status = StreamStatus.RUNNING
    
    def stop(self) -> None:
        """Stop stream processing."""
        with self._lock:
            self._status = StreamStatus.STOPPED
            if self._processing_task:
                self._processing_task.cancel()
    
    def get_status(self) -> StreamStatus:
        """Get current processing status.
        
        Returns:
            StreamStatus
        """
        with self._lock:
            return self._status
    
    def get_metrics(self) -> StreamMetrics:
        """Get current processing metrics.
        
        Returns:
            StreamMetrics
        """
        with self._lock:
            return StreamMetrics(
                total_events=self._metrics.total_events,
                total_windows=self._metrics.total_windows,
                windows_by_type=dict(self._metrics.windows_by_type),
                avg_latency=self._metrics.avg_latency,
                throughput=self._metrics.throughput,
                error_count=self._metrics.error_count
            )
    
    def clear_state(self) -> None:
        """Clear all windows and buffers."""
        with self._lock:
            self._windows.clear()
            self._event_buffer.clear()
            self._metrics = StreamMetrics()


# Type alias for async iterator
from typing import AsyncIterator
