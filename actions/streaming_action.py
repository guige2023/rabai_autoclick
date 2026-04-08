"""Streaming action module for RabAI AutoClick.

Provides real-time data stream processing with windowing, aggregation,
filtering, and support for various streaming sources.
"""

import sys
import os
import json
import time
import asyncio
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from collections import deque
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WindowType(Enum):
    """Stream windowing types."""
    TUMBLING = "tumbling"       # Fixed-size, non-overlapping
    SLIDING = "sliding"         # Fixed-size, overlapping
    SESSION = "session"         # Activity-based windows
    COUNT = "count"             # Count-based windows


class AggregationType(Enum):
    """Aggregation functions for streams."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    LIST = "list"
    DISTINCT = "distinct"


@dataclass
class StreamConfig:
    """Configuration for a data stream."""
    name: str
    source_type: str = "memory"  # memory, file, kafka, redis
    source_uri: str = ""
    buffer_size: int = 1000
    emit_interval_ms: int = 1000
    max_age_seconds: float = 60.0
    description: str = ""


@dataclass
class WindowConfig:
    """Configuration for stream windowing."""
    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 10.0
    slide_seconds: float = 10.0
    max_size: int = 100  # For count windows


@dataclass
class StreamEvent:
    """Represents a single event in the stream."""
    event_id: str
    timestamp: float
    data: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


class StreamWindow:
    """A window of stream events with aggregation."""
    
    def __init__(self, config: WindowConfig, window_id: str, start_time: float):
        self.config = config
        self.window_id = window_id
        self.start_time = start_time
        self.end_time = start_time + config.size_seconds
        self._events: List[StreamEvent] = []
        self._aggregations: Dict[str, Any] = {}
    
    def add_event(self, event: StreamEvent) -> bool:
        """Add an event to this window.
        
        Returns True if event was added, False if window is closed.
        """
        if event.timestamp > self.end_time:
            return False
        self._events.append(event)
        self._update_aggregations(event)
        return True
    
    def is_expired(self, current_time: float) -> bool:
        """Check if this window has expired and should be evicted."""
        return current_time > self.end_time + self.config.max_age_seconds
    
    def _update_aggregations(self, event: StreamEvent) -> None:
        """Update running aggregations with new event."""
        data = event.data
        if isinstance(data, dict):
            for key, value in data.items():
                if key not in self._aggregations:
                    self._aggregations[key] = {
                        "sum": 0.0, "count": 0, "min": None, "max": None,
                        "values": []
                    }
                agg = self._aggregations[key]
                agg["count"] += 1
                if isinstance(value, (int, float)):
                    agg["sum"] += value
                    if agg["min"] is None or value < agg["min"]:
                        agg["min"] = value
                    if agg["max"] is None or value > agg["max"]:
                        agg["max"] = value
                agg["values"].append(value)
    
    def get_aggregated(self, aggregation_type: AggregationType, 
                       field_path: Optional[str] = None) -> Any:
        """Get aggregated value for a field."""
        if not self._aggregations:
            return None
        
        if field_path:
            agg = self._aggregations.get(field_path, {})
        else:
            # Use first field
            agg = next(iter(self._aggregations.values()), {})
        
        if not agg or agg["count"] == 0:
            return None
        
        if aggregation_type == AggregationType.SUM:
            return agg["sum"]
        elif aggregation_type == AggregationType.AVG:
            return agg["sum"] / agg["count"]
        elif aggregation_type == AggregationType.MIN:
            return agg["min"]
        elif aggregation_type == AggregationType.MAX:
            return agg["max"]
        elif aggregation_type == AggregationType.COUNT:
            return agg["count"]
        elif aggregation_type == AggregationType.FIRST:
            return agg["values"][0] if agg["values"] else None
        elif aggregation_type == AggregationType.LAST:
            return agg["values"][-1] if agg["values"] else None
        elif aggregation_type == AggregationType.LIST:
            return agg["values"]
        elif aggregation_type == AggregationType.DISTINCT:
            return list(set(agg["values"]))
        return None
    
    def get_events(self) -> List[StreamEvent]:
        """Get all events in this window."""
        return self._events
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the window."""
        return {
            "window_id": self.window_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "event_count": len(self._events),
            "aggregations": {
                k: {kk: vv for kk, vv in v.items() if kk != "values"}
                for k, v in self._aggregations.items()
            }
        }


class StreamingProcessor:
    """Main stream processing engine."""
    
    def __init__(self):
        self._streams: Dict[str, StreamConfig] = {}
        self._windows: Dict[str, Dict[str, StreamWindow]] = {}  # stream_name -> window_id -> window
        self._handlers: Dict[str, List[Callable]] = {}  # stream_name -> output handlers
        self._filters: Dict[str, List[Callable]] = {}  # stream_name -> filter functions
        self._transforms: Dict[str, List[Callable]] = {}  # stream_name -> transform functions
        self._buffers: Dict[str, deque] = {}  # stream_name -> event buffer
        self._running = False
        self._processors: Dict[str, asyncio.Task] = {}
    
    def create_stream(self, config: StreamConfig) -> None:
        """Create a new data stream."""
        self._streams[config.name] = config
        self._windows[config.name] = {}
        self._handlers[config.name] = []
        self._filters[config.name] = []
        self._transforms[config.name] = []
        self._buffers[config.name] = deque(maxlen=config.buffer_size)
    
    def add_filter(self, stream_name: str, filter_func: Callable) -> None:
        """Add a filter function to a stream."""
        if stream_name in self._filters:
            self._filters[stream_name].append(filter_func)
    
    def add_transform(self, stream_name: str, transform_func: Callable) -> None:
        """Add a transform function to a stream."""
        if stream_name in self._transforms:
            self._transforms[stream_name].append(transform_func)
    
    def add_handler(self, stream_name: str, handler: Callable) -> None:
        """Add an output handler to a stream."""
        if stream_name in self._handlers:
            self._handlers[stream_name].append(handler)
    
    async def emit(self, stream_name: str, data: Any,
                   metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Emit an event to a stream.
        
        Args:
            stream_name: Name of the stream.
            data: Event data.
            metadata: Optional event metadata.
        
        Returns:
            True if event was processed successfully.
        """
        if stream_name not in self._streams:
            return False
        
        import uuid
        event = StreamEvent(
            event_id=str(uuid.uuid4()),
            timestamp=time.time(),
            data=data,
            metadata=metadata or {}
        )
        
        # Apply filters
        for f in self._filters[stream_name]:
            if not f(event):
                return True  # Filtered out, not an error
        
        # Apply transforms
        for t in self._transforms[stream_name]:
            event = t(event)
            if event is None:
                return True  # Transform consumed the event
        
        # Buffer the event
        self._buffers[stream_name].append(event)
        
        # Add to active windows
        await self._add_to_windows(stream_name, event)
        
        # Trigger handlers
        for handler in self._handlers[stream_name]:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception:
                pass  # Handler errors don't stop processing
        
        return True
    
    async def _add_to_windows(self, stream_name: str, event: StreamEvent) -> None:
        """Add an event to all relevant windows."""
        windows = self._windows[stream_name]
        current_time = time.time()
        
        # Find or create appropriate window
        for window_config in [
            WindowConfig(WindowType.TUMBLING, 10.0),
            WindowConfig(WindowType.SLIDING, 10.0, 5.0)
        ]:
            window_id = f"{stream_name}:{int(event.timestamp / window_config.size_seconds)}"
            
            if window_id not in windows:
                start_time = int(event.timestamp / window_config.size_seconds) * window_config.size_seconds
                windows[window_id] = StreamWindow(window_config, window_id, start_time)
            
            windows[window_id].add_event(event)
        
        # Evict expired windows
        expired = [wid for wid, win in windows.items() if win.is_expired(current_time)]
        for wid in expired:
            del windows[wid]
    
    def get_windows(self, stream_name: str) -> List[StreamWindow]:
        """Get all active windows for a stream."""
        return list(self._windows.get(stream_name, {}).values())
    
    def get_latest_events(self, stream_name: str, 
                          count: int = 10) -> List[StreamEvent]:
        """Get the most recent events from a stream."""
        buffer = self._buffers.get(stream_name, deque())
        return list(buffer)[-count:]
    
    def clear_stream(self, stream_name: str) -> None:
        """Clear all data for a stream."""
        if stream_name in self._buffers:
            self._buffers[stream_name].clear()
        if stream_name in self._windows:
            self._windows[stream_name].clear()
    
    def get_stream_stats(self, stream_name: str) -> Dict[str, Any]:
        """Get statistics for a stream."""
        buffer = self._buffers.get(stream_name, deque())
        windows = self._windows.get(stream_name, {})
        return {
            "stream_name": stream_name,
            "buffered_events": len(buffer),
            "active_windows": len(windows),
            "handlers_count": len(self._handlers.get(stream_name, [])),
            "filters_count": len(self._filters.get(stream_name, [])),
            "transforms_count": len(self._transforms.get(stream_name, []))
        }


class StreamingAction(BaseAction):
    """Process real-time data streams with windowing and aggregation.
    
    Supports creating streams, emitting events, filtering, transforming,
    and aggregating with tumbling/sliding/session windows.
    """
    action_type = "streaming"
    display_name = "流数据处理"
    description = "实时数据流处理，支持窗口、聚合、过滤和转换"
    
    def __init__(self):
        super().__init__()
        self._processor: Optional[StreamingProcessor] = None
        self._processor_obj = StreamingProcessor()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute streaming operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_stream", "emit", "get_windows",
                  "get_events", "get_stats", "add_filter", "add_transform",
                  "clear_stream"
                - For create_stream: name, source_type, buffer_size, etc.
                - For emit: stream_name, data, metadata
                - For get_*: stream_name, optional count
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_stream":
                return self._create_stream(params)
            elif operation == "emit":
                return self._emit_event(params)
            elif operation == "get_windows":
                return self._get_windows(params)
            elif operation == "get_events":
                return self._get_events(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            elif operation == "clear_stream":
                return self._clear_stream(params)
            elif operation == "window_aggregate":
                return self._window_aggregate(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Streaming error: {str(e)}")
    
    def _create_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new stream."""
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="stream name is required")
        
        config = StreamConfig(
            name=name,
            source_type=params.get("source_type", "memory"),
            source_uri=params.get("source_uri", ""),
            buffer_size=params.get("buffer_size", 1000),
            emit_interval_ms=params.get("emit_interval_ms", 1000),
            description=params.get("description", "")
        )
        self._processor_obj.create_stream(config)
        return ActionResult(
            success=True,
            message=f"Stream '{name}' created",
            data={"name": name, "buffer_size": config.buffer_size}
        )
    
    def _emit_event(self, params: Dict[str, Any]) -> ActionResult:
        """Emit an event to a stream."""
        stream_name = params.get("stream_name", "")
        data = params.get("data")
        metadata = params.get("metadata", {})
        
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(
                self._processor_obj.emit(stream_name, data, metadata)
            )
            return ActionResult(
                success=success,
                message=f"Event emitted to '{stream_name}'" if success else f"Stream '{stream_name}' not found"
            )
        finally:
            loop.close()
    
    def _get_windows(self, params: Dict[str, Any]) -> ActionResult:
        """Get active windows for a stream."""
        stream_name = params.get("stream_name", "")
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        windows = self._processor_obj.get_windows(stream_name)
        return ActionResult(
            success=True,
            message=f"Found {len(windows)} windows",
            data={"windows": [w.get_summary() for w in windows]}
        )
    
    def _get_events(self, params: Dict[str, Any]) -> ActionResult:
        """Get latest events from a stream."""
        stream_name = params.get("stream_name", "")
        count = params.get("count", 10)
        
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        events = self._processor_obj.get_latest_events(stream_name, count)
        return ActionResult(
            success=True,
            message=f"Got {len(events)} events",
            data={
                "events": [
                    {"event_id": e.event_id, "timestamp": e.timestamp,
                     "data": e.data, "metadata": e.metadata}
                    for e in events
                ]
            }
        )
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get stream statistics."""
        stream_name = params.get("stream_name", "")
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        stats = self._processor_obj.get_stream_stats(stream_name)
        return ActionResult(success=True, message="Stats retrieved", data=stats)
    
    def _clear_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Clear a stream's data."""
        stream_name = params.get("stream_name", "")
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        self._processor_obj.clear_stream(stream_name)
        return ActionResult(success=True, message=f"Stream '{stream_name}' cleared")
    
    def _window_aggregate(self, params: Dict[str, Any]) -> ActionResult:
        """Get aggregated data from a window."""
        stream_name = params.get("stream_name", "")
        window_id = params.get("window_id")
        agg_type = params.get("aggregation_type", "count")
        field_path = params.get("field_path")
        
        if not stream_name:
            return ActionResult(success=False, message="stream_name is required")
        
        windows = self._processor_obj.get_windows(stream_name)
        if window_id:
            target_windows = [w for w in windows if w.window_id == window_id]
        else:
            target_windows = windows[-1:] if windows else []
        
        if not target_windows:
            return ActionResult(success=False, message="No matching window found")
        
        window = target_windows[0]
        agg_enum = AggregationType(agg_type)
        result = window.get_aggregated(agg_enum, field_path)
        
        return ActionResult(
            success=True,
            message=f"Aggregation {agg_type}: {result}",
            data={"window_id": window.window_id, "aggregation": agg_type, "value": result}
        )
