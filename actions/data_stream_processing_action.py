"""
Data Stream Processing Action Module.

Provides stream processing capabilities including filtering, mapping,
aggregation, windowing, and real-time analytics for data streams.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Generator, Iterator
from dataclasses import dataclass, field
from enum import Enum
import threading
import queue
from datetime import datetime, timedelta
from collections import deque


class WindowType(Enum):
    """Window types for stream processing."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    """Represents a processing window."""
    window_type: WindowType
    size: timedelta
    slide: Optional[timedelta] = None
    start_time: Optional[datetime] = None
    items: List[Any] = field(default_factory=list)


@dataclass
class StreamConfig:
    """Configuration for stream processing."""
    buffer_size: int = 1000
    flush_interval: Optional[float] = None
    max_latency: float = 1.0


class StreamProcessor:
    """
    Core stream processor with filtering, mapping, and aggregation.
    
    Example:
        processor = StreamProcessor()
        processor.filter(lambda x: x["value"] > 10)
        processor.map(lambda x: x["value"] * 2)
        processor.aggregate("sum")
        
        for item in data_stream:
            result = processor.process(item)
    """
    
    def __init__(self):
        self.filters: List[Callable] = []
        self.mappers: List[Callable] = []
        self.aggregators: Dict[str, Callable] = {}
        self._state: Dict[str, Any] = {}
    
    def filter(self, predicate: Callable[[Any], bool]) -> "StreamProcessor":
        """Add a filter predicate."""
        self.filters.append(predicate)
        return self
    
    def map(self, transform: Callable[[Any], Any]) -> "StreamProcessor":
        """Add a transformation."""
        self.mappers.append(transform)
        return self
    
    def aggregate(self, name: str, func: Callable, initial: Any = None) -> "StreamProcessor":
        """Add an aggregator."""
        self.aggregators[name] = {"func": func, "initial": initial, "current": initial}
        return self
    
    def process(self, item: Any) -> Optional[Any]:
        """Process a single item through the pipeline."""
        # Apply filters
        for filter_fn in self.filters:
            if not filter_fn(item):
                return None
        
        # Apply mappers
        result = item
        for mapper in self.mappers:
            result = mapper(result)
        
        # Update aggregators
        for name, agg in self.aggregators.items():
            if agg["current"] is None:
                agg["current"] = result
            else:
                agg["current"] = agg["func"](agg["current"], result)
        
        return result
    
    def get_aggregates(self) -> Dict[str, Any]:
        """Get current aggregate values."""
        return {name: agg["current"] for name, agg in self.aggregators.items()}
    
    def reset(self):
        """Reset processor state."""
        self._state.clear()
        for agg in self.aggregators.values():
            agg["current"] = agg["initial"]


class WindowedProcessor:
    """
    Window-based stream processor.
    
    Example:
        processor = WindowedProcessor(
            window_type=WindowType.TUMBLING,
            size=timedelta(seconds=10)
        )
        
        for batch in processor.windowed_stream(data_stream):
            result = process_batch(batch)
    """
    
    def __init__(
        self,
        window_type: WindowType = WindowType.TUMBLING,
        size: timedelta = timedelta(seconds=10),
        slide: Optional[timedelta] = None
    ):
        self.window_type = window_type
        self.size = size
        self.slide = slide or size
        
        self.windows: deque = deque()
        self.current_window: Optional[Window] = None
        self._lock = threading.Lock()
    
    def add_item(self, item: Any, timestamp: Optional[datetime] = None) -> List[List[Any]]:
        """Add item to appropriate window and return any completed windows."""
        timestamp = timestamp or datetime.now()
        completed_windows = []
        
        with self._lock:
            # Initialize first window if needed
            if self.current_window is None:
                self.current_window = Window(
                    window_type=self.window_type,
                    size=self.size,
                    start_time=timestamp
                )
            
            # Check if item belongs to current window
            window_end = self.current_window.start_time + self.size
            
            if timestamp >= window_end:
                # Complete current window
                completed_windows.append(self.current_window.items)
                
                # Start new window
                self.current_window = Window(
                    window_type=self.window_type,
                    size=self.size,
                    start_time=timestamp
                )
            
            self.current_window.items.append(item)
        
        return completed_windows
    
    def get_current_window(self) -> Optional[List[Any]]:
        """Get items in current window."""
        with self._lock:
            if self.current_window:
                return list(self.current_window.items)
            return None
    
    def windowed_stream(
        self,
        stream: Iterator[Any]
    ) -> Generator[List[Any], None, None]:
        """Convert stream to windows."""
        for item in stream:
            completed = self.add_item(item)
            for window_items in completed:
                yield window_items
        
        # Yield final window
        final = self.get_current_window()
        if final:
            yield final


class SlidingWindowProcessor:
    """
    Sliding window processor for time-series data.
    
    Example:
        processor = SlidingWindowProcessor(
            window_size=timedelta(minutes=5),
            slide_size=timedelta(minutes=1)
        )
        
        stats = processor.compute_stats(data_points)
    """
    
    def __init__(
        self,
        window_size: timedelta,
        slide_size: timedelta
    ):
        self.window_size = window_size
        self.slide_size = slide_size
        
        self.buffer: deque = deque()
        self.last_slide_time: Optional[datetime] = None
        self._lock = threading.Lock()
    
    def add(self, item: Any, timestamp: Optional[datetime] = None) -> List[Any]:
        """Add item and return completed window if slide threshold reached."""
        timestamp = timestamp or datetime.now()
        completed = []
        
        with self._lock:
            # Remove expired items
            cutoff = timestamp - self.window_size
            while self.buffer and self.buffer[0][0] < cutoff:
                self.buffer.popleft()
            
            # Add new item
            self.buffer.append((timestamp, item))
            
            # Check if we should slide
            if self.last_slide_time is None:
                self.last_slide_time = timestamp
            elif timestamp - self.last_slide_time >= self.slide_size:
                completed = [item for _, item in self.buffer]
                self.last_slide_time = timestamp
        
        return completed
    
    def get_window(self) -> List[Any]:
        """Get current window contents."""
        with self._lock:
            return [item for _, item in self.buffer]
    
    def compute_stats(self) -> Dict[str, Any]:
        """Compute statistics over current window."""
        items = self.get_window()
        
        if not items:
            return {"count": 0}
        
        if isinstance(items[0], (int, float)):
            return {
                "count": len(items),
                "sum": sum(items),
                "avg": sum(items) / len(items),
                "min": min(items),
                "max": max(items)
            }
        
        return {"count": len(items)}


class StreamJoiner:
    """
    Joins multiple data streams.
    
    Example:
        joiner = StreamJoiner(left_key="id", right_key="user_id")
        
        joiner.add_to_leftStream(item_a)
        joiner.add_to_rightStream(item_b)
        
        matches = joiner.get_matches()
    """
    
    def __init__(
        self,
        left_key: str,
        right_key: str,
        window: timedelta = timedelta(minutes=5)
    ):
        self.left_key = left_key
        self.right_key = right_key
        self.window = window
        
        self.left_buffer: Dict[str, deque] = {}
        self.right_buffer: Dict[str, deque] = {}
        self.matches: List[Dict] = []
        
        self._lock = threading.Lock()
    
    def add_to_left(self, item: Dict, timestamp: Optional[datetime] = None) -> List[Dict]:
        """Add item to left stream."""
        timestamp = timestamp or datetime.now()
        matched = []
        
        with self._lock:
            key = item.get(self.left_key)
            if key:
                if key not in self.left_buffer:
                    self.left_buffer[key] = deque()
                self.left_buffer[key].append((timestamp, item))
                
                # Find matches in right buffer
                if key in self.right_buffer:
                    for rt, rv in list(self.right_buffer[key]):
                        if timestamp - rt <= self.window:
                            matched.append({**item, **rv})
                            self.matches.append(matched[-1])
        
        return matched
    
    def add_to_right(self, item: Dict, timestamp: Optional[datetime] = None) -> List[Dict]:
        """Add item to right stream."""
        timestamp = timestamp or datetime.now()
        matched = []
        
        with self._lock:
            key = item.get(self.right_key)
            if key:
                if key not in self.right_buffer:
                    self.right_buffer[key] = deque()
                self.right_buffer[key].append((timestamp, item))
                
                if key in self.left_buffer:
                    for lt, lv in list(self.left_buffer[key]):
                        if timestamp - lt <= self.window:
                            matched.append({**lv, **item})
                            self.matches.append(matched[-1])
        
        return matched
    
    def get_matches(self) -> List[Dict]:
        """Get all matched pairs."""
        with self._lock:
            return list(self.matches)


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataStreamProcessingAction(BaseAction):
    """
    Stream processing action for data pipelines.
    
    Parameters:
        items: List of items to process
        operations: List of operations (filter/map/aggregate)
        window_type: Window type (tumbling/sliding/session)
        window_size: Window size in seconds
    
    Example:
        action = DataStreamProcessingAction()
        result = action.execute({}, {
            "items": [1, 2, 3, 4, 5],
            "operations": [{"type": "filter", "fn": "x > 2"}],
            "window_type": "tumbling",
            "window_size": 5
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute stream processing."""
        items = params.get("items", [])
        operations = params.get("operations", [])
        window_type_str = params.get("window_type", "tumbling")
        window_size = params.get("window_size", 10)
        
        window_type = WindowType(window_type_str)
        
        # Setup processor
        processor = StreamProcessor()
        
        for op in operations:
            op_type = op.get("type")
            if op_type == "filter":
                # Add filter (simplified - would need actual function)
                processor.filter(lambda x: True)
            elif op_type == "map":
                # Add mapper
                processor.map(lambda x: x)
        
        # Process items
        processed = []
        for item in items:
            result = processor.process(item)
            if result is not None:
                processed.append(result)
        
        aggregates = processor.get_aggregates()
        
        return {
            "input_count": len(items),
            "output_count": len(processed),
            "window_type": window_type_str,
            "window_size_seconds": window_size,
            "aggregates": aggregates,
            "processed_at": datetime.now().isoformat()
        }
