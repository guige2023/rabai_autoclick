"""Data Stream Processor Action Module.

Processes streaming data with windowing, aggregation, and transformation.
Supports real-time data pipelines with backpressure handling.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, Generic, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class WindowType(Enum):
    """Types of streaming windows."""
    TUMBLING = auto()    # Fixed-size, non-overlapping
    HOPPING = auto()     # Fixed-size, overlapping
    SESSION = auto()     # Activity-based
    SLIDING = auto()     # Continuous


@dataclass
class WindowConfig:
    """Configuration for a streaming window."""
    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    hop_seconds: float = 60.0
    session_gap_seconds: float = 5.0
    max_size: int = 10000


@dataclass
class StreamWindow(Generic[T]):
    """A window of streaming data."""
    window_id: str
    start_time: float
    end_time: float
    data: List[T] = field(default_factory=list)
    
    @property
    def is_closed(self) -> bool:
        """Check if window is closed for new data."""
        return time.time() > self.end_time
    
    def add(self, item: T) -> None:
        """Add item to window."""
        self.data.append(item)
    
    def to_list(self) -> List[T]:
        """Get window data as list."""
        return list(self.data)


class StreamProcessor(Generic[T]):
    """Processes streaming data with windowing support.
    
    Supports:
    - Tumbling, hopping, session, and sliding windows
    - Transformation functions
    - Aggregation operations
    - Late data handling
    """
    
    def __init__(
        self,
        config: Optional[WindowConfig] = None,
        max_queue_size: int = 10000
    ):
        self.config = config or WindowConfig()
        self.max_queue_size = max_queue_size
        self._input_queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._windows: Dict[str, StreamWindow] = {}
        self._transformers: List[Callable[[List[T]], Awaitable[List[R]]]] = []
        self._aggregators: Dict[str, Callable[[List[T]], Any]] = {}
        self._sink: Optional[Callable[[Any], Awaitable[None]]] = None
        self._running = False
        self._process_task: Optional[asyncio.Task] = None
        self._metrics = {
            "items_received": 0,
            "items_processed": 0,
            "windows_created": 0,
            "windows_closed": 0,
            "late_items": 0
        }
        self._lock = asyncio.Lock()
    
    def add_transformer(self, transformer: Callable[[List[T]], Awaitable[List[R]]]) -> None:
        """Add a transformation function.
        
        Args:
            transformer: Async function that transforms a list of items
        """
        self._transformers.append(transformer)
    
    def add_aggregator(self, name: str, aggregator: Callable[[List[T]], Any]) -> None:
        """Add a named aggregator function.
        
        Args:
            name: Aggregator name
            aggregator: Function that aggregates window data
        """
        self._aggregators[name] = aggregator
    
    async def set_sink(self, sink: Callable[[Any], Awaitable[None]]) -> None:
        """Set the output sink function.
        
        Args:
            sink: Async function to receive processed results
        """
        self._sink = sink
    
    async def push(self, item: T, timestamp: Optional[float] = None) -> None:
        """Push an item into the stream.
        
        Args:
            item: Data item to process
            timestamp: Optional event timestamp
        """
        ts = timestamp or time.time()
        
        try:
            self._input_queue.put_nowait((item, ts))
            self._metrics["items_received"] += 1
        except asyncio.QueueFull:
            logger.warning("Stream processor queue full, applying backpressure")
            await self._input_queue.put((item, ts))
    
    async def start(self) -> None:
        """Start the stream processor."""
        self._running = True
        self._process_task = asyncio.create_task(self._process_loop())
        logger.info("Stream processor started")
    
    async def stop(self) -> None:
        """Stop the stream processor."""
        self._running = False
        
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                pass
        
        for window in self._windows.values():
            if not window.is_closed:
                await self._close_window(window)
        
        logger.info("Stream processor stopped")
    
    async def _process_loop(self) -> None:
        """Main processing loop."""
        while self._running:
            try:
                item, timestamp = await asyncio.wait_for(
                    self._input_queue.get(),
                    timeout=1.0
                )
                
                await self._route_item(item, timestamp)
                self._metrics["items_processed"] += 1
                
            except asyncio.TimeoutError:
                await self._check_idle_windows()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stream processor: {e}")
    
    async def _route_item(self, item: T, timestamp: float) -> None:
        """Route item to appropriate window(s)."""
        window_type = self.config.window_type
        
        if window_type == WindowType.TUMBLING:
            await self._route_to_tumbling_window(item, timestamp)
        elif window_type == WindowType.HOPPING:
            await self._route_to_hopping_window(item, timestamp)
        elif window_type == WindowType.SESSION:
            await self._route_to_session_window(item, timestamp)
        elif window_type == WindowType.SLIDING:
            await self._route_to_sliding_window(item, timestamp)
    
    def _get_window_id(self, timestamp: float) -> str:
        """Generate window ID for a timestamp."""
        if self.config.window_type == WindowType.TUMBLING:
            window_num = int(timestamp / self.config.size_seconds)
            return f"tumbling_{window_num}"
        elif self.config.window_type == WindowType.HOPPING:
            window_num = int(timestamp / self.config.hop_seconds)
            return f"hopping_{window_num}"
        return f"window_{timestamp}"
    
    async def _route_to_tumbling_window(self, item: T, timestamp: float) -> None:
        """Route item to tumbling window."""
        window_id = self._get_window_id(timestamp)
        
        async with self._lock:
            if window_id not in self._windows:
                start_time = float(window_id.split("_")[1]) * self.config.size_seconds
                end_time = start_time + self.config.size_seconds
                
                self._windows[window_id] = StreamWindow(
                    window_id=window_id,
                    start_time=start_time,
                    end_time=end_time
                )
                self._metrics["windows_created"] += 1
            
            window = self._windows[window_id]
        
        if window.is_closed:
            await self._close_window(window)
            self._metrics["late_items"] += 1
            window_id = self._get_window_id(timestamp)
            async with self._lock:
                if window_id not in self._windows:
                    start_time = float(window_id.split("_")[1]) * self.config.size_seconds
                    self._windows[window_id] = StreamWindow(
                        window_id=window_id,
                        start_time=start_time,
                        end_time=start_time + self.config.size_seconds
                    )
                window = self._windows[window_id]
        
        window.add(item)
    
    async def _route_to_hopping_window(self, item: T, timestamp: float) -> None:
        """Route item to hopping window."""
        window_id = self._get_window_id(timestamp)
        size_windows = int(self.config.size_seconds / self.config.hop_seconds)
        
        async with self._lock:
            for i in range(size_windows):
                wid = f"hopping_{int(timestamp / self.config.hop_seconds) - i}"
                if wid not in self._windows:
                    start_time = float(wid.split("_")[1]) * self.config.hop_seconds
                    self._windows[wid] = StreamWindow(
                        window_id=wid,
                        start_time=start_time,
                        end_time=start_time + self.config.size_seconds
                    )
                    self._metrics["windows_created"] += 1
                
                self._windows[wid].add(item)
    
    async def _route_to_session_window(self, item: T, timestamp: float) -> None:
        """Route item to session window."""
        async with self._lock:
            for window in self._windows.values():
                if not window.is_closed and timestamp - window.end_time < self.config.session_gap_seconds:
                    window.end_time = timestamp + 0.001
                    window.add(item)
                    return
            
            new_window = StreamWindow(
                window_id=f"session_{timestamp}",
                start_time=timestamp,
                end_time=timestamp + 0.001
            )
            new_window.add(item)
            self._windows[new_window.window_id] = new_window
            self._metrics["windows_created"] += 1
    
    async def _route_to_sliding_window(self, item: T, timestamp: float) -> None:
        """Route item to sliding window."""
        await self._route_to_tumbling_window(item, timestamp)
    
    async def _check_idle_windows(self) -> None:
        """Check and close idle windows."""
        now = time.time()
        
        async with self._lock:
            windows_to_close = [
                w for w in self._windows.values()
                if now > w.end_time + self.config.hop_seconds
            ]
        
        for window in windows_to_close:
            await self._close_window(window)
    
    async def _close_window(self, window: StreamWindow[T]) -> None:
        """Close a window and process its data."""
        if not window.data:
            async with self._lock:
                self._windows.pop(window.window_id, None)
            return
        
        data = window.to_list()
        
        for transformer in self._transformers:
            try:
                data = await transformer(data)
            except Exception as e:
                logger.error(f"Transformer error: {e}")
        
        if self._aggregators:
            results = {}
            for name, agg in self._aggregators.items():
                try:
                    results[name] = agg(data)
                except Exception as e:
                    logger.error(f"Aggregator '{name}' error: {e}")
            data = results
        
        if self._sink:
            try:
                await self._sink(data)
            except Exception as e:
                logger.error(f"Sink error: {e}")
        
        self._metrics["windows_closed"] += 1
        
        async with self._lock:
            self._windows.pop(window.window_id, None)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics."""
        return {
            **self._metrics,
            "queue_size": self._input_queue.qsize(),
            "active_windows": len(self._windows)
        }


class StreamJoiner(Generic[T, R]):
    """Joins multiple streams together.
    
    Supports:
    - Windowed joins
    - Time-based alignment
    - Outer joins
    """
    
    def __init__(
        self,
        window_seconds: float = 60.0,
        join_key_extractor: Optional[Callable[[T], str]] = None
    ):
        self.window_seconds = window_seconds
        self.join_key_extractor = join_key_extractor or (lambda x: str(x))
        self._streams: Dict[str, asyncio.Queue] = {}
        self._buffers: Dict[str, Dict[str, List]] = {}
        self._running = False
        self._lock = asyncio.Lock()
    
    def register_stream(self, name: str) -> asyncio.Queue:
        """Register a named stream.
        
        Args:
            name: Stream name
            
        Returns:
            Queue for this stream
        """
        queue = asyncio.Queue()
        self._streams[name] = queue
        self._buffers[name] = {}
        return queue
    
    async def join(
        self,
        left_stream: str,
        right_stream: str,
        condition: Callable[[T, R], bool]
    ) -> Awaitable[List]:
        """Wait for matching items from two streams.
        
        Args:
            left_stream: Name of left stream
            right_stream: Name of right stream
            condition: Join condition function
            
        Returns:
            List of joined results
        """
        results = []
        
        left_q = self._streams.get(left_stream)
        right_q = self._streams.get(right_stream)
        
        if not left_q or not right_q:
            raise ValueError("Invalid stream names")
        
        while self._running:
            try:
                left_item = await asyncio.wait_for(left_q.get(), timeout=1.0)
                key = self.join_key_extractor(left_item)
                
                if key in self._buffers.get(right_stream, {}):
                    for right_item in self._buffers[right_stream][key]:
                        if condition(left_item, right_item):
                            results.append((left_item, right_item))
                            self._buffers[right_stream][key].remove(right_item)
                
                if left_stream in self._buffers:
                    if key not in self._buffers[left_stream]:
                        self._buffers[left_stream][key] = []
                    self._buffers[left_stream][key].append(left_item)
                    
            except asyncio.TimeoutError:
                continue
        
        return results
