"""Data streaming action module for RabAI AutoClick.

Provides streaming data processing operations:
- ChunkedStreamAction: Process data in chunks
- BufferedStreamAction: Buffer streaming data
- WindowedStreamAction: Window-based stream processing
- TumblingWindowAction: Tumbling window aggregation
- SlidingWindowAction: Sliding window aggregation
"""

from typing import Any, Dict, List, Optional, Iterator, Callable
from datetime import datetime
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ChunkedStreamAction(BaseAction):
    """Process data in chunks."""
    action_type = "chunked_stream"
    display_name = "分块流处理"
    description = "将数据分块进行处理"
    
    def __init__(self):
        super().__init__()
        self._chunk_size = 100
        self._overlap = 0
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            chunk_size = params.get("chunk_size", 100)
            overlap = params.get("overlap", 0)
            
            if not isinstance(data, (list, tuple, str)):
                return ActionResult(
                    success=False, 
                    message="Data must be list, tuple, or string"
                )
            
            chunks = []
            step = chunk_size - overlap
            
            for i in range(0, len(data), step):
                chunk = data[i:i + chunk_size]
                chunks.append(chunk)
                if i + chunk_size >= len(data):
                    break
            
            return ActionResult(
                success=True,
                message=f"Created {len(chunks)} chunks",
                data={
                    "chunk_count": len(chunks),
                    "chunk_size": chunk_size,
                    "overlap": overlap,
                    "chunks": chunks
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class BufferedStreamAction(BaseAction):
    """Buffer streaming data."""
    action_type = "buffered_stream"
    display_name = "缓冲流处理"
    description = "缓冲流式数据"
    
    def __init__(self):
        super().__init__()
        self._buffer = deque(maxlen=1000)
        self._buffer_size = 100
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            buffer_size = params.get("buffer_size", 100)
            flush = params.get("flush", False)
            
            buffer = deque(maxlen=buffer_size)
            
            if flush:
                flushed_items = list(self._buffer)
                self._buffer.clear()
                
                return ActionResult(
                    success=True,
                    message=f"Flushed {len(flushed_items)} items",
                    data={
                        "items": flushed_items,
                        "buffer_size": len(flushed_items)
                    }
                )
            else:
                for item in items:
                    buffer.append(item)
                
                return ActionResult(
                    success=True,
                    message=f"Buffered {len(items)} items",
                    data={
                        "buffered_count": len(items),
                        "total_in_buffer": len(buffer),
                        "buffer_size": buffer_size
                    }
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class WindowedStreamAction(BaseAction):
    """Window-based stream processing."""
    action_type = "windowed_stream"
    display_name = "窗口流处理"
    description = "基于窗口的流处理"
    
    def __init__(self):
        super().__init__()
        self._windows: Dict[str, deque] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "default")
            items = params.get("items", [])
            window_size = params.get("window_size", 10)
            window_type = params.get("window_type", "tumbling")
            
            if stream_id not in self._windows:
                self._windows[stream_id] = deque(maxlen=window_size)
            
            window = self._windows[stream_id]
            
            for item in items:
                window.append(item)
            
            aggregation = params.get("aggregation", "none")
            result = self._aggregate(window, aggregation)
            
            return ActionResult(
                success=True,
                message=f"Windowed stream processing complete",
                data={
                    "stream_id": stream_id,
                    "window_size": window_size,
                    "window_type": window_type,
                    "items_in_window": len(window),
                    "aggregation": aggregation,
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _aggregate(self, window: deque, aggregation: str) -> Any:
        if not window:
            return None
        
        if aggregation == "none":
            return list(window)
        elif aggregation == "count":
            return len(window)
        elif aggregation == "sum" and all(isinstance(x, (int, float)) for x in window):
            return sum(window)
        elif aggregation == "avg" and all(isinstance(x, (int, float)) for x in window):
            return sum(window) / len(window)
        elif aggregation == "min":
            return min(window)
        elif aggregation == "max":
            return max(window)
        else:
            return list(window)


class TumblingWindowAction(BaseAction):
    """Tumbling window aggregation."""
    action_type = "tumbling_window"
    display_name = "翻滚窗口聚合"
    description = "翻滚窗口数据聚合"
    
    def __init__(self):
        super().__init__()
        self._window_data: Dict[str, List[Any]] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            window_id = params.get("window_id", "default")
            items = params.get("items", [])
            window_size = params.get("window_size", 10)
            aggregation = params.get("aggregation", "list")
            
            if window_id not in self._window_data:
                self._window_data[window_id] = []
            
            window = self._window_data[window_id]
            window.extend(items)
            
            while len(window) >= window_size:
                chunk = window[:window_size]
                window[:] = window[window_size:]
                
                result = self._compute_aggregation(chunk, aggregation)
                
                return ActionResult(
                    success=True,
                    message=f"Tumbling window completed",
                    data={
                        "window_id": window_id,
                        "window_size": window_size,
                        "aggregation": aggregation,
                        "result": result,
                        "remaining_items": len(window)
                    }
                )
            
            return ActionResult(
                success=True,
                message=f"Waiting for more items",
                data={
                    "window_id": window_id,
                    "items_collected": len(window),
                    "window_size": window_size,
                    "needed": window_size - len(window)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _compute_aggregation(self, chunk: List[Any], aggregation: str) -> Any:
        if aggregation == "list":
            return chunk
        elif aggregation == "count":
            return len(chunk)
        elif aggregation == "sum" and all(isinstance(x, (int, float)) for x in chunk):
            return sum(chunk)
        elif aggregation == "avg" and all(isinstance(x, (int, float)) for x in chunk):
            return sum(chunk) / len(chunk)
        elif aggregation == "min":
            return min(chunk) if chunk else None
        elif aggregation == "max":
            return max(chunk) if chunk else None
        else:
            return chunk


class SlidingWindowAction(BaseAction):
    """Sliding window aggregation."""
    action_type = "sliding_window"
    display_name = "滑动窗口聚合"
    description = "滑动窗口数据聚合"
    
    def __init__(self):
        super().__init__()
        self._sliding_windows: Dict[str, deque] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            window_id = params.get("window_id", "default")
            item = params.get("item")
            window_size = params.get("window_size", 10)
            step = params.get("step", 1)
            aggregation = params.get("aggregation", "list")
            
            if window_id not in self._sliding_windows:
                self._sliding_windows[window_id] = deque(maxlen=window_size)
            
            window = self._sliding_windows[window_id]
            
            if item is not None:
                window.append(item)
            
            if len(window) < window_size:
                return ActionResult(
                    success=True,
                    message=f"Collecting items",
                    data={
                        "window_id": window_id,
                        "items_collected": len(window),
                        "window_size": window_size,
                        "needed": window_size - len(window)
                    }
                )
            
            result = self._compute_aggregation(list(window), aggregation)
            
            return ActionResult(
                success=True,
                message=f"Sliding window ready",
                data={
                    "window_id": window_id,
                    "window_size": window_size,
                    "step": step,
                    "aggregation": aggregation,
                    "window_content": list(window),
                    "result": result
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _compute_aggregation(self, chunk: List[Any], aggregation: str) -> Any:
        if aggregation == "list":
            return chunk
        elif aggregation == "count":
            return len(chunk)
        elif aggregation == "sum" and all(isinstance(x, (int, float)) for x in chunk):
            return sum(chunk)
        elif aggregation == "avg" and all(isinstance(x, (int, float)) for x in chunk):
            return sum(chunk) / len(chunk)
        elif aggregation == "min":
            return min(chunk) if chunk else None
        elif aggregation == "max":
            return max(chunk) if chunk else None
        else:
            return chunk
