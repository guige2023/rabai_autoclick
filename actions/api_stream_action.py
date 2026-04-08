"""API Stream Action Module.

Provides streaming API capabilities including chunked responses,
real-time data streaming, and streaming aggregation.
"""

import sys
import os
import json
import time
import threading
from typing import Any, Dict, List, Optional, Callable, Iterator, Generator
from collections import deque
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StreamState(Enum):
    """Stream processing states."""
    IDLE = "idle"
    STREAMING = "streaming"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"


class StreamChunkAction(BaseAction):
    """Process data in chunks with configurable size and overlap.
    
    Supports streaming data sources, chunk transformation, and overlap handling.
    """
    action_type = "stream_chunk"
    display_name = "流式分块"
    description = "将数据流分块处理，支持重叠和变换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Process data as chunks.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list or iterator).
                - chunk_size: Size of each chunk.
                - overlap: Number of overlapping items.
                - transform: Optional chunk transform function.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with chunked data or error.
        """
        data = params.get('data', [])
        chunk_size = params.get('chunk_size', 100)
        overlap = params.get('overlap', 0)
        transform_var = params.get('transform_var', None)
        output_var = params.get('output_var', 'chunks')

        if not isinstance(data, (list, tuple, Iterator)):
            return ActionResult(
                success=False,
                message=f"Expected list or iterator for data, got {type(data).__name__}"
            )

        try:
            # Get transform function if specified
            transform = None
            if transform_var:
                transform = context.variables.get(transform_var)

            chunks = []
            chunk = []

            for i, item in enumerate(data):
                chunk.append(item)

                if len(chunk) >= chunk_size:
                    if transform:
                        chunk = transform(chunk)
                    chunks.append(chunk)

                    # Handle overlap
                    if overlap > 0:
                        chunk = chunk[-overlap:]
                    else:
                        chunk = []

            # Don't forget remaining items
            if chunk:
                if transform:
                    chunk = transform(chunk)
                chunks.append(chunk)

            context.variables[output_var] = chunks
            return ActionResult(
                success=True,
                data={'chunks': chunks, 'count': len(chunks)},
                message=f"Created {len(chunks)} chunks from data"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Chunk processing failed: {str(e)}"
            )


class StreamAggregatorAction(BaseAction):
    """Aggregate streaming data with windowing and buffering.
    
    Supports time-based and count-based windows, with aggregation functions.
    """
    action_type = "stream_aggregator"
    display_name = "流式聚合"
    description = "对流数据进行窗口聚合，支持时间和计数窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Aggregate streaming data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input stream/list.
                - window_type: 'time' or 'count'.
                - window_size: Window size (seconds or count).
                - agg_func: Aggregation function name.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with aggregated results or error.
        """
        data = params.get('data', [])
        window_type = params.get('window_type', 'count')
        window_size = params.get('window_size', 10)
        agg_func_name = params.get('agg_func', 'sum')
        output_var = params.get('output_var', 'aggregated')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            agg_func = self._get_agg_function(agg_func_name)
            windows = []

            if window_type == 'count':
                windows = self._count_based_windows(data, window_size, agg_func)
            elif window_type == 'time':
                windows = self._time_based_windows(data, window_size, agg_func)

            context.variables[output_var] = windows
            return ActionResult(
                success=True,
                data={'windows': windows, 'count': len(windows)},
                message=f"Created {len(windows)} {window_type} windows"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Stream aggregation failed: {str(e)}"
            )

    def _count_based_windows(
        self, data: List, size: int, agg_func: Callable
    ) -> List[Dict]:
        """Create count-based windows."""
        windows = []
        for i in range(0, len(data), size):
            window_data = data[i:i + size]
            windows.append({
                'window_id': len(windows),
                'start_index': i,
                'end_index': i + len(window_data),
                'count': len(window_data),
                'result': agg_func(window_data)
            })
        return windows

    def _time_based_windows(
        self, data: List, duration: float, agg_func: Callable
    ) -> List[Dict]:
        """Create time-based windows."""
        if not data:
            return []

        windows = []
        window_data = []
        window_start = data[0].get('timestamp', 0) if isinstance(data[0], dict) else 0

        for item in data:
            item_time = item.get('timestamp', 0) if isinstance(item, dict) else 0

            if item_time - window_start >= duration:
                if window_data:
                    windows.append({
                        'window_id': len(windows),
                        'start_time': window_start,
                        'end_time': item_time,
                        'count': len(window_data),
                        'result': agg_func(window_data)
                    })
                window_data = [item]
                window_start = item_time
            else:
                window_data.append(item)

        # Don't forget the last window
        if window_data:
            windows.append({
                'window_id': len(windows),
                'start_time': window_start,
                'end_time': window_data[-1].get('timestamp', 0),
                'count': len(window_data),
                'result': agg_func(window_data)
            })

        return windows

    def _get_agg_function(self, name: str) -> Callable:
        """Get aggregation function by name."""
        agg_funcs = {
            'sum': lambda d: sum(item.get('value', 0) for item in d if isinstance(item, dict)),
            'avg': lambda d: sum(item.get('value', 0) for item in d if isinstance(item, dict)) / len(d) if d else 0,
            'count': lambda d: len(d),
            'min': lambda d: min(item.get('value', float('inf')) for item in d if isinstance(item, dict)),
            'max': lambda d: max(item.get('value', float('-inf')) for item in d if isinstance(item, dict)),
            'first': lambda d: d[0] if d else None,
            'last': lambda d: d[-1] if d else None,
        }
        return agg_funcs.get(name, agg_funcs['count'])


class StreamBufferAction(BaseAction):
    """Buffer streaming data with configurable size and flush conditions.
    
    Supports buffering, batch processing, and watermark-based flushing.
    """
    action_type = "stream_buffer"
    display_name = "流式缓冲"
    description = "缓冲流数据，支持批量处理和水位触发"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Buffer and optionally flush stream data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data to buffer.
                - buffer_var: Name of buffer variable in context.
                - max_size: Max buffer size before auto-flush.
                - flush: Whether to flush the buffer.
                - flush_func: Optional flush transform function.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with buffered/flush result or error.
        """
        data = params.get('data', None)
        buffer_var = params.get('buffer_var', '_stream_buffer')
        max_size = params.get('max_size', 100)
        flush = params.get('flush', False)
        flush_func_var = params.get('flush_func_var', None)
        output_var = params.get('output_var', 'buffered')

        try:
            # Get or create buffer
            if not hasattr(context, '_stream_buffers'):
                context._stream_buffers = {}

            buffer = context._stream_buffers.get(buffer_var, deque())

            result = {}

            if flush:
                # Flush buffer
                flushed_data = list(buffer)
                buffer.clear()

                if flush_func_var:
                    flush_func = context.variables.get(flush_func_var)
                    if flush_func:
                        flushed_data = flush_func(flushed_data)

                result = {
                    'flushed': True,
                    'count': len(flushed_data),
                    'data': flushed_data
                }
            elif data is not None:
                # Add to buffer
                if isinstance(data, list):
                    buffer.extend(data)
                else:
                    buffer.append(data)

                result = {
                    'buffered': True,
                    'current_size': len(buffer),
                    'auto_flush': len(buffer) >= max_size
                }

                # Auto-flush if buffer is full
                if len(buffer) >= max_size:
                    flushed_data = list(buffer)
                    buffer.clear()
                    result['flushed'] = True
                    result['data'] = flushed_data

            context._stream_buffers[buffer_var] = buffer
            context.variables[output_var] = result

            return ActionResult(
                success=True,
                data=result,
                message=f"Buffer operation completed: {result.get('current_size', result.get('count', 0))} items"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Stream buffer failed: {str(e)}"
            )


class StreamSamplerAction(BaseAction):
    """Sample streaming data using various sampling strategies.
    
    Supports random, reservoir, and periodic sampling.
    """
    action_type = "stream_sampler"
    display_name = "流式采样"
    description = "对流数据进行采样，支持随机、水库存和周期采样"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sample from stream data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input stream/list.
                - strategy: 'random', 'reservoir', or 'periodic'.
                - rate: Sampling rate (0-1 for random, n for periodic).
                - size: Target sample size for reservoir.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with sampled data or error.
        """
        import random

        data = params.get('data', [])
        strategy = params.get('strategy', 'random')
        rate = params.get('rate', 0.1)
        size = params.get('size', 100)
        output_var = params.get('output_var', 'sample')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            if strategy == 'random':
                sample = random.sample(data, min(len(data), int(len(data) * rate)))
            elif strategy == 'reservoir':
                sample = self._reservoir_sampling(data, size)
            elif strategy == 'periodic':
                sample = self._periodic_sampling(data, int(rate))
            else:
                sample = data[:size]

            context.variables[output_var] = sample
            return ActionResult(
                success=True,
                data={'sample': sample, 'size': len(sample), 'strategy': strategy},
                message=f"Sampled {len(sample)} items using {strategy} strategy"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Stream sampling failed: {str(e)}"
            )

    def _reservoir_sampling(self, data: List, k: int) -> List:
        """Reservoir sampling algorithm."""
        import random
        if k >= len(data):
            return list(data)

        reservoir = data[:k]
        for i in range(k, len(data)):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = data[i]

        return reservoir

    def _periodic_sampling(self, data: List, interval: int) -> List:
        """Periodic sampling."""
        return data[::interval] if interval > 0 else data


class StreamWindowAction(BaseAction):
    """Sliding window operations on streaming data.
    
    Supports sliding, tumbling, and session windows.
    """
    action_type = "stream_window"
    display_name = "流式窗口"
    description = "对流数据进行滑动窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Apply sliding window to stream data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input stream/list.
                - window_size: Size of each window.
                - slide_interval: Slide step size.
                - window_type: 'sliding', 'tumbling', 'session'.
                - session_gap: Gap threshold for session windows.
                - agg_func: Aggregation function name.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with windowed data or error.
        """
        data = params.get('data', [])
        window_size = params.get('window_size', 10)
        slide_interval = params.get('slide_interval', 1)
        window_type = params.get('window_type', 'sliding')
        session_gap = params.get('session_gap', 5)
        agg_func_name = params.get('agg_func', 'avg')
        output_var = params.get('output_var', 'windows')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            agg_func = self._get_agg_function(agg_func_name)

            if window_type == 'sliding':
                windows = self._sliding_windows(data, window_size, slide_interval, agg_func)
            elif window_type == 'tumbling':
                windows = self._tumbling_windows(data, window_size, agg_func)
            elif window_type == 'session':
                windows = self._session_windows(data, session_gap, agg_func)
            else:
                windows = []

            context.variables[output_var] = windows
            return ActionResult(
                success=True,
                data={'windows': windows, 'count': len(windows)},
                message=f"Created {len(windows)} {window_type} windows"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Stream window failed: {str(e)}"
            )

    def _sliding_windows(
        self, data: List, size: int, slide: int, agg_func: Callable
    ) -> List[Dict]:
        """Create sliding windows."""
        windows = []
        for i in range(0, len(data) - size + 1, slide):
            window_data = data[i:i + size]
            windows.append({
                'window_id': len(windows),
                'start': i,
                'end': i + size,
                'data': window_data,
                'aggregated': agg_func(window_data)
            })
        return windows

    def _tumbling_windows(
        self, data: List, size: int, agg_func: Callable
    ) -> List[Dict]:
        """Create non-overlapping tumbling windows."""
        windows = []
        for i in range(0, len(data), size):
            window_data = data[i:i + size]
            if window_data:
                windows.append({
                    'window_id': len(windows),
                    'start': i,
                    'end': min(i + size, len(data)),
                    'data': window_data,
                    'aggregated': agg_func(window_data)
                })
        return windows

    def _session_windows(
        self, data: List, gap: int, agg_func: Callable
    ) -> List[Dict]:
        """Create session windows based on time gaps."""
        if not data:
            return []

        windows = []
        current_window = [data[0]]

        for item in data[1:]:
            item_time = item.get('timestamp', 0) if isinstance(item, dict) else 0
            last_time = current_window[-1].get('timestamp', 0) if isinstance(current_window[-1], dict) else 0

            if item_time - last_time <= gap:
                current_window.append(item)
            else:
                if current_window:
                    windows.append({
                        'window_id': len(windows),
                        'data': current_window,
                        'aggregated': agg_func(current_window)
                    })
                current_window = [item]

        # Don't forget the last window
        if current_window:
            windows.append({
                'window_id': len(windows),
                'data': current_window,
                'aggregated': agg_func(current_window)
            })

        return windows

    def _get_agg_function(self, name: str) -> Callable:
        """Get aggregation function by name."""
        agg_funcs = {
            'sum': lambda d: sum(item.get('value', 0) for item in d if isinstance(item, dict)),
            'avg': lambda d: sum(item.get('value', 0) for item in d if isinstance(item, dict)) / len(d) if d else 0,
            'count': lambda d: len(d),
            'min': lambda d: min(item.get('value', float('inf')) for item in d if isinstance(item, dict)),
            'max': lambda d: max(item.get('value', float('-inf')) for item in d if isinstance(item, dict)),
            'first': lambda d: d[0] if d else None,
            'last': lambda d: d[-1] if d else None,
        }
        return agg_funcs.get(name, agg_funcs['count'])
