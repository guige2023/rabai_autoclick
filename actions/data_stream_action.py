"""Data Stream Action Module for RabAI AutoClick.

Implements streaming data processing with windowing,
aggregation, and real-time transformation capabilities.
"""

import time
import threading
import sys
import os
from typing import Any, Callable, Dict, Iterator, List, Optional
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WindowType:
    """Stream windowing types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


class DataStreamAction(BaseAction):
    """Streaming data processing with windowed aggregation.

    Provides real-time stream processing with support for tumbling
    windows, sliding windows, and session-based grouping. Supports
    aggregation, filtering, and transformation operations.
    """
    action_type = "data_stream"
    display_name = "数据流处理"
    description = "流式数据处理，窗口聚合和实时转换"

    _streams: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stream operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'emit', 'consume', 'window',
                               'aggregate', 'filter', 'stats'
                - stream_name: str - name of the stream
                - window_type: str - 'tumbling', 'sliding', 'session'
                - window_size: float - window size in seconds
                - slide_interval: float (optional) - slide interval for sliding window
                - data: Any (optional) - data to emit into stream
                - transform: callable (optional) - transformation function

        Returns:
            ActionResult with stream operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'emit')

            if operation == 'create':
                return self._create_stream(params, start_time)
            elif operation == 'emit':
                return self._emit_data(params, start_time)
            elif operation == 'consume':
                return self._consume_stream(params, start_time)
            elif operation == 'window':
                return self._apply_window(params, start_time)
            elif operation == 'aggregate':
                return self._aggregate_window(params, start_time)
            elif operation == 'filter':
                return self._filter_stream(params, start_time)
            elif operation == 'stats':
                return self._get_stream_stats(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Stream action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_stream(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new data stream."""
        stream_name = params.get('stream_name', 'default')
        window_type = params.get('window_type', WindowType.TUMBLING)
        window_size = params.get('window_size', 60.0)
        slide_interval = params.get('slide_interval', window_size)

        with self._lock:
            if stream_name in self._streams:
                return ActionResult(
                    success=True,
                    message=f"Stream already exists: {stream_name}",
                    data={'stream_name': stream_name, 'created': False},
                    duration=time.time() - start_time
                )

            self._streams[stream_name] = {
                'name': stream_name,
                'window_type': window_type,
                'window_size': window_size,
                'slide_interval': slide_interval,
                'buffer': deque(maxlen=10000),
                'windows': {},
                'created_at': time.time(),
                'total_emitted': 0,
                'total_consumed': 0,
                'lock': threading.RLock()
            }

            return ActionResult(
                success=True,
                message=f"Stream created: {stream_name}",
                data={
                    'stream_name': stream_name,
                    'window_type': window_type,
                    'window_size': window_size
                },
                duration=time.time() - start_time
            )

    def _emit_data(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Emit data into a stream."""
        stream_name = params.get('stream_name', 'default')
        data = params.get('data')
        timestamp = params.get('timestamp', time.time())

        with self._lock:
            if stream_name not in self._streams:
                self._create_stream({'stream_name': stream_name}, start_time)

            stream = self._streams[stream_name]
            entry = {
                'data': data,
                'timestamp': timestamp,
                'emitted_at': time.time()
            }
            stream['buffer'].append(entry)
            stream['total_emitted'] += 1

            self._update_windows(stream, entry)

        return ActionResult(
            success=True,
            message=f"Data emitted to {stream_name}",
            data={
                'stream_name': stream_name,
                'buffer_size': len(stream['buffer']),
                'total_emitted': stream['total_emitted']
            },
            duration=time.time() - start_time
        )

    def _consume_stream(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Consume data from a stream."""
        stream_name = params.get('stream_name', 'default')
        batch_size = params.get('batch_size', 10)
        timeout = params.get('timeout', 1.0)

        with self._lock:
            if stream_name not in self._streams:
                return ActionResult(
                    success=False,
                    message=f"Stream not found: {stream_name}",
                    duration=time.time() - start_time
                )

            stream = self._streams[stream_name]
            consumed = []

            for _ in range(batch_size):
                if stream['buffer']:
                    entry = stream['buffer'].popleft()
                    consumed.append(entry)
                    stream['total_consumed'] += 1
                else:
                    break

        return ActionResult(
            success=True,
            message=f"Consumed {len(consumed)} items from {stream_name}",
            data={
                'stream_name': stream_name,
                'consumed': len(consumed),
                'items': [c['data'] for c in consumed],
                'remaining': len(stream['buffer'])
            },
            duration=time.time() - start_time
        )

    def _apply_window(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Apply windowing to stream data."""
        stream_name = params.get('stream_name', 'default')
        window_type = params.get('window_type', WindowType.TUMBLING)
        window_size = params.get('window_size', 60.0)

        with self._lock:
            if stream_name not in self._streams:
                return ActionResult(
                    success=False,
                    message=f"Stream not found: {stream_name}",
                    duration=time.time() - start_time
                )

            stream = self._streams[stream_name]
            now = time.time()
            window_start = now - window_size

            windowed_data = [
                entry for entry in stream['buffer']
                if entry['timestamp'] >= window_start
            ]

            return ActionResult(
                success=True,
                message=f"Window applied: {len(windowed_data)} items",
                data={
                    'stream_name': stream_name,
                    'window_type': window_type,
                    'window_size': window_size,
                    'items_in_window': len(windowed_data),
                    'data': [e['data'] for e in windowed_data]
                },
                duration=time.time() - start_time
            )

    def _aggregate_window(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Aggregate windowed stream data."""
        stream_name = params.get('stream_name', 'default')
        agg_type = params.get('agg_type', 'count')
        field = params.get('field', '')

        with self._lock:
            if stream_name not in self._streams:
                return ActionResult(
                    success=False,
                    message=f"Stream not found: {stream_name}",
                    duration=time.time() - start_time
                )

            stream = self._streams[stream_name]
            values = []

            for entry in stream['buffer']:
                data = entry['data']
                if isinstance(data, dict) and field:
                    values.append(data.get(field, 0))
                elif isinstance(data, (int, float)):
                    values.append(data)

            if not values:
                return ActionResult(
                    success=True,
                    message="No numeric values to aggregate",
                    data={'count': 0},
                    duration=time.time() - start_time
                )

            result = {}
            if agg_type == 'count':
                result = {'count': len(values)}
            elif agg_type == 'sum':
                result = {'sum': sum(values)}
            elif agg_type == 'avg':
                result = {'avg': sum(values) / len(values)}
            elif agg_type == 'min':
                result = {'min': min(values)}
            elif agg_type == 'max':
                result = {'max': max(values)}
            elif agg_type == 'mean':
                result = {'mean': sum(values) / len(values)}

            return ActionResult(
                success=True,
                message=f"Aggregation complete: {agg_type}",
                data={
                    'stream_name': stream_name,
                    'agg_type': agg_type,
                    'result': result,
                    'sample_size': len(values)
                },
                duration=time.time() - start_time
            )

    def _filter_stream(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Filter stream data based on predicate."""
        stream_name = params.get('stream_name', 'default')
        predicate = params.get('predicate', {})
        filter_field = predicate.get('field', '')
        filter_op = predicate.get('op', 'eq')
        filter_value = predicate.get('value')

        with self._lock:
            if stream_name not in self._streams:
                return ActionResult(
                    success=False,
                    message=f"Stream not found: {stream_name}",
                    duration=time.time() - start_time
                )

            stream = self._streams[stream_name]
            filtered = []

            for entry in stream['buffer']:
                data = entry['data']
                if isinstance(data, dict) and filter_field:
                    field_value = data.get(filter_field)
                    if self._evaluate_filter(field_value, filter_op, filter_value):
                        filtered.append(data)
                elif data == filter_value and filter_op == 'eq':
                    filtered.append(data)

            return ActionResult(
                success=True,
                message=f"Filtered: {len(filtered)} items match",
                data={
                    'stream_name': stream_name,
                    'predicate': predicate,
                    'filtered_count': len(filtered),
                    'items': filtered
                },
                duration=time.time() - start_time
            )

    def _get_stream_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get stream statistics."""
        stream_name = params.get('stream_name', 'default')

        with self._lock:
            if stream_name not in self._streams:
                return ActionResult(
                    success=False,
                    message=f"Stream not found: {stream_name}",
                    duration=time.time() - start_time
                )

            stream = self._streams[stream_name]

            return ActionResult(
                success=True,
                message=f"Stream stats: {stream_name}",
                data={
                    'stream_name': stream_name,
                    'buffer_size': len(stream['buffer']),
                    'total_emitted': stream['total_emitted'],
                    'total_consumed': stream['total_consumed'],
                    'window_type': stream['window_type'],
                    'created_at': stream['created_at']
                },
                duration=time.time() - start_time
            )

    def _update_windows(self, stream: Dict[str, Any], entry: Dict[str, Any]) -> None:
        """Update window state with new entry."""
        pass

    def _evaluate_filter(self, field_value: Any, op: str, filter_value: Any) -> bool:
        """Evaluate filter predicate."""
        if op == 'eq':
            return field_value == filter_value
        elif op == 'ne':
            return field_value != filter_value
        elif op == 'gt':
            return field_value > filter_value
        elif op == 'gte':
            return field_value >= filter_value
        elif op == 'lt':
            return field_value < filter_value
        elif op == 'lte':
            return field_value <= filter_value
        elif op == 'in':
            return field_value in filter_value
        elif op == 'contains':
            return filter_value in str(field_value)
        return False
