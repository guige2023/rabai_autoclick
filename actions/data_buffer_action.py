"""Data Buffer Action Module for RabAI AutoClick.

Provides buffered data processing with flush policies,
batch size limits, and time-based triggering.
"""

import time
import threading
import sys
import os
from typing import Any, Callable, Dict, List, Optional
from collections import deque
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlushPolicy:
    """Buffer flush policy types."""
    SIZE_BASED = "size_based"
    TIME_BASED = "time_based"
    MANUAL = "manual"
    SIZE_OR_TIME = "size_or_time"
    SIZE_AND_TIME = "size_and_time"


@dataclass
class BufferStats:
    """Buffer statistics."""
    total_items: int
    flush_count: int
    dropped_items: int
    last_flush_time: float
    current_size: int


class DataBufferAction(BaseAction):
    """Buffered data processing with configurable flush policies.

    Accumulates data items in a buffer and flushes them based on
    size, time, or manual triggers. Supports backpressure handling,
    overflow policies, and batch processing.
    """
    action_type = "data_buffer"
    display_name = "数据缓冲器"
    description = "数据缓冲处理，支持大小/时间/手动刷新策略"

    _buffers: Dict[str, Dict[str, Any]] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute buffer operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create', 'push', 'flush', 'stats', 'clear'
                - buffer_name: str - name of the buffer
                - max_size: int (optional) - max items before flush
                - flush_interval: float (optional) - seconds between flushes
                - flush_policy: str (optional) - flush policy type
                - item: Any (optional) - item to push to buffer
                - overflow_policy: str (optional) - 'drop_oldest', 'drop_newest', 'block'

        Returns:
            ActionResult with buffer operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'push')

            if operation == 'create':
                return self._create_buffer(params, start_time)
            elif operation == 'push':
                return self._push_item(params, start_time)
            elif operation == 'flush':
                return self._flush_buffer(params, start_time)
            elif operation == 'stats':
                return self._get_stats(params, start_time)
            elif operation == 'clear':
                return self._clear_buffer(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Buffer action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_buffer(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new data buffer."""
        buffer_name = params.get('buffer_name', 'default')
        max_size = params.get('max_size', 100)
        flush_interval = params.get('flush_interval', 60.0)
        flush_policy = params.get('flush_policy', FlushPolicy.SIZE_OR_TIME)
        overflow_policy = params.get('overflow_policy', 'drop_oldest')

        if buffer_name in self._buffers:
            return ActionResult(
                success=True,
                message=f"Buffer already exists: {buffer_name}",
                data={'buffer_name': buffer_name, 'created': False},
                duration=time.time() - start_time
            )

        self._buffers[buffer_name] = {
            'name': buffer_name,
            'max_size': max_size,
            'flush_interval': flush_interval,
            'flush_policy': flush_policy,
            'overflow_policy': overflow_policy,
            'items': deque(maxlen=max_size),
            'lock': threading.RLock(),
            'created_at': time.time(),
            'last_flush': time.time(),
            'total_items': 0,
            'flush_count': 0,
            'dropped_items': 0
        }

        return ActionResult(
            success=True,
            message=f"Buffer created: {buffer_name}",
            data={
                'buffer_name': buffer_name,
                'max_size': max_size,
                'flush_policy': flush_policy
            },
            duration=time.time() - start_time
        )

    def _push_item(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Push an item into the buffer."""
        buffer_name = params.get('buffer_name', 'default')
        item = params.get('item')

        if buffer_name not in self._buffers:
            self._create_buffer({'buffer_name': buffer_name}, start_time)

        buffer = self._buffers[buffer_name]

        with buffer['lock']:
            current_size = len(buffer['items'])

            if current_size >= buffer['max_size']:
                overflow_policy = buffer['overflow_policy']
                if overflow_policy == 'drop_oldest':
                    try:
                        buffer['items'].popleft()
                        buffer['dropped_items'] += 1
                    except IndexError:
                        pass
                elif overflow_policy == 'drop_newest':
                    buffer['dropped_items'] += 1
                    item = None
                elif overflow_policy == 'block':
                    buffer['items'].append(item)
                    buffer['total_items'] += 1
                    return ActionResult(
                        success=True,
                        message=f"Buffer full, item blocked: {buffer_name}",
                        data={
                            'buffer_name': buffer_name,
                            'blocked': True,
                            'current_size': current_size
                        },
                        duration=time.time() - start_time
                    )

            if item is not None:
                buffer['items'].append(item)
                buffer['total_items'] += 1

            should_flush = self._should_flush(buffer)

            if should_flush:
                flushed = self._perform_flush(buffer)
                buffer['flush_count'] += 1
                buffer['last_flush'] = time.time()
            else:
                flushed = 0

        return ActionResult(
            success=True,
            message=f"Item pushed to {buffer_name}",
            data={
                'buffer_name': buffer_name,
                'current_size': len(buffer['items']),
                'flushed_this_push': flushed,
                'total_items': buffer['total_items']
            },
            duration=time.time() - start_time
        )

    def _flush_buffer(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Manually flush a buffer."""
        buffer_name = params.get('buffer_name', 'default')

        if buffer_name not in self._buffers:
            return ActionResult(
                success=False,
                message=f"Buffer not found: {buffer_name}",
                duration=time.time() - start_time
            )

        buffer = self._buffers[buffer_name]

        with buffer['lock']:
            flushed = self._perform_flush(buffer)
            buffer['flush_count'] += 1
            buffer['last_flush'] = time.time()

        return ActionResult(
            success=True,
            message=f"Buffer flushed: {flushed} items",
            data={
                'buffer_name': buffer_name,
                'flushed_items': flushed,
                'flush_count': buffer['flush_count']
            },
            duration=time.time() - start_time
        )

    def _get_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get buffer statistics."""
        buffer_name = params.get('buffer_name', 'default')

        if buffer_name not in self._buffers:
            return ActionResult(
                success=False,
                message=f"Buffer not found: {buffer_name}",
                duration=time.time() - start_time
            )

        buffer = self._buffers[buffer_name]

        with buffer['lock']:
            stats = BufferStats(
                total_items=buffer['total_items'],
                flush_count=buffer['flush_count'],
                dropped_items=buffer['dropped_items'],
                last_flush_time=buffer['last_flush'],
                current_size=len(buffer['items'])
            )

        return ActionResult(
            success=True,
            message=f"Buffer stats: {buffer_name}",
            data={
                'buffer_name': buffer_name,
                'current_size': stats.current_size,
                'max_size': buffer['max_size'],
                'total_items': stats.total_items,
                'flush_count': stats.flush_count,
                'dropped_items': stats.dropped_items,
                'last_flush_time': stats.last_flush_time,
                'flush_policy': buffer['flush_policy']
            },
            duration=time.time() - start_time
        )

    def _clear_buffer(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear all items from a buffer."""
        buffer_name = params.get('buffer_name', 'default')

        if buffer_name not in self._buffers:
            return ActionResult(
                success=False,
                message=f"Buffer not found: {buffer_name}",
                duration=time.time() - start_time
            )

        buffer = self._buffers[buffer_name]
        cleared = len(buffer['items'])

        with buffer['lock']:
            buffer['items'].clear()

        return ActionResult(
            success=True,
            message=f"Buffer cleared: {cleared} items removed",
            data={'buffer_name': buffer_name, 'cleared': cleared},
            duration=time.time() - start_time
        )

    def _should_flush(self, buffer: Dict[str, Any]) -> bool:
        """Check if buffer should be flushed based on policy."""
        policy = buffer['flush_policy']
        current_size = len(buffer['items'])
        time_since_flush = time.time() - buffer['last_flush']

        if policy == FlushPolicy.SIZE_BASED:
            return current_size >= buffer['max_size']
        elif policy == FlushPolicy.TIME_BASED:
            return time_since_flush >= buffer['flush_interval']
        elif policy == FlushPolicy.SIZE_OR_TIME:
            return (current_size >= buffer['max_size'] or
                    time_since_flush >= buffer['flush_interval'])
        elif policy == FlushPolicy.SIZE_AND_TIME:
            return (current_size >= buffer['max_size'] and
                    time_since_flush >= buffer['flush_interval'])
        return False

    def _perform_flush(self, buffer: Dict[str, Any]) -> int:
        """Perform the actual flush operation."""
        flushed_count = len(buffer['items'])
        buffer['items'].clear()
        return flushed_count
