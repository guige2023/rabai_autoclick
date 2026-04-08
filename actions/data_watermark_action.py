"""Data Watermark Action Module.

Provides watermark tracking and late-arrival data handling
for streaming and time-series data processing.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WatermarkTrackerAction(BaseAction):
    """Track watermarks for streaming data.
    
    Supports event-time and processing-time watermarks with late data handling.
    """
    action_type = "watermark_tracker"
    display_name = "水位线追踪"
    description = "追踪流数据的水位线"

    def __init__(self):
        super().__init__()
        self._watermarks: Dict[str, float] = {}
        self._late_data: Dict[str, List] = defaultdict(list)
        self._allowed_lateness: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Track watermarks.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'update', 'get', 'check_late', 'get_late_data'.
                - stream_id: Stream identifier.
                - timestamp: Event timestamp to update watermark.
                - allowed_lateness: Allowed lateness in seconds.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with watermark result or error.
        """
        operation = params.get('operation', 'update')
        stream_id = params.get('stream_id', 'default')
        timestamp = params.get('timestamp', None)
        allowed_lateness = params.get('allowed_lateness', 300)
        output_var = params.get('output_var', 'watermark_result')

        try:
            # Set allowed lateness
            self._allowed_lateness[stream_id] = allowed_lateness

            if operation == 'update':
                return self._update_watermark(stream_id, timestamp, output_var)
            elif operation == 'get':
                return self._get_watermark(stream_id, output_var)
            elif operation == 'check_late':
                return self._check_late_data(stream_id, timestamp, output_var)
            elif operation == 'get_late_data':
                return self._get_late_data(stream_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Watermark tracker failed: {str(e)}"
            )

    def _update_watermark(
        self, stream_id: str, timestamp: float, output_var: str
    ) -> ActionResult:
        """Update watermark for a stream."""
        if timestamp is None:
            timestamp = time.time()

        # Only advance watermark
        current = self._watermarks.get(stream_id, 0)
        if timestamp > current:
            self._watermarks[stream_id] = timestamp

        # Clean up late data that's now within watermark
        self._cleanup_late_data(stream_id)

        result = {
            'stream_id': stream_id,
            'watermark': self._watermarks.get(stream_id, 0),
            'updated': timestamp > current
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Watermark for '{stream_id}': {result['watermark']}"
        )

    def _get_watermark(self, stream_id: str, output_var: str) -> ActionResult:
        """Get current watermark for a stream."""
        watermark = self._watermarks.get(stream_id, 0)

        result = {
            'stream_id': stream_id,
            'watermark': watermark,
            'timestamp': datetime.fromtimestamp(watermark).isoformat() if watermark else None
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Watermark for '{stream_id}': {watermark}"
        )

    def _check_late_data(
        self, stream_id: str, timestamp: float, output_var: str
    ) -> ActionResult:
        """Check if data is late."""
        if timestamp is None:
            return ActionResult(
                success=False,
                message="timestamp is required"
            )

        watermark = self._watermarks.get(stream_id, 0)
        allowed_lateness = self._allowed_lateness.get(stream_id, 300)
        watermark_boundary = watermark - allowed_lateness

        is_late = timestamp < watermark_boundary

        if is_late:
            # Store late data
            self._late_data[stream_id].append({
                'timestamp': timestamp,
                'received_at': time.time()
            })

        result = {
            'stream_id': stream_id,
            'event_timestamp': timestamp,
            'watermark': watermark,
            'is_late': is_late,
            'lateness': watermark - timestamp if is_late else 0
        }

        context.variables[output_var] = result
        return ActionResult(
            success=not is_late,
            data=result,
            message=f"Data {'is late' if is_late else 'on time'} by {result['lateness']:.2f}s"
        )

    def _get_late_data(self, stream_id: str, output_var: str) -> ActionResult:
        """Get stored late data for a stream."""
        late_data = self._late_data.get(stream_id, [])

        result = {
            'stream_id': stream_id,
            'late_count': len(late_data),
            'late_data': late_data
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved {len(late_data)} late data entries"
        )

    def _cleanup_late_data(self, stream_id: str):
        """Clean up late data that's now within watermark."""
        if stream_id not in self._late_data:
            return

        watermark = self._watermarks.get(stream_id, 0)
        allowed_lateness = self._allowed_lateness.get(stream_id, 300)
        cutoff = watermark - allowed_lateness

        self._late_data[stream_id] = [
            d for d in self._late_data[stream_id]
            if d['timestamp'] >= cutoff
        ]


class LateDataHandlerAction(BaseAction):
    """Handle late-arriving data in streaming pipelines.
    
    Supports various strategies: discard, emit, update, or hold.
    """
    action_type = "late_data_handler"
    display_name = "延迟数据处理"
    description = "处理流管道中延迟到达的数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Handle late data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'handle', 'configure', 'flush'.
                - strategy: 'discard', 'emit', 'update', 'hold'.
                - data: Late data item.
                - timestamp: Data timestamp.
                - stream_id: Stream identifier.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with handling result or error.
        """
        operation = params.get('operation', 'handle')
        strategy = params.get('strategy', 'emit')
        data = params.get('data', {})
        timestamp = params.get('timestamp', None)
        stream_id = params.get('stream_id', 'default')
        output_var = params.get('output_var', 'late_handler_result')

        try:
            if operation == 'handle':
                return self._handle_late_data(
                    strategy, data, timestamp, stream_id, output_var
                )
            elif operation == 'configure':
                return self._configure_handler(
                    stream_id, strategy, params, output_var
                )
            elif operation == 'flush':
                return self._flush_held_data(stream_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Late data handler failed: {str(e)}"
            )

    def _handle_late_data(
        self,
        strategy: str,
        data: Any,
        timestamp: float,
        stream_id: str,
        output_var: str
    ) -> ActionResult:
        """Handle late data according to strategy."""
        if not hasattr(self, '_held_data'):
            self._held_data = {}

        if stream_id not in self._held_data:
            self._held_data[stream_id] = deque()

        result = {
            'strategy': strategy,
            'handled': True,
            'action': None,
            'data': data
        }

        if strategy == 'discard':
            result['action'] = 'discarded'
            result['data'] = None

        elif strategy == 'emit':
            result['action'] = 'emitted'

        elif strategy == 'update':
            result['action'] = 'updated'
            # Emit for updating downstream state

        elif strategy == 'hold':
            self._held_data[stream_id].append({
                'data': data,
                'timestamp': timestamp,
                'held_at': time.time()
            })
            result['action'] = 'held'
            result['held_count'] = len(self._held_data[stream_id])
            result['data'] = None

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Late data {result['action']} using '{strategy}' strategy"
        )

    def _configure_handler(
        self,
        stream_id: str,
        strategy: str,
        config: Dict,
        output_var: str
    ) -> ActionResult:
        """Configure late data handler."""
        if not hasattr(self, '_config'):
            self._config = {}

        self._config[stream_id] = {
            'strategy': strategy,
            'max_hold': config.get('max_hold', 100),
            'flush_interval': config.get('flush_interval', 60)
        }

        context.variables[output_var] = self._config[stream_id]
        return ActionResult(
            success=True,
            data=self._config[stream_id],
            message=f"Handler configured for stream '{stream_id}'"
        )

    def _flush_held_data(self, stream_id: str, output_var: str) -> ActionResult:
        """Flush held data for a stream."""
        if not hasattr(self, '_held_data') or stream_id not in self._held_data:
            return ActionResult(
                success=True,
                data={'flushed': 0},
                message="No held data to flush"
            )

        flushed = list(self._held_data[stream_id])
        self._held_data[stream_id].clear()

        result = {
            'flushed': len(flushed),
            'data': flushed
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Flushed {len(flushed)} held data items"
        )


class WindowWatermarkAction(BaseAction):
    """Manage watermarks for time windows in streaming.
    
    Supports early, on-time, and late window triggering.
    """
    action_type = "window_watermark"
    display_name = "窗口水位线"
    description = "管理流中时间窗口的水位线"

    def __init__(self):
        super().__init__()
        self._windows: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage window watermarks.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add', 'trigger', 'get_pending'.
                - window_id: Window identifier.
                - window_start: Window start timestamp.
                - window_end: Window end timestamp.
                - watermark: Current watermark.
                - data: Data to add to window.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with window watermark result or error.
        """
        operation = params.get('operation', 'add')
        window_id = params.get('window_id', '')
        window_start = params.get('window_start', 0)
        window_end = params.get('window_end', 0)
        watermark = params.get('watermark', 0)
        data = params.get('data', None)
        output_var = params.get('output_var', 'window_result')

        try:
            if operation == 'add':
                return self._add_to_window(
                    window_id, window_start, window_end, watermark, data, output_var
                )
            elif operation == 'trigger':
                return self._trigger_window(window_id, watermark, output_var)
            elif operation == 'get_pending':
                return self._get_pending_windows(output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Window watermark failed: {str(e)}"
            )

    def _add_to_window(
        self,
        window_id: str,
        window_start: float,
        window_end: float,
        watermark: float,
        data: Any,
        output_var: str
    ) -> ActionResult:
        """Add data to a window."""
        key = f"{window_id}_{window_start}_{window_end}"

        if key not in self._windows:
            self._windows[key] = {
                'window_id': window_id,
                'start': window_start,
                'end': window_end,
                'data': [],
                'triggered': False
            }

        self._windows[key]['data'].append(data)

        result = {
            'window_key': key,
            'data_count': len(self._windows[key]['data']),
            'ready_to_trigger': watermark >= window_end
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Added data to window '{key}': {result['data_count']} items"
        )

    def _trigger_window(self, window_id: str, watermark: float, output_var: str) -> ActionResult:
        """Trigger windows that are ready."""
        triggered = []
        pending = []

        for key, window in self._windows.items():
            if window['triggered']:
                continue

            if watermark >= window['end']:
                window['triggered'] = True
                triggered.append({
                    'window_key': key,
                    'data': window['data'],
                    'count': len(window['data'])
                })
            elif watermark >= window['start']:
                pending.append(key)

        result = {
            'triggered': triggered,
            'pending_count': len(pending),
            'triggered_count': len(triggered)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Triggered {len(triggered)} windows, {len(pending)} pending"
        )

    def _get_pending_windows(self, output_var: str) -> ActionResult:
        """Get all pending windows."""
        pending = []
        for key, window in self._windows.items():
            if not window['triggered']:
                pending.append({
                    'window_key': key,
                    'start': window['start'],
                    'end': window['end'],
                    'data_count': len(window['data'])
                })

        context.variables[output_var] = pending
        return ActionResult(
            success=True,
            data={'pending': pending, 'count': len(pending)},
            message=f"Found {len(pending)} pending windows"
        )
