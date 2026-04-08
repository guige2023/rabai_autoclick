"""Data Watermark Action Module for RabAI AutoClick.

Watermark tracking for data freshness and event time
ordering in streaming data pipelines.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Watermark:
    """Watermark tracking entry."""
    watermark_id: str
    stream_name: str
    event_time: float
    processing_time: float
    watermark_value: float


class DataWatermarkAction(BaseAction):
    """Watermark tracking for data freshness.

    Tracks event time watermarks for streaming data to handle
    out-of-order events and late data detection. Supports
    watermark progress monitoring and lag calculation.
    """
    action_type = "data_watermark"
    display_name = "数据水印追踪"
    description = "数据新鲜度水印，事件时间排序"

    _watermarks: Dict[str, Watermark] = {}
    _watermark_history: List[Watermark] = []
    _max_history = 1000

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute watermark operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'advance', 'get_watermark', 'get_lag',
                               'check_late', 'list', 'reset'
                - stream_name: str - name of the data stream
                - watermark_id: str (optional) - watermark identifier
                - event_time: float (optional) - event timestamp
                - watermark_value: float (optional) - watermark value to set
                - grace_period: float (optional) - late data grace period

        Returns:
            ActionResult with watermark operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'advance')

            if operation == 'advance':
                return self._advance_watermark(params, start_time)
            elif operation == 'get_watermark':
                return self._get_watermark(params, start_time)
            elif operation == 'get_lag':
                return self._get_lag(params, start_time)
            elif operation == 'check_late':
                return self._check_late_data(params, start_time)
            elif operation == 'list':
                return self._list_watermarks(start_time)
            elif operation == 'reset':
                return self._reset_watermark(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Watermark action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _advance_watermark(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Advance the watermark for a stream."""
        stream_name = params.get('stream_name', 'default')
        watermark_value = params.get('watermark_value', time.time())
        watermark_id = params.get('watermark_id', f'wm_{stream_name}_{int(watermark_value)}')

        key = f"{stream_name}"

        new_watermark = Watermark(
            watermark_id=watermark_id,
            stream_name=stream_name,
            event_time=watermark_value,
            processing_time=time.time(),
            watermark_value=watermark_value
        )

        self._watermarks[key] = new_watermark
        self._watermark_history.append(new_watermark)

        if len(self._watermark_history) > self._max_history:
            self._watermark_history.pop(0)

        old_watermark = None
        if watermark_id in self._watermarks:
            old_watermark = self._watermarks[watermark_id].watermark_value

        return ActionResult(
            success=True,
            message=f"Watermark advanced: {stream_name}",
            data={
                'stream_name': stream_name,
                'watermark_id': watermark_id,
                'watermark_value': watermark_value,
                'previous_value': old_watermark,
                'processing_time': new_watermark.processing_time
            },
            duration=time.time() - start_time
        )

    def _get_watermark(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get current watermark for a stream."""
        stream_name = params.get('stream_name', 'default')

        key = f"{stream_name}"
        if key not in self._watermarks:
            return ActionResult(
                success=False,
                message=f"Watermark not found for stream: {stream_name}",
                duration=time.time() - start_time
            )

        wm = self._watermarks[key]

        return ActionResult(
            success=True,
            message=f"Watermark for {stream_name}",
            data={
                'stream_name': stream_name,
                'watermark_id': wm.watermark_id,
                'watermark_value': wm.watermark_value,
                'event_time': wm.event_time,
                'processing_time': wm.processing_time
            },
            duration=time.time() - start_time
        )

    def _get_lag(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate watermark lag."""
        stream_name = params.get('stream_name', 'default')
        current_time = params.get('current_time', time.time())

        key = f"{stream_name}"
        if key not in self._watermarks:
            return ActionResult(
                success=True,
                message="No watermark, lag unknown",
                data={'lag': None, 'stream_name': stream_name},
                duration=time.time() - start_time
            )

        wm = self._watermarks[key]
        lag = current_time - wm.watermark_value

        return ActionResult(
            success=True,
            message=f"Watermark lag: {lag:.2f}s",
            data={
                'stream_name': stream_name,
                'lag_seconds': lag,
                'watermark_value': wm.watermark_value,
                'current_time': current_time
            },
            duration=time.time() - start_time
        )

    def _check_late_data(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check if data is late based on watermark."""
        stream_name = params.get('stream_name', 'default')
        event_time = params.get('event_time', 0)
        grace_period = params.get('grace_period', 0)

        key = f"{stream_name}"
        watermark_value = 0

        if key in self._watermarks:
            watermark_value = self._watermarks[key].watermark_value

        is_late = event_time < (watermark_value - grace_period)
        lateness = watermark_value - event_time if is_late else 0

        return ActionResult(
            success=True,
            message=f"Late data check: {stream_name}",
            data={
                'stream_name': stream_name,
                'event_time': event_time,
                'watermark_value': watermark_value,
                'grace_period': grace_period,
                'is_late': is_late,
                'lateness': lateness
            },
            duration=time.time() - start_time
        )

    def _list_watermarks(self, start_time: float) -> ActionResult:
        """List all current watermarks."""
        watermarks = [
            {
                'stream_name': wm.stream_name,
                'watermark_id': wm.watermark_id,
                'watermark_value': wm.watermark_value,
                'processing_time': wm.processing_time
            }
            for wm in self._watermarks.values()
        ]

        return ActionResult(
            success=True,
            message=f"Watermarks: {len(watermarks)}",
            data={
                'watermarks': watermarks,
                'count': len(watermarks),
                'history_size': len(self._watermark_history)
            },
            duration=time.time() - start_time
        )

    def _reset_watermark(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reset watermark for a stream."""
        stream_name = params.get('stream_name', 'default')

        key = f"{stream_name}"
        existed = key in self._watermarks
        if existed:
            del self._watermarks[key]

        return ActionResult(
            success=True,
            message=f"Watermark reset: {stream_name}",
            data={
                'stream_name': stream_name,
                'was_present': existed
            },
            duration=time.time() - start_time
        )
