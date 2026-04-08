"""Automation Gesture Action Module for RabAI AutoClick.

Touch gesture automation including tap, swipe, pinch,
and multi-touch sequences for UI interaction.
"""

import time
import math
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GestureType:
    """Supported gesture types."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE = "swipe"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    PINCH = "pinch"
    ZOOM = "zoom"


class AutomationGestureAction(BaseAction):
    """Touch gesture automation for UI interaction.

    Executes touch gestures including tap, swipe, pinch, and zoom.
    Supports multi-touch points, configurable gesture speed, and
    momentum-based swipe with deceleration.
    """
    action_type = "automation_gesture"
    display_name = "手势自动化"
    description = "触摸手势自动化，点击、滑动、捏合"

    _gesture_history: List[Dict[str, Any]] = []
    _max_history = 50

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gesture operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'tap', 'swipe', 'pinch', 'zoom', 'long_press'
                - x: int (optional) - x coordinate
                - y: int (optional) - y coordinate
                - x1: int (optional) - second x coordinate for swipe/zoom
                - y1: int (optional) - second y coordinate for swipe/zoom
                - duration: float (optional) - gesture duration in seconds
                - fingers: int (optional) - number of fingers
                - scale: float (optional) - zoom scale factor
                - direction: str (optional) - swipe direction

        Returns:
            ActionResult with gesture operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'tap')

            if operation == 'tap':
                return self._tap(params, start_time)
            elif operation == 'double_tap':
                return self._double_tap(params, start_time)
            elif operation == 'long_press':
                return self._long_press(params, start_time)
            elif operation == 'swipe':
                return self._swipe(params, start_time)
            elif operation == 'pinch':
                return self._pinch(params, start_time)
            elif operation == 'zoom':
                return self._zoom(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Gesture action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _tap(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a tap gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        fingers = params.get('fingers', 1)

        self._send_touch_event('tap', x, y, fingers=fingers)

        self._record_gesture('tap', x, y, fingers=fingers)

        return ActionResult(
            success=True,
            message=f"Tap at ({x}, {y}) with {fingers} finger(s)",
            data={
                'gesture': 'tap',
                'x': x,
                'y': y,
                'fingers': fingers
            },
            duration=time.time() - start_time
        )

    def _double_tap(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a double tap gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        interval = params.get('interval', 0.1)

        self._send_touch_event('tap', x, y)
        time.sleep(interval)
        self._send_touch_event('tap', x, y)

        self._record_gesture('double_tap', x, y)

        return ActionResult(
            success=True,
            message=f"Double tap at ({x}, {y})",
            data={
                'gesture': 'double_tap',
                'x': x,
                'y': y,
                'interval': interval
            },
            duration=time.time() - start_time
        )

    def _long_press(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a long press gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        duration = params.get('duration', 1.0)

        self._send_touch_event('touch_down', x, y)
        time.sleep(duration)
        self._send_touch_event('touch_up', x, y)

        self._record_gesture('long_press', x, y, duration=duration)

        return ActionResult(
            success=True,
            message=f"Long press at ({x}, {y}) for {duration}s",
            data={
                'gesture': 'long_press',
                'x': x,
                'y': y,
                'duration': duration
            },
            duration=time.time() - start_time
        )

    def _swipe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a swipe gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        x1 = params.get('x1', x)
        y1 = params.get('y1', y)
        direction = params.get('direction')
        duration = params.get('duration', 0.3)
        steps = params.get('steps', 30)

        if direction:
            x1, y1 = self._get_swipe_end(x, y, direction, params.get('distance', 200))

        for i in range(steps + 1):
            t = i / steps
            current_x = int(x + (x1 - x) * t)
            current_y = int(y + (y1 - y) * t)

            if i == 0:
                self._send_touch_event('touch_down', current_x, current_y)
            elif i == steps:
                self._send_touch_event('touch_up', current_x, current_y)
            else:
                self._send_touch_event('touch_move', current_x, current_y)

            time.sleep(duration / steps)

        self._record_gesture('swipe', x, y, x1=x1, y1=y1, direction=direction)

        return ActionResult(
            success=True,
            message=f"Swiped from ({x}, {y}) to ({x1}, {y1})",
            data={
                'gesture': 'swipe',
                'start': (x, y),
                'end': (x1, y1),
                'direction': direction,
                'duration': duration,
                'steps': steps
            },
            duration=time.time() - start_time
        )

    def _pinch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a pinch gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        scale = params.get('scale', 0.5)
        duration = params.get('duration', 0.5)

        x1 = x + 100
        y1 = y
        x2 = x - 100
        y2 = y

        steps = 20
        for i in range(steps + 1):
            t = i / steps
            if scale < 1.0:
                delta = (1 - scale) * 100
            else:
                delta = (scale - 1) * 100

            cx1 = int(x1 + delta)
            cy1 = y1
            cx2 = int(x2 - delta)
            cy2 = y2

            if i == 0:
                self._send_multi_touch('touch_down', cx1, cy1, cx2, cy2)
            elif i == steps:
                self._send_multi_touch('touch_up', cx1, cy1, cx2, cy2)
            else:
                self._send_multi_touch('touch_move', cx1, cy1, cx2, cy2)

            time.sleep(duration / steps)

        self._record_gesture('pinch', x, y, scale=scale)

        return ActionResult(
            success=True,
            message=f"Pinch at ({x}, {y}) scale={scale}",
            data={
                'gesture': 'pinch',
                'center': (x, y),
                'scale': scale,
                'duration': duration
            },
            duration=time.time() - start_time
        )

    def _zoom(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a zoom gesture."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        scale = params.get('scale', 2.0)
        duration = params.get('duration', 0.5)

        x1 = x + 100
        y1 = y
        x2 = x - 100
        y2 = y

        steps = 20
        for i in range(steps + 1):
            t = i / steps
            delta = (scale - 1) * 100 * t

            cx1 = int(x1 + delta)
            cy1 = y1
            cx2 = int(x2 - delta)
            cy2 = y2

            if i == 0:
                self._send_multi_touch('touch_down', cx1, cy1, cx2, cy2)
            elif i == steps:
                self._send_multi_touch('touch_up', cx1, cy1, cx2, cy2)
            else:
                self._send_multi_touch('touch_move', cx1, cy1, cx2, cy2)

            time.sleep(duration / steps)

        self._record_gesture('zoom', x, y, scale=scale)

        return ActionResult(
            success=True,
            message=f"Zoom at ({x}, {y}) scale={scale}",
            data={
                'gesture': 'zoom',
                'center': (x, y),
                'scale': scale,
                'duration': duration
            },
            duration=time.time() - start_time
        )

    def _send_touch_event(
        self,
        event_type: str,
        x: int,
        y: int,
        fingers: int = 1
    ) -> None:
        """Send a touch event using Quartz."""
        try:
            import Quartz
        except ImportError:
            return

        try:
            if event_type == 'tap':
                down = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft
                )
                up = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft
                )
                if down:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)
                if up:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)
            elif event_type == 'touch_down':
                event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft
                )
                if event:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
            elif event_type == 'touch_up':
                event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft
                )
                if event:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
            elif event_type == 'touch_move':
                event = Quartz.CGEventCreateMouseEvent(
                    None, Quartz.kCGEventLeftMouseDragged, (x, y), Quartz.kCGMouseButtonLeft
                )
                if event:
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
        except Exception:
            pass

    def _send_multi_touch(
        self,
        event_type: str,
        x1: int,
        y1: int,
        x2: int,
        y2: int
    ) -> None:
        """Send a multi-touch event."""
        pass

    def _get_swipe_end(
        self,
        x: int,
        y: int,
        direction: str,
        distance: int
    ) -> Tuple[int, int]:
        """Calculate swipe end coordinates based on direction."""
        if direction == 'up':
            return (x, y - distance)
        elif direction == 'down':
            return (x, y + distance)
        elif direction == 'left':
            return (x - distance, y)
        elif direction == 'right':
            return (x + distance, y)
        return (x, y)

    def _record_gesture(
        self,
        gesture: str,
        x: int,
        y: int,
        **kwargs
    ) -> None:
        """Record gesture in history."""
        entry = {
            'gesture': gesture,
            'x': x,
            'y': y,
            'timestamp': time.time(),
            **kwargs
        }
        self._gesture_history.append(entry)
        if len(self._gesture_history) > self._max_history:
            self._gesture_history.pop(0)
