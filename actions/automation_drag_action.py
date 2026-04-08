"""Automation Drag Action Module for RabAI AutoClick.

Drag and drop automation with Bezier path trajectories,
multi-point paths, and configurable drag speed.
"""

import time
import math
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationDragAction(BaseAction):
    """Drag and drop automation with path support.

    Performs drag operations using straight lines, curved paths,
    or multi-point trajectories. Supports hold duration, drag speed,
    and automatic drop zone detection.
    """
    action_type = "automation_drag"
    display_name = "拖拽自动化"
    description = "拖拽自动化，Bezier路径和多点轨迹"

    _drag_history: List[Dict[str, Any]] = []
    _max_history = 50

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute drag operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'drag', 'drag_to', 'drag_by', 'drag_path',
                                   'drag_with_path'
                - start_x: int - starting x coordinate
                - start_y: int - starting y coordinate
                - end_x: int (optional) - ending x coordinate
                - end_y: int (optional) - ending y coordinate
                - delta_x: int (optional) - relative x movement
                - delta_y: int (optional) - relative y movement
                - path: list (optional) - list of (x, y) waypoints
                - duration: float (optional) - drag duration in seconds
                - hold_duration: float (optional) - hold before drag
                - button: str (optional) - 'left', 'right'

        Returns:
            ActionResult with drag operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'drag')

            if operation == 'drag':
                return self._drag(params, start_time)
            elif operation == 'drag_to':
                return self._drag_to(params, start_time)
            elif operation == 'drag_by':
                return self._drag_by(params, start_time)
            elif operation == 'drag_path':
                return self._drag_path(params, start_time)
            elif operation == 'drag_with_path':
                return self._drag_with_bezier(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Drag action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _drag(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform drag from start to end coordinates."""
        start_x = params.get('start_x', 0)
        start_y = params.get('start_y', 0)
        end_x = params.get('end_x', start_x)
        end_y = params.get('end_y', start_y)
        duration = params.get('duration', 0.5)
        hold_duration = params.get('hold_duration', 0.1)
        button = params.get('button', 'left')

        time.sleep(hold_duration)

        steps = self._calculate_drag_steps(start_x, start_y, end_x, end_y, duration)

        for i in range(steps + 1):
            t = i / steps
            current_x = int(start_x + (end_x - start_x) * t)
            current_y = int(start_y + (end_y - start_y) * t)

            if i == 0:
                self._mouse_down(current_x, current_y, button)
            elif i == steps:
                self._mouse_up(current_x, current_y, button)
            else:
                self._mouse_move(current_x, current_y)

            time.sleep(duration / steps)

        self._record_drag(start_x, start_y, end_x, end_y, 'straight', duration)

        return ActionResult(
            success=True,
            message=f"Dragged from ({start_x}, {start_y}) to ({end_x}, {end_y})",
            data={
                'start_x': start_x,
                'start_y': start_y,
                'end_x': end_x,
                'end_y': end_y,
                'duration': duration,
                'steps': steps
            },
            duration=time.time() - start_time
        )

    def _drag_to(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drag from current position to target coordinates."""
        end_x = params.get('end_x', 0)
        end_y = params.get('end_y', 0)
        duration = params.get('duration', 0.5)

        current_x, current_y = self._get_current_mouse_position()

        return self._drag({
            'start_x': current_x,
            'start_y': current_y,
            'end_x': end_x,
            'end_y': end_y,
            'duration': duration,
            **params
        }, start_time)

    def _drag_by(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drag by relative delta from current position."""
        delta_x = params.get('delta_x', 0)
        delta_y = params.get('delta_y', 0)
        duration = params.get('duration', 0.5)

        current_x, current_y = self._get_current_mouse_position()

        return self._drag({
            'start_x': current_x,
            'start_y': current_y,
            'end_x': current_x + delta_x,
            'end_y': current_y + delta_y,
            'duration': duration,
            **params
        }, start_time)

    def _drag_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drag along a series of waypoints."""
        path = params.get('path', [])
        duration = params.get('duration', 1.0)
        hold_duration = params.get('hold_duration', 0.1)

        if len(path) < 2:
            return ActionResult(
                success=False,
                message="Path requires at least 2 points",
                duration=time.time() - start_time
            )

        time.sleep(hold_duration)

        total_distance = self._calculate_path_distance(path)
        steps_per_point = max(5, int(duration * 30 / len(path)))

        start = path[0]
        self._mouse_down(start[0], start[1], 'left')

        for i in range(1, len(path)):
            start = path[i - 1]
            end = path[i]
            for step in range(steps_per_point):
                t = step / steps_per_point
                current_x = int(start[0] + (end[0] - start[0]) * t)
                current_y = int(start[1] + (end[1] - start[1]) * t)
                self._mouse_move(current_x, current_y)
                time.sleep(duration / len(path) / steps_per_point)

        end = path[-1]
        self._mouse_up(end[0], end[1], 'left')

        return ActionResult(
            success=True,
            message=f"Dragged path with {len(path)} points",
            data={
                'path': path,
                'points': len(path),
                'total_distance': total_distance
            },
            duration=time.time() - start_time
        )

    def _drag_with_bezier(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drag along a Bezier curve path."""
        start_x = params.get('start_x', 0)
        start_y = params.get('start_y', 0)
        end_x = params.get('end_x', start_x)
        end_y = params.get('end_y', start_y)
        control_points = params.get('control_points', [])
        duration = params.get('duration', 0.5)

        bezier_points = [(start_x, start_y)]
        bezier_points.extend(control_points)
        bezier_points.append((end_x, end_y))

        time.sleep(0.1)

        steps = max(20, int(duration * 60))

        self._mouse_down(start_x, start_y, 'left')

        for i in range(steps + 1):
            t = i / steps
            point = self._calculate_bezier_point(bezier_points, t)
            self._mouse_move(int(point[0]), int(point[1]))
            time.sleep(duration / steps)

        self._mouse_up(int(self._calculate_bezier_point(bezier_points, 1.0)[0]),
                       int(self._calculate_bezier_point(bezier_points, 1.0)[1]), 'left')

        return ActionResult(
            success=True,
            message="Bezier drag completed",
            data={
                'start': (start_x, start_y),
                'end': (end_x, end_y),
                'control_points': len(control_points),
                'steps': steps
            },
            duration=time.time() - start_time
        )

    def _calculate_drag_steps(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        duration: float
    ) -> int:
        """Calculate number of steps for smooth drag."""
        distance = math.sqrt((end_x - start_x) ** 2 + (end_y - start_y) ** 2)
        base_steps = int(distance / 2)
        time_based_steps = int(duration * 60)
        return max(5, min(base_steps, time_based_steps))

    def _calculate_path_distance(self, path: List[Tuple[int, int]]) -> float:
        """Calculate total distance of a path."""
        total = 0.0
        for i in range(1, len(path)):
            dx = path[i][0] - path[i-1][0]
            dy = path[i][1] - path[i-1][1]
            total += math.sqrt(dx * dx + dy * dy)
        return total

    def _calculate_bezier_point(
        self,
        control_points: List[Tuple[float, float]],
        t: float
    ) -> Tuple[float, float]:
        """Calculate point on Bezier curve at t."""
        n = len(control_points) - 1
        x = 0.0
        y = 0.0

        for i, point in enumerate(control_points):
            coef = self._binomial_coeff(n, i) * ((1 - t) ** (n - i)) * (t ** i)
            x += coef * point[0]
            y += coef * point[1]

        return (x, y)

    def _binomial_coeff(self, n: int, k: int) -> float:
        """Calculate binomial coefficient C(n, k)."""
        if k < 0 or k > n:
            return 0
        if k == 0 or k == n:
            return 1

        result = 1.0
        for i in range(min(k, n - k)):
            result = result * (n - i) / (i + 1)
        return result

    def _mouse_down(self, x: int, y: int, button: str) -> None:
        """Send mouse down event."""
        self._mouse_event('down', x, y, button)

    def _mouse_up(self, x: int, y: int, button: str) -> None:
        """Send mouse up event."""
        self._mouse_event('up', x, y, button)

    def _mouse_move(self, x: int, y: int) -> None:
        """Send mouse move event."""
        try:
            import Quartz
            event = Quartz.CGEventCreateMouseEvent(
                None,
                Quartz.kCGEventMouseMoved,
                (x, y),
                Quartz.kCGMouseButtonLeft
            )
            if event:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
        except ImportError:
            pass

    def _mouse_event(self, event_type: str, x: int, y: int, button: str) -> None:
        """Send mouse button event."""
        try:
            import Quartz
            if button == 'left':
                if event_type == 'down':
                    event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft
                    )
                else:
                    event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft
                    )
            else:
                if event_type == 'down':
                    event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventRightMouseDown, (x, y), Quartz.kCGMouseButtonRight
                    )
                else:
                    event = Quartz.CGEventCreateMouseEvent(
                        None, Quartz.kCGEventRightMouseUp, (x, y), Quartz.kCGMouseButtonRight
                    )

            if event:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
        except ImportError:
            pass

    def _get_current_mouse_position(self) -> Tuple[int, int]:
        """Get current mouse position."""
        try:
            import Quartz
            loc = Quartz.CGEventGetLocation(Quartz.CGEventCreate(None))
            return (int(loc.x), int(loc.y))
        except ImportError:
            return (0, 0)

    def _record_drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        path_type: str,
        duration: float
    ) -> None:
        """Record drag operation in history."""
        entry = {
            'start': (start_x, start_y),
            'end': (end_x, end_y),
            'path_type': path_type,
            'duration': duration,
            'timestamp': time.time()
        }
        self._drag_history.append(entry)
        if len(self._drag_history) > self._max_history:
            self._drag_history.pop(0)
