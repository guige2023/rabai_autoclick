"""Automation Mouse Path Action Module for RabAI AutoClick.

Records and replays complex mouse movement paths with
Bezier curves, bezier interpolation, and timing control.
"""

import time
import math
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BezierPoint:
    """A point on a Bezier curve."""
    def __init__(self, x: float, y: float, t: float = 0.0):
        self.x = x
        self.y = y
        self.t = t


class AutomationMousePathAction(BaseAction):
    """Mouse path recording and playback.

    Record mouse movement as path data with Bezier curve
    interpolation, configurable speed, and natural easing.
    Supports path smoothing, reversal, and path editing.
    """
    action_type = "automation_mouse_path"
    display_name = "鼠标路径自动化"
    description = "复杂鼠标路径录制与回放，Bezier曲线插值"

    _recorded_paths: Dict[str, List[Dict[str, float]]] = {}
    _active_recording: Optional[List[Dict[str, float]]] = None

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse path operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'start_recording', 'record_point',
                               'stop_recording', 'play', 'smooth',
                               'reverse', 'add_bezier', 'offset'
                - path_id: str - unique path identifier
                - x: float (optional) - x coordinate
                - y: float (optional) - y coordinate
                - duration: float (optional) - playback duration in seconds
                - speed: float (optional) - speed multiplier
                - smoothing: float (optional) - smoothing factor 0.0-1.0
                - bezier_points: list (optional) - list of control points

        Returns:
            ActionResult with path operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'play')

            if operation == 'start_recording':
                return self._start_recording(params, start_time)
            elif operation == 'record_point':
                return self._record_point(params, start_time)
            elif operation == 'stop_recording':
                return self._stop_recording(params, start_time)
            elif operation == 'play':
                return self._play_path(params, start_time)
            elif operation == 'smooth':
                return self._smooth_path(params, start_time)
            elif operation == 'reverse':
                return self._reverse_path(params, start_time)
            elif operation == 'add_bezier':
                return self._add_bezier_path(params, start_time)
            elif operation == 'offset':
                return self._offset_path(params, start_time)
            elif operation == 'list':
                return self._list_paths(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Mouse path action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _start_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Start recording mouse path."""
        path_id = params.get('path_id', f'path_{time.time()}')

        self._active_recording = []

        return ActionResult(
            success=True,
            message=f"Recording started: {path_id}",
            data={'path_id': path_id, 'recording': True, 'points': 0},
            duration=time.time() - start_time
        )

    def _record_point(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Record a point in the active path."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording",
                duration=time.time() - start_time
            )

        x = params.get('x', 0)
        y = params.get('y', 0)
        pressure = params.get('pressure', 1.0)
        button = params.get('button', '')

        point = {
            'x': x,
            'y': y,
            'timestamp': time.time(),
            'pressure': pressure,
            'button': button
        }

        self._active_recording.append(point)

        return ActionResult(
            success=True,
            message=f"Point recorded: ({x}, {y})",
            data={'point_count': len(self._active_recording)},
            duration=time.time() - start_time
        )

    def _stop_recording(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stop recording and save path."""
        if self._active_recording is None:
            return ActionResult(
                success=False,
                message="No active recording",
                duration=time.time() - start_time
            )

        path_id = params.get('path_id', f'path_{time.time()}')
        self._recorded_paths[path_id] = self._active_recording
        point_count = len(self._active_recording)
        self._active_recording = None

        return ActionResult(
            success=True,
            message=f"Recording saved: {path_id}",
            data={
                'path_id': path_id,
                'points': point_count,
                'duration': self._calculate_path_duration(self._recorded_paths[path_id])
            },
            duration=time.time() - start_time
        )

    def _play_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Play back a recorded mouse path."""
        path_id = params.get('path_id', '')
        duration = params.get('duration', 0)
        speed = params.get('speed', 1.0)
        easing = params.get('easing', 'linear')

        if path_id not in self._recorded_paths:
            return ActionResult(
                success=False,
                message=f"Path not found: {path_id}",
                duration=time.time() - start_time
            )

        path = self._recorded_paths[path_id]

        if not path:
            return ActionResult(
                success=False,
                message="Path has no points",
                duration=time.time() - start_time
            )

        path_duration = self._calculate_path_duration(path)
        playback_duration = duration if duration > 0 else path_duration
        point_interval = playback_duration / len(path) / speed

        played_points = 0
        for i, point in enumerate(path):
            if i > 0:
                delay = point_interval * self._easing_factor(easing, i / len(path))
                time.sleep(delay)

            self._move_mouse_to(point['x'], point['y'])
            played_points += 1

        return ActionResult(
            success=True,
            message=f"Path played: {path_id}",
            data={
                'path_id': path_id,
                'points_played': played_points,
                'duration': time.time() - start_time
            },
            duration=time.time() - start_time
        )

    def _smooth_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Smooth a path using moving average."""
        path_id = params.get('path_id', '')
        smoothing = params.get('smoothing', 0.5)
        window_size = params.get('window_size', 5)

        if path_id not in self._recorded_paths:
            return ActionResult(
                success=False,
                message=f"Path not found: {path_id}",
                duration=time.time() - start_time
            )

        original = self._recorded_paths[path_id]
        smoothed = self._apply_smoothing(original, window_size, smoothing)

        self._recorded_paths[path_id] = smoothed

        return ActionResult(
            success=True,
            message=f"Path smoothed: {path_id}",
            data={
                'path_id': path_id,
                'original_points': len(original),
                'smoothed_points': len(smoothed)
            },
            duration=time.time() - start_time
        )

    def _reverse_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Reverse a mouse path."""
        path_id = params.get('path_id', '')

        if path_id not in self._recorded_paths:
            return ActionResult(
                success=False,
                message=f"Path not found: {path_id}",
                duration=time.time() - start_time
            )

        reversed_path = list(reversed(self._recorded_paths[path_id]))
        self._recorded_paths[path_id] = reversed_path

        return ActionResult(
            success=True,
            message=f"Path reversed: {path_id}",
            data={'path_id': path_id, 'points': len(reversed_path)},
            duration=time.time() - start_time
        )

    def _add_bezier_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Generate a Bezier curve path between points."""
        bezier_points = params.get('bezier_points', [])
        num_samples = params.get('num_samples', 50)

        if len(bezier_points) < 2:
            return ActionResult(
                success=False,
                message="At least 2 bezier points required",
                duration=time.time() - start_time
            )

        path = self._generate_bezier_curve(bezier_points, num_samples)
        path_id = params.get('path_id', f'bezier_{time.time()}')
        self._recorded_paths[path_id] = path

        return ActionResult(
            success=True,
            message=f"Bezier path generated: {path_id}",
            data={
                'path_id': path_id,
                'points': len(path)
            },
            duration=time.time() - start_time
        )

    def _offset_path(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Offset a path by dx, dy."""
        path_id = params.get('path_id', '')
        dx = params.get('dx', 0)
        dy = params.get('dy', 0)

        if path_id not in self._recorded_paths:
            return ActionResult(
                success=False,
                message=f"Path not found: {path_id}",
                duration=time.time() - start_time
            )

        offset_path = []
        for point in self._recorded_paths[path_id]:
            offset_path.append({
                **point,
                'x': point['x'] + dx,
                'y': point['y'] + dy
            })

        self._recorded_paths[path_id] = offset_path

        return ActionResult(
            success=True,
            message=f"Path offset: ({dx}, {dy})",
            data={'path_id': path_id, 'dx': dx, 'dy': dy},
            duration=time.time() - start_time
        )

    def _list_paths(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all recorded paths."""
        paths = [
            {
                'path_id': pid,
                'points': len(p),
                'duration': self._calculate_path_duration(p)
            }
            for pid, p in self._recorded_paths.items()
        ]

        return ActionResult(
            success=True,
            message=f"Paths: {len(paths)}",
            data={'paths': paths, 'count': len(paths)},
            duration=time.time() - start_time
        )

    def _calculate_path_duration(self, path: List[Dict[str, float]]) -> float:
        """Calculate total duration of a path."""
        if len(path) < 2:
            return 0.0
        return path[-1]['timestamp'] - path[0]['timestamp']

    def _easing_factor(self, easing: str, t: float) -> float:
        """Calculate easing factor for interpolation."""
        if easing == 'linear':
            return 1.0
        elif easing == 'ease_in':
            return t * t
        elif easing == 'ease_out':
            return t * (2 - t)
        elif easing == 'ease_in_out':
            return t * t * (3 - 2 * t)
        return 1.0

    def _apply_smoothing(
        self,
        path: List[Dict[str, Any]],
        window_size: int,
        factor: float
    ) -> List[Dict[str, Any]]:
        """Apply moving average smoothing to path."""
        if len(path) < window_size:
            return path

        smoothed = []
        half_window = window_size // 2

        for i in range(len(path)):
            start = max(0, i - half_window)
            end = min(len(path), i + half_window + 1)

            avg_x = sum(path[j]['x'] for j in range(start, end)) / (end - start)
            avg_y = sum(path[j]['y'] for j in range(start, end)) / (end - start)

            orig_x = path[i]['x']
            orig_y = path[i]['y']

            smoothed.append({
                **path[i],
                'x': orig_x + (avg_x - orig_x) * factor,
                'y': orig_y + (avg_y - orig_y) * factor
            })

        return smoothed

    def _generate_bezier_curve(
        self,
        control_points: List[Tuple[float, float]],
        num_samples: int
    ) -> List[Dict[str, float]]:
        """Generate Bezier curve points from control points."""
        path = []
        for i in range(num_samples):
            t = i / (num_samples - 1)
            point = self._bezier_point_at_t(control_points, t)
            path.append({
                'x': point[0],
                'y': point[1],
                'timestamp': time.time() + t * 10,
                'pressure': 1.0,
                'button': ''
            })
        return path

    def _bezier_point_at_t(
        self,
        control_points: List[Tuple[float, float]],
        t: float
    ) -> Tuple[float, float]:
        """Calculate point on Bezier curve at parameter t."""
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

    def _move_mouse_to(self, x: float, y: float) -> None:
        """Move mouse to coordinates using Quartz."""
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
