"""Automation Scroll Action Module for RabAI AutoClick.

Screen scrolling automation with smooth scroll trajectories,
scroll-to-element, and momentum-based scrolling.
"""

import time
import math
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScrollDirection:
    """Scroll direction constants."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    HOME = "home"
    END = "end"


class AutomationScrollAction(BaseAction):
    """Scroll automation with multiple strategies.

    Provides smooth scrolling, scroll-to-element, momentum-based
    scrolling, and configurable scroll speed. Supports both
    mouse wheel and keyboard scroll operations.
    """
    action_type = "automation_scroll"
    display_name = "滚动自动化"
    description = "屏幕滚动自动化，平滑滚动和惯性滚动"

    _scroll_history: List[Dict[str, Any]] = []
    _max_history = 100

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scroll operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'scroll', 'scroll_to_element', 'scroll_to',
                                   'momentum_scroll', 'scroll_by'
                - direction: str (optional) - 'up', 'down', 'left', 'right'
                - amount: int (optional) - scroll amount in pixels or lines
                - smooth: bool (optional) - use smooth scrolling
                - duration: float (optional) - scroll duration in seconds
                - x: int (optional) - x coordinate for anchor scroll
                - y: int (optional) - y coordinate for anchor scroll
                - ease: str (optional) - easing function name

        Returns:
            ActionResult with scroll operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'scroll')

            if operation == 'scroll':
                return self._scroll(params, start_time)
            elif operation == 'scroll_to_element':
                return self._scroll_to_element(params, start_time)
            elif operation == 'scroll_to':
                return self._scroll_to(params, start_time)
            elif operation == 'momentum_scroll':
                return self._momentum_scroll(params, start_time)
            elif operation == 'scroll_by':
                return self._scroll_by(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Scroll action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _scroll(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform scroll operation."""
        direction = params.get('direction', ScrollDirection.DOWN)
        amount = params.get('amount', 3)
        smooth = params.get('smooth', True)
        duration = params.get('duration', 0.5)
        ease = params.get('ease', 'ease_in_out')
        x = params.get('x')
        y = params.get('y')

        if smooth:
            steps = self._calculate_scroll_steps(duration)
            for step in range(steps):
                t = step / steps
                eased_t = self._apply_easing(t, ease)
                delta = amount * eased_t / steps

                if direction == ScrollDirection.DOWN:
                    self._scroll_mouse(0, -int(delta * 3))
                elif direction == ScrollDirection.UP:
                    self._scroll_mouse(0, int(delta * 3))
                elif direction == ScrollDirection.LEFT:
                    self._scroll_mouse(int(delta * 3), 0)
                elif direction == ScrollDirection.RIGHT:
                    self._scroll_mouse(-int(delta * 3), 0)
                elif direction == ScrollDirection.HOME:
                    self._scroll_mouse(0, int(delta * 10))
                elif direction == ScrollDirection.END:
                    self._scroll_mouse(0, -int(delta * 10))

                time.sleep(duration / steps)
        else:
            self._scroll_mouse(
                0, -amount * 3 if direction == ScrollDirection.DOWN else amount * 3
            )

        self._record_scroll(direction, amount, smooth)

        return ActionResult(
            success=True,
            message=f"Scrolled {direction}: {amount}",
            data={
                'direction': direction,
                'amount': amount,
                'smooth': smooth,
                'duration': duration
            },
            duration=time.time() - start_time
        )

    def _scroll_to_element(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Scroll to a specific element by coordinates."""
        x = params.get('x', 0)
        y = params.get('y', 0)
        duration = params.get('duration', 1.0)
        ease = params.get('ease', 'ease_in_out')

        current_x, current_y = self._get_current_scroll_position()
        delta_x = x - current_x
        delta_y = y - current_y

        steps = self._calculate_scroll_steps(duration)

        for step in range(steps):
            t = step / steps
            eased_t = self._apply_easing(t, ease)
            move_x = int(delta_x * 1 / steps)
            move_y = int(delta_y * 1 / steps)

            self._scroll_mouse(-move_x, -move_y)
            time.sleep(duration / steps)

        return ActionResult(
            success=True,
            message=f"Scrolled to element ({x}, {y})",
            data={
                'target_x': x,
                'target_y': y,
                'steps': steps,
                'duration': duration
            },
            duration=time.time() - start_time
        )

    def _scroll_to(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Scroll to specific coordinates."""
        x = params.get('x', 0)
        y = params.get('y', 0)

        self._scroll_mouse(-x, -y)

        return ActionResult(
            success=True,
            message=f"Scrolled to ({x}, {y})",
            data={'x': x, 'y': y},
            duration=time.time() - start_time
        )

    def _momentum_scroll(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform momentum-based scrolling."""
        direction = params.get('direction', ScrollDirection.DOWN)
        velocity = params.get('velocity', 10)
        friction = params.get('friction', 0.95)
        min_velocity = params.get('min_velocity', 0.5)

        current_velocity = velocity
        scroll_count = 0

        while current_velocity >= min_velocity:
            delta = int(current_velocity * 3)

            if direction == ScrollDirection.DOWN:
                self._scroll_mouse(0, -delta)
            elif direction == ScrollDirection.UP:
                self._scroll_mouse(0, delta)
            elif direction == ScrollDirection.LEFT:
                self._scroll_mouse(delta, 0)
            elif direction == ScrollDirection.RIGHT:
                self._scroll_mouse(-delta, 0)

            current_velocity *= friction
            scroll_count += 1
            time.sleep(0.016)

        return ActionResult(
            success=True,
            message=f"Momentum scroll completed: {scroll_count} scrolls",
            data={
                'direction': direction,
                'initial_velocity': velocity,
                'scroll_count': scroll_count
            },
            duration=time.time() - start_time
        )

    def _scroll_by(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Scroll by relative amounts."""
        delta_x = params.get('delta_x', 0)
        delta_y = params.get('delta_y', 0)

        self._scroll_mouse(-delta_x, -delta_y)

        return ActionResult(
            success=True,
            message=f"Scrolled by ({delta_x}, {delta_y})",
            data={'delta_x': delta_x, 'delta_y': delta_y},
            duration=time.time() - start_time
        )

    def _scroll_mouse(self, delta_x: int, delta_y: int) -> None:
        """Send scroll event using Quartz."""
        try:
            import Quartz
            scroll_event = Quartz.CGEventCreateScrollWheelEvent(
                None,
                Quartz.kCGScrollWheelEventId,
                Quartz.kCGScrollEventUnitLine,
                delta_y,
                delta_x if abs(delta_x) > 0 else 0
            )
            if scroll_event:
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, scroll_event)
        except ImportError:
            pass

    def _calculate_scroll_steps(self, duration: float) -> int:
        """Calculate number of scroll steps for smooth scrolling."""
        base_steps = 10
        additional_steps = int(duration * 30)
        return max(base_steps, additional_steps)

    def _apply_easing(self, t: float, ease: str) -> float:
        """Apply easing function to parameter t."""
        if ease == 'linear':
            return t
        elif ease == 'ease_in':
            return t * t
        elif ease == 'ease_out':
            return t * (2 - t)
        elif ease == 'ease_in_out':
            return t * t * (3 - 2 * t)
        elif ease == 'bounce':
            if t < 0.5:
                return 2 * t * t
            else:
                return 1 - math.pow(-2 * t + 2, 2) / 2
        elif ease == 'elastic':
            return math.sin(t * math.pi * 2) * math.pow(1 - t, 2) + t
        return t

    def _get_current_scroll_position(self) -> Tuple[int, int]:
        """Get current scroll position."""
        return (0, 0)

    def _record_scroll(self, direction: str, amount: int, smooth: bool) -> None:
        """Record scroll in history."""
        entry = {
            'direction': direction,
            'amount': amount,
            'smooth': smooth,
            'timestamp': time.time()
        }
        self._scroll_history.append(entry)
        if len(self._scroll_history) > self._max_history:
            self._scroll_history.pop(0)
