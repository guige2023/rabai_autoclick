"""Touch action utilities for multi-touch UI automation.

Defines and executes touch actions (tap, swipe, pinch, rotate)
for touch-based UI automation.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class TouchActionType(Enum):
    """Types of touch actions."""
    TAP = auto()
    LONG_PRESS = auto()
    DOUBLE_TAP = auto()
    SWIPE = auto()
    DRAG = auto()
    PINCH = auto()
    ROTATE = auto()
    MULTI_TAP = auto()


@dataclass
class TouchPoint:
    """A single touch point.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        finger_id: Which finger (for multi-touch).
        pressure: Touch pressure (0.0-1.0).
    """
    x: float
    y: float
    finger_id: int = 0
    pressure: float = 1.0


@dataclass
class TouchAction:
    """A touch action with parameters.

    Attributes:
        action_type: The type of touch action.
        points: List of touch points for this action.
        duration: Duration in seconds (for long press, swipe).
        scale: Scale factor (for pinch).
        rotation: Rotation in degrees (for rotate).
        finger_count: Number of fingers (for multi-touch).
        target_element: Optional target element ID.
    """
    action_type: TouchActionType
    points: list[TouchPoint] = field(default_factory=list)
    duration: float = 0.3
    scale: float = 1.0
    rotation: float = 0.0
    finger_count: int = 1
    target_element: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def add_point(self, x: float, y: float, finger_id: int = 0) -> None:
        """Add a touch point."""
        self.points.append(TouchPoint(x=x, y=y, finger_id=finger_id))


def create_tap_action(
    x: float,
    y: float,
    duration: float = 0.1,
) -> TouchAction:
    """Create a single tap action."""
    action = TouchAction(action_type=TouchActionType.TAP, duration=duration)
    action.add_point(x, y)
    return action


def create_swipe_action(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    duration: float = 0.3,
    finger_count: int = 1,
) -> TouchAction:
    """Create a swipe action from start to end point."""
    action = TouchAction(
        action_type=TouchActionType.SWIPE,
        duration=duration,
        finger_count=finger_count,
    )
    action.add_point(start_x, start_y)
    action.add_point(end_x, end_y)
    return action


def create_pinch_action(
    center_x: float,
    center_y: float,
    scale: float = 1.0,
    duration: float = 0.5,
) -> TouchAction:
    """Create a pinch action.

    Args:
        center_x: Center X coordinate.
        center_y: Center Y coordinate.
        scale: Scale factor (< 1.0 = pinch close, > 1.0 = pinch open).
        duration: Duration in seconds.
    """
    action = TouchAction(
        action_type=TouchActionType.PINCH,
        scale=scale,
        duration=duration,
        finger_count=2,
    )
    action.add_point(center_x, center_y)
    return action


def create_drag_action(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    duration: float = 0.5,
) -> TouchAction:
    """Create a drag action."""
    action = TouchAction(
        action_type=TouchActionType.DRAG,
        duration=duration,
    )
    action.add_point(start_x, start_y)
    action.add_point(end_x, end_y)
    return action


def create_rotate_action(
    center_x: float,
    center_y: float,
    start_angle: float,
    end_angle: float,
    duration: float = 0.5,
) -> TouchAction:
    """Create a rotation action.

    Args:
        center_x: Center X coordinate.
        center_y: Center Y coordinate.
        start_angle: Start angle in degrees.
        end_angle: End angle in degrees.
        duration: Duration in seconds.
    """
    action = TouchAction(
        action_type=TouchActionType.ROTATE,
        rotation=end_angle - start_angle,
        duration=duration,
        finger_count=2,
    )
    action.add_point(center_x, center_y)
    return action


class TouchActionExecutor:
    """Executes touch actions on a device.

    This is an interface - actual execution requires a platform-specific
    implementation registered via register_driver().
    """

    def __init__(self) -> None:
        """Initialize executor."""
        self._driver: Optional[Any] = None

    def register_driver(self, driver: Any) -> None:
        """Register a platform-specific touch driver."""
        self._driver = driver

    def execute(self, action: TouchAction) -> bool:
        """Execute a touch action.

        Returns True if successful.
        """
        if self._driver is None:
            return False

        try:
            if action.action_type == TouchActionType.TAP:
                return self._driver.tap(
                    action.points[0].x, action.points[0].y,
                    action.duration,
                )
            elif action.action_type == TouchActionType.SWIPE:
                return self._driver.swipe(
                    action.points[0].x, action.points[0].y,
                    action.points[1].x, action.points[1].y,
                    action.duration,
                )
            elif action.action_type == TouchActionType.DRAG:
                return self._driver.drag(
                    action.points[0].x, action.points[0].y,
                    action.points[1].x, action.points[1].y,
                    action.duration,
                )
            elif action.action_type == TouchActionType.PINCH:
                return self._driver.pinch(
                    action.points[0].x, action.points[0].y,
                    action.scale, action.duration,
                )
            elif action.action_type == TouchActionType.ROTATE:
                return self._driver.rotate(
                    action.points[0].x, action.points[0].y,
                    action.rotation, action.duration,
                )
        except Exception:
            return False
        return False

    def execute_chain(self, actions: list[TouchAction]) -> list[bool]:
        """Execute a chain of touch actions.

        Returns list of success booleans for each action.
        """
        return [self.execute(a) for a in actions]
