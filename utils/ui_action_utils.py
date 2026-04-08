"""
UI action utilities for high-level automation actions.

Provides composite UI actions like click-and-wait,
drag-and-drop, and complex interactions.
"""

from __future__ import annotations

import time
from typing import Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum


class ActionType(Enum):
    """High-level UI action types."""
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    RIGHT_CLICK = "right_click"
    DRAG = "drag"
    DROP = "drop"
    TYPE = "type"
    PRESS = "press"
    SCROLL = "scroll"
    HOVER = "hover"
    WAIT = "wait"


@dataclass
class ActionResult:
    """Result of UI action."""
    action_type: ActionType
    success: bool
    message: str
    duration: float
    error: Optional[str] = None


@dataclass
class Point:
    """2D point."""
    x: int
    y: int
    
    def distance_to(self, other: 'Point') -> float:
        """Calculate distance to another point."""
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5


class UIActionExecutor:
    """Executes high-level UI actions."""
    
    def __init__(self):
        self._action_delays: dict = {}
    
    def click(self, x: int, y: int, button: str = "left") -> ActionResult:
        """
        Execute click action.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
            
        Returns:
            ActionResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEvent, CGEventCreateMouseEvent
            
            button_map = {
                "left": Quartz.kCGEventLeftMouseDown,
                "right": Quartz.kCGEventRightMouseDown,
                "middle": Quartz.kCGEventOtherMouseDown,
            }
            button_up_map = {
                "left": Quartz.kCGEventLeftMouseUp,
                "right": Quartz.kCGEventRightMouseUp,
                "middle": Quartz.kCGEventOtherMouseUp,
            }
            
            btn_type = button_map.get(button, Quartz.kCGEventLeftMouseDown)
            btn_up = button_up_map.get(button, Quartz.kCGEventLeftMouseUp)
            
            down = CGEventCreateMouseEvent(None, btn_type, (x, y), Quartz.kCGMouseButtonLeft)
            up = CGEventCreateMouseEvent(None, btn_up, (x, y), Quartz.kCGMouseButtonLeft)
            
            CGEvent.post(Quartz.kCGHIDEventTap, down)
            CGEvent.post(Quartz.kCGHIDEventTap, up)
            
            return ActionResult(
                action_type=ActionType.CLICK,
                success=True,
                message=f"Clicked at ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return ActionResult(
                action_type=ActionType.CLICK,
                success=False,
                message=f"Click failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def double_click(self, x: int, y: int) -> ActionResult:
        """
        Execute double click.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            ActionResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateMouseEvent
            
            for _ in range(2):
                down = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft)
                up = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft)
                CGEvent.post(Quartz.kCGHIDEventTap, down)
                CGEvent.post(Quartz.kCGHIDEventTap, up)
                time.sleep(0.05)
            
            return ActionResult(
                action_type=ActionType.DOUBLE_CLICK,
                success=True,
                message=f"Double clicked at ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return ActionResult(
                action_type=ActionType.DOUBLE_CLICK,
                success=False,
                message="Double click failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def drag(self, x1: int, y1: int, x2: int, y2: int,
            duration: float = 0.5, steps: int = 20) -> ActionResult:
        """
        Execute drag action.
        
        Args:
            x1: Start X.
            y1: Start Y.
            x2: End X.
            y2: End Y.
            duration: Total duration.
            steps: Number of steps.
            
        Returns:
            ActionResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateMouseEvent
            
            down = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDown, (x1, y1), Quartz.kCGMouseButtonLeft)
            CGEvent.post(Quartz.kCGHIDEventTap, down)
            
            for i in range(steps):
                t = (i + 1) / steps
                x = int(x1 + (x2 - x1) * t)
                y = int(y1 + (y2 - y1) * t)
                
                moved = CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft)
                CGEvent.post(Quartz.kCGHIDEventTap, moved)
                time.sleep(duration / steps)
            
            up = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseUp, (x2, y2), Quartz.kCGMouseButtonLeft)
            CGEvent.post(Quartz.kCGHIDEventTap, up)
            
            return ActionResult(
                action_type=ActionType.DRAG,
                success=True,
                message=f"Dragged from ({x1}, {y1}) to ({x2}, {y2})",
                duration=time.time() - start
            )
        except Exception as e:
            return ActionResult(
                action_type=ActionType.DRAG,
                success=False,
                message="Drag failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def hover(self, x: int, y: int) -> ActionResult:
        """
        Execute hover action.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            ActionResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateMouseEvent
            
            moved = CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft)
            CGEvent.post(Quartz.kCGHIDEventTap, moved)
            
            return ActionResult(
                action_type=ActionType.HOVER,
                success=True,
                message=f"Hovered at ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return ActionResult(
                action_type=ActionType.HOVER,
                success=False,
                message="Hover failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def wait(self, seconds: float) -> ActionResult:
        """
        Execute wait action.
        
        Args:
            seconds: Wait duration.
            
        Returns:
            ActionResult.
        """
        start = time.time()
        time.sleep(seconds)
        
        return ActionResult(
            action_type=ActionType.WAIT,
            success=True,
            message=f"Waited {seconds}s",
            duration=time.time() - start
        )


def click_and_wait(x: int, y: int, wait_seconds: float = 1.0) -> ActionResult:
    """
    Click and wait.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        wait_seconds: Wait duration.
        
    Returns:
        ActionResult.
    """
    executor = UIActionExecutor()
    click_result = executor.click(x, y)
    if click_result.success:
        executor.wait(wait_seconds)
    return click_result


def drag_and_drop(x1: int, y1: int, x2: int, y2: int,
                duration: float = 0.5) -> ActionResult:
    """
    Drag from point A to point B.
    
    Args:
        x1: Start X.
        y1: Start Y.
        x2: End X.
        y2: End Y.
        duration: Duration.
        
    Returns:
        ActionResult.
    """
    executor = UIActionExecutor()
    return executor.drag(x1, y1, x2, y2, duration)
