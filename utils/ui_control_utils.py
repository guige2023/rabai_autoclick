"""
UI control utilities for automation actions.

Provides high-level UI control operations like clicking,
typing, scrolling, and element interaction.
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass


@dataclass
class UIResult:
    """Result of UI operation."""
    success: bool
    message: str
    duration: float
    error: Optional[str] = None


@dataclass
class UIElement:
    """UI element data."""
    role: str
    title: str
    value: str
    position: Tuple[int, int]
    size: Tuple[int, int]
    enabled: bool
    focused: bool


class UIController:
    """High-level UI control operations."""
    
    def __init__(self, app_bundle_id: Optional[str] = None):
        """
        Initialize UI controller.
        
        Args:
            app_bundle_id: Optional app scope.
        """
        self.app_bundle_id = app_bundle_id
    
    def click(self, x: int, y: int, button: str = "left") -> UIResult:
        """
        Click at coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEvent, CGEventCreateMouseEvent
            
            button_type = {
                'left': Quartz.kCGEventLeftMouseDown,
                'right': Quartz.kCGEventRightMouseDown,
                'middle': Quartz.kCGEventOtherMouseDown,
            }.get(button, Quartz.kCGEventLeftMouseDown)
            
            down = CGEventCreateMouseEvent(None, button_type, (x, y), Quartz.kCGMouseButtonLeft)
            up_type = {
                'left': Quartz.kCGEventLeftMouseUp,
                'right': Quartz.kCGEventRightMouseUp,
                'middle': Quartz.kCGEventOtherMouseUp,
            }.get(button, Quartz.kCGEventLeftMouseUp)
            up = CGEventCreateMouseEvent(None, up_type, (x, y), Quartz.kCGMouseButtonLeft)
            
            CGEvent.post(Quartz.kCGHIDEventTap, down)
            CGEvent.post(Quartz.kCGHIDEventTap, up)
            
            return UIResult(
                success=True,
                message=f"Clicked at ({x}, {y}) with {button} button",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message=f"Click failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def double_click(self, x: int, y: int) -> UIResult:
        """
        Double click at coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEvent, CGEventCreateMouseEvent
            
            for _ in range(2):
                down = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft)
                up = CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft)
                CGEvent.post(Quartz.kCGHIDEventTap, down)
                CGEvent.post(Quartz.kCGHIDEventTap, up)
                time.sleep(0.05)
            
            return UIResult(
                success=True,
                message=f"Double clicked at ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message="Double click failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def right_click(self, x: int, y: int) -> UIResult:
        """Right click at coordinates."""
        return self.click(x, y, button="right")
    
    def type_text(self, text: str, delay: float = 0.01) -> UIResult:
        """
        Type text.
        
        Args:
            text: Text to type.
            delay: Delay between keystrokes.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateKeyboardEvent
            
            for char in text:
                code = ord(char) if char.isascii() else 0
                if code:
                    down = CGEventCreateKeyboardEvent(None, code, True)
                    up = CGEventCreateKeyboardEvent(None, code, False)
                    CGEvent.post(Quartz.kCGHIDEventTap, down)
                    CGEvent.post(Quartz.kCGHIDEventTap, up)
                    time.sleep(delay)
            
            return UIResult(
                success=True,
                message=f"Typed {len(text)} characters",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message="Type text failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def press_key(self, key_code: int) -> UIResult:
        """
        Press a key by code.
        
        Args:
            key_code: Virtual key code.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateKeyboardEvent
            
            down = CGEventCreateKeyboardEvent(None, key_code, True)
            up = CGEventCreateKeyboardEvent(None, key_code, False)
            CGEvent.post(Quartz.kCGHIDEventTap, down)
            CGEvent.post(Quartz.kCGHIDEventTap, up)
            
            return UIResult(
                success=True,
                message=f"Pressed key {key_code}",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message=f"Key press failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def scroll(self, x: int, y: int, dx: int, dy: int) -> UIResult:
        """
        Scroll at position.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            dx: Horizontal scroll amount.
            dy: Vertical scroll amount.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEventCreateScrollWheelEvent
            
            flags = Quartz.CGEventFlags(0)
            
            wheel1 = CGEventCreateScrollWheelEvent(
                None, flags, 2,
                -dy * 10,
                -dx * 10
            )
            
            CGEvent.post(Quartz.kCGHIDEventTap, wheel1)
            
            return UIResult(
                success=True,
                message=f"Scrolled ({dx}, {dy}) at ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message="Scroll failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def drag(self, x1: int, y1: int, x2: int, y2: int,
            duration: float = 0.5, steps: int = 20) -> UIResult:
        """
        Drag from one position to another.
        
        Args:
            x1: Start X.
            y1: Start Y.
            x2: End X.
            y2: End Y.
            duration: Total duration.
            steps: Number of steps.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEvent, CGEventCreateMouseEvent
            
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
            
            return UIResult(
                success=True,
                message=f"Dragged from ({x1}, {y1}) to ({x2}, {y2})",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message="Drag failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def move_mouse(self, x: int, y: int) -> UIResult:
        """
        Move mouse to position.
        
        Args:
            x: Target X.
            y: Target Y.
            
        Returns:
            UIResult.
        """
        start = time.time()
        
        try:
            import Quartz
            from Quartz import CGEvent, CGEventCreateMouseEvent
            
            moved = CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft)
            CGEvent.post(Quartz.kCGHIDEventTap, moved)
            
            return UIResult(
                success=True,
                message=f"Moved mouse to ({x}, {y})",
                duration=time.time() - start
            )
        except Exception as e:
            return UIResult(
                success=False,
                message="Move failed",
                duration=time.time() - start,
                error=str(e)
            )
    
    def focus_element(self, element: UIElement) -> UIResult:
        """
        Focus a UI element by clicking on it.
        
        Args:
            element: UIElement to focus.
            
        Returns:
            UIResult.
        """
        cx = element.position[0] + element.size[0] // 2
        cy = element.position[1] + element.size[1] // 2
        return self.click(cx, cy)
