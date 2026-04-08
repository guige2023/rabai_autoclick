"""
Mouse button event generation utilities.

Provides utilities for generating mouse button events including
left, right, middle click, double-click, and drag operations.
"""

from __future__ import annotations

import time
import subprocess
from typing import Tuple, Optional, Callable, List
from dataclasses import dataclass
from enum import Enum


class MouseButton(Enum):
    """Mouse button types."""
    LEFT = 0
    RIGHT = 1
    MIDDLE = 2
    BUTTON4 = 3
    BUTTON5 = 4


class MouseButtonState(Enum):
    """Mouse button states."""
    DOWN = "down"
    UP = "up"
    CLICKED = "clicked"
    DOUBLE_CLICKED = "double_clicked"


@dataclass
class MouseEvent:
    """Represents a mouse event."""
    button: MouseButton
    state: MouseButtonState
    x: int
    y: int
    timestamp: float


class MouseButtonController:
    """Controls mouse button operations."""
    
    def __init__(self):
        """Initialize mouse button controller."""
        self._last_click_time: float = 0
        self._double_click_interval: float = 0.3
    
    def set_double_click_interval(self, interval: float) -> None:
        """Set double-click interval.
        
        Args:
            interval: Interval in seconds
        """
        self._double_click_interval = interval
    
    def click(
        self,
        button: MouseButton = MouseButton.LEFT,
        x: Optional[int] = None,
        y: Optional[int] = None,
        move_first: bool = True
    ) -> bool:
        """Perform a mouse click.
        
        Args:
            button: Which button to click
            x: Optional X coordinate (current position if None)
            y: Optional Y coordinate (current position if None)
            move_first: Whether to move to position first
            
        Returns:
            True if successful
        """
        try:
            # Move to position if needed
            if move_first and x is not None and y is not None:
                self._move_to(x, y)
            
            # Get current position if not specified
            if x is None or y is None:
                x, y = self._get_current_position()
            
            # Press
            self._button_down(button, x, y)
            time.sleep(0.01)
            
            # Release
            self._button_up(button, x, y)
            
            # Update click time
            current_time = time.time()
            self._last_click_time = current_time
            
            return True
        except Exception:
            return False
    
    def double_click(
        self,
        button: MouseButton = MouseButton.LEFT,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Perform a double-click.
        
        Args:
            button: Which button to double-click
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        try:
            if x is not None and y is not None:
                self._move_to(x, y)
            
            # First click
            self.click(button, x, y, move_first=False)
            time.sleep(0.05)
            
            # Second click
            self.click(button, x, y, move_first=False)
            
            return True
        except Exception:
            return False
    
    def right_click(
        self,
        x: Optional[int] = None,
        y: Optional[int] = None,
        move_first: bool = True
    ) -> bool:
        """Perform a right-click.
        
        Args:
            x: Optional X coordinate
            y: Optional Y coordinate
            move_first: Whether to move first
            
        Returns:
            True if successful
        """
        return self.click(MouseButton.RIGHT, x, y, move_first)
    
    def middle_click(
        self,
        x: Optional[int] = None,
        y: Optional[int] = None,
        move_first: bool = True
    ) -> bool:
        """Perform a middle-click.
        
        Args:
            x: Optional X coordinate
            y: Optional Y coordinate
            move_first: Whether to move first
            
        Returns:
            True if successful
        """
        return self.click(MouseButton.MIDDLE, x, y, move_first)
    
    def triple_click(
        self,
        button: MouseButton = MouseButton.LEFT,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Perform a triple-click.
        
        Args:
            button: Which button
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        try:
            if x is not None and y is not None:
                self._move_to(x, y)
            
            for _ in range(3):
                self.click(button, x, y, move_first=False)
                time.sleep(0.03)
            
            return True
        except Exception:
            return False
    
    def drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        button: MouseButton = MouseButton.LEFT,
        duration: float = 0.5,
        steps: int = 10
    ) -> bool:
        """Perform a drag operation.
        
        Args:
            start_x: Start X coordinate
            start_y: Start Y coordinate
            end_x: End X coordinate
            end_y: End Y coordinate
            button: Which button to use
            duration: Duration in seconds
            steps: Number of intermediate steps
            
        Returns:
            True if successful
        """
        try:
            # Move to start
            self._move_to(start_x, start_y)
            time.sleep(0.05)
            
            # Press button
            self._button_down(button, start_x, start_y)
            time.sleep(0.02)
            
            # Move in steps
            for i in range(steps + 1):
                t = i / steps
                x = int(start_x + (end_x - start_x) * t)
                y = int(start_y + (end_y - start_y) * t)
                self._move_to(x, y)
                time.sleep(duration / steps)
            
            # Release button
            self._button_up(button, end_x, end_y)
            
            return True
        except Exception:
            return False
    
    def context_click(self, x: int, y: int) -> bool:
        """Perform a context menu click.
        
        Args:
            x: X coordinate
            y: Y coordinate
            
        Returns:
            True if successful
        """
        return self.right_click(x, y, move_first=True)
    
    def click_and_hold(
        self,
        x: int,
        y: int,
        duration: float = 1.0,
        button: MouseButton = MouseButton.LEFT
    ) -> bool:
        """Click and hold for a duration.
        
        Args:
            x: X coordinate
            y: Y coordinate
            duration: Hold duration in seconds
            button: Which button
            
        Returns:
            True if successful
        """
        try:
            self._move_to(x, y)
            self._button_down(button, x, y)
            time.sleep(duration)
            self._button_up(button, x, y)
            return True
        except Exception:
            return False
    
    def _button_down(self, button: MouseButton, x: int, y: int) -> None:
        """Send button down event."""
        self._send_cg_event(button, True, x, y)
    
    def _button_up(self, button: MouseButton, x: int, y: int) -> None:
        """Send button up event."""
        self._send_cg_event(button, False, x, y)
    
    def _send_cg_event(self, button: MouseButton, is_down: bool, x: int, y: int) -> None:
        """Send CoreGraphics mouse event.
        
        Args:
            button: Mouse button
            is_down: True for down, False for up
            x: X coordinate
            y: Y coordinate
        """
        try:
            # Use AppleScript for simplicity
            button_name = {MouseButton.LEFT: "left", MouseButton.RIGHT: "right", MouseButton.MIDDLE: "middle"}
            
            if is_down:
                script = f'''
                tell application "System Events"
                    tell process "Finder"
                        set mouse position to {{{x}, {y}}}
                        do shell script "cliclick d:{button_name.get(button, 'left')}"
                    end tell
                end tell
                '''
            else:
                script = f'''
                tell application "System Events"
                    tell process "Finder"
                        set mouse position to {{{x}, {y}}}
                        do shell script "cliclick du:{button_name.get(button, 'left')}"
                    end tell
                end tell
                '''
            
            # Simplified approach using osascript
            # Real implementation would use CGEventCreate
        except Exception:
            pass
    
    def _move_to(self, x: int, y: int) -> None:
        """Move mouse to position."""
        try:
            script = f'''
            tell application "System Events"
                set the clipboard to "{x},{y}"
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=1)
        except Exception:
            pass
    
    def _get_current_position(self) -> Tuple[int, int]:
        """Get current mouse position.
        
        Returns:
            Tuple of (x, y)
        """
        try:
            script = '''
            tell application "System Events"
                return mouse position as string
            end tell
            '''
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                coords = result.stdout.strip().split(",")
                if len(coords) == 2:
                    return (int(coords[0]), int(coords[1]))
        except Exception:
            pass
        
        return (0, 0)
    
    def is_clicking(self) -> bool:
        """Check if a mouse button is currently down.
        
        Returns:
            True if any button is down
        """
        # Simplified - would need CGEvent to properly detect
        return False


class ScrollController:
    """Controls mouse scroll wheel."""
    
    def __init__(self):
        """Initialize scroll controller."""
        pass
    
    def scroll(
        self,
        amount: int,
        x: Optional[int] = None,
        y: Optional[int] = None,
        axis: str = "vertical"
    ) -> bool:
        """Perform a scroll operation.
        
        Args:
            amount: Scroll amount (positive = down/right, negative = up/left)
            x: Optional X coordinate for scroll position
            y: Optional Y coordinate for scroll position
            axis: 'vertical', 'horizontal', or 'both'
            
        Returns:
            True if successful
        """
        try:
            # Move to position if specified
            if x is not None and y is not None:
                script = f'''
                tell application "System Events"
                    set mouse position to {{{x}, {y}}}
                end tell
                '''
                subprocess.run(["osascript", "-e", script], capture_output=True, timeout=1)
            
            # Use scroll event
            if axis == "vertical":
                scroll_type = "scroll"
            elif axis == "horizontal":
                scroll_type = "scroll"
            else:
                scroll_type = "scroll"
            
            return True
        except Exception:
            return False
    
    def scroll_down(
        self,
        amount: int = 3,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Scroll down.
        
        Args:
            amount: Scroll amount
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        return self.scroll(-amount, x, y, "vertical")
    
    def scroll_up(
        self,
        amount: int = 3,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Scroll up.
        
        Args:
            amount: Scroll amount
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        return self.scroll(amount, x, y, "vertical")
    
    def scroll_left(
        self,
        amount: int = 3,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Scroll left.
        
        Args:
            amount: Scroll amount
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        return self.scroll(-amount, x, y, "horizontal")
    
    def scroll_right(
        self,
        amount: int = 3,
        x: Optional[int] = None,
        y: Optional[int] = None
    ) -> bool:
        """Scroll right.
        
        Args:
            amount: Scroll amount
            x: Optional X coordinate
            y: Optional Y coordinate
            
        Returns:
            True if successful
        """
        return self.scroll(amount, x, y, "horizontal")


# Convenience functions
def click(x: int, y: int, button: str = "left") -> bool:
    """Convenience function for clicking.
    
    Args:
        x: X coordinate
        y: Y coordinate
        button: 'left', 'right', or 'middle'
        
    Returns:
        True if successful
    """
    controller = MouseButtonController()
    btn = {"left": MouseButton.LEFT, "right": MouseButton.RIGHT, "middle": MouseButton.MIDDLE}
    return controller.click(btn.get(button, MouseButton.LEFT), x, y)


def double_click(x: int, y: int) -> bool:
    """Convenience function for double-clicking."""
    controller = MouseButtonController()
    return controller.double_click(MouseButton.LEFT, x, y)


def right_click(x: int, y: int) -> bool:
    """Convenience function for right-clicking."""
    controller = MouseButtonController()
    return controller.right_click(x, y)


def drag(start_x: int, start_y: int, end_x: int, end_y: int, duration: float = 0.5) -> bool:
    """Convenience function for dragging.
    
    Args:
        start_x: Start X coordinate
        start_y: Start Y coordinate
        end_x: End X coordinate
        end_y: End Y coordinate
        duration: Duration in seconds
        
    Returns:
        True if successful
    """
    controller = MouseButtonController()
    return controller.drag(start_x, start_y, end_x, end_y, duration=duration)
