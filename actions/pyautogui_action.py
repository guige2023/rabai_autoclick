"""PyAutoGUI cross-platform GUI automation action module for RabAI AutoClick.

Provides keyboard and mouse automation for GUI control on macOS, Windows, and Linux.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PyAutoGUIMouseAction(BaseAction):
    """Perform mouse actions using PyAutoGUI.

    Supports click, double-click, right-click, drag, scroll, and move.
    """
    action_type = "pyautogui_mouse"
    display_name = "PyAutoGUI鼠标"
    description = "PyAutoGUI鼠标自动化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mouse action.

        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'click', 'double_click', 'right_click', 'move', 'drag', 'scroll'
                - x: X coordinate
                - y: Y coordinate
                - button: Mouse button ('left', 'right', 'middle')
                - clicks: Number of clicks
                - duration: Duration for drag/move
                - scroll_amount: Scroll amount (positive=up, negative=down)

        Returns:
            ActionResult with action result.
        """
        action = params.get('action', 'click')
        x = params.get('x', None)
        y = params.get('y', None)
        button = params.get('button', 'left')
        clicks = params.get('clicks', 1)
        duration = params.get('duration', 0.0)
        scroll_amount = params.get('scroll_amount', 0)

        try:
            import pyautogui
            pyautogui.FAILSAFE = True
            pyautogui.PAUSE = 0.1
        except ImportError:
            return ActionResult(success=False, message="pyautogui not installed. Run: pip install pyautogui")

        start = time.time()
        try:
            if action == 'move':
                if x is None or y is None:
                    return ActionResult(success=False, message="x and y are required for move")
                pyautogui.moveTo(x, y, duration=duration)
                return ActionResult(success=True, message=f"Moved to ({x}, {y})", duration=time.time()-start)

            if x is None or y is None:
                return ActionResult(success=False, message="x and y are required for this action")

            if action == 'click':
                pyautogui.click(x, y, clicks=clicks, button=button)
                return ActionResult(success=True, message=f"Clicked {clicks}x at ({x}, {y})", duration=time.time()-start)
            elif action == 'double_click':
                pyautogui.doubleClick(x, y, button=button)
                return ActionResult(success=True, message=f"Double-clicked at ({x}, {y})", duration=time.time()-start)
            elif action == 'right_click':
                pyautogui.rightClick(x, y)
                return ActionResult(success=True, message=f"Right-clicked at ({x}, {y})", duration=time.time()-start)
            elif action == 'drag':
                end_x = params.get('end_x', x)
                end_y = params.get('end_y', y)
                pyautogui.drag(end_x - x, end_y - y, duration=duration, button=button)
                return ActionResult(success=True, message=f"Dragged from ({x},{y}) to ({end_x},{end_y})", duration=time.time()-start)
            elif action == 'scroll':
                pyautogui.scroll(scroll_amount, x=x, y=y)
                return ActionResult(success=True, message=f"Scrolled {scroll_amount} at ({x}, {y})", duration=time.time()-start)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"PyAutoGUI error: {str(e)}")


class PyAutoGUIKeyboardAction(BaseAction):
    """Perform keyboard actions using PyAutoGUI.

    Supports typing, hotkeys, key press/release, and text writing.
    """
    action_type = "pyautogui_keyboard"
    display_name = "PyAutoGUI键盘"
    description = "PyAutoGUI键盘自动化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard action.

        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'type', 'hotkey', 'key_down', 'key_up', 'press'
                - text: Text to type (for type action)
                - keys: Key or key combination (e.g. 'ctrl+c', 'enter')
                - interval: Interval between keystrokes

        Returns:
            ActionResult with action result.
        """
        action = params.get('action', 'type')
        text = params.get('text', '')
        keys = params.get('keys', '')
        interval = params.get('interval', 0.0)

        try:
            import pyautogui
            pyautogui.FAILSAFE = True
            pyautogui.PAUSE = 0.1
        except ImportError:
            return ActionResult(success=False, message="pyautogui not installed")

        start = time.time()
        try:
            if action == 'type':
                if not text:
                    return ActionResult(success=False, message="text is required for type action")
                pyautogui.write(text, interval=interval)
                return ActionResult(success=True, message=f"Typed '{text}'", duration=time.time()-start)
            elif action == 'hotkey':
                if not keys:
                    return ActionResult(success=False, message="keys are required for hotkey action")
                key_list = [k.strip() for k in keys.split(',')]
                pyautogui.hotkey(*key_list)
                return ActionResult(success=True, message=f"Hotkey: {keys}", duration=time.time()-start)
            elif action == 'key_down':
                if not keys:
                    return ActionResult(success=False, message="key is required for key_down action")
                pyautogui.keyDown(keys)
                return ActionResult(success=True, message=f"Key down: {keys}", duration=time.time()-start)
            elif action == 'key_up':
                if not keys:
                    return ActionResult(success=False, message="key is required for key_up action")
                pyautogui.keyUp(keys)
                return ActionResult(success=True, message=f"Key up: {keys}", duration=time.time()-start)
            elif action == 'press':
                if not keys:
                    return ActionResult(success=False, message="keys are required for press action")
                pyautogui.press(keys)
                return ActionResult(success=True, message=f"Pressed: {keys}", duration=time.time()-start)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"PyAutoGUI keyboard error: {str(e)}")


class PyAutoGUIScreenshotAction(BaseAction):
    """Take screenshots using PyAutoGUI."""
    action_type = "pyautogui_screenshot"
    display_name = "PyAutoGUI截图"
    description = "PyAutoGUI屏幕截图"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Take screenshot.

        Args:
            context: Execution context.
            params: Dict with keys:
                - region: Optional (x, y, width, height) to capture specific region
                - output_path: Path to save screenshot

        Returns:
            ActionResult with screenshot path.
        """
        region = params.get('region', None)
        output_path = params.get('output_path', '/tmp/pyautogui_screenshot.png')

        try:
            import pyautogui
        except ImportError:
            return ActionResult(success=False, message="pyautogui not installed")

        start = time.time()
        try:
            if region:
                x, y, width, height = region
                img = pyautogui.screenshot(region=(x, y, width, height))
            else:
                img = pyautogui.screenshot()
            img.save(output_path)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Screenshot saved to {output_path}",
                data={'path': output_path, 'size': img.size}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Screenshot error: {str(e)}")
