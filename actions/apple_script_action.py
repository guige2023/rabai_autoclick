"""GUI automation action module for RabAI AutoClick.

Provides GUI operations:
- WindowListAction: List all open windows
- WindowFocusAction: Focus a specific window
- WindowMoveAction: Move and resize window
- WindowMinimizeAction: Minimize window
- WindowMaximizeAction: Maximize/restore window
- WindowCloseAction: Close window
- WindowScreenshotAction: Screenshot a window
- KeyTypeAction: Type text
- KeyPressAction: Press keyboard shortcut
- MouseClickAction: Click at coordinates
- MouseMoveAction: Move mouse cursor
- MouseDragAction: Drag mouse
"""

import subprocess
import time
from typing import Any, Dict, List, Optional, Tuple

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WindowListAction(BaseAction):
    """List all open windows."""
    action_type = "window_list"
    display_name = "列出窗口"
    description = "列出所有打开的窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = """
            tell application "System Events"
                set windowList to {}
                tell (first process whose frontmost is true)
                    set winCount to count of windows
                    repeat with i from 1 to winCount
                        tell window i
                            set winName to name
                            set winPos to position
                            set winSize to size
                            set winInfo to {winName, item 1 of winPos, item 2 of winPos, item 1 of winSize, item 2 of winSize}
                            set end of windowList to winInfo
                        end tell
                    end repeat
                end tell
            end tell
            return windowList
            """

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            windows = []
            for line in result.stdout.strip().split("\n"):
                if line.strip():
                    windows.append(line.strip())

            return ActionResult(
                success=True,
                message=f"Found {len(windows)} windows",
                data={"windows": windows, "count": len(windows)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"List windows error: {str(e)}")


class WindowFocusAction(BaseAction):
    """Focus a specific window by name."""
    action_type = "window_focus"
    display_name = "聚焦窗口"
    description = "聚焦指定窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            window_name = params.get("window_name", "")

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "{app_name}"
                activate
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            time.sleep(0.3)
            return ActionResult(success=True, message=f"Focused {app_name}")

        except Exception as e:
            return ActionResult(success=False, message=f"Focus error: {str(e)}")


class WindowMoveAction(BaseAction):
    """Move and resize a window."""
    action_type = "window_move"
    display_name = "移动窗口"
    description = "移动和调整窗口大小"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            window_index = params.get("window_index", 1)
            x = params.get("x", 0)
            y = params.get("y", 0)
            width = params.get("width", 800)
            height = params.get("height", 600)

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    set position of window {window_index} to {{{x}, {y}}}
                    set size of window {window_index} to {{{width}, {height}}}
                end tell
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            return ActionResult(
                success=True,
                message=f"Moved window to ({x}, {y}) with size {width}x{height}",
                data={"x": x, "y": y, "width": width, "height": height}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Move window error: {str(e)}")


class WindowMinimizeAction(BaseAction):
    """Minimize a window."""
    action_type = "window_minimize"
    display_name = "最小化窗口"
    description = "最小化窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            window_index = params.get("window_index", 1)

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    set miniaturized of window {window_index} to true
                end tell
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            return ActionResult(success=True, message="Window minimized")

        except Exception as e:
            return ActionResult(success=False, message=f"Minimize error: {str(e)}")


class WindowMaximizeAction(BaseAction):
    """Maximize or restore a window."""
    action_type = "window_maximize"
    display_name = "最大化窗口"
    description = "最大化或恢复窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            window_index = params.get("window_index", 1)

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "System Events"
                tell process "{app_name}"
                    set maximized of window {window_index} to true
                end tell
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            return ActionResult(success=True, message="Window maximized")

        except Exception as e:
            return ActionResult(success=False, message=f"Maximize error: {str(e)}")


class WindowCloseAction(BaseAction):
    """Close a window."""
    action_type = "window_close"
    display_name = "关闭窗口"
    description = "关闭窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            window_index = params.get("window_index", 1)

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "{app_name}"
                close window {window_index}
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            return ActionResult(success=True, message="Window closed")

        except Exception as e:
            return ActionResult(success=False, message=f"Close window error: {str(e)}")


class WindowScreenshotAction(BaseAction):
    """Screenshot a specific window."""
    action_type = "window_screenshot"
    display_name = "窗口截图"
    description = "截取窗口截图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            app_name = params.get("app_name", "")
            output_path = params.get("output_path", "/tmp/window_screenshot.png")

            if not app_name:
                return ActionResult(success=False, message="app_name is required")

            script = f'''
            tell application "System Events"
                tell (first process whose name is "{app_name}")
                    set frontmost to true
                end tell
            end tell
            delay 0.5
            '''

            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=10)
            time.sleep(0.5)

            result = subprocess.run(
                ["screencapture", "-x", "-w", output_path],
                capture_output=True, timeout=15
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"screencapture error: {result.stderr.decode()}")

            return ActionResult(
                success=True,
                message="Screenshot captured",
                data={"filepath": output_path}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Screenshot error: {str(e)}")


class KeyTypeAction(BaseAction):
    """Type text."""
    action_type = "key_type"
    display_name = "输入文本"
    description = "输入文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            delay = params.get("delay", 0.05)

            if not text:
                return ActionResult(success=False, message="text is required")

            for char in text:
                escaped_char = char.replace('"', '\\"').replace("'", "\\'")
                script = f'tell application "System Events" to keystroke "{escaped_char}"'
                subprocess.run(["osascript", "-e", script], capture_output=True, timeout=2)
                time.sleep(delay)

            return ActionResult(success=True, message=f"Typed {len(text)} characters")

        except Exception as e:
            return ActionResult(success=False, message=f"Type error: {str(e)}")


class KeyPressAction(BaseAction):
    """Press keyboard shortcut."""
    action_type = "key_press"
    display_name = "按键"
    description = "按键盘快捷键"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            keys = params.get("keys", [])
            modifiers = params.get("modifiers", [])

            if not keys:
                return ActionResult(success=False, message="keys required")

            modifier_map = {
                "command": "command down", "cmd": "command down",
                "control": "control down", "ctrl": "control down",
                "option": "option down", "alt": "option down",
                "shift": "shift down"
            }

            key_desc = " + ".join(keys)
            script_parts = ['tell application "System Events"']

            if modifiers:
                mod_str = ", ".join(modifier_map.get(m.lower(), m) for m in modifiers)
                script_parts.append(f'tell (every process whose frontmost is true)')
                script_parts.append(f'keystroke "{keys[0]}" using {{{mod_str}}}')
                script_parts.append("end tell")
            else:
                script_parts.append(f'keystroke "{keys[0]}"')

            script_parts.append("end tell")
            script = " ".join(script_parts)

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            return ActionResult(success=True, message=f"Pressed: {key_desc}")

        except Exception as e:
            return ActionResult(success=False, message=f"Key press error: {str(e)}")


class MouseClickAction(BaseAction):
    """Click at coordinates."""
    action_type = "mouse_click"
    display_name = "鼠标点击"
    description = "在指定坐标点击鼠标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            x = params.get("x", 0)
            y = params.get("y", 0)
            button = params.get("button", "left")
            click_count = params.get("click_count", 1)

            import Quartz

            button_map = {"left": Quartz.kCGEventLeftMouseDown, "right": Quartz.kCGEventRightMouseDown}
            button_up_map = {"left": Quartz.kCGEventLeftMouseUp, "right": Quartz.kCGEventRightMouseUp}

            down_type = button_map.get(button, Quartz.kCGEventLeftMouseDown)
            up_type = button_up_map.get(button, Quartz.kCGEventLeftMouseUp)

            for _ in range(click_count):
                down = Quartz.CGEventCreateMouseEvent(None, down_type, (x, y), Quartz.kCGMouseButtonLeft)
                up = Quartz.CGEventCreateMouseEvent(None, up_type, (x, y), Quartz.kCGMouseButtonLeft)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)
                time.sleep(0.1)

            return ActionResult(success=True, message=f"Clicked at ({x}, {y})")

        except ImportError:
            return ActionResult(success=False, message="Quartz not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Click error: {str(e)}")


class MouseMoveAction(BaseAction):
    """Move mouse cursor."""
    action_type = "mouse_move"
    display_name = "移动鼠标"
    description = "移动鼠标到指定坐标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            x = params.get("x", 0)
            y = params.get("y", 0)
            steps = params.get("steps", 1)

            import Quartz

            if steps <= 1:
                event = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (x, y), Quartz.kCGMouseButtonLeft)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
            else:
                current_pos = Quartz.CGEventGetLocation(Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (0, 0), Quartz.kCGMouseButtonLeft))
                start_x, start_y = int(current_pos.x), int(current_pos.y)
                for i in range(steps):
                    progress = (i + 1) / steps
                    cx = int(start_x + (x - start_x) * progress)
                    cy = int(start_y + (y - start_y) * progress)
                    event = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventMouseMoved, (cx, cy), Quartz.kCGMouseButtonLeft)
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
                    time.sleep(0.01)

            return ActionResult(success=True, message=f"Moved to ({x}, {y})")

        except ImportError:
            return ActionResult(success=False, message="Quartz not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Move error: {str(e)}")


class MouseDragAction(BaseAction):
    """Drag mouse from one point to another."""
    action_type = "mouse_drag"
    display_name = "鼠标拖拽"
    description = "从起点拖拽到终点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            start_x = params.get("start_x", 0)
            start_y = params.get("start_y", 0)
            end_x = params.get("end_x", 0)
            end_y = params.get("end_y", 0)
            steps = params.get("steps", 20)

            import Quartz

            down = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDown, (start_x, start_y), Quartz.kCGMouseButtonLeft)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)

            for i in range(steps):
                progress = (i + 1) / steps
                cx = int(start_x + (end_x - start_x) * progress)
                cy = int(start_y + (end_y - start_y) * progress)
                drag = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDragged, (cx, cy), Quartz.kCGMouseButtonLeft)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, drag)
                time.sleep(0.01)

            up = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseUp, (end_x, end_y), Quartz.kCGMouseButtonLeft)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)

            return ActionResult(success=True, message=f"Dragged from ({start_x}, {start_y}) to ({end_x}, {end_y})")

        except ImportError:
            return ActionResult(success=False, message="Quartz not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Drag error: {str(e)}")
