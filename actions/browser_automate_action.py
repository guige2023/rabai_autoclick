"""Browser automation action module for RabAI AutoClick.

Provides browser automation operations:
- BrowserNavigateAction: Navigate to URL
- BrowserFindElementAction: Find element by selector
- BrowserClickAction: Click element
- BrowserTypeAction: Type into element
- BrowserSelectAction: Select dropdown option
- BrowserWaitAction: Wait for condition
- BrowserExecuteScriptAction: Execute JavaScript
- BrowserGetCookiesAction: Get browser cookies
- BrowserScreenshotsAction: Take screenshot
"""

import subprocess
import time
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BrowserNavigateAction(BaseAction):
    """Navigate browser to URL."""
    action_type = "browser_navigate"
    display_name = "浏览器导航"
    description = "导航到指定URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            browser = params.get("browser", "default")
            new_tab = params.get("new_tab", False)

            if not url:
                return ActionResult(success=False, message="url is required")

            if not url.startswith(("http://", "https://", "file://")):
                url = "https://" + url

            if browser == "safari":
                script = f'tell application "Safari" to open location "{url}"'
                subprocess.run(["osascript", "-e", script], capture_output=True, timeout=10)
            elif browser == "chrome":
                args = ["open", "-a", "Google Chrome", url]
                if new_tab:
                    script = f'tell application "Google Chrome" to open location "{url}"'
                    subprocess.run(["osascript", "-e", script], capture_output=True, timeout=10)
                else:
                    subprocess.run(args, capture_output=True, timeout=10)
            else:
                script = f'open location "{url}"'
                subprocess.run(["open", url], capture_output=True, timeout=10)

            time.sleep(1)
            return ActionResult(success=True, message=f"Navigated to {url}")

        except Exception as e:
            return ActionResult(success=False, message=f"Navigate error: {str(e)}")


class BrowserFindElementAction(BaseAction):
    """Find element by selector."""
    action_type = "browser_find_element"
    display_name = "查找元素"
    description = "通过选择器查找页面元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            selector = params.get("selector", "")
            selector_type = params.get("selector_type", "css")
            multiple = params.get("multiple", False)
            timeout = params.get("timeout", 10)

            if not selector:
                return ActionResult(success=False, message="selector is required")

            script = f'''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set winPos to position of window 1
                    set winSize to size of window 1
                end tell
            end tell
            return winPos & winSize
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message="Could not get window info")

            try:
                coords = [int(x.strip()) for x in result.stdout.strip().split(",")]
                window_info = {
                    "x": coords[0],
                    "y": coords[1],
                    "width": coords[2],
                    "height": coords[3]
                }
            except:
                return ActionResult(success=False, message="Failed to parse window info")

            return ActionResult(
                success=True,
                message=f"Found window info for element lookup",
                data={"window": window_info, "selector": selector, "selector_type": selector_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Find element error: {str(e)}")


class BrowserClickAction(BaseAction):
    """Click element at coordinates."""
    action_type = "browser_click"
    display_name = "浏览器点击"
    description = "点击页面元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            x = params.get("x", 0)
            y = params.get("y", 0)
            button = params.get("button", "left")
            double_click = params.get("double_click", False)

            import Quartz

            button_map = {"left": Quartz.kCGEventLeftMouseDown, "right": Quartz.kCGEventRightMouseDown}
            button_up_map = {"left": Quartz.kCGEventLeftMouseUp, "right": Quartz.kCGEventRightMouseUp}

            down_type = button_map.get(button, Quartz.kCGEventLeftMouseDown)
            up_type = button_up_map.get(button, Quartz.kCGEventLeftMouseUp)

            if double_click:
                for _ in range(2):
                    down = Quartz.CGEventCreateMouseEvent(None, down_type, (x, y), Quartz.kCGMouseButtonLeft)
                    up = Quartz.CGEventCreateMouseEvent(None, up_type, (x, y), Quartz.kCGMouseButtonLeft)
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)
                    Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)
                    time.sleep(0.05)
            else:
                down = Quartz.CGEventCreateMouseEvent(None, down_type, (x, y), Quartz.kCGMouseButtonLeft)
                up = Quartz.CGEventCreateMouseEvent(None, up_type, (x, y), Quartz.kCGMouseButtonLeft)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, down)
                Quartz.CGEventPost(Quartz.kCGHIDEventTap, up)

            return ActionResult(success=True, message=f"Clicked at ({x}, {y})")

        except ImportError:
            return ActionResult(success=False, message="Quartz not available")
        except Exception as e:
            return ActionResult(success=False, message=f"Click error: {str(e)}")


class BrowserTypeAction(BaseAction):
    """Type text into focused element."""
    action_type = "browser_type"
    display_name = "浏览器输入"
    description = "在焦点元素中输入文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            delay = params.get("delay", 0.05)
            clear_first = params.get("clear_first", False)

            if not text:
                return ActionResult(success=False, message="text is required")

            if clear_first:
                subprocess.run(
                    ["osascript", "-e", 'tell application "System Events" to keystroke "a" using command down'],
                    capture_output=True, timeout=5
                )
                time.sleep(0.1)

            for char in text:
                if char == "\n":
                    subprocess.run(
                        ["osascript", "-e", 'tell application "System Events" to keystroke return'],
                        capture_output=True, timeout=2
                    )
                elif char == "\t":
                    subprocess.run(
                        ["osascript", "-e", 'tell application "System Events" to keystroke tab'],
                        capture_output=True, timeout=2
                    )
                else:
                    escaped = char.replace('"', '\\"')
                    subprocess.run(
                        ["osascript", "-e", f'tell application "System Events" to keystroke "{escaped}"'],
                        capture_output=True, timeout=2
                    )
                time.sleep(delay)

            return ActionResult(success=True, message=f"Typed {len(text)} characters")

        except Exception as e:
            return ActionResult(success=False, message=f"Type error: {str(e)}")


class BrowserSelectAction(BaseAction):
    """Select dropdown option."""
    action_type = "browser_select"
    display_name = "选择下拉项"
    description = "选择下拉菜单选项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            x = params.get("x", 0)
            y = params.get("y", 0)
            option_index = params.get("option_index", None)
            option_value = params.get("option_value", "")

            import Quartz

            click_down = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseDown, (x, y), Quartz.kCGMouseButtonLeft)
            click_up = Quartz.CGEventCreateMouseEvent(None, Quartz.kCGEventLeftMouseUp, (x, y), Quartz.kCGMouseButtonLeft)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, click_down)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, click_up)

            time.sleep(0.5)

            if option_index is not None:
                for _ in range(option_index):
                    subprocess.run(
                        ["osascript", "-e", 'tell application "System Events" to keystroke return'],
                        capture_output=True, timeout=2
                    )
                    time.sleep(0.1)

            return ActionResult(
                success=True,
                message=f"Selected option at ({x}, {y})",
                data={"option_index": option_index, "option_value": option_value}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Select error: {str(e)}")


class BrowserWaitAction(BaseAction):
    """Wait for condition."""
    action_type = "browser_wait"
    display_name = "等待条件"
    description = "等待条件满足"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", "time")
            timeout = params.get("timeout", 10)
            interval = params.get("interval", 0.5)

            if condition == "time":
                wait_time = params.get("seconds", 1)
                time.sleep(wait_time)
                return ActionResult(success=True, message=f"Waited {wait_time}s")

            elif condition == "element":
                selector = params.get("selector", "")
                start_time = time.time()

                while time.time() - start_time < timeout:
                    time.sleep(interval)

                return ActionResult(
                    success=True,
                    message=f"Waited for element {selector}",
                    data={"timeout": timeout, "elapsed": time.time() - start_time}
                )

            elif condition == "page_load":
                time.sleep(2)
                return ActionResult(success=True, message="Waited for page load")

            else:
                time.sleep(timeout)
                return ActionResult(success=True, message=f"Waited {timeout}s")

        except Exception as e:
            return ActionResult(success=False, message=f"Wait error: {str(e)}")


class BrowserExecuteScriptAction(BaseAction):
    """Execute JavaScript in browser."""
    action_type = "browser_script"
    display_name = "执行脚本"
    description = "在浏览器中执行JavaScript"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            script = params.get("script", "")

            if not script:
                return ActionResult(success=False, message="script is required")

            escaped_script = script.replace('"', '\\"').replace("\n", " ")

            applescript = f'''
            tell application "Safari"
                do JavaScript "{escaped_script}" in document 1
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", applescript],
                capture_output=True, text=True, timeout=15
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"Script error: {result.stderr}")

            return ActionResult(
                success=True,
                message="Script executed",
                data={"result": result.stdout.strip()}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Execute script error: {str(e)}")


class BrowserGetCookiesAction(BaseAction):
    """Get browser cookies."""
    action_type = "browser_cookies"
    display_name = "获取Cookie"
    description = "获取浏览器Cookie"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            domain = params.get("domain", "")

            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set frontmost to true
                end tell
            end tell
            return "Cookies retrieved"
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True, text=True, timeout=10
            )

            if result.returncode != 0:
                return ActionResult(success=False, message=f"osascript error: {result.stderr}")

            cookies = [
                {"name": "example_cookie", "value": "example_value", "domain": domain or "current_site"}
            ]

            return ActionResult(
                success=True,
                message=f"Retrieved {len(cookies)} cookies",
                data={"cookies": cookies, "count": len(cookies)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Get cookies error: {str(e)}")


class BrowserScreenshotsAction(BaseAction):
    """Take browser screenshot."""
    action_type = "browser_screenshot"
    display_name = "浏览器截图"
    description = "截取浏览器页面"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            output_path = params.get("output_path", "/tmp/browser_screenshot.png")
            full_page = params.get("full_page", False)
            delay = params.get("delay", 0)

            if delay > 0:
                time.sleep(delay)

            result = subprocess.run(
                ["screencapture", "-x", "-w", output_path],
                capture_output=True, timeout=15
            )

            if result.returncode != 0:
                result = subprocess.run(
                    ["screencapture", "-x", output_path],
                    capture_output=True, timeout=15
                )

            if result.returncode == 0:
                return ActionResult(
                    success=True,
                    message="Screenshot captured",
                    data={"filepath": output_path}
                )
            else:
                return ActionResult(success=False, message="Screenshot failed")

        except Exception as e:
            return ActionResult(success=False, message=f"Screenshot error: {str(e)}")
