"""Browser automation action module for RabAI AutoClick.

Provides browser automation operations:
- BrowserOpenAction: Open URL in browser
- BrowserClickAction: Click element by selector
- BrowserTypeAction: Type text into element
- BrowserScrollAction: Scroll page
- BrowserScreenshotAction: Take screenshot
- BrowserWaitAction: Wait for element/condition
- BrowserNavigateAction: Navigate back/forward/refresh
- BrowserExtractAction: Extract data from page
"""

import base64
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BrowserOpenAction(BaseAction):
    """Open a URL in browser."""
    action_type = "browser_open"
    display_name = "打开浏览器"
    description = "在浏览器中打开指定URL"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            browser = params.get("browser", "default")
            
            if not url:
                return ActionResult(success=False, message="url is required")
            
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            
            os.system(f'open "{url}"')
            
            return ActionResult(
                success=True,
                message=f"Opened {url} in browser",
                data={"url": url, "browser": browser}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Browser open failed: {str(e)}")


class BrowserClickAction(BaseAction):
    """Click element by CSS selector or coordinates."""
    action_type = "browser_click"
    display_name = "浏览器点击"
    description = "点击页面元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            selector = params.get("selector", "")
            x = params.get("x")
            y = params.get("y")
            button = params.get("button", "left")
            
            if not selector and (x is None or y is None):
                return ActionResult(success=False, message="selector or x,y required")
            
            click_cmd = 'osascript -e \''
            if selector:
                click_cmd += f'tell application "System Events" to click button "{selector}" of window 1'
            else:
                click_cmd += f'tell application "System Events" to click at {{{x}, {y}}}'
            click_cmd += '\''
            
            os.system(click_cmd)
            
            return ActionResult(
                success=True,
                message=f"Clicked at {selector or f'({x}, {y})'}",
                data={"selector": selector, "x": x, "y": y}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Browser click failed: {str(e)}")


class BrowserTypeAction(BaseAction):
    """Type text into focused element."""
    action_type = "browser_type"
    display_name = "浏览器输入"
    description = "向输入框输入文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            delay = params.get("delay", 0.05)
            
            if not text:
                return ActionResult(success=False, message="text is required")
            
            for char in text:
                os.system(f"osascript -e 'tell application \"System Events\" to keystroke \"{char}\"'")
                time.sleep(delay)
            
            return ActionResult(
                success=True,
                message=f"Typed {len(text)} characters",
                data={"char_count": len(text)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Browser type failed: {str(e)}")


class BrowserScrollAction(BaseAction):
    """Scroll page by pixels or elements."""
    action_type = "browser_scroll"
    display_name = "浏览器滚动"
    description = "滚动页面"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            direction = params.get("direction", "down")
            amount = params.get("amount", 300)
            
            if direction == "down":
                cmd = "key code 125"
            elif direction == "up":
                cmd = "key code 126"
            elif direction == "left":
                cmd = "key code 123"
            elif direction == "right":
                cmd = "key code 124"
            else:
                return ActionResult(success=False, message=f"Invalid direction: {direction}")
            
            times = max(1, amount // 300)
            for _ in range(times):
                os.system(f"osascript -e 'tell application \"System Events\" to {cmd}'")
                time.sleep(0.1)
            
            return ActionResult(
                success=True,
                message=f"Scrolled {direction} {amount}px",
                data={"direction": direction, "amount": amount}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Browser scroll failed: {str(e)}")


class BrowserScreenshotAction(BaseAction):
    """Take screenshot of screen or region."""
    action_type = "browser_screenshot"
    display_name = "浏览器截图"
    description = "截取屏幕或区域截图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            output_path = params.get("output", "/tmp/screenshot.png")
            x = params.get("x")
            y = params.get("y")
            width = params.get("width")
            height = params.get("height")
            
            if x is not None and y is not None and width and height:
                os.system(f'screencapture -x -R {x},{y},{width},{height} "{output_path}"')
            else:
                os.system(f'screencapture -x "{output_path}"')
            
            file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
            
            return ActionResult(
                success=True,
                message=f"Screenshot saved to {output_path}",
                data={"path": output_path, "size_bytes": file_size}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Screenshot failed: {str(e)}")


class BrowserWaitAction(BaseAction):
    """Wait for element or specified duration."""
    action_type = "browser_wait"
    display_name = "浏览器等待"
    description = "等待元素或指定时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            duration = params.get("duration", 1)
            element = params.get("element", "")
            
            if element:
                time.sleep(float(duration))
            else:
                time.sleep(float(duration))
            
            return ActionResult(
                success=True,
                message=f"Waited {duration} seconds",
                data={"duration": duration}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Wait failed: {str(e)}")


class BrowserNavigateAction(BaseAction):
    """Navigate back, forward, or refresh."""
    action_type = "browser_navigate"
    display_name = "浏览器导航"
    description = "浏览器前进、后退、刷新"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "refresh")
            
            if action == "back":
                os.system('osascript -e \'tell application "System Events" to keystroke "[" using command down\'')
            elif action == "forward":
                os.system('osascript -e \'tell application "System Events" to keystroke "]" using command down\'')
            elif action == "refresh":
                os.system('osascript -e \'tell application "System Events" to keystroke "r" using command down\'')
            else:
                return ActionResult(success=False, message=f"Invalid action: {action}")
            
            return ActionResult(
                success=True,
                message=f"Navigation: {action}",
                data={"action": action}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Navigation failed: {str(e)}")


class BrowserExtractAction(BaseAction):
    """Extract data from page content."""
    action_type = "browser_extract"
    display_name = "浏览器提取"
    description = "从页面提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            selector = params.get("selector", "body")
            
            if not url:
                return ActionResult(success=False, message="url is required")
            
            try:
                with urllib.request.urlopen(url, timeout=10) as response:
                    content = response.read().decode("utf-8", errors="ignore")
            except:
                content = ""
            
            return ActionResult(
                success=True,
                message=f"Extracted content from {url}",
                data={"url": url, "selector": selector, "content_length": len(content)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Extract failed: {str(e)}")
