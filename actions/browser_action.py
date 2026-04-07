"""Browser action module for RabAI AutoClick.

Provides browser operations:
- BrowserOpenUrlAction: Open URL in browser
- BrowserBackAction: Go back
- BrowserForwardAction: Go forward
- BrowserRefreshAction: Refresh page
- BrowserScrollAction: Scroll page
"""

import subprocess
import os
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BrowserOpenUrlAction(BaseAction):
    """Open URL in browser."""
    action_type = "browser_open_url"
    display_name = "打开网址"
    description = "在浏览器中打开网址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute open URL.

        Args:
            context: Execution context.
            params: Dict with url, browser.

        Returns:
            ActionResult indicating success.
        """
        url = params.get('url', '')
        browser = params.get('browser', 'default')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            if browser == 'default':
                script = f'''osascript -e 'open location "{resolved_url}"' '''
            elif browser.lower() == 'safari':
                script = f'''osascript -e 'tell application "Safari" to open location "{resolved_url}"' '''
            elif browser.lower() == 'chrome':
                script = f'''osascript -e 'tell application "Google Chrome" to open location "{resolved_url}"' '''
            elif browser.lower() == 'firefox':
                script = f'''osascript -e 'tell application "Firefox" to open location "{resolved_url}"' '''
            else:
                script = f'''osascript -e 'open location "{resolved_url}"' '''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"已在{browser}中打开: {resolved_url}",
                data={'url': resolved_url, 'browser': browser}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"打开网址失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'browser': 'default'}


class BrowserBackAction(BaseAction):
    """Go back."""
    action_type = "browser_back"
    display_name = "后退"
    description = "浏览器后退"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute go back.

        Args:
            context: Execution context.
            params: Dict with browser.

        Returns:
            ActionResult indicating success.
        """
        browser = params.get('browser', 'safari')

        try:
            resolved_browser = context.resolve_value(browser) if browser else 'safari'

            script = f'''osascript -e 'tell application "{resolved_browser}" to activate' -e 'tell application "System Events" to keystroke "[" using command down' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="浏览器已后退"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"浏览器后退失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'browser': 'safari'}


class BrowserForwardAction(BaseAction):
    """Go forward."""
    action_type = "browser_forward"
    display_name = "前进"
    description = "浏览器前进"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute go forward.

        Args:
            context: Execution context.
            params: Dict with browser.

        Returns:
            ActionResult indicating success.
        """
        browser = params.get('browser', 'safari')

        try:
            resolved_browser = context.resolve_value(browser) if browser else 'safari'

            script = f'''osascript -e 'tell application "{resolved_browser}" to activate' -e 'tell application "System Events" to keystroke "]" using command down' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="浏览器已前进"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"浏览器前进失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'browser': 'safari'}


class BrowserRefreshAction(BaseAction):
    """Refresh page."""
    action_type = "browser_refresh"
    display_name = "刷新"
    description = "刷新浏览器页面"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute refresh.

        Args:
            context: Execution context.
            params: Dict with browser.

        Returns:
            ActionResult indicating success.
        """
        browser = params.get('browser', 'safari')

        try:
            resolved_browser = context.resolve_value(browser) if browser else 'safari'

            script = f'''osascript -e 'tell application "{resolved_browser}" to activate' -e 'tell application "System Events" to keystroke "r" using command down' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="浏览器已刷新"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"刷新失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'browser': 'safari'}


class BrowserScrollAction(BaseAction):
    """Scroll page."""
    action_type = "browser_scroll"
    display_name = "滚动页面"
    description = "滚动浏览器页面"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scroll.

        Args:
            context: Execution context.
            params: Dict with direction, amount, browser.

        Returns:
            ActionResult indicating success.
        """
        direction = params.get('direction', 'down')
        amount = params.get('amount', 3)
        browser = params.get('browser', 'safari')

        try:
            resolved_direction = context.resolve_value(direction)
            resolved_amount = context.resolve_value(amount)
            resolved_browser = context.resolve_value(browser) if browser else 'safari'

            if resolved_direction == 'up':
                key = 'up'
            else:
                key = 'down'

            script = f'''osascript -e 'tell application "{resolved_browser}" to activate' -e 'tell application "System Events" to key code {123 if key == "up" else 124} using command down' '''
            for _ in range(int(resolved_amount)):
                subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"页面已滚动{resolved_direction}: {resolved_amount} 次"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"滚动页面失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['direction']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'amount': 3, 'browser': 'safari'}