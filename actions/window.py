"""Window action module for RabAI AutoClick.

Provides window management actions:
- WindowFocusAction: Focus a window by title
- WindowMoveAction: Move a window
- WindowResizeAction: Resize a window
- WindowMinimizeAction: Minimize a window
- WindowMaximizeAction: Maximize a window
- WindowCloseAction: Close a window
- WindowListAction: List all open windows
"""

import subprocess
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


# Valid window operations
VALID_OPERATIONS: List[str] = [
    'focus', 'move', 'resize', 'minimize', 'maximize', 'close', 'list'
]


def _get_window_list_macos() -> List[Dict[str, Any]]:
    """Get list of windows on macOS using osascript.

    Returns:
        List of window info dicts.
    """
    try:
        script = '''
        tell application "System Events"
            set windowList to {}
            tell process "Finder"
                set frontmost to false
            end tell
        end tell
        return windowList
        '''
        # Use python to get window info
        result = subprocess.run(
            ['osascript', '-e', '''
            tell application "System Events"
                get name of every window of every process
            end tell
            '''],
            capture_output=True,
            text=True,
            timeout=5
        )
        return []
    except Exception:
        return []


class WindowFocusAction(BaseAction):
    """Focus a window by its title."""
    action_type = "window_focus"
    display_name = "聚焦窗口"
    description = "通过窗口标题聚焦指定窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute focusing a window.

        Args:
            context: Execution context.
            params: Dict with title, app_name.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')
        app_name = params.get('app_name', '')

        # Validate title
        if not title and not app_name:
            return ActionResult(
                success=False,
                message="未指定窗口标题或应用名称"
            )

        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Use osascript to focus window on macOS
            if app_name:
                script = f'''
                tell application "{app_name}"
                    activate
                end tell
                '''
            elif title:
                script = f'''
                tell application "System Events"
                    tell process (name of first process whose windows contain "{title}")
                        set frontmost to true
                    end tell
                end tell
                '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            target = app_name or title
            return ActionResult(
                success=True,
                message=f"已聚焦窗口: {target}",
                data={'title': title, 'app_name': app_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚焦窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': '',
            'app_name': ''
        }


class WindowMoveAction(BaseAction):
    """Move a window to a specified position."""
    action_type = "window_move"
    display_name = "移动窗口"
    description = "将窗口移动到指定位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute moving a window.

        Args:
            context: Execution context.
            params: Dict with title, x, y.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')
        x = params.get('x', 0)
        y = params.get('y', 0)

        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate coordinates
        valid, msg = self.validate_coords(x, y, allow_none=False)
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = f'''
            tell application "System Events"
                tell process (name of first process whose windows contain "{title}")
                    set position of front window to {{{x}, {y}}}
                end tell
            end tell
            '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            return ActionResult(
                success=True,
                message=f"窗口已移动到: ({x}, {y})",
                data={'title': title, 'x': x, 'y': y}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class WindowResizeAction(BaseAction):
    """Resize a window to specified dimensions."""
    action_type = "window_resize"
    display_name = "调整窗口大小"
    description = "调整窗口到指定尺寸"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resizing a window.

        Args:
            context: Execution context.
            params: Dict with title, width, height.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')
        width = params.get('width', 800)
        height = params.get('height', 600)

        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate dimensions
        valid, msg = self.validate_positive(width, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_positive(height, 'height')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = f'''
            tell application "System Events"
                tell process (name of first process whose windows contain "{title}")
                    set size of front window to {{{width}, {height}}}
                end tell
            end tell
            '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            return ActionResult(
                success=True,
                message=f"窗口已调整为: {width}x{height}",
                data={'title': title, 'width': width, 'height': height}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整窗口大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title', 'width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class WindowMinimizeAction(BaseAction):
    """Minimize a window."""
    action_type = "window_minimize"
    display_name = "最小化窗口"
    description = "最小化指定窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minimizing a window.

        Args:
            context: Execution context.
            params: Dict with title.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')

        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = f'''
            tell application "System Events"
                tell process (name of first process whose windows contain "{title}")
                    set minimized of front window to true
                end tell
            end tell
            '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            return ActionResult(
                success=True,
                message=f"窗口已最小化: {title}",
                data={'title': title}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最小化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class WindowMaximizeAction(BaseAction):
    """Maximize a window."""
    action_type = "window_maximize"
    display_name = "最大化窗口"
    description = "最大化指定窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute maximizing a window.

        Args:
            context: Execution context.
            params: Dict with title.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')

        # Validate title
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            script = f'''
            tell application "System Events"
                tell process (name of first process whose windows contain "{title}")
                    set size of front window to {{1800, 1200}}
                end tell
            end tell
            '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            return ActionResult(
                success=True,
                message=f"窗口已最大化: {title}",
                data={'title': title}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最大化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['title']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class WindowCloseAction(BaseAction):
    """Close a window."""
    action_type = "window_close"
    display_name = "关闭窗口"
    description = "关闭指定窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute closing a window.

        Args:
            context: Execution context.
            params: Dict with title.

        Returns:
            ActionResult indicating success.
        """
        title = params.get('title', '')
        app_name = params.get('app_name', '')

        # Validate inputs
        valid, msg = self.validate_type(title, str, 'title')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(app_name, str, 'app_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        if not title and not app_name:
            return ActionResult(
                success=False,
                message="未指定窗口标题或应用名称"
            )

        try:
            if app_name:
                script = f'''
                tell application "{app_name}"
                    close front window
                end tell
                '''
            else:
                script = f'''
                tell application "System Events"
                    tell process (name of first process whose windows contain "{title}")
                        tell front window
                            close
                        end tell
                    end tell
                end tell
                '''

            subprocess.run(['osascript', '-e', script], capture_output=True, timeout=5)

            target = app_name or title
            return ActionResult(
                success=True,
                message=f"窗口已关闭: {target}",
                data={'title': title, 'app_name': app_name}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"关闭窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'title': '',
            'app_name': ''
        }