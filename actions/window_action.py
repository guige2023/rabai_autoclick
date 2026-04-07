"""Window action module for RabAI AutoClick.

Provides window operations:
- WindowGetTitleAction: Get window title
- WindowSetTitleAction: Set window title
- WindowMinimizeAction: Minimize window
- WindowMaximizeAction: Maximize window
- WindowRestoreAction: Restore window
- WindowCloseAction: Close window
- WindowMoveAction: Move window
- WindowResizeAction: Resize window
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WindowGetTitleAction(BaseAction):
    """Get window title."""
    action_type = "window_get_title"
    display_name = "获取窗口标题"
    description = "获取前台窗口标题"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get title.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with window title.
        """
        output_var = params.get('output_var', 'window_title')

        try:
            # Use AppleScript to get frontmost app name
            script = '''osascript -e 'tell application "System Events" to get name of first process whose frontmost is true''''
            result = subprocess.run(script, shell=True, capture_output=True, text=True)
            title = result.stdout.strip() if result.returncode == 0 else 'Unknown'
            context.set(output_var, title)

            return ActionResult(
                success=True,
                message=f"窗口标题: {title}",
                data={
                    'title': title,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取窗口标题失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'window_title'}


class WindowMinimizeAction(BaseAction):
    """Minimize window."""
    action_type = "window_minimize"
    display_name = "最小化窗口"
    description = "最小化前台窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute minimize.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        try:
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to minimize window 1''''
            else:
                script = '''osascript -e 'tell application "System Events" to tell (first process whose frontmost is true) to set miniaturized of first window to true''''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="窗口已最小化"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最小化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}


class WindowMaximizeAction(BaseAction):
    """Maximize window."""
    action_type = "window_maximize"
    display_name = "最大化窗口"
    description = "最大化前台窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute maximize.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        try:
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to set bounds of window 1 to {0, 0, 1920, 1080}''''
            else:
                script = '''osascript -e 'tell application "System Events" to tell (first process whose frontmost is true) to set bounds of first window to {0, 0, 1920, 1080}''''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="窗口已最大化"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最大化窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}


class WindowRestoreAction(BaseAction):
    """Restore window."""
    action_type = "window_restore"
    display_name = "还原窗口"
    description = "还原窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute restore.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        try:
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to activate''''
            else:
                script = '''osascript -e 'tell application "System Events" to tell (first process whose frontmost is true) to set miniaturized of first window to false'''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="窗口已还原"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"还原窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}


class WindowCloseAction(BaseAction):
    """Close window."""
    action_type = "window_close"
    display_name = "关闭窗口"
    description = "关闭窗口"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute close.

        Args:
            context: Execution context.
            params: Dict with app_name.

        Returns:
            ActionResult indicating success.
        """
        app_name = params.get('app_name', '')

        try:
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to close window 1''''
            else:
                script = '''osascript -e 'tell application "System Events" to keystroke "w" using command down''''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="窗口已关闭"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"关闭窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}


class WindowMoveAction(BaseAction):
    """Move window."""
    action_type = "window_move"
    display_name = "移动窗口"
    description = "移动窗口到指定位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute move.

        Args:
            context: Execution context.
            params: Dict with x, y, app_name.

        Returns:
            ActionResult indicating success.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        app_name = params.get('app_name', '')

        try:
            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to set bounds of window 1 to {{{resolved_x}, {resolved_y}, {resolved_x + 800}, {resolved_y + 600}}}' '''
            else:
                script = f'''osascript -e 'tell application "System Events" to tell (first process whose frontmost is true) to set bounds of first window to {{{resolved_x}, {resolved_y}, {resolved_x + 800}, {resolved_y + 600}}}' '''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"窗口已移动到 ({resolved_x}, {resolved_y})",
                data={'x': resolved_x, 'y': resolved_y}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移动窗口失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}


class WindowResizeAction(BaseAction):
    """Resize window."""
    action_type = "window_resize"
    display_name = "调整窗口大小"
    description = "调整窗口大小"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute resize.

        Args:
            context: Execution context.
            params: Dict with width, height, app_name.

        Returns:
            ActionResult indicating success.
        """
        width = params.get('width', 800)
        height = params.get('height', 600)
        app_name = params.get('app_name', '')

        try:
            resolved_width = int(context.resolve_value(width))
            resolved_height = int(context.resolve_value(height))
            resolved_app = context.resolve_value(app_name) if app_name else ''

            if resolved_app:
                script = f'''osascript -e 'tell application "{resolved_app}" to set bounds of window 1 to {{0, 0, {resolved_width}, {resolved_height}}}' '''
            else:
                script = f'''osascript -e 'tell application "System Events" to tell (first process whose frontmost is true) to set bounds of first window to {{0, 0, {resolved_width}, {resolved_height}}}' '''

            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"窗口已调整为 {resolved_width}x{resolved_height}",
                data={'width': resolved_width, 'height': resolved_height}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"调整窗口大小失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'app_name': ''}