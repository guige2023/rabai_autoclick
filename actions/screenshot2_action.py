"""Screenshot2 action module for RabAI AutoClick.

Provides advanced screenshot operations:
- ScreenshotRegionAction: Take screenshot of region
- ScreenshotSaveAction: Save screenshot with name
- ScreenshotCopyAction: Copy screenshot to clipboard
"""

import subprocess
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScreenshotRegionAction(BaseAction):
    """Take screenshot of region."""
    action_type = "screenshot_region"
    display_name = "截取区域"
    description = "截取屏幕指定区域"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute region screenshot.

        Args:
            context: Execution context.
            params: Dict with x, y, width, height, output_path.

        Returns:
            ActionResult indicating success.
        """
        x = params.get('x', 0)
        y = params.get('y', 0)
        width = params.get('width', 100)
        height = params.get('height', 100)
        output_path = params.get('output_path', '')

        try:
            resolved_x = int(context.resolve_value(x))
            resolved_y = int(context.resolve_value(y))
            resolved_width = int(context.resolve_value(width))
            resolved_height = int(context.resolve_value(height))

            if not output_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"/tmp/screenshot_region_{timestamp}.png"

            resolved_output = context.resolve_value(output_path)

            # Use screencapture on macOS for region
            cmd = f'screencapture -x -R{resolved_x},{resolved_y},{resolved_width},{resolved_height} "{resolved_output}"'
            subprocess.run(cmd, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"区域截图已保存: {resolved_output}",
                data={
                    'path': resolved_output,
                    'region': (resolved_x, resolved_y, resolved_width, resolved_height)
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截取区域失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['x', 'y', 'width', 'height']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': ''}


class ScreenshotSaveAction(BaseAction):
    """Save screenshot with name."""
    action_type = "screenshot_save"
    display_name = "保存截图"
    description = "保存截图到指定路径"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute save screenshot.

        Args:
            context: Execution context.
            params: Dict with output_path, include_cursor.

        Returns:
            ActionResult indicating success.
        """
        output_path = params.get('output_path', '')
        include_cursor = params.get('include_cursor', False)

        try:
            if not output_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"/tmp/screenshot_{timestamp}.png"

            resolved_output = context.resolve_value(output_path)

            cursor_flag = '' if include_cursor else '-x'

            cmd = f'screencapture {cursor_flag} "{resolved_output}"'
            subprocess.run(cmd, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"截图已保存: {resolved_output}",
                data={'path': resolved_output}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"保存截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_path': '', 'include_cursor': False}


class ScreenshotCopyAction(BaseAction):
    """Copy screenshot to clipboard."""
    action_type = "screenshot_copy"
    display_name = "复制截图"
    description = "复制截图到剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy screenshot.

        Args:
            context: Execution context.
            params: Dict with include_cursor.

        Returns:
            ActionResult indicating success.
        """
        include_cursor = params.get('include_cursor', False)

        try:
            cursor_flag = '' if include_cursor else '-x'

            # Copy screenshot to clipboard
            cmd = f'screencapture {cursor_flag} -c'
            subprocess.run(cmd, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="截图已复制到剪贴板"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'include_cursor': False}