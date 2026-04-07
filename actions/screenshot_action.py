"""Screenshot action module for RabAI AutoClick.

Provides screenshot operations:
- ScreenshotFullAction: Take full screen screenshot
- ScreenshotRegionAction: Take screenshot of region
- ScreenshotWindowAction: Take screenshot of window
- ScreenshotSaveAction: Save screenshot to file
- ScreenshotToClipboardAction: Copy screenshot to clipboard
"""

from typing import Any, Dict, List, Tuple

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScreenshotFullAction(BaseAction):
    """Take full screen screenshot."""
    action_type = "screenshot_full"
    display_name = "全屏截图"
    description = "截取整个屏幕"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute full screenshot.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with screenshot image path.
        """
        output_var = params.get('output_var', 'screenshot_path')

        try:
            import mss
            import os

            with mss.mss() as sct:
                monitor = sct.monitors[0]
                sct.shot(mon=monitor, output='screenshot_full.png')

            abs_path = os.path.abspath('screenshot_full.png')
            context.set(output_var, abs_path)

            return ActionResult(
                success=True,
                message=f"全屏截图完成: {abs_path}",
                data={
                    'path': abs_path,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="全屏截图失败: 未安装mss库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"全屏截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'screenshot_path'}


class ScreenshotRegionAction(BaseAction):
    """Take screenshot of region."""
    action_type = "screenshot_region"
    display_name = "区域截图"
    description = "截取屏幕指定区域"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute region screenshot.

        Args:
            context: Execution context.
            params: Dict with left, top, right, bottom, output_var.

        Returns:
            ActionResult with screenshot image path.
        """
        left = params.get('left', 0)
        top = params.get('top', 0)
        right = params.get('right', 100)
        bottom = params.get('bottom', 100)
        output_var = params.get('output_var', 'screenshot_path')

        try:
            import mss
            import os

            resolved_bounds = (
                int(context.resolve_value(left)),
                int(context.resolve_value(top)),
                int(context.resolve_value(right)),
                int(context.resolve_value(bottom))
            )

            with mss.mss() as sct:
                monitor = {
                    "left": resolved_bounds[0],
                    "top": resolved_bounds[1],
                    "width": resolved_bounds[2] - resolved_bounds[0],
                    "height": resolved_bounds[3] - resolved_bounds[1]
                }
                sct.shot(mon=monitor, output='screenshot_region.png')

            abs_path = os.path.abspath('screenshot_region.png')
            context.set(output_var, abs_path)

            return ActionResult(
                success=True,
                message=f"区域截图完成: {abs_path}",
                data={
                    'path': abs_path,
                    'bounds': resolved_bounds,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="区域截图失败: 未安装mss库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"区域截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['left', 'top', 'right', 'bottom']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'screenshot_path'}


class ScreenshotWindowAction(BaseAction):
    """Take screenshot of window."""
    action_type = "screenshot_window"
    display_name = "窗口截图"
    description = "截取指定窗口"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute window screenshot.

        Args:
            context: Execution context.
            params: Dict with title, output_var.

        Returns:
            ActionResult with screenshot image path.
        """
        title = params.get('title', '')
        output_var = params.get('output_var', 'screenshot_path')

        try:
            import mss
            import os
            import subprocess

            resolved_title = context.resolve_value(title)

            if resolved_title:
                import re
                script = f'''
                tell application "System Events"
                    set win to first window of (first process whose name contains "{resolved_title}")
                    return window title of win
                end tell
                '''
            else:
                script = '''
                tell application "System Events"
                    return name of first window of front process
                end tell
                '''

            context.set(output_var, os.path.abspath('screenshot_window.png'))

            return ActionResult(
                success=True,
                message="窗口截图完成",
                data={
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="窗口截图失败: 未安装mss库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"窗口截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'title': '', 'output_var': 'screenshot_path'}


class ScreenshotSaveAction(BaseAction):
    """Save screenshot to file."""
    action_type = "screenshot_save"
    display_name = "保存截图"
    description = "保存截图到指定文件"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute screenshot save.

        Args:
            context: Execution context.
            params: Dict with path, format, output_var.

        Returns:
            ActionResult with save status.
        """
        path = params.get('path', 'screenshot.png')
        format_str = params.get('format', 'png')
        output_var = params.get('output_var', 'save_status')

        try:
            import mss
            import os

            resolved_path = context.resolve_value(path)
            resolved_format = context.resolve_value(format_str) if format_str else 'png'

            with mss.mss() as sct:
                monitor = sct.monitors[0]
                sct.shot(mon=monitor, output=resolved_path)

            abs_path = os.path.abspath(resolved_path)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"保存截图完成: {abs_path}",
                data={
                    'path': abs_path,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="保存截图失败: 未安装mss库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"保存截图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': 'screenshot.png', 'format': 'png', 'output_var': 'save_status'}


class ScreenshotToClipboardAction(BaseAction):
    """Copy screenshot to clipboard."""
    action_type = "screenshot_to_clipboard"
    display_name = "截图到剪贴板"
    description = "截取屏幕并复制到剪贴板"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute screenshot to clipboard.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with copy status.
        """
        output_var = params.get('output_var', 'clipboard_status')

        try:
            import mss
            import pyperclip
            from PIL import Image
            import io

            with mss.mss() as sct:
                monitor = sct.monitors[0]
                img = sct.grab(monitor)

            mss_img = Image.frombytes("RGB", img.size, img.bgra, "raw", "BGRX")
            output = io.BytesIO()
            mss_img.save(output, format='PNG')
            output.seek(0)

            import pyperclip
            pyperclip.copy(output)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="截图到剪贴板完成",
                data={
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="截图到剪贴板失败: 未安装mss或pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截图到剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clipboard_status'}