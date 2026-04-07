"""Clipboard3 action module for RabAI AutoClick.

Provides additional clipboard operations:
- ClipboardHasTextAction: Check if clipboard has text
- ClipboardClearAction: Clear clipboard
- ClipboardGetImageAction: Get image from clipboard
- ClipboardSetImageAction: Set image to clipboard
"""

import pyperclip
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClipboardHasTextAction(BaseAction):
    """Check if clipboard has text."""
    action_type = "clipboard3_has_text"
    display_name = "剪贴板有文本"
    description = "检查剪贴板是否包含文本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute has text check.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with check result.
        """
        output_var = params.get('output_var', 'has_text')

        try:
            text = pyperclip.paste()
            result = bool(text and text.strip())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"剪贴板有文本: {'是' if result else '否'}",
                data={
                    'has_text': result,
                    'preview': text[:100] if text else '',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_text'}


class ClipboardClearAction(BaseAction):
    """Clear clipboard."""
    action_type = "clipboard3_clear"
    display_name = "清空剪贴板"
    description = "清空剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear result.
        """
        output_var = params.get('output_var', 'clear_result')

        try:
            pyperclip.copy('')
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="剪贴板已清空",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_result'}


class ClipboardGetImageAction(BaseAction):
    """Get image from clipboard."""
    action_type = "clipboard3_get_image"
    display_name = "获取剪贴板图片"
    description = "从剪贴板获取图片"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get image.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with image data.
        """
        output_var = params.get('output_var', 'clipboard_image')

        try:
            from PIL import Image
            import io
            import win32clipboard

            win32clipboard.OpenClipboard()
            try:
                if win32clipboard.IsClipboardFormatAvailable(win32clipboard.CF_DIB):
                    data = win32clipboard.GetClipboardData(win32clipboard.CF_DIB)
                    image = Image.frombytes('RGB', (1, 1), data)
                else:
                    image = None
            finally:
                win32clipboard.CloseClipboard()

            if image:
                context.set(output_var, image)
                return ActionResult(
                    success=True,
                    message=f"获取剪贴板图片: {image.size}",
                    data={
                        'size': image.size,
                        'mode': image.mode,
                        'output_var': output_var
                    }
                )
            else:
                context.set(output_var, None)
                return ActionResult(
                    success=False,
                    message="剪贴板中没有图片"
                )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 和 win32clipboard 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取剪贴板图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clipboard_image'}


class ClipboardSetImageAction(BaseAction):
    """Set image to clipboard."""
    action_type = "clipboard3_set_image"
    display_name = "设置剪贴板图片"
    description = "将图片复制到剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set image.

        Args:
            context: Execution context.
            params: Dict with image_data, output_var.

        Returns:
            ActionResult with set result.
        """
        image_data = params.get('image_data', None)
        output_var = params.get('output_var', 'set_image_result')

        try:
            from PIL import Image
            import win32clipboard
            import io

            resolved = context.resolve_value(image_data)

            if isinstance(resolved, Image.Image):
                image = resolved
            elif isinstance(resolved, str):
                image = Image.open(resolved)
            else:
                return ActionResult(
                    success=False,
                    message="无效的图片数据"
                )

            output = io.BytesIO()
            image.convert('RGB').save(output, format='BMP')
            data = output.getvalue()[14:]

            win32clipboard.OpenClipboard()
            try:
                win32clipboard.EmptyClipboard()
                win32clipboard.SetClipboardData(win32clipboard.CF_DIB, data)
            finally:
                win32clipboard.CloseClipboard()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"设置剪贴板图片: {image.size}",
                data={
                    'size': image.size,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="需要 Pillow 和 win32clipboard 库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置剪贴板图片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['image_data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_image_result'}