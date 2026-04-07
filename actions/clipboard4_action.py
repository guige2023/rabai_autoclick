"""Clipboard4 action module for RabAI AutoClick.

Provides additional clipboard operations:
- ClipboardAppendAction: Append to clipboard
- ClipboardPrependAction: Prepend to clipboard
- ClipboardReplaceAction: Replace clipboard content
- ClipboardImageAction: Copy image to clipboard
- ClipboardHtmlAction: Copy HTML to clipboard
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClipboardAppendAction(BaseAction):
    """Append to clipboard."""
    action_type = "clipboard4_append"
    display_name = "剪贴板追加"
    description = "追加内容到剪贴板"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard append.

        Args:
            context: Execution context.
            params: Dict with content, separator, output_var.

        Returns:
            ActionResult with append status.
        """
        content = params.get('content', '')
        separator = params.get('separator', '\n')
        output_var = params.get('output_var', 'append_status')

        try:
            import pyperclip
            current = pyperclip.paste()
            resolved = context.resolve_value(content)
            resolved_sep = context.resolve_value(separator) if separator else '\n'

            new_content = current + resolved_sep + str(resolved) if current else str(resolved)
            pyperclip.copy(new_content)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"剪贴板追加完成",
                data={
                    'content': resolved,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="剪贴板追加失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板追加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '\n', 'output_var': 'append_status'}


class ClipboardPrependAction(BaseAction):
    """Prepend to clipboard."""
    action_type = "clipboard4_prepend"
    display_name = "剪贴板前置"
    description = "在剪贴板内容前添加"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard prepend.

        Args:
            context: Execution context.
            params: Dict with content, separator, output_var.

        Returns:
            ActionResult with prepend status.
        """
        content = params.get('content', '')
        separator = params.get('separator', '\n')
        output_var = params.get('output_var', 'prepend_status')

        try:
            import pyperclip
            current = pyperclip.paste()
            resolved = context.resolve_value(content)
            resolved_sep = context.resolve_value(separator) if separator else '\n'

            new_content = str(resolved) + resolved_sep + current if current else str(resolved)
            pyperclip.copy(new_content)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"剪贴板前置完成",
                data={
                    'content': resolved,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="剪贴板前置失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板前置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '\n', 'output_var': 'prepend_status'}


class ClipboardReplaceAction(BaseAction):
    """Replace clipboard content."""
    action_type = "clipboard4_replace"
    display_name = "剪贴板替换"
    description = "替换剪贴板中的特定内容"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard replace.

        Args:
            context: Execution context.
            params: Dict with old, new, output_var.

        Returns:
            ActionResult with replace status.
        """
        old = params.get('old', '')
        new = params.get('new', '')
        output_var = params.get('output_var', 'replace_status')

        try:
            import pyperclip
            current = pyperclip.paste()
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)

            new_content = current.replace(str(resolved_old), str(resolved_new))
            pyperclip.copy(new_content)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"剪贴板替换完成",
                data={
                    'old': resolved_old,
                    'new': resolved_new,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="剪贴板替换失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板替换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'replace_status'}


class ClipboardImageAction(BaseAction):
    """Copy image to clipboard."""
    action_type = "clipboard4_image"
    display_name = "复制图片到剪贴板"
    description = "复制图片到剪贴板"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard image.

        Args:
            context: Execution context.
            params: Dict with file_path, output_var.

        Returns:
            ActionResult with copy status.
        """
        file_path = params.get('file_path', '')
        output_var = params.get('output_var', 'image_copy_status')

        valid, msg = self.validate_type(file_path, str, 'file_path')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_path = context.resolve_value(file_path)

            try:
                from PIL import Image
                import pyperclip

                img = Image.open(resolved_path)
                pyperclip.copy(img)

            except ImportError:
                return ActionResult(
                    success=False,
                    message="复制图片到剪贴板失败: 未安装Pillow或pyperclip库"
                )

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"复制图片到剪贴板完成",
                data={
                    'file_path': resolved_path,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制图片到剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['file_path']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'image_copy_status'}


class ClipboardHtmlAction(BaseAction):
    """Copy HTML to clipboard."""
    action_type = "clipboard4_html"
    display_name = "复制HTML到剪贴板"
    description = "复制HTML到剪贴板"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard HTML.

        Args:
            context: Execution context.
            params: Dict with html, output_var.

        Returns:
            ActionResult with copy status.
        """
        html = params.get('html', '')
        output_var = params.get('output_var', 'html_copy_status')

        try:
            import pyperclip
            resolved = context.resolve_value(html)

            html_clip = pyperclip.HTMLEvaluator()
            pyperclip.copy(resolved, evaluator=html_clip)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"复制HTML到剪贴板完成",
                data={
                    'html': resolved,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="复制HTML到剪贴板失败: 未安装pyperclip库"
            )
        except AttributeError:
            import pyperclip
            pyperclip.copy(str(resolved))
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"复制HTML到剪贴板完成(纯文本)",
                data={
                    'html': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制HTML到剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['html']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'html_copy_status'}