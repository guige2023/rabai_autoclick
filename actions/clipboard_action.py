"""Clipboard action module for RabAI AutoClick.

Provides clipboard operations:
- ClipboardReadAction: Read from clipboard
- ClipboardWriteAction: Write to clipboard
- ClipboardClearAction: Clear clipboard
- ClipboardContainsAction: Check clipboard content type
- ClipboardHistoryAction: Get clipboard history
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClipboardReadAction(BaseAction):
    """Read from clipboard."""
    action_type = "clipboard_read"
    display_name = "读取剪贴板"
    description = "从剪贴板读取内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard read.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clipboard content.
        """
        output_var = params.get('output_var', 'clipboard_content')

        try:
            import pyperclip
            content = pyperclip.paste()
            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"读取剪贴板完成: {len(content)} 字符",
                data={
                    'content': content,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="读取剪贴板失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"读取剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clipboard_content'}


class ClipboardWriteAction(BaseAction):
    """Write to clipboard."""
    action_type = "clipboard_write"
    display_name = "写入剪贴板"
    description = "写入内容到剪贴板"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard write.

        Args:
            context: Execution context.
            params: Dict with content, output_var.

        Returns:
            ActionResult with write status.
        """
        content = params.get('content', '')
        output_var = params.get('output_var', 'write_status')

        try:
            import pyperclip
            resolved = context.resolve_value(content)
            pyperclip.copy(str(resolved))
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"写入剪贴板完成",
                data={
                    'content': resolved,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="写入剪贴板失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"写入剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'write_status'}


class ClipboardClearAction(BaseAction):
    """Clear clipboard."""
    action_type = "clipboard_clear"
    display_name = "清空剪贴板"
    description = "清空剪贴板内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard clear.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear status.
        """
        output_var = params.get('output_var', 'clear_status')

        try:
            import pyperclip
            pyperclip.copy('')
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message="清空剪贴板完成",
                data={
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="清空剪贴板失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clear_status'}


class ClipboardContainsAction(BaseAction):
    """Check clipboard content type."""
    action_type = "clipboard_contains"
    display_name = "剪贴板内容检查"
    description = "检查剪贴板内容类型"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard contains check.

        Args:
            context: Execution context.
            params: Dict with check_type, output_var.

        Returns:
            ActionResult with check result.
        """
        check_type = params.get('check_type', 'text')
        output_var = params.get('output_var', 'contains_result')

        try:
            import pyperclip
            content = pyperclip.paste()

            result = False
            if check_type == 'text':
                result = bool(content)
            elif check_type == 'url':
                import re
                result = bool(re.match(r'https?://', content))
            elif check_type == 'email':
                import re
                result = bool(re.match(r'[\w.-]+@[\w.-]+\.\w+', content))
            elif check_type == 'number':
                result = content.strip().replace('.', '').replace('-', '').isdigit()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"剪贴板{check_type}检查: {'是' if result else '否'}",
                data={
                    'check_type': check_type,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="剪贴板内容检查失败: 未安装pyperclip库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"剪贴板内容检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'check_type': 'text', 'output_var': 'contains_result'}


class ClipboardHistoryAction(BaseAction):
    """Get clipboard history."""
    action_type = "clipboard_history"
    display_name = "剪贴板历史"
    description = "获取剪贴板历史记录"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clipboard history.

        Args:
            context: Execution context.
            params: Dict with max_items, output_var.

        Returns:
            ActionResult with clipboard history.
        """
        max_items = params.get('max_items', 10)
        output_var = params.get('output_var', 'clipboard_history')

        try:
            resolved_max = int(context.resolve_value(max_items))

            try:
                import pyperclip
                current = pyperclip.paste()

                history = [current]

                context.set(output_var, history[:resolved_max])

            except ImportError:
                return ActionResult(
                    success=False,
                    message="获取剪贴板历史失败: 未安装pyperclip库"
                )

            return ActionResult(
                success=True,
                message=f"获取剪贴板历史完成: {len(history[:resolved_max])} 项",
                data={
                    'history': history[:resolved_max],
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取剪贴板历史失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_items': 10, 'output_var': 'clipboard_history'}