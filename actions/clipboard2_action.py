"""Clipboard2 action module for RabAI AutoClick.

Provides advanced clipboard operations:
- ClipboardGetAction: Get clipboard content
- ClipboardSetAction: Set clipboard content
- ClipboardClearAction: Clear clipboard
- ClipboardAppendAction: Append to clipboard
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClipboardGetAction(BaseAction):
    """Get clipboard content."""
    action_type = "clipboard_get"
    display_name = "获取剪贴板"
    description = "获取剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get clipboard.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clipboard content.
        """
        output_var = params.get('output_var', 'clipboard_content')

        try:
            script = '''osascript -e 'get the clipboard as text' '''
            result = subprocess.run(script, shell=True, capture_output=True, text=True)

            content = result.stdout.strip() if result.returncode == 0 else ''
            context.set(output_var, content)

            return ActionResult(
                success=True,
                message=f"剪贴板内容: {len(content)} 字符",
                data={
                    'content': content,
                    'length': len(content),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'clipboard_content'}


class ClipboardSetAction(BaseAction):
    """Set clipboard content."""
    action_type = "clipboard_set"
    display_name = "设置剪贴板"
    description = "设置剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set clipboard.

        Args:
            context: Execution context.
            params: Dict with content.

        Returns:
            ActionResult indicating success.
        """
        content = params.get('content', '')

        try:
            resolved_content = context.resolve_value(content)

            # Escape quotes in the content
            escaped = resolved_content.replace('"', '\\"').replace('\n', '\\n')
            script = f'''osascript -e 'set the clipboard to "{escaped}"' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"剪贴板已设置: {len(resolved_content)} 字符",
                data={'length': len(resolved_content)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ClipboardClearAction(BaseAction):
    """Clear clipboard."""
    action_type = "clipboard_clear"
    display_name = "清空剪贴板"
    description = "清空剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear clipboard.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating success.
        """
        try:
            script = '''osascript -e 'set the clipboard to ""' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="剪贴板已清空"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class ClipboardAppendAction(BaseAction):
    """Append to clipboard."""
    action_type = "clipboard_append"
    display_name = "追加剪贴板"
    description = "追加内容到剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute append to clipboard.

        Args:
            context: Execution context.
            params: Dict with content, separator.

        Returns:
            ActionResult indicating success.
        """
        content = params.get('content', '')
        separator = params.get('separator', '\n')

        try:
            # First get existing content
            get_script = '''osascript -e 'get the clipboard as text' '''
            result = subprocess.run(get_script, shell=True, capture_output=True, text=True)
            existing = result.stdout.strip() if result.returncode == 0 else ''

            resolved_content = context.resolve_value(content)
            resolved_separator = context.resolve_value(separator) if separator else '\n'

            new_content = existing + resolved_separator + resolved_content

            # Set new content
            escaped = new_content.replace('"', '\\"').replace('\n', '\\n')
            set_script = f'''osascript -e 'set the clipboard to "{escaped}"' '''
            subprocess.run(set_script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"已追加到剪贴板: {len(resolved_content)} 字符",
                data={'total_length': len(new_content)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"追加剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['content']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '\n'}