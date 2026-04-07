"""Clipboard action module for RabAI AutoClick.

Provides clipboard operations:
- CopyAction: Copy text to clipboard
- PasteAction: Paste from clipboard
- GetClipboardAction: Get clipboard content
"""

import pyperclip
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CopyAction(BaseAction):
    """Copy text to clipboard."""
    action_type = "clipboard_copy"
    display_name = "复制到剪贴板"
    description = "将文本复制到系统剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copying text to clipboard.

        Args:
            context: Execution context.
            params: Dict with text or variable.

        Returns:
            ActionResult indicating success.
        """
        text = params.get('text', '')
        variable = params.get('variable', None)

        # If variable is specified, get value from context
        if variable:
            valid, msg = self.validate_type(variable, str, 'variable')
            if not valid:
                return ActionResult(success=False, message=msg)
            text = context.get(variable, '')

        # Validate text
        if text is None:
            text = ''
        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            pyperclip.copy(text)
            truncated = text[:50] + '...' if len(text) > 50 else text
            return ActionResult(
                success=True,
                message=f"已复制到剪贴板: {truncated}",
                data={'text': text, 'length': len(text)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制到剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'text': '',
            'variable': None
        }


class PasteAction(BaseAction):
    """Paste text from clipboard using keyboard simulation."""
    action_type = "clipboard_paste"
    display_name = "粘贴"
    description = "模拟键盘粘贴剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pasting from clipboard.

        Args:
            context: Execution context.
            params: Dict with interval, enter_after.

        Returns:
            ActionResult indicating success.
        """
        import pyautogui
        import time

        interval = params.get('interval', 0.05)
        enter_after = params.get('enter_after', False)

        # Validate interval
        valid, msg = self.validate_type(interval, (int, float), 'interval')
        if not valid:
            return ActionResult(success=False, message=msg)
        if interval < 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'interval' must be >= 0, got {interval}"
            )

        # Validate enter_after
        valid, msg = self.validate_type(enter_after, bool, 'enter_after')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Get clipboard content for message
            clipboard_text = pyperclip.paste()
            truncated = clipboard_text[:50] + '...' if len(clipboard_text) > 50 else clipboard_text

            # Simulate paste keyboard shortcut
            pyautogui.hotkey('command', 'v')
            time.sleep(0.1)

            if enter_after:
                time.sleep(0.1)
                pyautogui.press('enter')

            return ActionResult(
                success=True,
                message=f"粘贴成功: {truncated}",
                data={'text': clipboard_text, 'length': len(clipboard_text)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"粘贴失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'interval': 0.05,
            'enter_after': False
        }


class GetClipboardAction(BaseAction):
    """Get the current clipboard content."""
    action_type = "get_clipboard"
    display_name = "获取剪贴板"
    description = "获取当前剪贴板的内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting clipboard content.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clipboard content.
        """
        output_var = params.get('output_var', 'clipboard_content')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            text = pyperclip.paste()

            # Store in context variable if specified
            if output_var:
                context.set(output_var, text)

            truncated = text[:50] + '...' if len(text) > 50 else text
            return ActionResult(
                success=True,
                message=f"获取剪贴板成功: {truncated}",
                data={'text': text, 'length': len(text), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'output_var': 'clipboard_content'
        }


class ClearClipboardAction(BaseAction):
    """Clear the clipboard content."""
    action_type = "clear_clipboard"
    display_name = "清空剪贴板"
    description = "清空系统剪贴板的内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clearing clipboard.

        Args:
            context: Execution context.
            params: Dict with confirm.

        Returns:
            ActionResult indicating success.
        """
        confirm = params.get('confirm', True)

        valid, msg = self.validate_type(confirm, bool, 'confirm')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            pyperclip.copy('')
            return ActionResult(
                success=True,
                message="剪贴板已清空",
                data={'cleared': True}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空剪贴板失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'confirm': True}