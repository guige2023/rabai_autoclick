"""Pyperclip clipboard action module for RabAI AutoClick.

Provides cross-platform clipboard operations using Pyperclip.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClipboardCopyAction(BaseAction):
    """Copy text to clipboard using Pyperclip."""
    action_type = "clipboard_copy"
    display_name = "剪贴板复制"
    description = "复制文本到剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Copy text to clipboard.

        Args:
            context: Execution context.
            params: Dict with keys:
                - text: Text to copy
                - append: Whether to append to existing clipboard content

        Returns:
            ActionResult with operation result.
        """
        text = params.get('text', '')
        append = params.get('append', False)

        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            import pyperclip
        except ImportError:
            return ActionResult(success=False, message="pyperclip not installed. Run: pip install pyperclip")

        start = time.time()
        try:
            if append:
                existing = pyperclip.paste()
                text = existing + text
            pyperclip.copy(text)
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Copied {len(text)} chars to clipboard",
                data={'length': len(text)},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard copy error: {str(e)}")


class ClipboardPasteAction(BaseAction):
    """Read text from clipboard using Pyperclip."""
    action_type = "clipboard_paste"
    display_name = "剪贴板读取"
    description = "从剪贴板读取文本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Read clipboard text.

        Args:
            context: Execution context.
            params: Dict with keys:
                - max_length: Maximum length to read (0 = unlimited)

        Returns:
            ActionResult with clipboard content.
        """
        max_length = params.get('max_length', 0)

        try:
            import pyperclip
        except ImportError:
            return ActionResult(success=False, message="pyperclip not installed")

        start = time.time()
        try:
            text = pyperclip.paste()
            if max_length > 0 and len(text) > max_length:
                text = text[:max_length]
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Read {len(text)} chars from clipboard",
                data={'text': text, 'length': len(text)},
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard paste error: {str(e)}")


class ClipboardClearAction(BaseAction):
    """Clear clipboard content."""
    action_type = "clipboard_clear"
    display_name = "剪贴板清空"
    description = "清空剪贴板内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear clipboard.

        Args:
            context: Execution context.
            params: No specific params needed.

        Returns:
            ActionResult with operation result.
        """
        try:
            import pyperclip
        except ImportError:
            return ActionResult(success=False, message="pyperclip not installed")

        start = time.time()
        try:
            pyperclip.copy('')
            duration = time.time() - start
            return ActionResult(
                success=True,
                message="Clipboard cleared",
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard clear error: {str(e)}")
