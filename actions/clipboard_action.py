"""Clipboard action module for RabAI AutoClick.

Provides clipboard operations:
- ClipboardReadAction: Read text from clipboard
- ClipboardWriteAction: Write text to clipboard
- ClipboardClearAction: Clear clipboard contents
- ClipboardAppendAction: Append text to clipboard
- ClipboardGetLinesAction: Get clipboard as list of lines
- ClipboardMatchAction: Check if clipboard matches pattern
"""

from typing import Any, Dict, List, Optional, Union
import subprocess
import sys

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ClipboardReadAction(BaseAction):
    """Read text from system clipboard."""
    action_type = "clipboard_read"
    display_name = "剪贴板读取"
    description = "从系统剪贴板读取文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard read operation.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clipboard text.
        """
        output_var = params.get('output_var', 'clipboard_text')

        try:
            if sys.platform == 'darwin':
                result = subprocess.run(
                    ['pbpaste'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                text = result.stdout
            elif sys.platform == 'win32':
                result = subprocess.run(
                    ['powershell', '-command', 'Get-Clipboard'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                text = result.stdout
            else:
                result = subprocess.run(
                    ['xclip', '-selection', 'clipboard', '-o'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                text = result.stdout

            context.set(output_var, text)
            return ActionResult(success=True, data=text,
                               message=f"Read clipboard: {len(text)} chars")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Clipboard read timeout")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard read error: {str(e)}")


class ClipboardWriteAction(BaseAction):
    """Write text to system clipboard."""
    action_type = "clipboard_write"
    display_name = "剪贴板写入"
    description = "将文本写入系统剪贴板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard write operation.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with write status.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'clipboard_write_result')

        if text is None:
            return ActionResult(success=False, message="text is required")

        try:
            resolved_text = context.resolve_value(text)

            if sys.platform == 'darwin':
                process = subprocess.Popen(
                    ['pbcopy'],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                process.communicate(input=resolved_text.encode('utf-8'), timeout=5)
            elif sys.platform == 'win32':
                process = subprocess.Popen(
                    ['powershell', '-command', 'Set-Clipboard', '-Value', resolved_text],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                process.communicate(timeout=5)
            else:
                process = subprocess.Popen(
                    ['xclip', '-selection', 'clipboard'],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                process.communicate(input=resolved_text.encode('utf-8'), timeout=5)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Wrote {len(resolved_text)} chars to clipboard")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except subprocess.TimeoutExpired:
            return ActionResult(success=False, message="Clipboard write timeout")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard write error: {str(e)}")


class ClipboardClearAction(BaseAction):
    """Clear clipboard contents."""
    action_type = "clipboard_clear"
    display_name = "剪贴板清空"
    description = "清空剪贴板内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard clear operation.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with clear status.
        """
        output_var = params.get('output_var', 'clipboard_clear_result')

        try:
            if sys.platform == 'darwin':
                subprocess.run(['pbcopy'], input=b'', timeout=5)
            elif sys.platform == 'win32':
                subprocess.run(
                    ['powershell', '-command', 'Set-Clipboard', '-Value', ''],
                    timeout=5
                )
            else:
                subprocess.run(['xclip', '-selection', 'clipboard'], input=b'', timeout=5)

            context.set(output_var, True)
            return ActionResult(success=True, data=True, message="Clipboard cleared")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard clear error: {str(e)}")


class ClipboardAppendAction(BaseAction):
    """Append text to current clipboard content."""
    action_type = "clipboard_append"
    display_name = "剪贴板追加"
    description = "向剪贴板追加文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard append operation.

        Args:
            context: Execution context.
            params: Dict with text, separator, output_var.

        Returns:
            ActionResult with new clipboard content.
        """
        text = params.get('text', '')
        separator = params.get('separator', '\n')
        output_var = params.get('output_var', 'clipboard_append_result')

        if text is None:
            return ActionResult(success=False, message="text is required")

        try:
            current = ''
            if sys.platform == 'darwin':
                result = subprocess.run(['pbpaste'], capture_output=True, text=True, timeout=5)
                current = result.stdout
            elif sys.platform == 'win32':
                result = subprocess.run(
                    ['powershell', '-command', 'Get-Clipboard'],
                    capture_output=True, text=True, timeout=5
                )
                current = result.stdout
            else:
                result = subprocess.run(
                    ['xclip', '-selection', 'clipboard', '-o'],
                    capture_output=True, text=True, timeout=5
                )
                current = result.stdout

            resolved_text = context.resolve_value(text)
            resolved_sep = context.resolve_value(separator)
            new_text = current + resolved_sep + resolved_text

            if sys.platform == 'darwin':
                process = subprocess.Popen(
                    ['pbcopy'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                process.communicate(input=new_text.encode('utf-8'), timeout=5)
            elif sys.platform == 'win32':
                subprocess.run(
                    ['powershell', '-command', 'Set-Clipboard', '-Value', new_text],
                    timeout=5
                )
            else:
                process = subprocess.Popen(
                    ['xclip', '-selection', 'clipboard'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                process.communicate(input=new_text.encode('utf-8'), timeout=5)

            context.set(output_var, True)
            return ActionResult(success=True, data=True,
                               message=f"Appended to clipboard: {len(new_text)} total chars")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard append error: {str(e)}")


class ClipboardGetLinesAction(BaseAction):
    """Get clipboard content as list of lines."""
    action_type = "clipboard_get_lines"
    display_name = "剪贴板行列表"
    description = "获取剪贴板内容为行列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard get lines operation.

        Args:
            context: Execution context.
            params: Dict with strip, filter_empty, output_var.

        Returns:
            ActionResult with list of lines.
        """
        strip = params.get('strip', True)
        filter_empty = params.get('filter_empty', False)
        output_var = params.get('output_var', 'clipboard_lines')

        try:
            if sys.platform == 'darwin':
                result = subprocess.run(['pbpaste'], capture_output=True, text=True, timeout=5)
                text = result.stdout
            elif sys.platform == 'win32':
                result = subprocess.run(
                    ['powershell', '-command', 'Get-Clipboard'],
                    capture_output=True, text=True, timeout=5
                )
                text = result.stdout
            else:
                result = subprocess.run(
                    ['xclip', '-selection', 'clipboard', '-o'],
                    capture_output=True, text=True, timeout=5
                )
                text = result.stdout

            resolved_strip = context.resolve_value(strip)
            resolved_filter = context.resolve_value(filter_empty)

            if resolved_strip:
                lines = [line.strip() for line in text.splitlines()]
            else:
                lines = text.splitlines()

            if resolved_filter:
                lines = [line for line in lines if line]

            context.set(output_var, lines)
            return ActionResult(success=True, data=lines,
                               message=f"Got {len(lines)} lines from clipboard")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard get lines error: {str(e)}")


class ClipboardMatchAction(BaseAction):
    """Check if clipboard matches a pattern."""
    action_type = "clipboard_match"
    display_name = "剪贴板匹配"
    description = "检查剪贴板内容是否匹配模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute clipboard match operation.

        Args:
            context: Execution context.
            params: Dict with pattern, regex, output_var.

        Returns:
            ActionResult with match status.
        """
        pattern = params.get('pattern', '')
        regex = params.get('regex', False)
        output_var = params.get('output_var', 'clipboard_match_result')

        if not pattern:
            return ActionResult(success=False, message="pattern is required")

        try:
            if sys.platform == 'darwin':
                result = subprocess.run(['pbpaste'], capture_output=True, text=True, timeout=5)
                text = result.stdout
            elif sys.platform == 'win32':
                result = subprocess.run(
                    ['powershell', '-command', 'Get-Clipboard'],
                    capture_output=True, text=True, timeout=5
                )
                text = result.stdout
            else:
                result = subprocess.run(
                    ['xclip', '-selection', 'clipboard', '-o'],
                    capture_output=True, text=True, timeout=5
                )
                text = result.stdout

            resolved_pattern = context.resolve_value(pattern)
            resolved_regex = context.resolve_value(regex)

            if resolved_regex:
                import re
                match = re.search(resolved_pattern, text)
                matched = match is not None
            else:
                matched = resolved_pattern in text

            context.set(output_var, matched)
            return ActionResult(success=True, data=matched,
                               message=f"Clipboard match: {matched}")

        except FileNotFoundError:
            return ActionResult(success=False, message="Clipboard command not found")
        except Exception as e:
            return ActionResult(success=False, message=f"Clipboard match error: {str(e)}")
