"""Terminal action module for RabAI AutoClick.

Provides terminal operations:
- TerminalExecuteAction: Execute terminal command
- TerminalTypeAction: Type text
- TerminalPressKeyAction: Press a key
- TerminalCopyAction: Copy to clipboard
- TerminalPasteAction: Paste from clipboard
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TerminalExecuteAction(BaseAction):
    """Execute terminal command."""
    action_type = "terminal_execute"
    display_name = "执行终端命令"
    description = "执行终端命令"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute terminal command.

        Args:
            context: Execution context.
            params: Dict with command, shell, timeout, output_var.

        Returns:
            ActionResult with command output.
        """
        command = params.get('command', '')
        shell = params.get('shell', True)
        timeout = params.get('timeout', 30)
        output_var = params.get('output_var', 'terminal_result')

        valid, msg = self.validate_type(command, str, 'command')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_command = context.resolve_value(command)

            result = subprocess.run(
                resolved_command,
                shell=shell,
                capture_output=True,
                text=True,
                timeout=int(timeout)
            )

            output = {
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode,
                'success': result.returncode == 0
            }

            context.set(output_var, output)

            return ActionResult(
                success=True,
                message=f"命令执行完成: 返回码 {result.returncode}",
                data=output
            )
        except subprocess.TimeoutExpired:
            return ActionResult(
                success=False,
                message=f"命令执行超时: {timeout}秒"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"执行终端命令失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['command']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'shell': True, 'timeout': 30, 'output_var': 'terminal_result'}


class TerminalTypeAction(BaseAction):
    """Type text."""
    action_type = "terminal_type"
    display_name = "输入文本"
    description = "模拟键盘输入文本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute type text.

        Args:
            context: Execution context.
            params: Dict with text.

        Returns:
            ActionResult indicating success.
        """
        text = params.get('text', '')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)

            # Use AppleScript to type text
            script = f'''osascript -e 'tell application "System Events" to keystroke "{resolved_text.replace('"', '\\"')}"' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"已输入文本: {len(resolved_text)} 字符"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"输入文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class TerminalPressKeyAction(BaseAction):
    """Press a key."""
    action_type = "terminal_press_key"
    display_name = "按键"
    description = "模拟按下按键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute press key.

        Args:
            context: Execution context.
            params: Dict with key, modifiers.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', 'return')
        modifiers = params.get('modifiers', [])

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)
            resolved_modifiers = context.resolve_value(modifiers) if modifiers else []

            modifier_str = ''
            if resolved_modifiers:
                modifier_map = {
                    'command': 'command down',
                    'cmd': 'command down',
                    'shift': 'shift down',
                    'option': 'option down',
                    'opt': 'option down',
                    'control': 'control down',
                    'ctrl': 'control down'
                }
                modifier_parts = [modifier_map.get(m.lower(), m.lower()) for m in resolved_modifiers]
                modifier_str = ' using ' + ', '.join(modifier_parts)

            script = f'''osascript -e 'tell application "System Events" to keystroke "{resolved_key}"{modifier_str}' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"已按键: {resolved_key}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'modifiers': []}


class TerminalCopyAction(BaseAction):
    """Copy to clipboard."""
    action_type = "terminal_copy"
    display_name = "复制"
    description = "复制到剪贴板"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with text.

        Returns:
            ActionResult indicating success.
        """
        text = params.get('text', '')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)

            # Use AppleScript to copy to clipboard
            script = f'''osascript -e 'set the clipboard to "{resolved_text.replace('"', '\\"')}"' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"已复制到剪贴板: {len(resolved_text)} 字符"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class TerminalPasteAction(BaseAction):
    """Paste from clipboard."""
    action_type = "terminal_paste"
    display_name = "粘贴"
    description = "从剪贴板粘贴"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute paste.

        Args:
            context: Execution context.
            params: Dict with.

        Returns:
            ActionResult indicating success.
        """
        try:
            # Use AppleScript to paste from clipboard
            script = '''osascript -e 'tell application "System Events" to keystroke "v" using command down' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message="已从剪贴板粘贴"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"粘贴失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {}