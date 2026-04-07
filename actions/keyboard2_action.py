"""Keyboard2 action module for RabAI AutoClick.

Provides advanced keyboard operations:
- KeyboardTypeAction: Type text with special chars
- KeyboardShortcutAction: Execute keyboard shortcut
- KeyboardHoldAction: Hold a key
- KeyboardReleaseAction: Release a key
"""

import subprocess
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class KeyboardTypeAction(BaseAction):
    """Type text with special chars."""
    action_type = "keyboard_type"
    display_name = "输入文本"
    description = "输入带特殊字符的文本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute type.

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

            # Use cliclick on macOS to type text
            subprocess.run(['cliclick', 't', resolved_text], capture_output=True)

            return ActionResult(
                success=True,
                message=f"已输入文本: {len(resolved_text)} 字符"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="输入文本失败: cliclick未安装"
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


class KeyboardShortcutAction(BaseAction):
    """Execute keyboard shortcut."""
    action_type = "keyboard_shortcut"
    display_name = "执行快捷键"
    description = "执行键盘快捷键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute shortcut.

        Args:
            context: Execution context.
            params: Dict with keys.

        Returns:
            ActionResult indicating success.
        """
        keys = params.get('keys', [])

        valid, msg = self.validate_type(keys, (list, tuple), 'keys')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_keys = [context.resolve_value(k) for k in keys]

            # Use AppleScript for keyboard shortcuts
            key_map = {
                'return': 'return',
                'enter': 'enter',
                'tab': 'tab',
                'space': 'space',
                'delete': 'delete',
                'escape': 'escape',
                'up': 'up arrow',
                'down': 'down arrow',
                'left': 'left arrow',
                'right': 'right arrow',
                'home': 'home',
                'end': 'end',
                'pageup': 'page up',
                'pagedown': 'page down'
            }

            # Build the keystroke string
            keystroke_parts = []
            modifiers = []

            for key in resolved_keys:
                key_lower = key.lower()
                if key_lower in ['cmd', 'command']:
                    modifiers.append('command')
                elif key_lower in ['ctrl', 'control']:
                    modifiers.append('control')
                elif key_lower in ['alt', 'option']:
                    modifiers.append('option')
                elif key_lower in ['shift']:
                    modifiers.append('shift')
                else:
                    keystroke_parts.append(key_map.get(key_lower, key))

            modifier_str = ' using ' + ','.join([f'{m} down' for m in modifiers]) if modifiers else ''

            script = f'''osascript -e 'tell application "System Events" to keystroke "{keystroke_parts[0]}"{modifier_str}' '''
            subprocess.run(script, shell=True, capture_output=True)

            return ActionResult(
                success=True,
                message=f"快捷键已执行: {'+'.join(resolved_keys)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"执行快捷键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['keys']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class KeyboardHoldAction(BaseAction):
    """Hold a key."""
    action_type = "keyboard_hold"
    display_name = "按住按键"
    description = "按住键盘按键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute hold.

        Args:
            context: Execution context.
            params: Dict with key.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            # Use cliclick on macOS to hold key down
            subprocess.run(['cliclick', 'kd', resolved_key], capture_output=True)

            return ActionResult(
                success=True,
                message=f"已按住按键: {resolved_key}"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="按住按键失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按住按键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class KeyboardReleaseAction(BaseAction):
    """Release a key."""
    action_type = "keyboard_release"
    display_name = "释放按键"
    description = "释放键盘按键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute release.

        Args:
            context: Execution context.
            params: Dict with key.

        Returns:
            ActionResult indicating success.
        """
        key = params.get('key', '')

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_key = context.resolve_value(key)

            # Use cliclick on macOS to release key
            subprocess.run(['cliclick', 'ku', resolved_key], capture_output=True)

            return ActionResult(
                success=True,
                message=f"已释放按键: {resolved_key}"
            )
        except FileNotFoundError:
            return ActionResult(
                success=False,
                message="释放按键失败: cliclick未安装"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"释放按键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}