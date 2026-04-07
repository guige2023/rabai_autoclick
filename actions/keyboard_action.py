"""Keyboard action module for RabAI AutoClick.

Provides keyboard operations:
- KeyboardTypeAction: Type text
- KeyboardPressAction: Press key
- KeyboardHotkeyAction: Press hotkey combination
- KeyboardWriteAction: Write to active window
- KeyboardSleepAction: Wait/delay
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class KeyboardTypeAction(BaseAction):
    """Type text."""
    action_type = "keyboard_type"
    display_name = "键盘输入文本"
    description = "输入文本内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard type.

        Args:
            context: Execution context.
            params: Dict with text, interval, output_var.

        Returns:
            ActionResult with type status.
        """
        text = params.get('text', '')
        interval = params.get('interval', 0)
        output_var = params.get('output_var', 'type_status')

        try:
            import pyautogui

            resolved_text = context.resolve_value(text)
            resolved_interval = float(context.resolve_value(interval)) if interval else 0

            pyautogui.write(resolved_text, interval=resolved_interval)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"键盘输入完成: {len(resolved_text)} 字符",
                data={
                    'text': resolved_text,
                    'length': len(resolved_text),
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="键盘输入失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"键盘输入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'interval': 0, 'output_var': 'type_status'}


class KeyboardPressAction(BaseAction):
    """Press key."""
    action_type = "keyboard_press"
    display_name = "键盘按键"
    description = "按下指定按键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard press.

        Args:
            context: Execution context.
            params: Dict with key, output_var.

        Returns:
            ActionResult with press status.
        """
        key = params.get('key', '')
        output_var = params.get('output_var', 'press_status')

        try:
            import pyautogui

            resolved_key = context.resolve_value(key)

            pyautogui.press(resolved_key)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"键盘按键完成: {resolved_key}",
                data={
                    'key': resolved_key,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="键盘按键失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"键盘按键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'press_status'}


class KeyboardHotkeyAction(BaseAction):
    """Press hotkey combination."""
    action_type = "keyboard_hotkey"
    display_name = "键盘组合键"
    description = "按下组合键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard hotkey.

        Args:
            context: Execution context.
            params: Dict with keys, output_var.

        Returns:
            ActionResult with hotkey status.
        """
        keys = params.get('keys', [])
        output_var = params.get('output_var', 'hotkey_status')

        try:
            import pyautogui

            resolved_keys = context.resolve_value(keys)

            if isinstance(resolved_keys, str):
                resolved_keys = resolved_keys.split('+')

            pyautogui.hotkey(*resolved_keys)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"键盘组合键完成: {'+'.join(resolved_keys)}",
                data={
                    'keys': resolved_keys,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="键盘组合键失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"键盘组合键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['keys']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'hotkey_status'}


class KeyboardWriteAction(BaseAction):
    """Write to active window."""
    action_type = "keyboard_write"
    display_name = "键盘写入"
    description = "向活动窗口写入内容"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard write.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with write status.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'write_status')

        try:
            import pyautogui

            resolved_text = context.resolve_value(text)

            pyautogui.typewrite(resolved_text)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"键盘写入完成",
                data={
                    'text': resolved_text,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="键盘写入失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"键盘写入失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'write_status'}


class KeyboardSleepAction(BaseAction):
    """Wait/delay."""
    action_type = "keyboard_sleep"
    display_name = "键盘等待"
    description = "等待一段时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keyboard sleep.

        Args:
            context: Execution context.
            params: Dict with seconds, output_var.

        Returns:
            ActionResult with sleep status.
        """
        seconds = params.get('seconds', 1)
        output_var = params.get('output_var', 'sleep_status')

        try:
            import pyautogui

            resolved_seconds = float(context.resolve_value(seconds))

            pyautogui.sleep(resolved_seconds)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"键盘等待完成: {resolved_seconds} 秒",
                data={
                    'seconds': resolved_seconds,
                    'output_var': output_var
                }
            )
        except ImportError:
            return ActionResult(
                success=False,
                message="键盘等待失败: 未安装pyautogui库"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"键盘等待失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'seconds': 1, 'output_var': 'sleep_status'}