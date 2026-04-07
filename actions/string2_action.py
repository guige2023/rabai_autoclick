"""String2 action module for RabAI AutoClick.

Provides advanced string operations:
- StringReverseAction: Reverse string
- StringRepeatAction: Repeat string
- StringTruncateAction: Truncate string
- StringPadAction: Pad string
- StringWrapAction: Wrap string
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringReverseAction(BaseAction):
    """Reverse string."""
    action_type = "string_reverse"
    display_name = "反转字符串"
    description = "反转字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with reversed string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'reversed_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转字符串: {len(result)} 字符",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_string'}


class StringRepeatAction(BaseAction):
    """Repeat string."""
    action_type = "string_repeat"
    display_name = "重复字符串"
    description = "重复字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute repeat.

        Args:
            context: Execution context.
            params: Dict with value, count, output_var.

        Returns:
            ActionResult with repeated string.
        """
        value = params.get('value', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'repeated_string')

        try:
            resolved = context.resolve_value(value)
            resolved_count = int(context.resolve_value(count))

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved * resolved_count
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重复字符串: {resolved_count} 次",
                data={
                    'original': resolved,
                    'count': resolved_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重复字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'repeated_string'}


class StringTruncateAction(BaseAction):
    """Truncate string."""
    action_type = "string_truncate"
    display_name = "截断字符串"
    description = "截断字符串到指定长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute truncate.

        Args:
            context: Execution context.
            params: Dict with value, length, suffix, output_var.

        Returns:
            ActionResult with truncated string.
        """
        value = params.get('value', '')
        length = params.get('length', 50)
        suffix = params.get('suffix', '...')
        output_var = params.get('output_var', 'truncated_string')

        try:
            resolved = context.resolve_value(value)
            resolved_length = int(context.resolve_value(length))
            resolved_suffix = context.resolve_value(suffix)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            if len(resolved) <= resolved_length:
                result = resolved
            else:
                result = resolved[:resolved_length - len(resolved_suffix)] + resolved_suffix

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"截断字符串: {len(result)} 字符",
                data={
                    'original_length': len(resolved),
                    'truncated_length': len(result),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截断字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'length']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'suffix': '...', 'output_var': 'truncated_string'}


class StringPadAction(BaseAction):
    """Pad string."""
    action_type = "string_pad"
    display_name = "填充字符串"
    description = "填充字符串到指定长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pad.

        Args:
            context: Execution context.
            params: Dict with value, length, char, mode, output_var.

        Returns:
            ActionResult with padded string.
        """
        value = params.get('value', '')
        length = params.get('length', 10)
        char = params.get('char', ' ')
        mode = params.get('mode', 'left')
        output_var = params.get('output_var', 'padded_string')

        try:
            resolved = context.resolve_value(value)
            resolved_length = int(context.resolve_value(length))
            resolved_char = context.resolve_value(char)
            resolved_mode = context.resolve_value(mode)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            if len(resolved) >= resolved_length:
                result = resolved
            else:
                pad_len = resolved_length - len(resolved)
                if resolved_mode == 'left':
                    result = resolved_char * pad_len + resolved
                elif resolved_mode == 'right':
                    result = resolved + resolved_char * pad_len
                else:
                    left_pad = pad_len // 2
                    right_pad = pad_len - left_pad
                    result = resolved_char * left_pad + resolved + resolved_char * right_pad

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"填充字符串: {len(result)} 字符",
                data={
                    'original': resolved,
                    'length': resolved_length,
                    'mode': resolved_mode,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"填充字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'length']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'char': ' ', 'mode': 'left', 'output_var': 'padded_string'}


class StringWrapAction(BaseAction):
    """Wrap string."""
    action_type = "string_wrap"
    display_name = "包装字符串"
    description = "将字符串包装成多行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute wrap.

        Args:
            context: Execution context.
            params: Dict with value, width, output_var.

        Returns:
            ActionResult with wrapped string.
        """
        value = params.get('value', '')
        width = params.get('width', 80)
        output_var = params.get('output_var', 'wrapped_string')

        try:
            resolved = context.resolve_value(value)
            resolved_width = int(context.resolve_value(width))

            if not isinstance(resolved, str):
                resolved = str(resolved)

            words = resolved.split()
            lines = []
            current_line = []
            current_length = 0

            for word in words:
                if current_length + len(word) + (1 if current_line else 0) <= resolved_width:
                    current_line.append(word)
                    current_length += len(word) + (1 if len(current_line) > 1 else 0)
                else:
                    if current_line:
                        lines.append(' '.join(current_line))
                    current_line = [word]
                    current_length = len(word)

            if current_line:
                lines.append(' '.join(current_line))

            result = '\n'.join(lines)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包装字符串: {len(lines)} 行",
                data={
                    'original_length': len(resolved),
                    'lines': len(lines),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"包装字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'wrapped_string'}
