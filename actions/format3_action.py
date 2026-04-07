"""Format3 action module for RabAI AutoClick.

Provides additional formatting operations:
- FormatPadLeftAction: Pad string left
- FormatPadRightAction: Pad string right
- FormatCenterAction: Center align string
- FormatJoinAction: Join strings
- FormatSplitLinesAction: Split text into lines
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatPadLeftAction(BaseAction):
    """Pad string left."""
    action_type = "format3_pad_left"
    display_name = "左填充"
    description = "在字符串左侧填充字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pad left.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with padded string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'padded_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_width = int(context.resolve_value(width))
            resolved_fill = str(context.resolve_value(fillchar))

            result = resolved.rjust(resolved_width, resolved_fill)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"左填充完成: '{result}'",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fillchar': resolved_fill,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"左填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'padded_result'}


class FormatPadRightAction(BaseAction):
    """Pad string right."""
    action_type = "format3_pad_right"
    display_name = "右填充"
    description = "在字符串右侧填充字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pad right.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with padded string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'padded_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_width = int(context.resolve_value(width))
            resolved_fill = str(context.resolve_value(fillchar))

            result = resolved.ljust(resolved_width, resolved_fill)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"右填充完成: '{result}'",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fillchar': resolved_fill,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"右填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'padded_result'}


class FormatCenterAction(BaseAction):
    """Center align string."""
    action_type = "format3_center"
    display_name = "居中对齐"
    description = "将字符串居中对齐"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute center.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with centered string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'centered_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_width = int(context.resolve_value(width))
            resolved_fill = str(context.resolve_value(fillchar))

            result = resolved.center(resolved_width, resolved_fill)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"居中对齐完成: '{result}'",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fillchar': resolved_fill,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"居中对齐失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'centered_result'}


class FormatJoinAction(BaseAction):
    """Join strings."""
    action_type = "format3_join"
    display_name = "字符串连接"
    description = "使用分隔符连接字符串列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join.

        Args:
            context: Execution context.
            params: Dict with strings, separator, output_var.

        Returns:
            ActionResult with joined string.
        """
        strings = params.get('strings', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'joined_result')

        try:
            resolved_strings = context.resolve_value(strings)
            resolved_sep = context.resolve_value(separator) if separator else ''

            if isinstance(resolved_strings, (list, tuple)):
                result = resolved_sep.join(str(s) for s in resolved_strings)
            else:
                result = str(resolved_strings)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接完成: {len(result)} 字符",
                data={
                    'strings': resolved_strings,
                    'separator': resolved_sep,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['strings']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'joined_result'}


class FormatSplitLinesAction(BaseAction):
    """Split text into lines."""
    action_type = "format3_split_lines"
    display_name = "分割行"
    description = "将文本分割成行列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split lines.

        Args:
            context: Execution context.
            params: Dict with text, keepends, output_var.

        Returns:
            ActionResult with lines list.
        """
        text = params.get('text', '')
        keepends = params.get('keepends', False)
        output_var = params.get('output_var', 'lines')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            resolved_keepends = bool(context.resolve_value(keepends)) if keepends else False

            lines = resolved.splitlines(keepends=resolved_keepends)
            context.set(output_var, lines)

            return ActionResult(
                success=True,
                message=f"分割完成: {len(lines)} 行",
                data={
                    'original': resolved,
                    'line_count': len(lines),
                    'lines': lines,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keepends': False, 'output_var': 'lines'}
