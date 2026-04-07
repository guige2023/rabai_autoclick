"""Format action module for RabAI AutoClick.

Provides text formatting operations:
- FormatPadAction: Pad string
- FormatCenterAction: Center string
- FormatLjustAction: Left justify string
- FormatRjustAction: Right justify string
- FormatZfillAction: Zero fill string
- FormatCapitalizeAction: Capitalize string
- FormatTitleAction: Title case string
- FormatSwapcaseAction: Swap case string
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatPadAction(BaseAction):
    """Pad string."""
    action_type = "format_pad"
    display_name = "填充字符串"
    description = "填充字符串到指定长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute padding.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, side, output_var.

        Returns:
            ActionResult with padded string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        side = params.get('side', 'left')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(width, int, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(fillchar, str, 'fillchar')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid_sides = ['left', 'right', 'center']
        valid, msg = self.validate_in(side, valid_sides, 'side')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_width = context.resolve_value(width)
            resolved_fillchar = context.resolve_value(fillchar)

            if side == 'left':
                result = str(resolved).rjust(int(resolved_width), resolved_fillchar)
            elif side == 'right':
                result = str(resolved).ljust(int(resolved_width), resolved_fillchar)
            else:
                result = str(resolved).center(int(resolved_width), resolved_fillchar)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串填充完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'side': 'left', 'output_var': 'format_result'}


class FormatCenterAction(BaseAction):
    """Center string."""
    action_type = "format_center"
    display_name = "居中字符串"
    description = "将字符串居中对齐"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute centering.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with centered string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(width, int, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_width = context.resolve_value(width)
            resolved_fillchar = context.resolve_value(fillchar)

            result = str(resolved).center(int(resolved_width), resolved_fillchar)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串居中完成",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串居中失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'format_result'}


class FormatLjustAction(BaseAction):
    """Left justify string."""
    action_type = "format_ljust"
    display_name = "左对齐字符串"
    description = "将字符串左对齐"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute left justifying.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with left justified string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(width, int, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_width = context.resolve_value(width)
            resolved_fillchar = context.resolve_value(fillchar)

            result = str(resolved).ljust(int(resolved_width), resolved_fillchar)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串左对齐完成",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串左对齐失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'format_result'}


class FormatRjustAction(BaseAction):
    """Right justify string."""
    action_type = "format_rjust"
    display_name = "右对齐字符串"
    description = "将字符串右对齐"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute right justifying.

        Args:
            context: Execution context.
            params: Dict with value, width, fillchar, output_var.

        Returns:
            ActionResult with right justified string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(width, int, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_width = context.resolve_value(width)
            resolved_fillchar = context.resolve_value(fillchar)

            result = str(resolved).rjust(int(resolved_width), resolved_fillchar)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串右对齐完成",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串右对齐失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'output_var': 'format_result'}


class FormatZfillAction(BaseAction):
    """Zero fill string."""
    action_type = "format_zfill"
    display_name = "零填充字符串"
    description = "用零填充字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zfill.

        Args:
            context: Execution context.
            params: Dict with value, width, output_var.

        Returns:
            ActionResult with zero filled string.
        """
        value = params.get('value', '')
        width = params.get('width', 10)
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(width, int, 'width')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_width = context.resolve_value(width)

            result = str(resolved).zfill(int(resolved_width))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"零填充完成",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"零填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'format_result'}


class FormatCapitalizeAction(BaseAction):
    """Capitalize string."""
    action_type = "format_capitalize"
    display_name = "首字母大写"
    description = "将字符串首字母大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute capitalizing.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with capitalized string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).capitalize()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"首字母大写完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"首字母大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'format_result'}


class FormatTitleAction(BaseAction):
    """Title case string."""
    action_type = "format_title"
    display_name = "标题格式"
    description = "将字符串转换为标题格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title casing.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with title cased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标题格式转换完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"标题格式转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'format_result'}


class FormatSwapcaseAction(BaseAction):
    """Swap case string."""
    action_type = "format_swapcase"
    display_name = "大小写交换"
    description = "交换字符串的大小写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute swapcasing.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with swapcased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'format_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).swapcase()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写交换完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大小写交换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'format_result'}