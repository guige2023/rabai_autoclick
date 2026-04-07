"""String4 action module for RabAI AutoClick.

Provides additional string operations:
- StringCapitalizeAction: Capitalize string
- StringTitleAction: Title case string
- StringSwapcaseAction: Swap case
- StringZfillAction: Pad with zeros
- StringCenterAction: Center string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringCapitalizeAction(BaseAction):
    """Capitalize string."""
    action_type = "string4_capitalize"
    display_name = "字符串首字母大写"
    description = "将字符串首字母大写"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute capitalize.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with capitalized string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'capitalized_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            result = resolved.capitalize()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"首字母大写完成",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'capitalized_result'}


class StringTitleAction(BaseAction):
    """Title case string."""
    action_type = "string4_title"
    display_name = "字符串标题化"
    description = "将字符串转换为标题格式"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with title case string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'title_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            result = resolved.title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标题化完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"标题化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'title_result'}


class StringSwapcaseAction(BaseAction):
    """Swap case."""
    action_type = "string4_swapcase"
    display_name = "字符串大小写翻转"
    description = "翻转字符串的大小写"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute swapcase.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with swapcase string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'swapcase_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            result = resolved.swapcase()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写翻转完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大小写翻转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'swapcase_result'}


class StringZfillAction(BaseAction):
    """Pad with zeros."""
    action_type = "string4_zfill"
    display_name = "字符串补零"
    description = "在字符串前补零"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute zfill.

        Args:
            context: Execution context.
            params: Dict with text, width, output_var.

        Returns:
            ActionResult with zero-padded string.
        """
        text = params.get('text', '')
        width = params.get('width', 10)
        output_var = params.get('output_var', 'zfill_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width))
            result = resolved.zfill(resolved_width)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串补零完成",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串补零失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'width': 10, 'output_var': 'zfill_result'}


class StringCenterAction(BaseAction):
    """Center string."""
    action_type = "string4_center"
    display_name = "字符串居中"
    description = "将字符串居中对齐"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute center.

        Args:
            context: Execution context.
            params: Dict with text, width, fillchar, output_var.

        Returns:
            ActionResult with centered string.
        """
        text = params.get('text', '')
        width = params.get('width', 80)
        fillchar = params.get('fillchar', ' ')
        output_var = params.get('output_var', 'center_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width))
            resolved_fillchar = str(context.resolve_value(fillchar)) if fillchar else ' '

            if len(resolved_fillchar) != 1:
                return ActionResult(
                    success=False,
                    message=f"字符串居中失败: 填充字符必须为单个字符"
                )

            result = resolved.center(resolved_width, resolved_fillchar)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串居中完成",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fillchar': resolved_fillchar,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串居中失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'width': 80, 'fillchar': ' ', 'output_var': 'center_result'}