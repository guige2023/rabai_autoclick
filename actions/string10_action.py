"""String10 action module for RabAI AutoClick.

Provides additional string operations:
- StringRpartitionAction: Partition from right
- StringSplitlinesAction: Split by lines
- StringZfillAction: Zero-fill string
- StringIsdecimalAction: Check if decimal
- StringIsnumericAction: Check if numeric
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringRpartitionAction(BaseAction):
    """Partition from right."""
    action_type = "string10_rpartition"
    display_name = "从右分区"
    description = "从右侧按分隔符将字符串分为三部分"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rpartition.

        Args:
            context: Execution context.
            params: Dict with value, separator, output_var.

        Returns:
            ActionResult with partition tuple.
        """
        value = params.get('value', '')
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'rpartition_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(separator, str, 'separator')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_sep = context.resolve_value(separator)

            result = resolved.rpartition(resolved_sep)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"从右分区: {result}",
                data={
                    'original': resolved,
                    'separator': resolved_sep,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"从右分区失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'separator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'rpartition_result'}


class StringSplitlinesAction(BaseAction):
    """Split by lines."""
    action_type = "string10_splitlines"
    display_name = "按行分割"
    description = "按行分割字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute splitlines.

        Args:
            context: Execution context.
            params: Dict with value, keepends, output_var.

        Returns:
            ActionResult with lines list.
        """
        value = params.get('value', '')
        keepends = params.get('keepends', False)
        output_var = params.get('output_var', 'splitlines_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_keepends = bool(context.resolve_value(keepends)) if keepends else False

            result = resolved.splitlines(keepends=resolved_keepends)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"按行分割: {len(result)} 行",
                data={
                    'original': resolved,
                    'keepends': resolved_keepends,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"按行分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keepends': False, 'output_var': 'splitlines_result'}


class StringZfillAction(BaseAction):
    """Zero-fill string."""
    action_type = "string10_zfill"
    display_name = "零填充"
    description = "在字符串左侧填充零"

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
            ActionResult with zero-filled string.
        """
        value = params.get('value', '')
        width = params.get('width', 0)
        output_var = params.get('output_var', 'zfill_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_width = int(context.resolve_value(width))

            result = resolved.zfill(resolved_width)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"零填充: {result}",
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
                message=f"零填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'zfill_result'}


class StringIsdecimalAction(BaseAction):
    """Check if decimal."""
    action_type = "string10_isdecimal"
    display_name = "判断十进制"
    description = "检查字符串是否为十进制数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is decimal.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decimal check.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'isdecimal_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isdecimal()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"十进制判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断十进制失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isdecimal_result'}


class StringIsnumericAction(BaseAction):
    """Check if numeric."""
    action_type = "string10_isnumeric"
    display_name: "判断数字"
    description = "检查字符串是否为数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute isnumeric.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with numeric check.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'isnumeric_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isnumeric()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数字判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isnumeric_result'}
