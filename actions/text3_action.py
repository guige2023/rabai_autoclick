"""Text3 action module for RabAI AutoClick.

Provides additional text operations:
- TextLengthAction: Get text length
- TextUpperFirstAction: Uppercase first letter
- TextReverseAction: Reverse text
- TextRepeatAction: Repeat text
- TextCompressAction: Compress whitespace
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextLengthAction(BaseAction):
    """Get text length."""
    action_type = "text3_length"
    display_name = "文本长度"
    description = "获取文本长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text length.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with text length.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'length_result')

        try:
            resolved = str(context.resolve_value(value))
            result = len(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本长度: {result}",
                data={
                    'value': resolved,
                    'length': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文本长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'length_result'}


class TextUpperFirstAction(BaseAction):
    """Uppercase first letter."""
    action_type = "text3_upper_first"
    display_name: "首字母大写"
    description = "将文本首字母大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upper first.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with upper first string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'upper_first_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved[0].upper() + resolved[1:] if resolved else ''
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"首字母大写: {result}",
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
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'upper_first_result'}


class TextReverseAction(BaseAction):
    """Reverse text."""
    action_type = "text3_reverse"
    display_name = "反转文本"
    description = "反转文本顺序"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse text.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with reversed text.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'reverse_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转文本: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reverse_result'}


class TextRepeatAction(BaseAction):
    """Repeat text."""
    action_type = "text3_repeat"
    display_name = "重复文本"
    description = "重复文本指定次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute repeat text.

        Args:
            context: Execution context.
            params: Dict with value, count, output_var.

        Returns:
            ActionResult with repeated text.
        """
        value = params.get('value', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'repeat_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_count = int(context.resolve_value(count))

            result = resolved * resolved_count
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重复文本: {resolved_count}次",
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
                message=f"重复文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'repeat_result'}


class TextCompressAction(BaseAction):
    """Compress whitespace."""
    action_type = "text3_compress"
    display_name = "压缩空白"
    description = "将多个连续空白字符压缩为单个"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compress whitespace.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with compressed text.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'compress_result')

        try:
            import re
            resolved = str(context.resolve_value(value))
            result = re.sub(r'\s+', ' ', resolved).strip()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"压缩空白完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"压缩空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compress_result'}
