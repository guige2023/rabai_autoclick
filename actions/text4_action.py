"""Text4 action module for RabAI AutoClick.

Provides additional text operations:
- TextReverseAction: Reverse text
- TextRepeatAction: Repeat text
- TextPadLeftAction: Pad text left
- TextPadRightAction: Pad text right
- TextSwapCaseAction: Swap text case
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextReverseAction(BaseAction):
    """Reverse text."""
    action_type = "text4_reverse"
    display_name = "反转文本"
    description = "反转文本内容"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with reversed text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'reversed_text')

        try:
            resolved = context.resolve_value(text)
            result = str(resolved)[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转文本: {result[:20]}...",
                data={
                    'original': resolved,
                    'reversed': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_text'}


class TextRepeatAction(BaseAction):
    """Repeat text."""
    action_type = "text4_repeat"
    display_name = "重复文本"
    description = "重复文本指定次数"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute repeat.

        Args:
            context: Execution context.
            params: Dict with text, count, output_var.

        Returns:
            ActionResult with repeated text.
        """
        text = params.get('text', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'repeated_text')

        try:
            resolved = context.resolve_value(text)
            resolved_count = int(context.resolve_value(count)) if count else 1

            result = str(resolved) * resolved_count
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重复文本: 重复{resolved_count}次",
                data={
                    'original': resolved,
                    'count': resolved_count,
                    'repeated': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重复文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'repeated_text'}


class TextPadLeftAction(BaseAction):
    """Pad text left."""
    action_type = "text4_pad_left"
    display_name = "左侧填充"
    description = "在文本左侧填充字符"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pad left.

        Args:
            context: Execution context.
            params: Dict with text, width, fill_char, output_var.

        Returns:
            ActionResult with padded text.
        """
        text = params.get('text', '')
        width = params.get('width', 10)
        fill_char = params.get('fill_char', ' ')
        output_var = params.get('output_var', 'padded_text')

        try:
            resolved = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width)) if width else 10
            resolved_fill = context.resolve_value(fill_char) if fill_char else ' '

            if len(resolved_fill) != 1:
                resolved_fill = ' '

            result = str(resolved).rjust(resolved_width, resolved_fill)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"左侧填充: 宽度{resolved_width}",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fill_char': resolved_fill,
                    'padded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"左侧填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fill_char': ' ', 'output_var': 'padded_text'}


class TextPadRightAction(BaseAction):
    """Pad text right."""
    action_type = "text4_pad_right"
    display_name = "右侧填充"
    description = "在文本右侧填充字符"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pad right.

        Args:
            context: Execution context.
            params: Dict with text, width, fill_char, output_var.

        Returns:
            ActionResult with padded text.
        """
        text = params.get('text', '')
        width = params.get('width', 10)
        fill_char = params.get('fill_char', ' ')
        output_var = params.get('output_var', 'padded_text')

        try:
            resolved = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width)) if width else 10
            resolved_fill = context.resolve_value(fill_char) if fill_char else ' '

            if len(resolved_fill) != 1:
                resolved_fill = ' '

            result = str(resolved).ljust(resolved_width, resolved_fill)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"右侧填充: 宽度{resolved_width}",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fill_char': resolved_fill,
                    'padded': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"右侧填充失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fill_char': ' ', 'output_var': 'padded_text'}


class TextSwapCaseAction(BaseAction):
    """Swap text case."""
    action_type = "text4_swap_case"
    display_name = "大小写交换"
    description = "交换文本的大小写"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute swap case.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with swapped case text.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'swapped_text')

        try:
            resolved = context.resolve_value(text)
            result = str(resolved).swapcase()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写交换: {result[:20]}...",
                data={
                    'original': resolved,
                    'swapped': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大小写交换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'swapped_text'}