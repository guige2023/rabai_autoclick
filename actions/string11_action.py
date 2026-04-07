"""String11 action module for RabAI AutoClick.

Provides additional string operations:
- StringCapitalizeAction: Capitalize string
- StringSwapcaseAction: Swap case
- StringCountAction: Count substring occurrences
- StringFindAction: Find substring index
- StringIsAlphaAction: Check if alphabetic
- StringIsDigitAction: Check if digit
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringCapitalizeAction(BaseAction):
    """Capitalize string."""
    action_type = "string11_capitalize"
    display_name = "首字母大写"
    description = "首字母大写"
    version = "11.0"

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
        output_var = params.get('output_var', 'capitalized_text')

        try:
            resolved = context.resolve_value(text)

            if isinstance(resolved, str):
                result = resolved.capitalize()
            else:
                result = str(resolved).capitalize()

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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'capitalized_text'}


class StringSwapcaseAction(BaseAction):
    """Swap case."""
    action_type = "string11_swapcase"
    display_name = "大小写交换"
    description = "交换大小写"
    version = "11.0"

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
            ActionResult with swapped case string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'swapcase_text')

        try:
            resolved = context.resolve_value(text)

            if isinstance(resolved, str):
                result = resolved.swapcase()
            else:
                result = str(resolved).swapcase()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写交换: {result}",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'swapcase_text'}


class StringCountAction(BaseAction):
    """Count substring occurrences."""
    action_type = "string11_count"
    display_name = "统计子串"
    description = "统计子串出现次数"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with text, substring, output_var.

        Returns:
            ActionResult with count.
        """
        text = params.get('text', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'count_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_substring = context.resolve_value(substring)

            result = resolved_text.count(resolved_substring)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"统计子串: {result}",
                data={
                    'text': resolved_text,
                    'substring': resolved_substring,
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class StringFindAction(BaseAction):
    """Find substring index."""
    action_type = "string11_find"
    display_name = "查找子串"
    description = "查找子串位置"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with text, substring, output_var.

        Returns:
            ActionResult with index.
        """
        text = params.get('text', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'find_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_substring = context.resolve_value(substring)

            result = resolved_text.find(resolved_substring)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查找子串: {result}",
                data={
                    'text': resolved_text,
                    'substring': resolved_substring,
                    'index': result,
                    'found': result != -1,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'find_result'}


class StringIsAlphaAction(BaseAction):
    """Check if alphabetic."""
    action_type = "string11_isalpha"
    display_name = "检查字母"
    description = "检查是否全是字母"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute isalpha.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with isalpha result.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'isalpha_result')

        try:
            resolved = context.resolve_value(text)

            result = resolved.isalpha() if isinstance(resolved, str) else str(resolved).isalpha()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"检查字母: {'是' if result else '否'}",
                data={
                    'text': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字母失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isalpha_result'}


class StringIsDigitAction(BaseAction):
    """Check if digit."""
    action_type = "string11_isdigit"
    display_name = "检查数字"
    description = "检查是否全是数字"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute isdigit.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with isdigit result.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'isdigit_result')

        try:
            resolved = context.resolve_value(text)

            result = resolved.isdigit() if isinstance(resolved, str) else str(resolved).isdigit()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"检查数字: {'是' if result else '否'}",
                data={
                    'text': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'isdigit_result'}