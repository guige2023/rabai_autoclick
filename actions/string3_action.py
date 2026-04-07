"""String3 action module for RabAI AutoClick.

Provides additional string operations:
- StringCapitalizeAction: Capitalize string
- StringTitleAction: Title case string
- StringSwapcaseAction: Swap case string
- StringLowerAction: Lower case string
- StringUpperAction: Upper case string
- StringStripWhitespaceAction: Strip whitespace
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringCapitalizeAction(BaseAction):
    """Capitalize string."""
    action_type = "string_capitalize"
    display_name = "首字母大写"
    description = "首字母大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute capitalize.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with capitalized string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'capitalized_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.capitalize()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"首字母大写: {result}",
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
        return {'output_var': 'capitalized_string'}


class StringTitleAction(BaseAction):
    """Title case string."""
    action_type = "string_title"
    display_name = "标题大小写"
    description = "每个单词首字母大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with title cased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'title_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标题大小写: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"标题大小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'title_string'}


class StringSwapcaseAction(BaseAction):
    """Swap case string."""
    action_type = "string_swapcase"
    display_name = "大小写互换"
    description = "大小写互换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute swapcase.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with swapcased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'swapcase_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.swapcase()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写互换: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大小写互换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'swapcase_string'}


class StringLowerAction(BaseAction):
    """Lower case string."""
    action_type = "string_lower"
    display_name = "转小写"
    description: "转为小写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lower.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with lowercased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'lower_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转小写: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lower_string'}


class StringUpperAction(BaseAction):
    """Upper case string."""
    action_type = "string_upper"
    display_name = "转大写"
    description = "转为大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upper.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with uppercased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'upper_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.upper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转大写: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'upper_string'}


class StringStripWhitespaceAction(BaseAction):
    """Strip whitespace."""
    action_type = "string_strip_whitespace"
    display_name = "去除空白"
    description = "去除字符串两端的空白"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with stripped string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'stripped_string')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                resolved = str(resolved)

            result = resolved.strip()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除空白: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stripped_string'}
