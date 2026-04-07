"""String7 action module for RabAI AutoClick.

Provides additional string operations:
- StringStripAction: Strip whitespace
- StringLstripAction: Strip left whitespace
- StringRstripAction: Strip right whitespace
- StringStartswithAction: Check if starts with
- StringEndswithAction: Check if ends with
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringStripAction(BaseAction):
    """Strip whitespace."""
    action_type = "string7_strip"
    display_name = "去除首尾空白"
    description = "去除字符串首尾空白字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip.

        Args:
            context: Execution context.
            params: Dict with value, chars, output_var.

        Returns:
            ActionResult with stripped string.
        """
        value = params.get('value', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'strip_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_chars = context.resolve_value(chars) if chars is not None else None

            result = resolved.strip(resolved_chars)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除首尾空白: '{result}'",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除首尾空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'output_var': 'strip_result'}


class StringLstripAction(BaseAction):
    """Strip left whitespace."""
    action_type = "string7_lstrip"
    display_name = "去除左侧空白"
    description = "去除字符串左侧空白字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lstrip.

        Args:
            context: Execution context.
            params: Dict with value, chars, output_var.

        Returns:
            ActionResult with left stripped string.
        """
        value = params.get('value', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'lstrip_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_chars = context.resolve_value(chars) if chars is not None else None

            result = resolved.lstrip(resolved_chars)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除左侧空白: '{result}'",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除左侧空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'output_var': 'lstrip_result'}


class StringRstripAction(BaseAction):
    """Strip right whitespace."""
    action_type = "string7_rstrip"
    display_name = "去除右侧空白"
    description = "去除字符串右侧空白字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rstrip.

        Args:
            context: Execution context.
            params: Dict with value, chars, output_var.

        Returns:
            ActionResult with right stripped string.
        """
        value = params.get('value', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'rstrip_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_chars = context.resolve_value(chars) if chars is not None else None

            result = resolved.rstrip(resolved_chars)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除右侧空白: '{result}'",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除右侧空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'output_var': 'rstrip_result'}


class StringStartswithAction(BaseAction):
    """Check if starts with."""
    action_type = "string7_startswith"
    display_name = "检查开头"
    description = "检查字符串是否以指定内容开头"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute startswith.

        Args:
            context: Execution context.
            params: Dict with value, prefix, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'startswith_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_prefix = str(context.resolve_value(prefix))

            result = resolved.startswith(resolved_prefix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"开头检查: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'prefix': resolved_prefix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查开头失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'prefix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'startswith_result'}


class StringEndswithAction(BaseAction):
    """Check if ends with."""
    action_type = "string7_endswith"
    display_name = "检查结尾"
    description = "检查字符串是否以指定内容结尾"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute endswith.

        Args:
            context: Execution context.
            params: Dict with value, suffix, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'endswith_result')

        try:
            resolved = str(context.resolve_value(value))
            resolved_suffix = str(context.resolve_value(suffix))

            result = resolved.endswith(resolved_suffix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"结尾检查: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'suffix': resolved_suffix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查结尾失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'suffix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'endswith_result'}
