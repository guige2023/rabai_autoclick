"""Transform7 action module for RabAI AutoClick.

Provides additional transformation operations:
- TransformUpperAction: Convert to uppercase
- TransformLowerAction: Convert to lowercase
- TransformTitleAction: Convert to title case
- TransformStripAction: Strip whitespace
- TransformReplaceAction: Replace text
- TransformReverseAction: Reverse string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformUpperAction(BaseAction):
    """Convert to uppercase."""
    action_type = "transform7_upper"
    display_name = "转换为大写"
    description = "转换为大写字母"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute uppercase transform.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = resolved.upper()
            else:
                result = str(resolved).upper()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为大写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transformed_value'}


class TransformLowerAction(BaseAction):
    """Convert to lowercase."""
    action_type = "transform7_lower"
    display_name = "转换为小写"
    description = "转换为小写字母"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lowercase transform.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = resolved.lower()
            else:
                result = str(resolved).lower()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为小写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transformed_value'}


class TransformTitleAction(BaseAction):
    """Convert to title case."""
    action_type = "transform7_title"
    display_name = "转换标题大小写"
    description = "转换为首字母大写"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title transform.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = resolved.title()
            else:
                result = str(resolved).title()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换标题大小写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换标题大小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'transformed_value'}


class TransformStripAction(BaseAction):
    """Strip whitespace."""
    action_type = "transform7_strip"
    display_name = "去除空白"
    description = "去除字符串首尾空白"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip transform.

        Args:
            context: Execution context.
            params: Dict with value, chars, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)
            resolved_chars = context.resolve_value(chars) if chars else None

            if isinstance(resolved, str):
                if resolved_chars:
                    result = resolved.strip(resolved_chars)
                else:
                    result = resolved.strip()
            else:
                result = str(resolved).strip()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除空白: {result}",
                data={
                    'original': resolved,
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
        return {'chars': None, 'output_var': 'transformed_value'}


class TransformReplaceAction(BaseAction):
    """Replace text."""
    action_type = "transform7_replace"
    display_name = "替换文本"
    description = "替换字符串中的文本"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace transform.

        Args:
            context: Execution context.
            params: Dict with value, old, new, count, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        count = params.get('count', -1)
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)
            resolved_count = context.resolve_value(count) if count else -1

            if isinstance(resolved, str):
                result = resolved.replace(resolved_old, resolved_new, resolved_count)
            else:
                result = str(resolved).replace(resolved_old, resolved_new, resolved_count)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换文本: {result}",
                data={
                    'original': resolved,
                    'old': resolved_old,
                    'new': resolved_new,
                    'count': resolved_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"替换文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': -1, 'output_var': 'transformed_value'}


class TransformReverseAction(BaseAction):
    """Reverse string."""
    action_type = "transform7_reverse"
    display_name = "反转字符串"
    description = "反转字符串"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse transform.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with transformed value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'transformed_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, str):
                result = resolved[::-1]
            elif isinstance(resolved, (list, tuple)):
                result = list(resolved)[::-1]
            else:
                result = str(resolved)[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转字符串: {result}",
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
        return {'output_var': 'transformed_value'}