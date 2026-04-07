"""Validation2 action module for RabAI AutoClick.

Provides additional validation operations:
- ValidationIsEmptyAction: Check if value is empty
- ValidationIsNumberAction: Check if value is number
- ValidationIsIntegerAction: Check if value is integer
- ValidationIsFloatAction: Check if value is float
- ValidationIsEmailAction: Check if value is email
- ValidationIsUrlAction: Check if value is URL
- ValidationIsPhoneAction: Check if value is phone
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidationIsEmptyAction(BaseAction):
    """Check if value is empty."""
    action_type = "validation_is_empty"
    display_name = "检查空值"
    description = "检查值是否为空"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is empty check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_empty')

        try:
            resolved = context.resolve_value(value)

            is_empty = (
                resolved is None or
                resolved == '' or
                (isinstance(resolved, (list, dict, tuple, str)) and len(resolved) == 0)
            )

            context.set(output_var, is_empty)

            return ActionResult(
                success=True,
                message=f"是否为空: {'是' if is_empty else '否'}",
                data={
                    'value': resolved,
                    'is_empty': is_empty,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查空值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_empty'}


class ValidationIsNumberAction(BaseAction):
    """Check if value is number."""
    action_type = "validation_is_number"
    display_name = "检查数字"
    description = "检查值是否为数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is number check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_number')

        try:
            resolved = context.resolve_value(value)

            is_number = isinstance(resolved, (int, float)) and not isinstance(resolved, bool)

            context.set(output_var, is_number)

            return ActionResult(
                success=True,
                message=f"是否为数字: {'是' if is_number else '否'}",
                data={
                    'value': resolved,
                    'is_number': is_number,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_number'}


class ValidationIsIntegerAction(BaseAction):
    """Check if value is integer."""
    action_type = "validation_is_integer"
    display_name = "检查整数"
    description = "检查值是否为整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is integer check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_integer')

        try:
            resolved = context.resolve_value(value)

            is_integer = isinstance(resolved, int) and not isinstance(resolved, bool)

            context.set(output_var, is_integer)

            return ActionResult(
                success=True,
                message=f"是否为整数: {'是' if is_integer else '否'}",
                data={
                    'value': resolved,
                    'is_integer': is_integer,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_integer'}


class ValidationIsFloatAction(BaseAction):
    """Check if value is float."""
    action_type = "validation_is_float"
    display_name = "检查浮点数"
    description = "检查值是否为浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is float check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_float')

        try:
            resolved = context.resolve_value(value)

            is_float = isinstance(resolved, float)

            context.set(output_var, is_float)

            return ActionResult(
                success=True,
                message=f"是否为浮点数: {'是' if is_float else '否'}",
                data={
                    'value': resolved,
                    'is_float': is_float,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查浮点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_float'}


class ValidationIsEmailAction(BaseAction):
    """Check if value is email."""
    action_type = "validation_is_email"
    display_name = "检查邮箱"
    description = "检查值是否为邮箱地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is email check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_email')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                is_email = False
            else:
                pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                is_email = bool(re.match(pattern, resolved))

            context.set(output_var, is_email)

            return ActionResult(
                success=True,
                message=f"是否为邮箱: {'是' if is_email else '否'}",
                data={
                    'value': resolved,
                    'is_email': is_email,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查邮箱失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_email'}


class ValidationIsUrlAction(BaseAction):
    """Check if value is URL."""
    action_type = "validation_is_url"
    display_name = "检查URL"
    description = "检查值是否为URL"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is URL check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_url')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                is_url = False
            else:
                pattern = r'^https?://[^\s/$.?#].[^\s]*$'
                is_url = bool(re.match(pattern, resolved))

            context.set(output_var, is_url)

            return ActionResult(
                success=True,
                message=f"是否为URL: {'是' if is_url else '否'}",
                data={
                    'value': resolved,
                    'is_url': is_url,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查URL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_url'}


class ValidationIsPhoneAction(BaseAction):
    """Check if value is phone."""
    action_type = "validation_is_phone"
    display_name = "检查电话"
    description = "检查值是否为电话号码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is phone check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_phone')

        try:
            resolved = context.resolve_value(value)

            if not isinstance(resolved, str):
                is_phone = False
            else:
                cleaned = re.sub(r'[\s\-\(\)]', '', resolved)
                pattern = r'^\+?[0-9]{7,15}$'
                is_phone = bool(re.match(pattern, cleaned))

            context.set(output_var, is_phone)

            return ActionResult(
                success=True,
                message=f"是否为电话: {'是' if is_phone else '否'}",
                data={
                    'value': resolved,
                    'is_phone': is_phone,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查电话失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_phone'}