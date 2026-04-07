"""Validate2 action module for RabAI AutoClick.

Provides additional validation operations:
- ValidateRegexAction: Validate regex pattern
- ValidateLengthAction: Validate length
- ValidateRangeAction: Validate number range
- ValidateEmailAction: Validate email
- ValidateUrlAction: Validate URL
- ValidateUuidAction: Validate UUID
"""

import re
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateRegexAction(BaseAction):
    """Validate regex pattern."""
    action_type = "validate_regex"
    display_name = "验证正则"
    description = "验证正则表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex validate.

        Args:
            context: Execution context.
            params: Dict with pattern, value, output_var.

        Returns:
            ActionResult with validation result.
        """
        pattern = params.get('pattern', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'regex_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_value = context.resolve_value(value)

            regex = re.compile(resolved_pattern)
            result = regex.match(resolved_value) is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则验证: {'通过' if result else '失败'}",
                data={
                    'pattern': resolved_pattern,
                    'value': resolved_value,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"无效的正则表达式: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证正则失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'regex_result'}


class ValidateLengthAction(BaseAction):
    """Validate length."""
    action_type = "validate_length"
    display_name = "验证长度"
    description = "验证字符串或列表长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute length validate.

        Args:
            context: Execution context.
            params: Dict with value, min_length, max_length, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        min_length = params.get('min_length', None)
        max_length = params.get('max_length', None)
        output_var = params.get('output_var', 'length_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_min = context.resolve_value(min_length) if min_length is not None else None
            resolved_max = context.resolve_value(max_length) if max_length is not None else None

            length = len(resolved_value)

            result = True
            if resolved_min is not None and length < resolved_min:
                result = False
            if resolved_max is not None and length > resolved_max:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"长度验证: {'通过' if result else '失败'} (长度={length})",
                data={
                    'length': length,
                    'min_length': resolved_min,
                    'max_length': resolved_max,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_length': None, 'max_length': None, 'output_var': 'length_result'}


class ValidateRangeAction(BaseAction):
    """Validate number range."""
    action_type = "validate_range"
    display_name = "验证范围"
    description = "验证数字是否在范围内"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range validate.

        Args:
            context: Execution context.
            params: Dict with value, min_value, max_value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', 0)
        min_value = params.get('min_value', None)
        max_value = params.get('max_value', None)
        output_var = params.get('output_var', 'range_result')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_min = float(context.resolve_value(min_value)) if min_value is not None else None
            resolved_max = float(context.resolve_value(max_value)) if max_value is not None else None

            result = True
            if resolved_min is not None and resolved_value < resolved_min:
                result = False
            if resolved_max is not None and resolved_value > resolved_max:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围验证: {'通过' if result else '失败'} (值={resolved_value})",
                data={
                    'value': resolved_value,
                    'min_value': resolved_min,
                    'max_value': resolved_max,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_value': None, 'max_value': None, 'output_var': 'range_result'}


class ValidateEmailAction(BaseAction):
    """Validate email."""
    action_type = "validate_email"
    display_name = "验证邮箱"
    description = "验证邮箱地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute email validate.

        Args:
            context: Execution context.
            params: Dict with email, output_var.

        Returns:
            ActionResult with validation result.
        """
        email = params.get('email', '')
        output_var = params.get('output_var', 'email_result')

        try:
            resolved_email = context.resolve_value(email)

            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            result = re.match(pattern, resolved_email) is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"邮箱验证: {'通过' if result else '失败'}",
                data={
                    'email': resolved_email,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证邮箱失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['email']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'email_result'}


class ValidateUrlAction(BaseAction):
    """Validate URL."""
    action_type = "validate_url"
    display_name = "验证URL"
    description = "验证URL地址"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL validate.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_result')

        try:
            resolved_url = context.resolve_value(url)

            pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            result = re.match(pattern, resolved_url) is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL验证: {'通过' if result else '失败'}",
                data={
                    'url': resolved_url,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证URL失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_result'}


class ValidateUuidAction(BaseAction):
    """Validate UUID."""
    action_type = "validate_uuid"
    display_name = "验证UUID"
    description = "验证UUID格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute UUID validate.

        Args:
            context: Execution context.
            params: Dict with uuid, output_var.

        Returns:
            ActionResult with validation result.
        """
        uuid = params.get('uuid', '')
        output_var = params.get('output_var', 'uuid_result')

        try:
            resolved_uuid = context.resolve_value(uuid)

            pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            result = re.match(pattern, resolved_uuid.lower()) is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UUID验证: {'通过' if result else '失败'}",
                data={
                    'uuid': resolved_uuid,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"验证UUID失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['uuid']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'uuid_result'}
