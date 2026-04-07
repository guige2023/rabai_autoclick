"""Validate action module for RabAI AutoClick.

Provides data validation operations:
- ValidateEmailAction: Validate email format
- ValidateUrlAction: Validate URL format
- ValidatePhoneAction: Validate phone number
- ValidateIpAddressAction: Validate IP address
- ValidateNumberRangeAction: Validate number in range
- ValidateLengthAction: Validate string length
- ValidatePatternAction: Validate against regex pattern
- ValidateCreditCardAction: Validate credit card number
"""

import re
import ipaddress
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateEmailAction(BaseAction):
    """Validate email format."""
    action_type = "validate_email"
    display_name = "验证邮箱"
    description = "验证邮箱格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute email validation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            # Simple email regex
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            result = bool(re.match(pattern, resolved))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"邮箱验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"邮箱验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validation_result'}


class ValidateUrlAction(BaseAction):
    """Validate URL format."""
    action_type = "validate_url"
    display_name = "验证URL"
    description = "验证URL格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL validation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            # Simple URL regex
            pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            result = bool(re.match(pattern, resolved))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validation_result'}


class ValidatePhoneAction(BaseAction):
    """Validate phone number."""
    action_type = "validate_phone"
    display_name = "验证电话"
    description = "验证电话号码格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute phone validation.

        Args:
            context: Execution context.
            params: Dict with value, country, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        country = params.get('country', 'US')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            # Remove common formatting characters
            cleaned = re.sub(r'[\s\-\(\)\.]', '', resolved)

            # Basic validation: 10+ digits
            result = bool(re.match(r'^\+?\d{10,15}$', cleaned))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"电话验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"电话验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'country': 'US', 'output_var': 'validation_result'}


class ValidateIpAddressAction(BaseAction):
    """Validate IP address."""
    action_type = "validate_ip"
    display_name = "验证IP地址"
    description = "验证IP地址格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute IP validation.

        Args:
            context: Execution context.
            params: Dict with value, version, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        version = params.get('version', 'both')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            try:
                if version == '4':
                    ipaddress.IPv4Address(resolved)
                    result = True
                elif version == '6':
                    ipaddress.IPv6Address(resolved)
                    result = True
                else:
                    ipaddress.ip_address(resolved)
                    result = True
            except ValueError:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"IP验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': resolved,
                    'version': version,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"IP验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'version': 'both', 'output_var': 'validation_result'}


class ValidateNumberRangeAction(BaseAction):
    """Validate number in range."""
    action_type = "validate_range"
    display_name = "验证数字范围"
    description = "验证数字是否在指定范围内"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute number range validation.

        Args:
            context: Execution context.
            params: Dict with value, min_val, max_val, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', 0)
        min_val = params.get('min_val', None)
        max_val = params.get('max_val', None)
        output_var = params.get('output_var', 'validation_result')

        try:
            resolved_value = context.resolve_value(value)
            num_value = float(resolved_value)

            result = True
            if min_val is not None:
                resolved_min = context.resolve_value(min_val)
                if num_value < float(resolved_min):
                    result = False

            if max_val is not None and result:
                resolved_max = context.resolve_value(max_val)
                if num_value > float(resolved_max):
                    result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': num_value,
                    'min': min_val,
                    'max': max_val,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"范围验证失败: 无效的数字 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_val': None, 'max_val': None, 'output_var': 'validation_result'}


class ValidateLengthAction(BaseAction):
    """Validate string length."""
    action_type = "validate_length"
    display_name = "验证字符串长度"
    description = "验证字符串长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string length validation.

        Args:
            context: Execution context.
            params: Dict with value, min_length, max_length, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        min_length = params.get('min_length', None)
        max_length = params.get('max_length', None)
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            str_value = str(resolved)
            length = len(str_value)

            result = True
            if min_length is not None:
                resolved_min = context.resolve_value(min_length)
                if length < int(resolved_min):
                    result = False

            if max_length is not None and result:
                resolved_max = context.resolve_value(max_length)
                if length > int(resolved_max):
                    result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"长度验证: {'有效' if result else '无效'} ({length} 字符)",
                data={
                    'valid': result,
                    'length': length,
                    'min_length': min_length,
                    'max_length': max_length,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"长度验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'min_length': None, 'max_length': None, 'output_var': 'validation_result'}


class ValidatePatternAction(BaseAction):
    """Validate against regex pattern."""
    action_type = "validate_pattern"
    display_name = "验证正则匹配"
    description = "验证字符串是否匹配正则表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pattern validation.

        Args:
            context: Execution context.
            params: Dict with value, pattern, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        pattern = params.get('pattern', '')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_pattern = context.resolve_value(pattern)

            result = bool(re.match(resolved_pattern, resolved_value))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则验证: {'匹配' if result else '不匹配'}",
                data={
                    'valid': result,
                    'value': resolved_value,
                    'pattern': resolved_pattern,
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
                message=f"正则验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validation_result'}


class ValidateCreditCardAction(BaseAction):
    """Validate credit card number."""
    action_type = "validate_credit_card"
    display_name = "验证信用卡号"
    description = "验证信用卡号格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute credit card validation.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with validation result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'validation_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            # Remove spaces and dashes
            cleaned = re.sub(r'[\s\-]', '', resolved)

            # Check if all digits and 13-19 digits
            if not re.match(r'^\d{13,19}$', cleaned):
                result = False
            else:
                # Luhn algorithm
                digits = [int(d) for d in cleaned]
                checksum = 0
                for i, d in enumerate(reversed(digits)):
                    if i % 2 == 1:
                        d *= 2
                        if d > 9:
                            d -= 9
                    checksum += d
                result = checksum % 10 == 0

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"信用卡验证: {'有效' if result else '无效'}",
                data={
                    'valid': result,
                    'value': resolved,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"信用卡验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'validation_result'}