"""Validate5 action module for RabAI AutoClick.

Provides additional validation operations:
- ValidateEmailAction: Validate email
- ValidateURLAction: Validate URL
- ValidatePhoneAction: Validate phone number
- ValidateIPAddressAction: Validate IP address
- ValidateCreditCardAction: Validate credit card
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateEmailAction(BaseAction):
    """Validate email."""
    action_type = "validate5_email"
    display_name = "验证邮箱"
    description = "验证邮箱格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute email validation.

        Args:
            context: Execution context.
            params: Dict with email, output_var.

        Returns:
            ActionResult with validation result.
        """
        email = params.get('email', '')
        output_var = params.get('output_var', 'email_valid')

        try:
            import re

            resolved = context.resolve_value(email)

            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            result = bool(re.match(pattern, resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"邮箱验证: {'有效' if result else '无效'}",
                data={
                    'email': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"邮箱验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['email']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'email_valid'}


class ValidateURLAction(BaseAction):
    """Validate URL."""
    action_type = "validate5_url"
    display_name = "验证URL"
    description = "验证URL格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL validation.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_valid')

        try:
            import re

            resolved = context.resolve_value(url)

            pattern = r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$'
            result = bool(re.match(pattern, resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL验证: {'有效' if result else '无效'}",
                data={
                    'url': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_valid'}


class ValidatePhoneAction(BaseAction):
    """Validate phone number."""
    action_type = "validate5_phone"
    display_name = "验证电话"
    description = "验证电话号码格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute phone validation.

        Args:
            context: Execution context.
            params: Dict with phone, output_var.

        Returns:
            ActionResult with validation result.
        """
        phone = params.get('phone', '')
        output_var = params.get('output_var', 'phone_valid')

        try:
            import re

            resolved = context.resolve_value(phone)

            pattern = r'^\+?1?\d{9,15}$'
            cleaned = re.sub(r'[\s\-\(\)]', '', resolved)
            result = bool(re.match(pattern, cleaned))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"电话验证: {'有效' if result else '无效'}",
                data={
                    'phone': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"电话验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['phone']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'phone_valid'}


class ValidateIPAddressAction(BaseAction):
    """Validate IP address."""
    action_type = "validate5_ip"
    display_name = "验证IP地址"
    description = "验证IP地址格式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute IP validation.

        Args:
            context: Execution context.
            params: Dict with ip, output_var.

        Returns:
            ActionResult with validation result.
        """
        ip = params.get('ip', '')
        output_var = params.get('output_var', 'ip_valid')

        try:
            import re

            resolved = context.resolve_value(ip)

            ipv4_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
            if re.match(ipv4_pattern, resolved):
                parts = resolved.split('.')
                result = all(0 <= int(part) <= 255 for part in parts)
            else:
                result = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"IP验证: {'有效' if result else '无效'}",
                data={
                    'ip': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"IP验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['ip']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ip_valid'}


class ValidateCreditCardAction(BaseAction):
    """Validate credit card."""
    action_type = "validate5_credit_card"
    display_name = "验证信用卡"
    description = "验证信用卡号"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute credit card validation.

        Args:
            context: Execution context.
            params: Dict with card_number, output_var.

        Returns:
            ActionResult with validation result.
        """
        card_number = params.get('card_number', '')
        output_var = params.get('output_var', 'card_valid')

        try:
            import re

            resolved = context.resolve_value(card_number)

            cleaned = re.sub(r'[\s\-]', '', resolved)

            if not cleaned.isdigit():
                result = False
            else:
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
                    'card_number': resolved,
                    'valid': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"信用卡验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['card_number']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'card_valid'}