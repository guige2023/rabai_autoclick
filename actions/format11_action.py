"""Format11 action module for RabAI AutoClick.

Provides additional format operations:
- FormatPhoneAction: Format phone number
- FormatCreditCardAction: Format credit card
- FormatSSNAction: Format SSN
- FormatZipCodeAction: Format ZIP code
- FormatPaddingAction: Pad string
- FormatTruncateAction: Truncate string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatPhoneAction(BaseAction):
    """Format phone number."""
    action_type = "format11_phone"
    display_name = "格式化电话"
    description = "格式化电话号码"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute phone format.

        Args:
            context: Execution context.
            params: Dict with phone, format, output_var.

        Returns:
            ActionResult with formatted phone.
        """
        phone = params.get('phone', '')
        format_type = params.get('format', 'US')
        output_var = params.get('output_var', 'formatted_phone')

        try:
            import re

            resolved_phone = context.resolve_value(phone)
            resolved_format = context.resolve_value(format_type) if format_type else 'US'

            # Remove non-digits
            digits = re.sub(r'\D', '', resolved_phone)

            if resolved_format == 'US' and len(digits) == 10:
                result = f'({digits[:3]}) {digits[3:6]}-{digits[6:]}'
            elif resolved_format == 'US' and len(digits) == 11:
                result = f'+{digits[0]} ({digits[1:4]}) {digits[4:7]}-{digits[7:]}'
            elif resolved_format == 'CN' and len(digits) == 11:
                result = f'{digits[:3]}-{digits[3:7]}-{digits[7:]}'
            else:
                result = digits

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化电话: {result}",
                data={
                    'original': resolved_phone,
                    'format': resolved_format,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化电话失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['phone']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': 'US', 'output_var': 'formatted_phone'}


class FormatCreditCardAction(BaseAction):
    """Format credit card."""
    action_type = "format11_credit_card"
    display_name = "格式化信用卡"
    description = "格式化信用卡号"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute credit card format.

        Args:
            context: Execution context.
            params: Dict with card_number, separator, output_var.

        Returns:
            ActionResult with formatted card.
        """
        card_number = params.get('card_number', '')
        separator = params.get('separator', ' ')
        output_var = params.get('output_var', 'formatted_card')

        try:
            import re

            resolved = context.resolve_value(card_number)
            resolved_sep = context.resolve_value(separator) if separator else ' '

            # Remove non-digits
            digits = re.sub(r'\D', '', resolved)

            if len(digits) < 13 or len(digits) > 19:
                return ActionResult(
                    success=False,
                    message=f"无效的信用卡号长度: {len(digits)}"
                )

            # Format with separator every 4 digits
            result = resolved_sep.join([digits[i:i+4] for i in range(0, len(digits), 4)])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化信用卡: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化信用卡失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['card_number']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': ' ', 'output_var': 'formatted_card'}


class FormatSSNAction(BaseAction):
    """Format SSN."""
    action_type = "format11_ssn"
    display_name = "格式化社会安全号"
    description = "格式化SSN"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SSN format.

        Args:
            context: Execution context.
            params: Dict with ssn, output_var.

        Returns:
            ActionResult with formatted SSN.
        """
        ssn = params.get('ssn', '')
        output_var = params.get('output_var', 'formatted_ssn')

        try:
            import re

            resolved = context.resolve_value(ssn)

            # Remove non-digits
            digits = re.sub(r'\D', '', resolved)

            if len(digits) != 9:
                return ActionResult(
                    success=False,
                    message=f"无效的SSN长度: {len(digits)}"
                )

            result = f'{digits[:3]}-{digits[3:2]}-{digits[5:]}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化SSN: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化SSN失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['ssn']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_ssn'}


class FormatZipCodeAction(BaseAction):
    """Format ZIP code."""
    action_type = "format11_zipcode"
    display_name = "格式化邮编"
    description = "格式化邮编"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ZIP code format.

        Args:
            context: Execution context.
            params: Dict with zipcode, country, output_var.

        Returns:
            ActionResult with formatted ZIP.
        """
        zipcode = params.get('zipcode', '')
        country = params.get('country', 'US')
        output_var = params.get('output_var', 'formatted_zipcode')

        try:
            import re

            resolved = context.resolve_value(zipcode)
            resolved_country = context.resolve_value(country) if country else 'US'

            # Remove non-digits
            digits = re.sub(r'\D', '', resolved)

            if resolved_country == 'US':
                if len(digits) == 9:
                    result = f'{digits[:5]}-{digits[5:]}'
                else:
                    result = digits[:5]
            else:
                result = digits

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化邮编: {result}",
                data={
                    'original': resolved,
                    'country': resolved_country,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化邮编失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['zipcode']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'country': 'US', 'output_var': 'formatted_zipcode'}


class FormatPaddingAction(BaseAction):
    """Pad string."""
    action_type = "format11_padding"
    display_name = "填充字符串"
    description = "填充字符串"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute padding.

        Args:
            context: Execution context.
            params: Dict with text, width, fillchar, side, output_var.

        Returns:
            ActionResult with padded string.
        """
        text = params.get('text', '')
        width = params.get('width', 10)
        fillchar = params.get('fillchar', ' ')
        side = params.get('side', 'left')
        output_var = params.get('output_var', 'padded_text')

        try:
            resolved = context.resolve_value(text)
            resolved_width = int(context.resolve_value(width)) if width else 10
            resolved_fillchar = str(context.resolve_value(fillchar)) if fillchar else ' '
            resolved_side = context.resolve_value(side) if side else 'left'

            if len(resolved_fillchar) != 1:
                resolved_fillchar = ' '

            if resolved_side == 'left':
                result = resolved.ljust(resolved_width, resolved_fillchar)
            elif resolved_side == 'right':
                result = resolved.rjust(resolved_width, resolved_fillchar)
            else:
                result = resolved.center(resolved_width, resolved_fillchar)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"填充字符串: {result}",
                data={
                    'original': resolved,
                    'width': resolved_width,
                    'fillchar': resolved_fillchar,
                    'side': resolved_side,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"填充字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'width']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'fillchar': ' ', 'side': 'left', 'output_var': 'padded_text'}


class FormatTruncateAction(BaseAction):
    """Truncate string."""
    action_type = "format11_truncate"
    display_name = "截断字符串"
    description = "截断字符串"
    version = "11.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute truncate.

        Args:
            context: Execution context.
            params: Dict with text, length, suffix, output_var.

        Returns:
            ActionResult with truncated string.
        """
        text = params.get('text', '')
        length = params.get('length', 50)
        suffix = params.get('suffix', '...')
        output_var = params.get('output_var', 'truncated_text')

        try:
            resolved = context.resolve_value(text)
            resolved_length = int(context.resolve_value(length)) if length else 50
            resolved_suffix = context.resolve_value(suffix) if suffix else '...'

            if len(resolved) <= resolved_length:
                result = resolved
            else:
                result = resolved[:resolved_length - len(resolved_suffix)] + resolved_suffix

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"截断字符串: {result}",
                data={
                    'original': resolved,
                    'length': resolved_length,
                    'suffix': resolved_suffix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"截断字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'length']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'suffix': '...', 'output_var': 'truncated_text'}