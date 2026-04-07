"""Format4 action module for RabAI AutoClick.

Provides additional formatting operations:
- FormatNumberCurrencyAction: Format as currency
- FormatNumberPercentAction: Format as percentage
- FormatNumberBytesAction: Format as bytes
- FormatDateCustomAction: Custom date formatting
- FormatListAction: Format list as string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatNumberCurrencyAction(BaseAction):
    """Format as currency."""
    action_type = "format4_currency"
    display_name = "格式化货币"
    description = "格式化数字为货币"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute currency format.

        Args:
            context: Execution context.
            params: Dict with value, currency, locale, output_var.

        Returns:
            ActionResult with formatted currency.
        """
        value = params.get('value', 0)
        currency = params.get('currency', 'USD')
        locale = params.get('locale', 'en_US')
        output_var = params.get('output_var', 'formatted_currency')

        try:
            import locale as loc

            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_currency = context.resolve_value(currency) if currency else 'USD'

            currencies = {
                'USD': '$',
                'EUR': '€',
                'GBP': '£',
                'JPY': '¥',
                'CNY': '¥'
            }

            symbol = currencies.get(resolved_currency, resolved_currency)
            result = f'{symbol}{resolved_value:,.2f}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"货币格式化: {result}",
                data={
                    'value': resolved_value,
                    'currency': resolved_currency,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"货币格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'currency': 'USD', 'locale': 'en_US', 'output_var': 'formatted_currency'}


class FormatNumberPercentAction(BaseAction):
    """Format as percentage."""
    action_type = "format4_percent"
    display_name = "格式化百分比"
    description = "格式化数字为百分比"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute percent format.

        Args:
            context: Execution context.
            params: Dict with value, decimals, output_var.

        Returns:
            ActionResult with formatted percentage.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        output_var = params.get('output_var', 'formatted_percent')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_decimals = int(context.resolve_value(decimals)) if decimals else 2

            result = f'{resolved_value * 100:.{resolved_decimals}f}%'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"百分比格式化: {result}",
                data={
                    'value': resolved_value,
                    'decimals': resolved_decimals,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"百分比格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'output_var': 'formatted_percent'}


class FormatNumberBytesAction(BaseAction):
    """Format as bytes."""
    action_type = "format4_bytes"
    display_name = "格式化字节"
    description = "格式化字节数为人类可读"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute bytes format.

        Args:
            context: Execution context.
            params: Dict with bytes, output_var.

        Returns:
            ActionResult with formatted bytes.
        """
        bytes_val = params.get('bytes', 0)
        output_var = params.get('output_var', 'formatted_bytes')

        try:
            resolved = int(context.resolve_value(bytes_val)) if bytes_val else 0

            units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
            size = float(resolved)
            unit_index = 0

            while size >= 1024 and unit_index < len(units) - 1:
                size /= 1024
                unit_index += 1

            result = f'{size:.2f} {units[unit_index]}'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字节格式化: {result}",
                data={
                    'bytes': resolved,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字节格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['bytes']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_bytes'}


class FormatDateCustomAction(BaseAction):
    """Custom date formatting."""
    action_type = "format4_date"
    display_name = "自定义日期格式化"
    description = "自定义日期格式化"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute custom date format.

        Args:
            context: Execution context.
            params: Dict with date, format_str, output_var.

        Returns:
            ActionResult with formatted date.
        """
        date = params.get('date', 'now')
        format_str = params.get('format_str', '%Y-%m-%d')
        output_var = params.get('output_var', 'formatted_date')

        try:
            from datetime import datetime

            resolved_date = context.resolve_value(date)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            if resolved_date == 'now':
                dt = datetime.now()
            elif isinstance(resolved_date, str):
                dt = datetime.fromisoformat(resolved_date)
            else:
                dt = resolved_date

            result = dt.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期格式化: {result}",
                data={
                    'date': str(dt),
                    'format': resolved_format,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date', 'format_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_date'}


class FormatListAction(BaseAction):
    """Format list as string."""
    action_type = "format4_list"
    display_name = "格式化列表"
    description = "格式化列表为字符串"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list format.

        Args:
            context: Execution context.
            params: Dict with list, separator, output_var.

        Returns:
            ActionResult with formatted string.
        """
        input_list = params.get('list', [])
        separator = params.get('separator', ', ')
        output_var = params.get('output_var', 'formatted_list')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_sep = context.resolve_value(separator) if separator else ', '

            result = resolved_sep.join(str(item) for item in resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表格式化: {len(resolved)}项",
                data={
                    'list': resolved,
                    'separator': resolved_sep,
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': ', ', 'output_var': 'formatted_list'}