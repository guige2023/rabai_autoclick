"""Format number action module for RabAI AutoClick.

Provides number formatting operations:
- FormatNumberAction: Format number with separators
- FormatCurrencyAction: Format as currency
- FormatPercentAction: Format as percentage
- FormatScientificAction: Format in scientific notation
"""

import locale
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatNumberAction(BaseAction):
    """Format number with separators."""
    action_type = "format_number"
    display_name = "格式化数字"
    description = "格式化数字为带分隔符的字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute number formatting.

        Args:
            context: Execution context.
            params: Dict with value, decimals, separator, output_var.

        Returns:
            ActionResult with formatted string.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 0)
        separator = params.get('separator', ',')
        output_var = params.get('output_var', 'format_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_decimals = context.resolve_value(decimals)
            resolved_separator = context.resolve_value(separator)

            num = float(resolved_value)
            formatted = f"{num:.{int(resolved_decimals)}f}"

            # Add thousand separators
            parts = formatted.split('.')
            int_part = parts[0]
            if resolved_separator and resolved_separator != '.':
                int_part = f"{int(int_part):,}".replace(',', resolved_separator)
            result = int_part + ('.' + parts[1] if len(parts) > 1 else '')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数字格式化: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"数字格式化失败: 无效的数字 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数字格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 0, 'separator': ',', 'output_var': 'format_result'}


class FormatCurrencyAction(BaseAction):
    """Format as currency."""
    action_type = "format_currency"
    display_name = "格式化货币"
    description = "格式化为货币字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute currency formatting.

        Args:
            context: Execution context.
            params: Dict with value, currency, locale_str, output_var.

        Returns:
            ActionResult with formatted currency string.
        """
        value = params.get('value', 0)
        currency = params.get('currency', 'USD')
        locale_str = params.get('locale', 'en_US.UTF-8')
        output_var = params.get('output_var', 'format_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_currency = context.resolve_value(currency)

            num = float(resolved_value)

            # Simple currency formatting without locale dependency
            currency_symbols = {
                'USD': '$',
                'EUR': '€',
                'GBP': '£',
                'JPY': '¥',
                'CNY': '¥',
                'KRW': '₩',
                'INR': '₹',
            }

            symbol = currency_symbols.get(resolved_currency.upper(), resolved_currency + ' ')
            result = f"{symbol}{num:,.2f}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"货币格式化: {result}",
                data={
                    'result': result,
                    'currency': resolved_currency,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"货币格式化失败: 无效的数字 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"货币格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'currency': 'USD', 'locale': 'en_US.UTF-8', 'output_var': 'format_result'}


class FormatPercentAction(BaseAction):
    """Format as percentage."""
    action_type = "format_percent"
    display_name = "格式化百分比"
    description = "格式化为百分比字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute percent formatting.

        Args:
            context: Execution context.
            params: Dict with value, decimals, output_var.

        Returns:
            ActionResult with formatted percentage string.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        output_var = params.get('output_var', 'format_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_decimals = context.resolve_value(decimals)

            num = float(resolved_value) * 100
            result = f"{num:.{int(resolved_decimals)}f}%"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"百分比格式化: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"百分比格式化失败: 无效的数字 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"百分比格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'output_var': 'format_result'}


class FormatScientificAction(BaseAction):
    """Format in scientific notation."""
    action_type = "format_scientific"
    display_name = "格式化科学计数法"
    description = "格式化为科学计数法字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scientific notation formatting.

        Args:
            context: Execution context.
            params: Dict with value, decimals, output_var.

        Returns:
            ActionResult with formatted scientific string.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        output_var = params.get('output_var', 'format_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_decimals = context.resolve_value(decimals)

            num = float(resolved_value)
            result = f"{num:.{int(resolved_decimals)}e}"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"科学计数法格式化: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except (ValueError, TypeError) as e:
            return ActionResult(
                success=False,
                message=f"科学计数法格式化失败: 无效的数字 - {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"科学计数法格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'output_var': 'format_result'}