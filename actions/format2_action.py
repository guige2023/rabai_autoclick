"""Format2 action module for RabAI AutoClick.

Provides additional formatting operations:
- FormatDateAction: Format date
- FormatTimeAction: Format time
- FormatDatetimeAction: Format datetime
- FormatNumberAction: Format number
- FormatCurrencyAction: Format currency
- FormatPercentageAction: Format percentage
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FormatDateAction(BaseAction):
    """Format date."""
    action_type = "format_date"
    display_name = "格式化日期"
    description = "格式化日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date format.

        Args:
            context: Execution context.
            params: Dict with date_value, format_str, output_var.

        Returns:
            ActionResult with formatted date.
        """
        date_value = params.get('date_value', None)
        format_str = params.get('format_str', '%Y-%m-%d')
        output_var = params.get('output_var', 'formatted_date')

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_format = context.resolve_value(format_str)

            if date_value is None:
                dt = datetime.now()
            else:
                resolved_date = context.resolve_value(date_value)
                if isinstance(resolved_date, str):
                    dt = datetime.fromisoformat(resolved_date)
                elif isinstance(resolved_date, datetime):
                    dt = resolved_date
                else:
                    dt = datetime.fromtimestamp(resolved_date)

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期格式化: {result}",
                data={
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'date_value': None, 'format_str': '%Y-%m-%d', 'output_var': 'formatted_date'}


class FormatTimeAction(BaseAction):
    """Format time."""
    action_type = "format_time"
    display_name = "格式化时间"
    description = "格式化时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute time format.

        Args:
            context: Execution context.
            params: Dict with time_value, format_str, output_var.

        Returns:
            ActionResult with formatted time.
        """
        time_value = params.get('time_value', None)
        format_str = params.get('format_str', '%H:%M:%S')
        output_var = params.get('output_var', 'formatted_time')

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_format = context.resolve_value(format_str)

            if time_value is None:
                dt = datetime.now()
            else:
                resolved_time = context.resolve_value(time_value)
                if isinstance(resolved_time, str):
                    dt = datetime.fromisoformat(resolved_time)
                elif isinstance(resolved_time, datetime):
                    dt = resolved_time
                else:
                    dt = datetime.fromtimestamp(resolved_time)

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间格式化: {result}",
                data={
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'time_value': None, 'format_str': '%H:%M:%S', 'output_var': 'formatted_time'}


class FormatDatetimeAction(BaseAction):
    """Format datetime."""
    action_type = "format_datetime"
    display_name = "格式化日期时间"
    description = "格式化日期时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime format.

        Args:
            context: Execution context.
            params: Dict with datetime_value, format_str, output_var.

        Returns:
            ActionResult with formatted datetime.
        """
        datetime_value = params.get('datetime_value', None)
        format_str = params.get('format_str', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_datetime')

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_format = context.resolve_value(format_str)

            if datetime_value is None:
                dt = datetime.now()
            else:
                resolved_dt = context.resolve_value(datetime_value)
                if isinstance(resolved_dt, str):
                    dt = datetime.fromisoformat(resolved_dt)
                elif isinstance(resolved_dt, datetime):
                    dt = resolved_dt
                else:
                    dt = datetime.fromtimestamp(resolved_dt)

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期时间格式化: {result}",
                data={
                    'formatted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日期时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_value': None, 'format_str': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_datetime'}


class FormatNumberAction(BaseAction):
    """Format number."""
    action_type = "format_number"
    display_name = "格式化数字"
    description = "格式化数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute number format.

        Args:
            context: Execution context.
            params: Dict with value, decimals, separator, output_var.

        Returns:
            ActionResult with formatted number.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        separator = params.get('separator', ',')
        output_var = params.get('output_var', 'formatted_number')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_decimals = int(context.resolve_value(decimals))
            resolved_sep = context.resolve_value(separator)

            formatted = f"{resolved_value:,.{resolved_decimals}f}"
            if resolved_sep != ',':
                formatted = formatted.replace(',', resolved_sep)

            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"数字格式化: {formatted}",
                data={
                    'original': resolved_value,
                    'formatted': formatted,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'separator': ',', 'output_var': 'formatted_number'}


class FormatCurrencyAction(BaseAction):
    """Format currency."""
    action_type = "format_currency"
    display_name = "格式化货币"
    description = "格式化货币"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute currency format.

        Args:
            context: Execution context.
            params: Dict with value, currency, symbol, output_var.

        Returns:
            ActionResult with formatted currency.
        """
        value = params.get('value', 0)
        currency = params.get('currency', 'USD')
        symbol = params.get('symbol', '$')
        output_var = params.get('output_var', 'formatted_currency')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_currency = context.resolve_value(currency)
            resolved_symbol = context.resolve_value(symbol)

            formatted = f"{resolved_symbol}{resolved_value:,.2f}"

            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"货币格式化: {formatted}",
                data={
                    'original': resolved_value,
                    'currency': resolved_currency,
                    'formatted': formatted,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化货币失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'currency': 'USD', 'symbol': '$', 'output_var': 'formatted_currency'}


class FormatPercentageAction(BaseAction):
    """Format percentage."""
    action_type = "format_percentage"
    display_name = "格式化百分比"
    description = "格式化百分比"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute percentage format.

        Args:
            context: Execution context.
            params: Dict with value, decimals, output_var.

        Returns:
            ActionResult with formatted percentage.
        """
        value = params.get('value', 0)
        decimals = params.get('decimals', 2)
        output_var = params.get('output_var', 'formatted_percentage')

        try:
            resolved_value = float(context.resolve_value(value))
            resolved_decimals = int(context.resolve_value(decimals))

            formatted = f"{resolved_value * 100:.{resolved_decimals}f}%"

            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"百分比格式化: {formatted}",
                data={
                    'original': resolved_value,
                    'formatted': formatted,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化百分比失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'decimals': 2, 'output_var': 'formatted_percentage'}
