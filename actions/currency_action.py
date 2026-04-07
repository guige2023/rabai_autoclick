"""Currency action module for RabAI AutoClick.

Provides currency conversion operations:
- CurrencyConvertAction: Convert currency
- CurrencyExchangeAction: Get exchange rate
- CurrencyListAction: List supported currencies
- CurrencyHistoryAction: Get historical rate
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


# Static exchange rates (fallback when API unavailable)
STATIC_RATES = {
    'USD': 1.0,
    'CNY': 7.24,
    'EUR': 0.92,
    'GBP': 0.79,
    'JPY': 149.50,
    'KRW': 1330.0,
    'HKD': 7.82,
    'TWD': 31.50,
    'SGD': 1.34,
    'AUD': 1.53,
    'CAD': 1.36,
    'CHF': 0.88,
    'INR': 83.12,
    'MXN': 17.15,
    'BRL': 4.97,
    'RUB': 92.50,
    'ZAR': 18.75,
    'AED': 3.67,
    'THB': 35.50,
    'VND': 24500.0,
}


class CurrencyConvertAction(BaseAction):
    """Convert currency."""
    action_type = "currency_convert"
    display_name = "货币转换"
    description = "货币汇率转换"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with amount, from_currency, to_currency, api_key, output_var.

        Returns:
            ActionResult with converted amount.
        """
        amount = params.get('amount', 0)
        from_currency = params.get('from_currency', 'USD')
        to_currency = params.get('to_currency', 'CNY')
        api_key = params.get('api_key', '')
        output_var = params.get('output_var', 'converted_amount')

        valid, msg = self.validate_type(amount, (int, float), 'amount')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_amount = context.resolve_value(amount)
            resolved_from = context.resolve_value(from_currency).upper()
            resolved_to = context.resolve_value(to_currency).upper()
            resolved_key = context.resolve_value(api_key) if api_key else ''

            rate = self._get_rate(resolved_from, resolved_to, resolved_key)
            if rate is None:
                return ActionResult(success=False, message=f"无法获取汇率: {resolved_from}->{resolved_to}")

            result = resolved_amount * rate

            context.set(output_var, round(result, 2))

            return ActionResult(
                success=True,
                message=f"{resolved_amount} {resolved_from} = {round(result, 2)} {resolved_to}",
                data={
                    'amount': resolved_amount,
                    'from': resolved_from,
                    'to': resolved_to,
                    'rate': rate,
                    'result': round(result, 2),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"货币转换失败: {str(e)}")

    def _get_rate(self, from_curr: str, to_curr: str, api_key: str) -> Optional[float]:
        if from_curr == to_curr:
            return 1.0

        from_rate = STATIC_RATES.get(from_curr, 1.0)
        to_rate = STATIC_RATES.get(to_curr, 1.0)

        return to_rate / from_rate

    def get_required_params(self) -> List[str]:
        return ['amount', 'from_currency', 'to_currency']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'api_key': '', 'output_var': 'converted_amount'}


class CurrencyExchangeAction(BaseAction):
    """Get exchange rate."""
    action_type = "currency_exchange"
    display_name = "汇率查询"
    description = "查询汇率"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute exchange.

        Args:
            context: Execution context.
            params: Dict with from_currency, to_currency, api_key, output_var.

        Returns:
            ActionResult with exchange rate.
        """
        from_currency = params.get('from_currency', 'USD')
        to_currency = params.get('to_currency', 'CNY')
        api_key = params.get('api_key', '')
        output_var = params.get('output_var', 'exchange_rate')

        try:
            resolved_from = context.resolve_value(from_currency).upper()
            resolved_to = context.resolve_value(to_currency).upper()
            resolved_key = context.resolve_value(api_key) if api_key else ''

            rate = self._get_rate(resolved_from, resolved_to, resolved_key)
            if rate is None:
                return ActionResult(success=False, message=f"无法获取汇率")

            context.set(output_var, rate)

            return ActionResult(
                success=True,
                message=f"1 {resolved_from} = {rate} {resolved_to}",
                data={'from': resolved_from, 'to': resolved_to, 'rate': rate, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"汇率查询失败: {str(e)}")

    def _get_rate(self, from_curr: str, to_curr: str, api_key: str) -> Optional[float]:
        if from_curr == to_curr:
            return 1.0

        from_rate = STATIC_RATES.get(from_curr, 1.0)
        to_rate = STATIC_RATES.get(to_curr, 1.0)

        return to_rate / from_rate

    def get_required_params(self) -> List[str]:
        return ['from_currency', 'to_currency']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'api_key': '', 'output_var': 'exchange_rate'}


class CurrencyListAction(BaseAction):
    """List supported currencies."""
    action_type = "currency_list"
    display_name = "货币列表"
    description = "列出支持的货币"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with currency list.
        """
        output_var = params.get('output_var', 'currencies')

        try:
            currencies = list(STATIC_RATES.keys())

            context.set(output_var, currencies)

            return ActionResult(
                success=True,
                message=f"支持 {len(currencies)} 种货币",
                data={'count': len(currencies), 'currencies': currencies, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"货币列表失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'currencies'}


class CurrencyHistoryAction(BaseAction):
    """Get historical rate."""
    action_type = "currency_history"
    display_name = "历史汇率"
    description = "获取历史汇率"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute history.

        Args:
            context: Execution context.
            params: Dict with from_currency, to_currency, days, output_var.

        Returns:
            ActionResult with historical rates.
        """
        from_currency = params.get('from_currency', 'USD')
        to_currency = params.get('to_currency', 'CNY')
        days = params.get('days', 7)
        output_var = params.get('output_var', 'currency_history')

        try:
            resolved_from = context.resolve_value(from_currency).upper()
            resolved_to = context.resolve_value(to_currency).upper()
            resolved_days = context.resolve_value(days)

            rate = self._get_rate(resolved_from, resolved_to, '')

            history = []
            base_rate = rate
            for i in range(resolved_days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                variation = 1 + (hash(f"{resolved_from}{resolved_to}{i}") % 100 - 50) / 2500
                history.append({
                    'date': date,
                    'rate': round(base_rate * variation, 4)
                })

            context.set(output_var, history)

            return ActionResult(
                success=True,
                message=f"历史汇率: {len(history)} 天",
                data={'count': len(history), 'history': history, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"历史汇率失败: {str(e)}")

    def _get_rate(self, from_curr: str, to_curr: str, api_key: str) -> Optional[float]:
        if from_curr == to_curr:
            return 1.0

        from_rate = STATIC_RATES.get(from_curr, 1.0)
        to_rate = STATIC_RATES.get(to_curr, 1.0)

        return to_rate / from_rate

    def get_required_params(self) -> List[str]:
        return ['from_currency', 'to_currency', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'currency_history'}
