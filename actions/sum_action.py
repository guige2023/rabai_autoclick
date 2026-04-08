"""Sum action module for RabAI AutoClick.

Provides list sum operations with
field extraction and filtering support.
"""

import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ListSumAction(BaseAction):
    """Sum list of numbers or dict fields.
    
    Supports field extraction and null handling.
    """
    action_type = "list_sum"
    display_name = "列表求和"
    description = "对列表数值求和"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sum list.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, field, skip_nulls,
                   precision, save_to_var.
        
        Returns:
            ActionResult with sum.
        """
        items = params.get('items', [])
        field = params.get('field', None)
        skip_nulls = params.get('skip_nulls', True)
        precision = params.get('precision', 2)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        total = 0
        count = 0
        nulls_skipped = 0

        for item in items:
            if field:
                if isinstance(item, dict):
                    value = item.get(field)
                else:
                    value = getattr(item, field, None)
            else:
                value = item

            if value is None:
                if skip_nulls:
                    nulls_skipped += 1
                    continue
                return ActionResult(
                    success=False,
                    message=f"Null value encountered at index {count}"
                )

            try:
                total += float(value)
                count += 1
            except (ValueError, TypeError) as e:
                return ActionResult(
                    success=False,
                    message=f"Cannot convert value to number: {value}"
                )

        result = round(total, precision)

        result_data = {
            'sum': result,
            'count': count,
            'nulls_skipped': nulls_skipped,
            'field': field
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"求和完成: {result}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'skip_nulls': True,
            'precision': 2,
            'save_to_var': None
        }


class ListProductAction(BaseAction):
    """Calculate product of list values.
    
    Multiplies all values together.
    """
    action_type = "list_product"
    display_name = "列表乘积"
    description = "计算列表数值乘积"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate product.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, field, precision,
                   save_to_var.
        
        Returns:
            ActionResult with product.
        """
        items = params.get('items', [])
        field = params.get('field', None)
        precision = params.get('precision', 2)
        save_to_var = params.get('save_to_var', None)

        if not items:
            return ActionResult(success=False, message="Items list is empty")

        product = 1
        count = 0

        for item in items:
            if field:
                if isinstance(item, dict):
                    value = item.get(field, 0)
                else:
                    value = getattr(item, field, 0)
            else:
                value = item

            try:
                product *= float(value)
                count += 1
            except (ValueError, TypeError):
                return ActionResult(
                    success=False,
                    message=f"Cannot convert value to number: {value}"
                )

        result = round(product, precision)

        result_data = {
            'product': result,
            'count': count,
            'field': field
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"乘积计算完成: {result}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'precision': 2,
            'save_to_var': None
        }
