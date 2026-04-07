"""Sort action module for RabAI AutoClick.

Provides sorting operations:
- SortNumbersAction: Sort numbers
- SortStringsAction: Sort strings
- SortDictByKeyAction: Sort dict by key
- SortDictByValueAction: Sort dict by value
- SortReverseAction: Reverse order
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortNumbersAction(BaseAction):
    """Sort numbers."""
    action_type = "sort_numbers"
    display_name = "数字排序"
    description = "对数字排序"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with numbers, order, output_var.

        Returns:
            ActionResult with sorted numbers.
        """
        numbers = params.get('numbers', [])
        order = params.get('order', 'asc')
        output_var = params.get('output_var', 'sorted_numbers')

        valid, msg = self.validate_type(numbers, list, 'numbers')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_nums = context.resolve_value(numbers)
            resolved_order = context.resolve_value(order)

            reverse = resolved_order == 'desc'
            sorted_nums = sorted(resolved_nums, key=lambda x: float(x) if isinstance(x, (int, float, str)) else 0, reverse=reverse)

            context.set(output_var, sorted_nums)

            return ActionResult(
                success=True,
                message=f"数字排序完成: {len(sorted_nums)} 个",
                data={'sorted': sorted_nums, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"数字排序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['numbers', 'order']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_numbers'}


class SortStringsAction(BaseAction):
    """Sort strings."""
    action_type = "sort_strings"
    display_name = "字符串排序"
    description = "对字符串排序"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with strings, order, case_sensitive, output_var.

        Returns:
            ActionResult with sorted strings.
        """
        strings = params.get('strings', [])
        order = params.get('order', 'asc')
        case_sensitive = params.get('case_sensitive', False)
        output_var = params.get('output_var', 'sorted_strings')

        valid, msg = self.validate_type(strings, list, 'strings')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_strs = context.resolve_value(strings)
            resolved_order = context.resolve_value(order)
            resolved_case = context.resolve_value(case_sensitive)

            reverse = resolved_order == 'desc'

            if resolved_case:
                sorted_strs = sorted(resolved_strs, key=lambda x: str(x), reverse=reverse)
            else:
                sorted_strs = sorted(resolved_strs, key=lambda x: str(x).lower(), reverse=reverse)

            context.set(output_var, sorted_strs)

            return ActionResult(
                success=True,
                message=f"字符串排序完成: {len(sorted_strs)} 个",
                data={'sorted': sorted_strs, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串排序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['strings', 'order']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'case_sensitive': False, 'output_var': 'sorted_strings'}


class SortDictByKeyAction(BaseAction):
    """Sort dict by key."""
    action_type = "sort_dict_by_key"
    display_name = "字典按键排序"
    description = "按键排序字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with dictionary, order, output_var.

        Returns:
            ActionResult with sorted dict.
        """
        dictionary = params.get('dictionary', {})
        order = params.get('order', 'asc')
        output_var = params.get('output_var', 'sorted_dict')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_order = context.resolve_value(order)

            reverse = resolved_order == 'desc'
            sorted_items = sorted(resolved_dict.items(), key=lambda x: str(x[0]).lower(), reverse=reverse)
            sorted_dict = dict(sorted_items)

            context.set(output_var, sorted_dict)

            return ActionResult(
                success=True,
                message=f"字典按键排序完成: {len(sorted_dict)} 个键",
                data={'sorted': sorted_dict, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典按键排序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'order']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_dict'}


class SortDictByValueAction(BaseAction):
    """Sort dict by value."""
    action_type = "sort_dict_by_value"
    display_name = "字典按值排序"
    description = "按值排序字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort.

        Args:
            context: Execution context.
            params: Dict with dictionary, order, output_var.

        Returns:
            ActionResult with sorted dict.
        """
        dictionary = params.get('dictionary', {})
        order = params.get('order', 'asc')
        output_var = params.get('output_var', 'sorted_dict')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_order = context.resolve_value(order)

            reverse = resolved_order == 'desc'

            def sort_key(x):
                v = x[1]
                if isinstance(v, (int, float)):
                    return v
                return str(v).lower()

            sorted_items = sorted(resolved_dict.items(), key=sort_key, reverse=reverse)
            sorted_dict = dict(sorted_items)

            context.set(output_var, sorted_dict)

            return ActionResult(
                success=True,
                message=f"字典按值排序完成: {len(sorted_dict)} 个键",
                data={'sorted': sorted_dict, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典按值排序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'order']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sorted_dict'}


class SortReverseAction(BaseAction):
    """Reverse order."""
    action_type = "sort_reverse"
    display_name = "反转顺序"
    description = "反转列表顺序"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with reversed list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'reversed_items')

        valid, msg = self.validate_type(items, list, 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            reversed_items = list(reversed(resolved_items))

            context.set(output_var, reversed_items)

            return ActionResult(
                success=True,
                message=f"顺序反转完成: {len(reversed_items)} 个",
                data={'reversed': reversed_items, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"反转顺序失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'reversed_items'}
