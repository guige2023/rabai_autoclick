"""Aggregate action module for RabAI AutoClick.

Provides data aggregation operations:
- AggregateCountAction: Count items
- AggregateSumAction: Sum values
- AggregateAvgAction: Average values
- AggregateMinAction: Minimum value
- AggregateMaxAction: Maximum value
- AggregateFirstAction: Get first item
- AggregateLastAction: Get last item
- AggregatePluckAction: Pluck values from items
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateCountAction(BaseAction):
    """Count items."""
    action_type = "aggregate_count"
    display_name = "聚合计数"
    description = "统计元素数量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count aggregation.

        Args:
            context: Execution context.
            params: Dict with items, condition, output_var.

        Returns:
            ActionResult with count.
        """
        items = params.get('items', [])
        condition = params.get('condition', None)
        output_var = params.get('output_var', 'count_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)

            if condition is not None:
                resolved_cond = context.resolve_value(condition)
                count = 0
                for item in resolved_items:
                    context.set('_count_item', item)
                    try:
                        if context.safe_exec(f"return_value = {resolved_cond}"):
                            count += 1
                    except Exception:
                        pass
            else:
                count = len(resolved_items)

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"计数: {count}",
                data={
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': None, 'output_var': 'count_result'}


class AggregateSumAction(BaseAction):
    """Sum values."""
    action_type = "aggregate_sum"
    display_name = "聚合求和"
    description = "求和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum aggregation.

        Args:
            context: Execution context.
            params: Dict with items, expression, output_var.

        Returns:
            ActionResult with sum.
        """
        items = params.get('items', [])
        expression = params.get('expression', 'x')
        output_var = params.get('output_var', 'sum_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_expr = context.resolve_value(expression)

            total = 0
            for item in resolved_items:
                context.set('_sum_item', item)
                try:
                    val = context.safe_exec(f"return_value = {resolved_expr}")
                    total += float(val)
                except Exception:
                    pass

            context.set(output_var, total)

            return ActionResult(
                success=True,
                message=f"求和: {total}",
                data={
                    'sum': total,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expression': 'x', 'output_var': 'sum_result'}


class AggregateAvgAction(BaseAction):
    """Average values."""
    action_type = "aggregate_avg"
    display_name = "聚合平均值"
    description = "计算平均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average aggregation.

        Args:
            context: Execution context.
            params: Dict with items, expression, output_var.

        Returns:
            ActionResult with average.
        """
        items = params.get('items', [])
        expression = params.get('expression', 'x')
        output_var = params.get('output_var', 'avg_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_expr = context.resolve_value(expression)

            values = []
            for item in resolved_items:
                context.set('_avg_item', item)
                try:
                    val = context.safe_exec(f"return_value = {resolved_expr}")
                    values.append(float(val))
                except Exception:
                    pass

            if len(values) == 0:
                context.set(output_var, 0)
                return ActionResult(
                    success=True,
                    message="没有可计算的值",
                    data={'avg': 0, 'output_var': output_var}
                )

            avg = sum(values) / len(values)
            context.set(output_var, avg)

            return ActionResult(
                success=True,
                message=f"平均值: {avg}",
                data={
                    'avg': avg,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expression': 'x', 'output_var': 'avg_result'}


class AggregateMinAction(BaseAction):
    """Minimum value."""
    action_type = "aggregate_min"
    display_name = "聚合最小值"
    description = "获取最小值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min aggregation.

        Args:
            context: Execution context.
            params: Dict with items, expression, output_var.

        Returns:
            ActionResult with minimum.
        """
        items = params.get('items', [])
        expression = params.get('expression', 'x')
        output_var = params.get('output_var', 'min_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_expr = context.resolve_value(expression)

            values = []
            for item in resolved_items:
                context.set('_min_item', item)
                try:
                    val = context.safe_exec(f"return_value = {resolved_expr}")
                    values.append((float(val), item))
                except Exception:
                    pass

            if len(values) == 0:
                context.set(output_var, None)
                return ActionResult(
                    success=True,
                    message="没有可计算的值",
                    data={'min': None, 'output_var': output_var}
                )

            min_val, min_item = min(values, key=lambda x: x[0])
            context.set(output_var, min_val)

            return ActionResult(
                success=True,
                message=f"最小值: {min_val}",
                data={
                    'min': min_val,
                    'item': min_item,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expression': 'x', 'output_var': 'min_result'}


class AggregateMaxAction(BaseAction):
    """Maximum value."""
    action_type = "aggregate_max"
    display_name = "聚合最大值"
    description = "获取最大值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max aggregation.

        Args:
            context: Execution context.
            params: Dict with items, expression, output_var.

        Returns:
            ActionResult with maximum.
        """
        items = params.get('items', [])
        expression = params.get('expression', 'x')
        output_var = params.get('output_var', 'max_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_expr = context.resolve_value(expression)

            values = []
            for item in resolved_items:
                context.set('_max_item', item)
                try:
                    val = context.safe_exec(f"return_value = {resolved_expr}")
                    values.append((float(val), item))
                except Exception:
                    pass

            if len(values) == 0:
                context.set(output_var, None)
                return ActionResult(
                    success=True,
                    message="没有可计算的值",
                    data={'max': None, 'output_var': output_var}
                )

            max_val, max_item = max(values, key=lambda x: x[0])
            context.set(output_var, max_val)

            return ActionResult(
                success=True,
                message=f"最大值: {max_val}",
                data={
                    'max': max_val,
                    'item': max_item,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'expression': 'x', 'output_var': 'max_result'}


class AggregateFirstAction(BaseAction):
    """Get first item."""
    action_type = "aggregate_first"
    display_name = "聚合第一个"
    description = "获取第一个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute first aggregation.

        Args:
            context: Execution context.
            params: Dict with items, condition, output_var.

        Returns:
            ActionResult with first item.
        """
        items = params.get('items', [])
        condition = params.get('condition', None)
        output_var = params.get('output_var', 'first_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)

            if condition is not None:
                resolved_cond = context.resolve_value(condition)
                for item in resolved_items:
                    context.set('_first_item', item)
                    try:
                        if context.safe_exec(f"return_value = {resolved_cond}"):
                            context.set(output_var, item)
                            return ActionResult(
                                success=True,
                                message=f"第一个: {item}",
                                data={'first': item, 'output_var': output_var}
                            )
                    except Exception:
                        pass

                context.set(output_var, None)
                return ActionResult(
                    success=True,
                    message="没有找到匹配的项",
                    data={'first': None, 'output_var': output_var}
                )
            else:
                if len(resolved_items) > 0:
                    result = resolved_items[0]
                    context.set(output_var, result)
                    return ActionResult(
                        success=True,
                        message=f"第一个: {result}",
                        data={'first': result, 'output_var': output_var}
                    )
                else:
                    context.set(output_var, None)
                    return ActionResult(
                        success=True,
                        message="列表为空",
                        data={'first': None, 'output_var': output_var}
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合第一个失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': None, 'output_var': 'first_result'}


class AggregateLastAction(BaseAction):
    """Get last item."""
    action_type = "aggregate_last"
    display_name = "聚合最后一个"
    description = "获取最后一个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute last aggregation.

        Args:
            context: Execution context.
            params: Dict with items, condition, output_var.

        Returns:
            ActionResult with last item.
        """
        items = params.get('items', [])
        condition = params.get('condition', None)
        output_var = params.get('output_var', 'last_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)

            if condition is not None:
                resolved_cond = context.resolve_value(condition)
                last_match = None
                for item in resolved_items:
                    context.set('_last_item', item)
                    try:
                        if context.safe_exec(f"return_value = {resolved_cond}"):
                            last_match = item
                    except Exception:
                        pass

                if last_match is not None:
                    context.set(output_var, last_match)
                    return ActionResult(
                        success=True,
                        message=f"最后一个: {last_match}",
                        data={'last': last_match, 'output_var': output_var}
                    )
                else:
                    context.set(output_var, None)
                    return ActionResult(
                        success=True,
                        message="没有找到匹配的项",
                        data={'last': None, 'output_var': output_var}
                    )
            else:
                if len(resolved_items) > 0:
                    result = resolved_items[-1]
                    context.set(output_var, result)
                    return ActionResult(
                        success=True,
                        message=f"最后一个: {result}",
                        data={'last': result, 'output_var': output_var}
                    )
                else:
                    context.set(output_var, None)
                    return ActionResult(
                        success=True,
                        message="列表为空",
                        data={'last': None, 'output_var': output_var}
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合最后一个失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'condition': None, 'output_var': 'last_result'}


class AggregatePluckAction(BaseAction):
    """Pluck values from items."""
    action_type = "aggregate_pluck"
    display_name = "聚合提取"
    description = "从每个元素中提取值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pluck aggregation.

        Args:
            context: Execution context.
            params: Dict with items, key, output_var.

        Returns:
            ActionResult with plucked values.
        """
        items = params.get('items', [])
        key = params.get('key', '')
        output_var = params.get('output_var', 'pluck_result')

        valid, msg = self.validate_type(items, (list, tuple), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_items = context.resolve_value(items)
            resolved_key = context.resolve_value(key)

            result = []
            for item in resolved_items:
                context.set('_pluck_item', item)
                try:
                    if isinstance(item, dict):
                        val = item.get(resolved_key)
                    else:
                        val = context.safe_exec(f"return_value = {resolved_key}")
                    result.append(val)
                except Exception:
                    result.append(None)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"提取完成: {len(result)} 项",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合提取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'pluck_result'}