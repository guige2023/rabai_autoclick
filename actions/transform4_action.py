"""Transform4 action module for RabAI AutoClick.

Provides additional transform operations:
- TransformMapAction: Map function over items
- TransformFilterAction: Filter items
- TransformReduceAction: Reduce items to single value
- TransformGroupByAction: Group items by key
- TransformFlattenAction: Flatten nested structure
"""

from functools import reduce
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Map function over items."""
    action_type = "transform4_map"
    display_name = "映射转换"
    description = "对列表每个元素应用函数"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute map.

        Args:
            context: Execution context.
            params: Dict with items, func, output_var.

        Returns:
            ActionResult with mapped items.
        """
        items = params.get('items', [])
        func = params.get('func', 'lambda x: x')
        output_var = params.get('output_var', 'mapped_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"映射转换失败: 输入不是列表"
                )

            func_str = str(context.resolve_value(func))

            try:
                func_obj = eval(func_str)
                result = [func_obj(item) for item in resolved]
            except:
                result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"映射转换完成: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"映射转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'func': 'lambda x: x', 'output_var': 'mapped_result'}


class TransformFilterAction(BaseAction):
    """Filter items."""
    action_type = "transform4_filter"
    display_name = "过滤转换"
    description = "根据条件过滤元素"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute filter.

        Args:
            context: Execution context.
            params: Dict with items, predicate, output_var.

        Returns:
            ActionResult with filtered items.
        """
        items = params.get('items', [])
        predicate = params.get('predicate', 'lambda x: True')
        output_var = params.get('output_var', 'filtered_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"过滤转换失败: 输入不是列表"
                )

            pred_str = str(context.resolve_value(predicate))

            try:
                pred_obj = eval(pred_str)
                result = [item for item in resolved if pred_obj(item)]
            except:
                result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"过滤转换完成: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"过滤转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'predicate': 'lambda x: True', 'output_var': 'filtered_result'}


class TransformReduceAction(BaseAction):
    """Reduce items to single value."""
    action_type = "transform4_reduce"
    display_name = "聚合转换"
    description = "将列表元素聚合为单个值"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reduce.

        Args:
            context: Execution context.
            params: Dict with items, func, initial, output_var.

        Returns:
            ActionResult with reduced value.
        """
        items = params.get('items', [])
        func = params.get('func', 'lambda acc, x: acc + x')
        initial = params.get('initial', None)
        output_var = params.get('output_var', 'reduced_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"聚合转换失败: 输入不是列表"
                )

            func_str = str(context.resolve_value(func))

            try:
                func_obj = eval(func_str)
                if initial is not None:
                    resolved_initial = context.resolve_value(initial)
                    result = reduce(func_obj, resolved, resolved_initial)
                else:
                    result = reduce(func_obj, resolved)
            except:
                result = None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"聚合转换完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"聚合转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'func': 'lambda acc, x: acc + x', 'initial': None, 'output_var': 'reduced_result'}


class TransformGroupByAction(BaseAction):
    """Group items by key."""
    action_type = "transform4_groupby"
    display_name = "分组转换"
    description = "按键分组元素"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute group by.

        Args:
            context: Execution context.
            params: Dict with items, key_func, output_var.

        Returns:
            ActionResult with grouped items.
        """
        items = params.get('items', [])
        key_func = params.get('key_func', 'lambda x: x')
        output_var = params.get('output_var', 'grouped_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"分组转换失败: 输入不是列表"
                )

            key_str = str(context.resolve_value(key_func))

            try:
                key_obj = eval(key_str)
                grouped = {}
                for item in resolved:
                    key = key_obj(item)
                    if key not in grouped:
                        grouped[key] = []
                    grouped[key].append(item)
                result = grouped
            except:
                result = {}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分组转换完成: {len(result)} 组",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分组转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_func': 'lambda x: x', 'output_var': 'grouped_result'}


class TransformFlattenAction(BaseAction):
    """Flatten nested structure."""
    action_type = "transform4_flatten"
    display_name = "扁平化转换"
    description = "将嵌套结构展平"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with flattened list.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'flattened_result')

        try:
            resolved = context.resolve_value(items)

            def _flatten(obj):
                result = []
                if isinstance(obj, (list, tuple)):
                    for item in obj:
                        result.extend(_flatten(item))
                else:
                    result.append(obj)
                return result

            result = _flatten(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"扁平化转换完成: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"扁平化转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'flattened_result'}