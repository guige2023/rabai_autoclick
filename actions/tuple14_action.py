"""Tuple14 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleCreateAction: Create tuple
- TupleIndexAction: Get index of value
- TupleCountAction: Count occurrences
- TupleSliceAction: Slice tuple
- TupleConcatAction: Concatenate tuples
- TupleUnpackAction: Unpack tuple
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleCreateAction(BaseAction):
    """Create tuple."""
    action_type = "tuple14_create"
    display_name = "创建元组"
    description = "创建元组"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, values, output_var.

        Returns:
            ActionResult with create result.
        """
        name = params.get('name', 'tuple')
        values = params.get('values', [])
        output_var = params.get('output_var', 'create_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple'
            resolved_values = context.resolve_value(values) if values else []

            if not isinstance(resolved_values, (list, tuple)):
                resolved_values = [resolved_values]

            result = tuple(resolved_values)

            if not hasattr(context, '_tuples'):
                context._tuples = {}
            context._tuples[resolved_name] = result

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"创建元组: {resolved_name} = {result}",
                data={
                    'name': resolved_name,
                    'tuple': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'create_result'}


class TupleIndexAction(BaseAction):
    """Get index of value."""
    action_type = "tuple14_index"
    display_name = "元组索引"
    description = "获取元组值索引"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with index result.
        """
        name = params.get('name', 'tuple')
        value = params.get('value', None)
        output_var = params.get('output_var', 'index_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_tuples') or resolved_name not in context._tuples:
                return ActionResult(
                    success=False,
                    message=f"元组不存在: {resolved_name}"
                )

            t = context._tuples[resolved_name]
            index = t.index(resolved_value)

            context.set(output_var, index)

            return ActionResult(
                success=True,
                message=f"元组索引: {resolved_name}[{index}] = {resolved_value}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'index': index,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"值不在元组中: {resolved_value}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class TupleCountAction(BaseAction):
    """Count occurrences."""
    action_type = "tuple14_count"
    display_name: "元组计数"
    description = "计算元组值出现次数"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with count result.
        """
        name = params.get('name', 'tuple')
        value = params.get('value', None)
        output_var = params.get('output_var', 'count_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple'
            resolved_value = context.resolve_value(value) if value else None

            if not hasattr(context, '_tuples') or resolved_name not in context._tuples:
                return ActionResult(
                    success=False,
                    message=f"元组不存在: {resolved_name}"
                )

            t = context._tuples[resolved_name]
            count = t.count(resolved_value)

            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"元组计数: {resolved_name}.count({resolved_value}) = {count}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class TupleSliceAction(BaseAction):
    """Slice tuple."""
    action_type = "tuple14_slice"
    display_name = "元组切片"
    description = "元组切片"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with name, start, end, step, output_var.

        Returns:
            ActionResult with slice result.
        """
        name = params.get('name', 'tuple')
        start = params.get('start', 0)
        end = params.get('end', None)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'slice_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple'
            resolved_start = int(context.resolve_value(start)) if start else 0
            resolved_end = int(context.resolve_value(end)) if end else None
            resolved_step = int(context.resolve_value(step)) if step else 1

            if not hasattr(context, '_tuples') or resolved_name not in context._tuples:
                return ActionResult(
                    success=False,
                    message=f"元组不存在: {resolved_name}"
                )

            t = context._tuples[resolved_name]
            result = t[resolved_start:resolved_end:resolved_step]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组切片: {resolved_name}[{resolved_start}:{resolved_end}:{resolved_step}]",
                data={
                    'name': resolved_name,
                    'slice': result,
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组切片失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'step': 1, 'output_var': 'slice_result'}


class TupleConcatAction(BaseAction):
    """Concatenate tuples."""
    action_type = "tuple14_concat"
    display_name = "元组连接"
    description = "连接元组"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute concat.

        Args:
            context: Execution context.
            params: Dict with name, other_name, output_var.

        Returns:
            ActionResult with concat result.
        """
        name = params.get('name', 'tuple1')
        other_name = params.get('other_name', 'tuple2')
        output_var = params.get('output_var', 'concat_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple1'
            resolved_other = context.resolve_value(other_name) if other_name else 'tuple2'

            if not hasattr(context, '_tuples'):
                context._tuples = {}

            t1 = context._tuples.get(resolved_name, ())
            t2 = context._tuples.get(resolved_other, ())

            result = t1 + t2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组连接: {resolved_name} + {resolved_other} = {len(result)}项",
                data={
                    'tuple1': resolved_name,
                    'tuple2': resolved_other,
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'other_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'concat_result'}


class TupleUnpackAction(BaseAction):
    """Unpack tuple."""
    action_type = "tuple14_unpack"
    display_name = "元组解包"
    description = "解包元组"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute unpack.

        Args:
            context: Execution context.
            params: Dict with name, var_names, output_var.

        Returns:
            ActionResult with unpack result.
        """
        name = params.get('name', 'tuple')
        var_names = params.get('var_names', [])
        output_var = params.get('output_var', 'unpack_result')

        try:
            resolved_name = context.resolve_value(name) if name else 'tuple'
            resolved_vars = context.resolve_value(var_names) if var_names else []

            if not hasattr(context, '_tuples') or resolved_name not in context._tuples:
                return ActionResult(
                    success=False,
                    message=f"元组不存在: {resolved_name}"
                )

            t = context._tuples[resolved_name]

            if not isinstance(resolved_vars, (list, tuple)):
                resolved_vars = [resolved_vars]

            if len(resolved_vars) != len(t):
                return ActionResult(
                    success=False,
                    message=f"变量数量不匹配: {len(resolved_vars)} != {len(t)}"
                )

            result = dict(zip(resolved_vars, t))

            for var, val in result.items():
                context.set(var, val)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组解包: {resolved_name} -> {result}",
                data={
                    'name': resolved_name,
                    'unpacked': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组解包失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'var_names']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'unpack_result'}