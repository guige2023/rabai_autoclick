"""Tuple5 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleToListAction: Convert tuple to list
- TupleFromListAction: Convert list to tuple
- TupleIndexAction: Find index of element
- TupleCountAction: Count element occurrences
- TupleConcatAction: Concatenate tuples
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleToListAction(BaseAction):
    """Convert tuple to list."""
    action_type = "tuple5_to_list"
    display_name = "元组转列表"
    description = "将元组转换为列表"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple to list.

        Args:
            context: Execution context.
            params: Dict with tuple, output_var.

        Returns:
            ActionResult with list.
        """
        tuple_param = params.get('tuple', ())
        output_var = params.get('output_var', 'list_result')

        try:
            resolved = context.resolve_value(tuple_param)

            if isinstance(resolved, tuple):
                result = list(resolved)
            elif isinstance(resolved, list):
                result = resolved
            else:
                return ActionResult(
                    success=False,
                    message=f"元组转列表失败: 输入不是元组或列表"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转列表完成: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_result'}


class TupleFromListAction(BaseAction):
    """Convert list to tuple."""
    action_type = "tuple5_from_list"
    display_name = "列表转元组"
    description = "将列表转换为元组"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list to tuple.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with tuple.
        """
        list_param = params.get('list', [])
        output_var = params.get('output_var', 'tuple_result')

        try:
            resolved = context.resolve_value(list_param)

            if isinstance(resolved, (list, tuple)):
                result = tuple(resolved)
            else:
                return ActionResult(
                    success=False,
                    message=f"列表转元组失败: 输入不是列表或元组"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转元组完成: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_result'}


class TupleIndexAction(BaseAction):
    """Find index of element."""
    action_type = "tuple5_index"
    display_name = "元组索引"
    description = "查找元素在元组中的索引"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple index.

        Args:
            context: Execution context.
            params: Dict with tuple, item, output_var.

        Returns:
            ActionResult with index.
        """
        tuple_param = params.get('tuple', ())
        item = params.get('item', None)
        output_var = params.get('output_var', 'index_result')

        try:
            resolved_tuple = context.resolve_value(tuple_param)
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_tuple, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"元组索引失败: 输入不是元组或列表"
                )

            result = list(resolved_tuple).index(resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组索引: 位置 {result}",
                data={
                    'tuple': resolved_tuple,
                    'item': resolved_item,
                    'index': result,
                    'output_var': output_var
                }
            )
        except ValueError:
            return ActionResult(
                success=False,
                message=f"元组索引失败: 元素不存在"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class TupleCountAction(BaseAction):
    """Count element occurrences."""
    action_type = "tuple5_count"
    display_name = "元组计数"
    description = "计算元素在元组中的出现次数"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple count.

        Args:
            context: Execution context.
            params: Dict with tuple, item, output_var.

        Returns:
            ActionResult with count.
        """
        tuple_param = params.get('tuple', ())
        item = params.get('item', None)
        output_var = params.get('output_var', 'count_result')

        try:
            resolved_tuple = context.resolve_value(tuple_param)
            resolved_item = context.resolve_value(item)

            if not isinstance(resolved_tuple, (list, tuple)):
                return ActionResult(
                    success=False,
                    message=f"元组计数失败: 输入不是元组或列表"
                )

            result = list(resolved_tuple).count(resolved_item)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组计数: {result} 次",
                data={
                    'tuple': resolved_tuple,
                    'item': resolved_item,
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class TupleConcatAction(BaseAction):
    """Concatenate tuples."""
    action_type = "tuple5_concat"
    display_name = "元组拼接"
    description = "拼接多个元组"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple concat.

        Args:
            context: Execution context.
            params: Dict with tuples, output_var.

        Returns:
            ActionResult with concatenated tuple.
        """
        tuples = params.get('tuples', [])
        output_var = params.get('output_var', 'concat_result')

        try:
            resolved = context.resolve_value(tuples)

            if isinstance(resolved, (list, tuple)) and len(resolved) > 0:
                if isinstance(resolved[0], (list, tuple)):
                    result = ()
                    for t in resolved:
                        result += tuple(t)
                else:
                    result = tuple(resolved)
            else:
                return ActionResult(
                    success=False,
                    message=f"元组拼接失败: 输入格式错误"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组拼接完成: {len(result)} 项",
                data={
                    'tuples': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组拼接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuples']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'concat_result'}