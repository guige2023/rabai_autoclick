"""Tuple3 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleToListAction: Convert tuple to list
- TupleToSetAction: Convert tuple to set
- TupleConcatenateAction: Concatenate tuples
- TupleRepeatAction: Repeat tuple
- TupleMinAction: Get minimum element
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleToListAction(BaseAction):
    """Convert tuple to list."""
    action_type = "tuple3_to_list"
    display_name = "元组转列表"
    description = "将元组转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to list.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with list.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'list_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            result = list(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转列表: {len(result)} 个元素",
                data={
                    'tuple': t,
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
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_result'}


class TupleToSetAction(BaseAction):
    """Convert tuple to set."""
    action_type = "tuple3_to_set"
    display_name = "元组转集合"
    description = "将元组转换为集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to set.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with set.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'set_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list, set)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组、列表或集合"
                )

            result = set(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转集合: {len(result)} 个元素",
                data={
                    'tuple': t,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组转集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class TupleConcatenateAction(BaseAction):
    """Concatenate tuples."""
    action_type = "tuple3_concatenate"
    display_name = "连接元组"
    description = "连接两个元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute concatenate.

        Args:
            context: Execution context.
            params: Dict with tuple1, tuple2, output_var.

        Returns:
            ActionResult with concatenated tuple.
        """
        tuple1 = params.get('tuple1', '')
        tuple2 = params.get('tuple2', '')
        output_var = params.get('output_var', 'concatenated_tuple')

        valid, msg = self.validate_type(tuple1, str, 'tuple1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(tuple2, str, 'tuple2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved1 = context.resolve_value(tuple1)
            resolved2 = context.resolve_value(tuple2)

            t1 = context.get(resolved1) if isinstance(resolved1, str) else resolved1
            t2 = context.get(resolved2) if isinstance(resolved2, str) else resolved2

            if not isinstance(t1, (tuple, list)):
                t1 = (t1,)
            if not isinstance(t2, (tuple, list)):
                t2 = (t2,)

            result = tuple(t1) + tuple(t2)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接元组: {len(result)} 个元素",
                data={
                    'tuple1': t1,
                    'tuple2': t2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple1', 'tuple2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'concatenated_tuple'}


class TupleRepeatAction(BaseAction):
    """Repeat tuple."""
    action_type = "tuple3_repeat"
    display_name = "重复元组"
    description = "重复元组指定次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute repeat.

        Args:
            context: Execution context.
            params: Dict with tuple_var, count, output_var.

        Returns:
            ActionResult with repeated tuple.
        """
        tuple_var = params.get('tuple_var', '')
        count = params.get('count', 1)
        output_var = params.get('output_var', 'repeated_tuple')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_count = int(context.resolve_value(count))

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                t = (t,)

            result = tuple(t) * resolved_count
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"重复元组: {len(result)} 个元素",
                data={
                    'tuple': t,
                    'count': resolved_count,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"重复元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'count']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'repeated_tuple'}


class TupleMinAction(BaseAction):
    """Get minimum element."""
    action_type = "tuple3_min"
    display_name = "元组最小值"
    description = "获取元组最小元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with minimum.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'min_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            if len(t) == 0:
                return ActionResult(
                    success=False,
                    message="元组不能为空"
                )

            result = min(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组最小值: {result}",
                data={
                    'tuple': t,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取元组最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'min_result'}
