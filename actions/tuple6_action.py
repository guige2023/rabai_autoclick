"""Tuple6 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleToListAction: Convert tuple to list
- TupleToSetAction: Convert tuple to set
- TupleIndexAction: Find element index
- TupleCountAction: Count element occurrences
- TupleSliceAction: Slice tuple
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleToListAction(BaseAction):
    """Convert tuple to list."""
    action_type = "tuple6_to_list"
    display_name = "元组转列表"
    description = "将元组转换为列表"
    version = "6.0"

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
        input_tuple = params.get('tuple', ())
        output_var = params.get('output_var', 'list_result')

        try:
            resolved = context.resolve_value(input_tuple)

            if not isinstance(resolved, (list, tuple)):
                resolved = (resolved,)

            result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转列表: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
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


class TupleToSetAction(BaseAction):
    """Convert tuple to set."""
    action_type = "tuple6_to_set"
    display_name = "元组转集合"
    description = "将元组转换为集合"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple to set.

        Args:
            context: Execution context.
            params: Dict with tuple, output_var.

        Returns:
            ActionResult with set.
        """
        input_tuple = params.get('tuple', ())
        output_var = params.get('output_var', 'set_result')

        try:
            resolved = context.resolve_value(input_tuple)

            if not isinstance(resolved, (list, tuple)):
                resolved = (resolved,)

            result = list(set(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转集合: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组转集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class TupleIndexAction(BaseAction):
    """Find element index."""
    action_type = "tuple6_index"
    display_name = "查找元素索引"
    description = "在元组中查找元素索引"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute index.

        Args:
            context: Execution context.
            params: Dict with tuple, element, output_var.

        Returns:
            ActionResult with index.
        """
        input_tuple = params.get('tuple', ())
        element = params.get('element', None)
        output_var = params.get('output_var', 'index_result')

        try:
            resolved_tuple = context.resolve_value(input_tuple)
            resolved_element = context.resolve_value(element) if element is not None else None

            if not isinstance(resolved_tuple, (list, tuple)):
                resolved_tuple = (resolved_tuple,)

            try:
                result = resolved_tuple.index(resolved_element)
                found = True
            except ValueError:
                result = -1
                found = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元素索引: {result}" if found else "元素未找到",
                data={
                    'tuple': list(resolved_tuple),
                    'element': resolved_element,
                    'index': result,
                    'found': found,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找元素索引失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple', 'element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'index_result'}


class TupleCountAction(BaseAction):
    """Count element occurrences."""
    action_type = "tuple6_count"
    display_name = "统计元素出现次数"
    description = "统计元素在元组中出现的次数"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with tuple, element, output_var.

        Returns:
            ActionResult with count.
        """
        input_tuple = params.get('tuple', ())
        element = params.get('element', None)
        output_var = params.get('output_var', 'count_result')

        try:
            resolved_tuple = context.resolve_value(input_tuple)
            resolved_element = context.resolve_value(element) if element is not None else None

            if not isinstance(resolved_tuple, (list, tuple)):
                resolved_tuple = (resolved_tuple,)

            result = resolved_tuple.count(resolved_element)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元素出现次数: {result}",
                data={
                    'tuple': list(resolved_tuple),
                    'element': resolved_element,
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"统计元素出现次数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple', 'element']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class TupleSliceAction(BaseAction):
    """Slice tuple."""
    action_type = "tuple6_slice"
    display_name = "切片元组"
    description = "对元组进行切片"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with tuple, start, end, step, output_var.

        Returns:
            ActionResult with sliced tuple.
        """
        input_tuple = params.get('tuple', ())
        start = params.get('start', 0)
        end = params.get('end', None)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'slice_result')

        try:
            resolved_tuple = context.resolve_value(input_tuple)
            resolved_start = int(context.resolve_value(start)) if start else 0
            resolved_end = int(context.resolve_value(end)) if end else None
            resolved_step = int(context.resolve_value(step)) if step else 1

            if not isinstance(resolved_tuple, (list, tuple)):
                resolved_tuple = (resolved_tuple,)

            result = list(resolved_tuple[resolved_start:resolved_end:resolved_step])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组切片: {len(result)}个元素",
                data={
                    'original': list(resolved_tuple),
                    'sliced': result,
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'step': 1, 'output_var': 'slice_result'}