"""Range action module for RabAI AutoClick.

Provides range operations:
- RangeCreateAction: Create range
- RangeGetAction: Get range element
- RangeSliceAction: Slice range
- RangeContainsAction: Check if value in range
- RangeToListAction: Convert range to list
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RangeCreateAction(BaseAction):
    """Create range."""
    action_type = "range_create"
    display_name = "创建范围"
    description = "创建数值范围"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with start, stop, step, output_var.

        Returns:
            ActionResult indicating created.
        """
        start = params.get('start', 0)
        stop = params.get('stop', 10)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'range_result')

        try:
            resolved_start = int(context.resolve_value(start))
            resolved_stop = int(context.resolve_value(stop))
            resolved_step = int(context.resolve_value(step))

            result = list(range(resolved_start, resolved_stop, resolved_step))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围创建: {len(result)} 项",
                data={
                    'start': resolved_start,
                    'stop': resolved_stop,
                    'step': resolved_step,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'stop': 10, 'step': 1, 'output_var': 'range_result'}


class RangeGetAction(BaseAction):
    """Get range element."""
    action_type = "range_get"
    display_name = "获取范围元素"
    description = "获取范围元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with range_var, index, output_var.

        Returns:
            ActionResult with element.
        """
        range_var = params.get('range_var', '')
        index = params.get('index', 0)
        output_var = params.get('output_var', 'range_element')

        valid, msg = self.validate_type(range_var, str, 'range_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(range_var)
            resolved_index = int(context.resolve_value(index))

            r = context.get(resolved_var)
            if not isinstance(r, (list, tuple, range)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是范围或列表"
                )

            if abs(resolved_index) >= len(r):
                return ActionResult(
                    success=False,
                    message=f"索引 {resolved_index} 超出范围"
                )

            result = r[resolved_index]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取元素: {result}",
                data={
                    'index': resolved_index,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取范围元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['range_var', 'index']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'range_element'}


class RangeSliceAction(BaseAction):
    """Slice range."""
    action_type = "range_slice"
    display_name = "切片范围"
    description = "切片范围"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute slice.

        Args:
            context: Execution context.
            params: Dict with range_var, start, end, output_var.

        Returns:
            ActionResult with sliced range.
        """
        range_var = params.get('range_var', '')
        start = params.get('start', 0)
        end = params.get('end', None)
        output_var = params.get('output_var', 'range_slice')

        valid, msg = self.validate_type(range_var, str, 'range_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(range_var)
            resolved_start = int(context.resolve_value(start))
            resolved_end = int(context.resolve_value(end)) if end is not None else None

            r = context.get(resolved_var)
            if not isinstance(r, (list, tuple, range)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是范围或列表"
                )

            result = r[resolved_start:resolved_end]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围切片: {len(result)} 项",
                data={
                    'start': resolved_start,
                    'end': resolved_end,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"切片范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['range_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'output_var': 'range_slice'}


class RangeContainsAction(BaseAction):
    """Check if value in range."""
    action_type = "range_contains"
    display_name = "范围包含"
    description = "检查值是否在范围内"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with range_var, value, output_var.

        Returns:
            ActionResult with containment result.
        """
        range_var = params.get('range_var', '')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'contains_result')

        valid, msg = self.validate_type(range_var, str, 'range_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(range_var)
            resolved_value = int(context.resolve_value(value))

            r = context.get(resolved_var)
            if not isinstance(r, (list, tuple, range)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是范围或列表"
                )

            result = resolved_value in r
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围包含: {result}",
                data={
                    'range_var': resolved_var,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查范围包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['range_var', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class RangeToListAction(BaseAction):
    """Convert range to list."""
    action_type = "range_to_list"
    display_name = "范围转列表"
    description = "将范围转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to list.

        Args:
            context: Execution context.
            params: Dict with range_var, output_var.

        Returns:
            ActionResult with list.
        """
        range_var = params.get('range_var', '')
        output_var = params.get('output_var', 'range_list')

        valid, msg = self.validate_type(range_var, str, 'range_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(range_var)

            r = context.get(resolved_var)
            if not isinstance(r, (list, tuple, range)):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是范围或列表"
                )

            result = list(r)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围转列表: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['range_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'range_list'}
