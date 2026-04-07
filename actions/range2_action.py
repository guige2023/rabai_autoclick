"""Range2 action module for RabAI AutoClick.

Provides additional range operations:
- RangeToListAction: Convert range to list
- RangeContainsAction: Check if value in range
- RangeOverlapAction: Check if ranges overlap
- RangeIntersectAction: Get intersection of ranges
- RangeToStringAction: Convert range to string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RangeToListAction(BaseAction):
    """Convert range to list."""
    action_type = "range2_to_list"
    display_name = "范围转列表"
    description = "将范围转换为列表"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range to list.

        Args:
            context: Execution context.
            params: Dict with start, end, step, output_var.

        Returns:
            ActionResult with list.
        """
        start = params.get('start', 0)
        end = params.get('end', 10)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'range_list')

        try:
            resolved_start = int(context.resolve_value(start)) if start else 0
            resolved_end = int(context.resolve_value(end)) if end else 10
            resolved_step = int(context.resolve_value(step)) if step else 1

            result = list(range(resolved_start, resolved_end, resolved_step))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围转列表: {len(result)}个元素",
                data={
                    'start': resolved_start,
                    'end': resolved_end,
                    'step': resolved_step,
                    'list': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'step': 1, 'output_var': 'range_list'}


class RangeContainsAction(BaseAction):
    """Check if value in range."""
    action_type = "range2_contains"
    display_name = "判断值在范围内"
    description = "判断值是否在指定范围内"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with value, start, end, output_var.

        Returns:
            ActionResult with contains result.
        """
        value = params.get('value', 0)
        start = params.get('start', 0)
        end = params.get('end', 10)
        output_var = params.get('output_var', 'contains_result')

        try:
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_start = float(context.resolve_value(start)) if start else 0
            resolved_end = float(context.resolve_value(end)) if end else 10

            result = resolved_start <= resolved_value <= resolved_end

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"值在范围内: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'start': resolved_start,
                    'end': resolved_end,
                    'contains': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断值在范围内失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'start', 'end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class RangeOverlapAction(BaseAction):
    """Check if ranges overlap."""
    action_type = "range2_overlap"
    display_name = "判断范围重叠"
    description = "判断两个范围是否重叠"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute overlap.

        Args:
            context: Execution context.
            params: Dict with start1, end1, start2, end2, output_var.

        Returns:
            ActionResult with overlap result.
        """
        start1 = params.get('start1', 0)
        end1 = params.get('end1', 10)
        start2 = params.get('start2', 5)
        end2 = params.get('end2', 15)
        output_var = params.get('output_var', 'overlap_result')

        try:
            resolved_start1 = float(context.resolve_value(start1)) if start1 else 0
            resolved_end1 = float(context.resolve_value(end1)) if end1 else 10
            resolved_start2 = float(context.resolve_value(start2)) if start2 else 5
            resolved_end2 = float(context.resolve_value(end2)) if end2 else 15

            result = resolved_start1 <= resolved_end2 and resolved_start2 <= resolved_end1

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围重叠: {'是' if result else '否'}",
                data={
                    'range1': (resolved_start1, resolved_end1),
                    'range2': (resolved_start2, resolved_end2),
                    'overlaps': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断范围重叠失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start1', 'end1', 'start2', 'end2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'overlap_result'}


class RangeIntersectAction(BaseAction):
    """Get intersection of ranges."""
    action_type = "range2_intersect"
    display_name = "范围交集"
    description = "获取两个范围的交集"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersect.

        Args:
            context: Execution context.
            params: Dict with start1, end1, start2, end2, output_var.

        Returns:
            ActionResult with intersection.
        """
        start1 = params.get('start1', 0)
        end1 = params.get('end1', 10)
        start2 = params.get('start2', 5)
        end2 = params.get('end2', 15)
        output_var = params.get('output_var', 'intersection_result')

        try:
            resolved_start1 = float(context.resolve_value(start1)) if start1 else 0
            resolved_end1 = float(context.resolve_value(end1)) if end1 else 10
            resolved_start2 = float(context.resolve_value(start2)) if start2 else 5
            resolved_end2 = float(context.resolve_value(end2)) if end2 else 15

            overlap_start = max(resolved_start1, resolved_start2)
            overlap_end = min(resolved_end1, resolved_end2)

            if overlap_start <= overlap_end:
                result = [overlap_start, overlap_end]
                has_overlap = True
            else:
                result = None
                has_overlap = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围交集: {result if has_overlap else '无交集'}",
                data={
                    'range1': (resolved_start1, resolved_end1),
                    'range2': (resolved_start2, resolved_end2),
                    'intersection': result,
                    'has_overlap': has_overlap,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start1', 'end1', 'start2', 'end2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'intersection_result'}


class RangeToStringAction(BaseAction):
    """Convert range to string."""
    action_type = "range2_to_string"
    display_name = "范围转字符串"
    description = "将范围转换为字符串"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range to string.

        Args:
            context: Execution context.
            params: Dict with start, end, step, output_var.

        Returns:
            ActionResult with string representation.
        """
        start = params.get('start', 0)
        end = params.get('end', 10)
        step = params.get('step', 1)
        output_var = params.get('output_var', 'range_string')

        try:
            resolved_start = int(context.resolve_value(start)) if start else 0
            resolved_end = int(context.resolve_value(end)) if end else 10
            resolved_step = int(context.resolve_value(step)) if step else 1

            if resolved_step == 1:
                result = f"range({resolved_start}, {resolved_end})"
            else:
                result = f"range({resolved_start}, {resolved_end}, {resolved_step})"

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围字符串: {result}",
                data={
                    'range_string': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"范围转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'step': 1, 'output_var': 'range_string'}