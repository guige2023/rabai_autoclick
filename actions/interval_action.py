"""Interval action module for RabAI AutoClick.

Provides interval/range operations:
- IntervalCreateAction: Create interval
- IntervalContainsAction: Check if value in interval
- IntervalOverlapAction: Check if intervals overlap
- IntervalUnionAction: Union intervals
- IntervalIntersectAction: Intersect intervals
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IntervalCreateAction(BaseAction):
    """Create interval."""
    action_type = "interval_create"
    display_name = "创建区间"
    description = "创建数值区间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create.

        Args:
            context: Execution context.
            params: Dict with name, start, end, closed.

        Returns:
            ActionResult indicating created.
        """
        name = params.get('name', '')
        start = params.get('start', 0)
        end = params.get('end', 0)
        closed = params.get('closed', 'left')

        valid, msg = self.validate_type(name, str, 'name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(name)
            resolved_start = float(context.resolve_value(start))
            resolved_end = float(context.resolve_value(end))
            resolved_closed = context.resolve_value(closed)

            interval = {
                'start': resolved_start,
                'end': resolved_end,
                'closed': resolved_closed
            }
            context.set(f'_interval_{resolved_name}', interval)

            return ActionResult(
                success=True,
                message=f"区间 {resolved_name} 创建: [{resolved_start}, {resolved_end})",
                data={
                    'name': resolved_name,
                    'start': resolved_start,
                    'end': resolved_end,
                    'closed': resolved_closed
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建区间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'start', 'end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'closed': 'left'}


class IntervalContainsAction(BaseAction):
    """Check if value in interval."""
    action_type = "interval_contains"
    display_name = "区间包含"
    description = "检查值是否在区间内"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with interval_name, value, output_var.

        Returns:
            ActionResult with containment result.
        """
        interval_name = params.get('interval_name', '')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'contains_result')

        valid, msg = self.validate_type(interval_name, str, 'interval_name')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_name = context.resolve_value(interval_name)
            resolved_value = float(context.resolve_value(value))

            interval = context.get(f'_interval_{resolved_name}')
            if interval is None:
                return ActionResult(
                    success=False,
                    message=f"区间 {resolved_name} 不存在"
                )

            start = interval['start']
            end = interval['end']
            closed = interval['closed']

            if closed == 'left':
                result = start <= resolved_value < end
            elif closed == 'right':
                result = start < resolved_value <= end
            elif closed == 'both':
                result = start <= resolved_value <= end
            else:
                result = start < resolved_value < end

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"区间包含: {result}",
                data={
                    'interval_name': resolved_name,
                    'value': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查区间包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval_name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class IntervalOverlapAction(BaseAction):
    """Check if intervals overlap."""
    action_type = "interval_overlap"
    display_name = "区间重叠"
    description = "检查区间是否重叠"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute overlap check.

        Args:
            context: Execution context.
            params: Dict with interval1, interval2, output_var.

        Returns:
            ActionResult with overlap result.
        """
        interval1 = params.get('interval1', '')
        interval2 = params.get('interval2', '')
        output_var = params.get('output_var', 'overlap_result')

        valid, msg = self.validate_type(interval1, str, 'interval1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(interval2, str, 'interval2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_i1 = context.resolve_value(interval1)
            resolved_i2 = context.resolve_value(interval2)

            i1 = context.get(f'_interval_{resolved_i1}')
            i2 = context.get(f'_interval_{resolved_i2}')

            if i1 is None or i2 is None:
                return ActionResult(
                    success=False,
                    message="区间不存在"
                )

            result = i1['end'] > i2['start'] and i2['end'] > i1['start']

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"区间重叠: {result}",
                data={
                    'interval1': resolved_i1,
                    'interval2': resolved_i2,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查区间重叠失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval1', 'interval2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'overlap_result'}


class IntervalUnionAction(BaseAction):
    """Union intervals."""
    action_type = "interval_union"
    display_name = "区间并集"
    description = "合并两个区间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute union.

        Args:
            context: Execution context.
            params: Dict with interval1, interval2, output_name.

        Returns:
            ActionResult with union result.
        """
        interval1 = params.get('interval1', '')
        interval2 = params.get('interval2', '')
        output_name = params.get('output_name', 'union_interval')

        valid, msg = self.validate_type(interval1, str, 'interval1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(interval2, str, 'interval2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_i1 = context.resolve_value(interval1)
            resolved_i2 = context.resolve_value(interval2)

            i1 = context.get(f'_interval_{resolved_i1}')
            i2 = context.get(f'_interval_{resolved_i2}')

            if i1 is None or i2 is None:
                return ActionResult(
                    success=False,
                    message="区间不存在"
                )

            start = min(i1['start'], i2['start'])
            end = max(i1['end'], i2['end'])

            result = {
                'start': start,
                'end': end,
                'closed': 'both'
            }
            context.set(f'_interval_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"区间并集: [{start}, {end}]",
                data={
                    'interval1': resolved_i1,
                    'interval2': resolved_i2,
                    'output_name': output_name,
                    'start': start,
                    'end': end
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算区间并集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval1', 'interval2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'union_interval'}


class IntervalIntersectAction(BaseAction):
    """Intersect intervals."""
    action_type = "interval_intersect"
    display_name = "区间交集"
    description = "计算两个区间的交集"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute intersect.

        Args:
            context: Execution context.
            params: Dict with interval1, interval2, output_name.

        Returns:
            ActionResult with intersect result.
        """
        interval1 = params.get('interval1', '')
        interval2 = params.get('interval2', '')
        output_name = params.get('output_name', 'intersect_interval')

        valid, msg = self.validate_type(interval1, str, 'interval1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(interval2, str, 'interval2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_i1 = context.resolve_value(interval1)
            resolved_i2 = context.resolve_value(interval2)

            i1 = context.get(f'_interval_{resolved_i1}')
            i2 = context.get(f'_interval_{resolved_i2}')

            if i1 is None or i2 is None:
                return ActionResult(
                    success=False,
                    message="区间不存在"
                )

            start = max(i1['start'], i2['start'])
            end = min(i1['end'], i2['end'])

            if start >= end:
                return ActionResult(
                    success=True,
                    message="区间无交集",
                    data={
                        'interval1': resolved_i1,
                        'interval2': resolved_i2,
                        'has_intersection': False
                    }
                )

            result = {
                'start': start,
                'end': end,
                'closed': 'both'
            }
            context.set(f'_interval_{output_name}', result)

            return ActionResult(
                success=True,
                message=f"区间交集: [{start}, {end}]",
                data={
                    'interval1': resolved_i1,
                    'interval2': resolved_i2,
                    'output_name': output_name,
                    'start': start,
                    'end': end
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算区间交集失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval1', 'interval2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_name': 'intersect_interval'}
