"""Datetime2 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeDiffAction: Calculate datetime difference
- DatetimeCompareAction: Compare datetimes
- DatetimeParseAction: Parse datetime string
- DatetimeComponentsAction: Get datetime components
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeDiffAction(BaseAction):
    """Calculate datetime difference."""
    action_type = "datetime_diff"
    display_name = "计算时间差"
    description = "计算两个日期时间的差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diff.

        Args:
            context: Execution context.
            params: Dict with datetime1, datetime2, output_var.

        Returns:
            ActionResult with difference.
        """
        datetime1 = params.get('datetime1', '')
        datetime2 = params.get('datetime2', '')
        output_var = params.get('output_var', 'datetime_diff')

        valid, msg = self.validate_type(datetime1, str, 'datetime1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(datetime2, str, 'datetime2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt1 = context.resolve_value(datetime1)
            resolved_dt2 = context.resolve_value(datetime2)

            dt1 = datetime.fromisoformat(resolved_dt1)
            dt2 = datetime.fromisoformat(resolved_dt2)

            diff = dt1 - dt2
            diff_seconds = diff.total_seconds()

            result = {
                'days': diff.days,
                'seconds': diff_seconds,
                'total_seconds': diff_seconds
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间差: {diff.days} 天, {diff_seconds:.2f} 秒",
                data={
                    'datetime1': resolved_dt1,
                    'datetime2': resolved_dt2,
                    'days': diff.days,
                    'seconds': diff_seconds,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算时间差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime1', 'datetime2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_diff'}


class DatetimeCompareAction(BaseAction):
    """Compare datetimes."""
    action_type = "datetime_compare"
    display_name = "比较时间"
    description = "比较两个日期时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute compare.

        Args:
            context: Execution context.
            params: Dict with datetime1, datetime2, output_var.

        Returns:
            ActionResult with comparison result.
        """
        datetime1 = params.get('datetime1', '')
        datetime2 = params.get('datetime2', '')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(datetime1, str, 'datetime1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(datetime2, str, 'datetime2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt1 = context.resolve_value(datetime1)
            resolved_dt2 = context.resolve_value(datetime2)

            dt1 = datetime.fromisoformat(resolved_dt1)
            dt2 = datetime.fromisoformat(resolved_dt2)

            if dt1 < dt2:
                result = -1
                comparison = '小于'
            elif dt1 > dt2:
                result = 1
                comparison = '大于'
            else:
                result = 0
                comparison = '等于'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"比较结果: {comparison}",
                data={
                    'datetime1': resolved_dt1,
                    'datetime2': resolved_dt2,
                    'result': result,
                    'comparison': comparison,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"比较时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime1', 'datetime2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class DatetimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime_parse"
    display_name = "解析时间"
    description = "解析日期时间字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format_str, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        datetime_str = params.get('datetime_str', '')
        format_str = params.get('format_str', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str)

            result = datetime.strptime(resolved_str, resolved_format)
            iso_str = result.isoformat()
            context.set(output_var, iso_str)

            return ActionResult(
                success=True,
                message=f"解析时间: {iso_str}",
                data={
                    'original': resolved_str,
                    'format': resolved_format,
                    'datetime': iso_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'format_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_datetime'}


class DatetimeComponentsAction(BaseAction):
    """Get datetime components."""
    action_type = "datetime_components"
    display_name = "获取时间组件"
    description = "获取日期时间的各个组件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute components.

        Args:
            context: Execution context.
            params: Dict with datetime_str, output_var.

        Returns:
            ActionResult with components.
        """
        datetime_str = params.get('datetime_str', '')
        output_var = params.get('output_var', 'datetime_components')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(datetime_str)

            dt = datetime.fromisoformat(resolved_str)

            result = {
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'hour': dt.hour,
                'minute': dt.minute,
                'second': dt.second,
                'microsecond': dt.microsecond,
                'weekday': dt.weekday(),
                'isoweekday': dt.isoweekday()
            }
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间组件: {dt.year}-{dt.month:02d}-{dt.day:02d}",
                data={
                    'datetime': resolved_str,
                    'components': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时间组件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_components'}
