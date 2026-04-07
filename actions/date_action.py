"""Date action module for RabAI AutoClick.

Provides advanced date operations:
- DateNowAction: Get current date
- DateAddDaysAction: Add days to date
- DateDiffAction: Calculate date difference
- DateFormatAction: Format date
"""

import datetime
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateNowAction(BaseAction):
    """Get current date."""
    action_type = "date_now"
    display_name = "获取当前日期"
    description = "获取当前日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get current date.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with current date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'current_date')

        try:
            resolved_format = context.resolve_value(format_str)
            result = datetime.datetime.now().strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"当前日期: {result}",
                data={
                    'date': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'current_date'}


class DateAddDaysAction(BaseAction):
    """Add days to date."""
    action_type = "date_add_days"
    display_name = "日期加天数"
    description = "在日期上增加天数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add days.

        Args:
            context: Execution context.
            params: Dict with date, days, format, output_var.

        Returns:
            ActionResult with new date.
        """
        date = params.get('date', '')
        days = params.get('days', 0)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'new_date')

        valid, msg = self.validate_type(date, str, 'date')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date)
            resolved_days = context.resolve_value(days)
            resolved_format = context.resolve_value(format_str)

            # Parse input date
            parsed_date = datetime.datetime.strptime(resolved_date, resolved_format)
            new_date = parsed_date + datetime.timedelta(days=int(resolved_days))
            result = new_date.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"新日期: {result}",
                data={
                    'result': result,
                    'days_added': resolved_days,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期加天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'new_date'}


class DateDiffAction(BaseAction):
    """Calculate date difference."""
    action_type = "date_diff"
    display_name = "日期差"
    description = "计算两个日期的差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date difference.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with date difference.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'date_diff')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str)

            parsed_date1 = datetime.datetime.strptime(resolved_date1, resolved_format)
            parsed_date2 = datetime.datetime.strptime(resolved_date2, resolved_format)

            diff = parsed_date2 - parsed_date1
            result = abs(diff.days)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期差: {result} 天",
                data={
                    'diff_days': result,
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算日期差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'date_diff'}


class DateFormatAction(BaseAction):
    """Format date."""
    action_type = "date_format"
    display_name = "格式化日期"
    description = "格式化日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format date.

        Args:
            context: Execution context.
            params: Dict with date, input_format, output_format, output_var.

        Returns:
            ActionResult with formatted date.
        """
        date = params.get('date', '')
        input_format = params.get('input_format', '%Y-%m-%d')
        output_format = params.get('output_format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_date')

        valid, msg = self.validate_type(date, str, 'date')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date)
            resolved_input = context.resolve_value(input_format)
            resolved_output = context.resolve_value(output_format)

            parsed_date = datetime.datetime.strptime(resolved_date, resolved_input)
            result = parsed_date.strftime(resolved_output)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化日期: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date', 'input_format', 'output_format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_date'}