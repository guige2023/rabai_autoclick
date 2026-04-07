"""Calendar action module for RabAI AutoClick.

Provides calendar/schedule operations:
- CalendarNowAction: Get current datetime
- CalendarAddDaysAction: Add days to date
- CalendarDiffAction: Calculate date difference
- CalendarFormatAction: Format date
- CalendarParseAction: Parse date string
- CalendarRangeAction: Generate date range
- CalendarTodayAction: Get today's date
- CalendarWeekdayAction: Get day of week
- CalendarBetweenAction: Generate dates between two dates
"""

import re
import subprocess
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Union

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CalendarNowAction(BaseAction):
    """Get current datetime."""
    action_type = "calendar_now"
    display_name = "获取当前时间"
    description = "获取当前日期时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current datetime.
        """
        output_var = params.get('output_var', 'now')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            now = datetime.now()
            context.set(output_var, now.isoformat())

            return ActionResult(
                success=True,
                message=f"当前时间: {now.isoformat()}",
                data={'now': now.isoformat(), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'now'}


class CalendarAddDaysAction(BaseAction):
    """Add days to date."""
    action_type = "calendar_add_days"
    display_name = "日期加天数"
    description = "向日期添加指定天数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with date, days, output_var.

        Returns:
            ActionResult with result date.
        """
        date_param = params.get('date', '')
        days = params.get('days', 0)
        output_var = params.get('output_var', 'result_date')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_param) if date_param else datetime.now().isoformat()
            resolved_days = context.resolve_value(days)

            # Parse input date
            if isinstance(resolved_date, str):
                dt = datetime.fromisoformat(resolved_date.replace('Z', '+00:00'))
            else:
                dt = resolved_date

            result = dt + timedelta(days=int(resolved_days))
            context.set(output_var, result.isoformat())

            return ActionResult(
                success=True,
                message=f"{resolved_days} 天后: {result.isoformat()}",
                data={'result': result.isoformat(), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result_date'}


class CalendarDiffAction(BaseAction):
    """Calculate date difference."""
    action_type = "calendar_diff"
    display_name = "日期差计算"
    description = "计算两个日期之间的差值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diff.

        Args:
            context: Execution context.
            params: Dict with date1, date2, output_var.

        Returns:
            ActionResult with difference.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        output_var = params.get('output_var', 'date_diff')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_d1 = context.resolve_value(date1)
            resolved_d2 = context.resolve_value(date2)

            dt1 = datetime.fromisoformat(resolved_d1.replace('Z', '+00:00'))
            dt2 = datetime.fromisoformat(resolved_d2.replace('Z', '+00:00'))

            diff = abs((dt1 - dt2).total_seconds())

            result = {
                'total_seconds': diff,
                'days': diff / 86400,
                'hours': diff / 3600,
                'minutes': diff / 60
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期差: {result['days']:.1f} 天",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期差计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'date_diff'}


class CalendarFormatAction(BaseAction):
    """Format date."""
    action_type = "calendar_format"
    display_name = "日期格式化"
    description = "格式化日期"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format.

        Args:
            context: Execution context.
            params: Dict with date, format, output_var.

        Returns:
            ActionResult with formatted date.
        """
        date_param = params.get('date', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_date')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_param) if date_param else datetime.now().isoformat()
            resolved_fmt = context.resolve_value(format_str)

            if isinstance(resolved_date, str):
                dt = datetime.fromisoformat(resolved_date.replace('Z', '+00:00'))
            else:
                dt = resolved_date

            formatted = dt.strftime(resolved_fmt)
            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"格式化日期: {formatted}",
                data={'formatted': formatted, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'formatted_date'}


class CalendarParseAction(BaseAction):
    """Parse date string."""
    action_type = "calendar_parse"
    display_name = "日期解析"
    description = "解析日期字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with parsed date.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'parsed_date')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(date_str)
            resolved_fmt = context.resolve_value(format_str)

            dt = datetime.strptime(resolved_str, resolved_fmt)
            context.set(output_var, dt.isoformat())

            return ActionResult(
                success=True,
                message=f"解析日期: {dt.isoformat()}",
                data={'parsed': dt.isoformat(), 'output_var': output_var}
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期解析失败: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_date'}


class CalendarRangeAction(BaseAction):
    """Generate date range."""
    action_type = "calendar_range"
    display_name = "生成日期范围"
    description = "生成两个日期之间的所有日期"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range.

        Args:
            context: Execution context.
            params: Dict with start_date, end_date, format, output_var.

        Returns:
            ActionResult with date list.
        """
        start_date = params.get('start_date', '')
        end_date = params.get('end_date', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'date_range')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_start = context.resolve_value(start_date)
            resolved_end = context.resolve_value(end_date)
            resolved_fmt = context.resolve_value(format_str)

            start = datetime.fromisoformat(resolved_start.replace('Z', '+00:00'))
            end = datetime.fromisoformat(resolved_end.replace('Z', '+00:00'))

            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime(resolved_fmt))
                current += timedelta(days=1)

            context.set(output_var, dates)

            return ActionResult(
                success=True,
                message=f"生成了 {len(dates)} 个日期",
                data={'count': len(dates), 'dates': dates, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成日期范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start_date', 'end_date']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'date_range'}


class CalendarTodayAction(BaseAction):
    """Get today's date."""
    action_type = "calendar_today"
    display_name = "获取今天日期"
    description = "获取今天的日期"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute today.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with today's date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'today')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_fmt = context.resolve_value(format_str)
            today = date.today()
            formatted = today.strftime(resolved_fmt)
            context.set(output_var, formatted)

            return ActionResult(
                success=True,
                message=f"今天是: {formatted}",
                data={'today': formatted, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取今天日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'today'}


class CalendarWeekdayAction(BaseAction):
    """Get day of week."""
    action_type = "calendar_weekday"
    display_name = "获取星期几"
    description = "获取日期对应的星期几"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute weekday.

        Args:
            context: Execution context.
            params: Dict with date, output_var.

        Returns:
            ActionResult with weekday.
        """
        date_param = params.get('date', '')
        output_var = params.get('output_var', 'weekday')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if date_param:
                resolved_date = context.resolve_value(date_param)
                if isinstance(resolved_date, str):
                    dt = datetime.fromisoformat(resolved_date.replace('Z', '+00:00'))
                else:
                    dt = resolved_date
            else:
                dt = datetime.now()

            weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            weekday_name = weekdays[dt.weekday()]
            weekday_num = dt.weekday()  # 0=Monday, 6=Sunday

            result = {
                'name': weekday_name,
                'number': weekday_num,
                'is_weekend': weekday_num >= 5
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"星期{weekday_num + 1} ({weekday_name})",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取星期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'date': '', 'output_var': 'weekday'}


class CalendarBetweenAction(BaseAction):
    """Generate dates between two dates."""
    action_type = "calendar_between"
    display_name = "生成间隔日期"
    description = "生成两个日期之间每隔N天的日期"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute between.

        Args:
            context: Execution context.
            params: Dict with start_date, end_date, interval, format, output_var.

        Returns:
            ActionResult with date list.
        """
        start_date = params.get('start_date', '')
        end_date = params.get('end_date', '')
        interval = params.get('interval', 1)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'between_dates')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_start = context.resolve_value(start_date)
            resolved_end = context.resolve_value(end_date)
            resolved_interval = context.resolve_value(interval)
            resolved_fmt = context.resolve_value(format_str)

            start = datetime.fromisoformat(resolved_start.replace('Z', '+00:00'))
            end = datetime.fromisoformat(resolved_end.replace('Z', '+00:00'))

            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime(resolved_fmt))
                current += timedelta(days=int(resolved_interval))

            context.set(output_var, dates)

            return ActionResult(
                success=True,
                message=f"生成了 {len(dates)} 个日期 (间隔 {resolved_interval} 天)",
                data={'count': len(dates), 'dates': dates, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"生成间隔日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['start_date', 'end_date']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'interval': 1, 'format': '%Y-%m-%d', 'output_var': 'between_dates'}
