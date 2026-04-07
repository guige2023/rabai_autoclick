"""Date3 action module for RabAI AutoClick.

Provides additional date operations:
- DateDiffDaysAction: Calculate days between dates
- DateIsLeapYearAction: Check if leap year
- DateToIsoAction: Convert date to ISO string
- DateFromIsoAction: Parse ISO date string
- DateComponentsAction: Extract year, month, day components
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateDiffDaysAction(BaseAction):
    """Calculate days between dates."""
    action_type = "date3_diff_days"
    display_name = "日期相差天数"
    description = "计算两个日期之间的天数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diff days.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with difference in days.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'days_diff')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = (dt1 - dt2).days
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期相差: {abs(diff)} 天",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'diff_days': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算日期差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'days_diff'}


class DateIsLeapYearAction(BaseAction):
    """Check if leap year."""
    action_type = "date3_is_leap_year"
    display_name = "判断闰年"
    description = "判断是否为闰年"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is leap year.

        Args:
            context: Execution context.
            params: Dict with year, output_var.

        Returns:
            ActionResult with leap year check.
        """
        year = params.get('year', 2024)
        output_var = params.get('output_var', 'is_leap_year')

        try:
            resolved_year = int(context.resolve_value(year))
            is_leap = (resolved_year % 4 == 0 and resolved_year % 100 != 0) or (resolved_year % 400 == 0)
            context.set(output_var, is_leap)

            return ActionResult(
                success=True,
                message=f"{resolved_year}年: {'是闰年' if is_leap else '不是闰年'}",
                data={
                    'year': resolved_year,
                    'is_leap_year': is_leap,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断闰年失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['year']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_leap_year'}


class DateToIsoAction(BaseAction):
    """Convert date to ISO string."""
    action_type = "date3_to_iso"
    display_name = "日期转ISO"
    description = "将日期转换为ISO格式字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date to ISO.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with ISO string.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'iso_date')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            iso_str = dt.isoformat()
            context.set(output_var, iso_str)

            return ActionResult(
                success=True,
                message=f"ISO日期: {iso_str}",
                data={
                    'original': resolved_date,
                    'iso': iso_str,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换ISO失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'iso_date'}


class DateFromIsoAction(BaseAction):
    """Parse ISO date string."""
    action_type = "date3_from_iso"
    display_name = "解析ISO日期"
    description = "解析ISO格式日期字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute from ISO.

        Args:
            context: Execution context.
            params: Dict with iso_str, output_format, output_var.

        Returns:
            ActionResult with parsed date.
        """
        iso_str = params.get('iso_str', '')
        output_format = params.get('output_format', '%Y-%m-%d')
        output_var = params.get('output_var', 'parsed_date')

        valid, msg = self.validate_type(iso_str, str, 'iso_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_iso = context.resolve_value(iso_str)
            resolved_format = context.resolve_value(output_format) if output_format else '%Y-%m-%d'

            dt = datetime.fromisoformat(resolved_iso)
            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"解析日期: {result}",
                data={
                    'iso': resolved_iso,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析ISO日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['iso_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_format': '%Y-%m-%d', 'output_var': 'parsed_date'}


class DateComponentsAction(BaseAction):
    """Extract year, month, day components."""
    action_type = "date3_components"
    display_name = "提取日期组件"
    description = "提取日期的年、月、日组件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date components.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with components.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'date_components')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            from datetime import datetime
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            components = {
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'weekday': dt.weekday(),
                'isoweekday': dt.isoweekday()
            }
            context.set(output_var, components)

            return ActionResult(
                success=True,
                message=f"日期组件: {dt.year}/{dt.month}/{dt.day}",
                data={
                    'original': resolved_date,
                    'components': components,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提取日期组件失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'date_components'}
