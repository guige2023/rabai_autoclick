"""Date6 action module for RabAI AutoClick.

Provides additional date operations:
- DateDiffDaysAction: Calculate difference in days
- DateDiffWeeksAction: Calculate difference in weeks
- DateDiffMonthsAction: Calculate difference in months
- DateDiffYearsAction: Calculate difference in years
- DateCompareAction: Compare two dates
"""

from datetime import datetime
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateDiffDaysAction(BaseAction):
    """Calculate difference in days."""
    action_type = "date6_diff_days"
    display_name = "日期间差天数"
    description = "计算两个日期之间的天数差"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date diff days.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with difference in days.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'diff_days')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = (dt2 - dt1).days
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期间差天数: {diff} 天",
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
                message=f"日期间差天数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'diff_days'}


class DateDiffWeeksAction(BaseAction):
    """Calculate difference in weeks."""
    action_type = "date6_diff_weeks"
    display_name = "日期间差周数"
    description = "计算两个日期之间的周数差"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date diff weeks.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with difference in weeks.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'diff_weeks')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = (dt2 - dt1).days // 7
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期间差周数: {diff} 周",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'diff_weeks': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期间差周数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'diff_weeks'}


class DateDiffMonthsAction(BaseAction):
    """Calculate difference in months."""
    action_type = "date6_diff_months"
    display_name = "日期间差月数"
    description = "计算两个日期之间的月数差"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date diff months.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with difference in months.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'diff_months')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = (dt2.year - dt1.year) * 12 + (dt2.month - dt1.month)
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期间差月数: {diff} 月",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'diff_months': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期间差月数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'diff_months'}


class DateDiffYearsAction(BaseAction):
    """Calculate difference in years."""
    action_type = "date6_diff_years"
    display_name = "日期间差年数"
    description = "计算两个日期之间的年数差"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date diff years.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with difference in years.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'diff_years')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = dt2.year - dt1.year
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期间差年数: {diff} 年",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'diff_years': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期间差年数计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'diff_years'}


class DateCompareAction(BaseAction):
    """Compare two dates."""
    action_type = "date6_compare"
    display_name = "日期比较"
    description = "比较两个日期的大小"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date compare.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with comparison result.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            if dt1 < dt2:
                result = -1
                comparison = '早于'
            elif dt1 > dt2:
                result = 1
                comparison = '晚于'
            else:
                result = 0
                comparison = '等于'

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期比较: {resolved_date1} {comparison} {resolved_date2}",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'result': result,
                    'comparison': comparison,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'compare_result'}