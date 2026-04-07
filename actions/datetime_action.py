"""Date/time action module for RabAI AutoClick.

Provides date/time actions:
- GetTimeAction: Get current time
- WaitUntilAction: Wait until specific time
- DateMathAction: Perform date arithmetic
"""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GetTimeAction(BaseAction):
    """Get current time and date."""
    action_type = "get_time"
    display_name = "获取时间"
    description = "获取当前日期和时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting current time.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with current time data.
        """
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'current_time')

        # Validate format
        valid, msg = self.validate_type(format_str, str, 'format')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            now = datetime.now()
            formatted = now.strftime(format_str)

            result_data = {
                'timestamp': now.timestamp(),
                'datetime': now.isoformat(),
                'formatted': formatted,
                'year': now.year,
                'month': now.month,
                'day': now.day,
                'hour': now.hour,
                'minute': now.minute,
                'second': now.second,
                'weekday': now.strftime('%A'),
                'format': format_str
            }

            # Store in context
            context.set(output_var, formatted)
            context.set(f'{output_var}_timestamp', now.timestamp())
            context.set(f'{output_var}_datetime', now.isoformat())

            return ActionResult(
                success=True,
                message=f"当前时间: {formatted}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format': '%Y-%m-%d %H:%M:%S',
            'output_var': 'current_time'
        }


class WaitUntilAction(BaseAction):
    """Wait until a specific time."""
    action_type = "wait_until"
    display_name = "等待到指定时间"
    description = "等待直到指定时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute waiting until specific time.

        Args:
            context: Execution context.
            params: Dict with target_time (HH:MM:SS or ISO format), timeout.

        Returns:
            ActionResult indicating success or timeout.
        """
        target_time = params.get('target_time', '')
        timeout = params.get('timeout', 3600)

        # Validate target_time
        if not target_time:
            return ActionResult(
                success=False,
                message="未指定目标时间"
            )
        valid, msg = self.validate_type(target_time, str, 'target_time')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate timeout
        valid, msg = self.validate_type(timeout, (int, float), 'timeout')
        if not valid:
            return ActionResult(success=False, message=msg)
        if timeout <= 0:
            return ActionResult(
                success=False,
                message=f"Parameter 'timeout' must be > 0, got {timeout}"
            )

        try:
            # Parse target time
            now = datetime.now()

            # Try different formats
            target = None
            formats = [
                '%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%H:%M',
            ]

            for fmt in formats:
                try:
                    target = datetime.strptime(target_time, fmt)
                    # If only time was provided, use today's date
                    if fmt in ('%H:%M:%S', '%H:%M'):
                        target = target.replace(
                            year=now.year,
                            month=now.month,
                            day=now.day
                        )
                    break
                except ValueError:
                    continue

            if target is None:
                return ActionResult(
                    success=False,
                    message=f"无法解析时间格式: {target_time}"
                )

            # If target is in the past, add one day
            if target < now:
                target += timedelta(days=1)

            # Calculate wait time
            wait_seconds = (target - now).total_seconds()

            if wait_seconds > timeout:
                return ActionResult(
                    success=False,
                    message=f"等待时间超过超时: {wait_seconds:.0f}s > {timeout}s"
                )

            # Wait
            time.sleep(min(wait_seconds, timeout))

            # Check if we actually reached the target
            now_after = datetime.now()
            reached = now_after >= target

            return ActionResult(
                success=reached,
                message=f"等待到 {target_time}: {'已到达' if reached else '超时'}",
                data={
                    'target_time': target.isoformat(),
                    'waited_seconds': wait_seconds,
                    'reached': reached
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"等待失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['target_time']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timeout': 3600}


class DateMathAction(BaseAction):
    """Perform date/time arithmetic."""
    action_type = "date_math"
    display_name = "日期计算"
    description = "对日期和时间进行计算"

    VALID_OPERATIONS: List[str] = [
        'add_days', 'add_hours', 'add_minutes', 'add_seconds',
        'subtract_days', 'subtract_hours', 'subtract_minutes', 'subtract_seconds',
        'diff_days', 'diff_hours', 'diff_minutes'
    ]

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date arithmetic.

        Args:
            context: Execution context.
            params: Dict with operation, value, input_date, format, output_var.

        Returns:
            ActionResult with calculated date.
        """
        operation = params.get('operation', '')
        value = params.get('value', 0)
        input_date = params.get('input_date', None)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'date_result')

        # Validate operation
        if not operation:
            return ActionResult(
                success=False,
                message="未指定操作"
            )
        valid, msg = self.validate_in(operation, self.VALID_OPERATIONS, 'operation')
        if not valid:
            return ActionResult(success=False, message=msg)

        # Validate value
        valid, msg = self.validate_type(value, (int, float), 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            # Parse input date or use now
            if input_date:
                base_date = datetime.fromisoformat(input_date)
            else:
                base_date = datetime.now()

            # Perform operation
            delta = None
            if operation == 'add_days':
                delta = timedelta(days=value)
            elif operation == 'add_hours':
                delta = timedelta(hours=value)
            elif operation == 'add_minutes':
                delta = timedelta(minutes=value)
            elif operation == 'add_seconds':
                delta = timedelta(seconds=value)
            elif operation == 'subtract_days':
                delta = timedelta(days=-value)
            elif operation == 'subtract_hours':
                delta = timedelta(hours=-value)
            elif operation == 'subtract_minutes':
                delta = timedelta(minutes=-value)
            elif operation == 'subtract_seconds':
                delta = timedelta(seconds=-value)

            result_date = base_date + delta if delta else base_date

            # Calculate difference
            diff_result = None
            if operation.startswith('diff_'):
                diff_unit = operation.split('_')[1]
                if delta:
                    if diff_unit == 'days':
                        diff_result = delta.total_seconds() / 86400
                    elif diff_unit == 'hours':
                        diff_result = delta.total_seconds() / 3600
                    elif diff_unit == 'minutes':
                        diff_result = delta.total_seconds() / 60

            formatted = result_date.strftime(format_str)

            result_data = {
                'input_date': base_date.isoformat() if input_date else datetime.now().isoformat(),
                'operation': operation,
                'value': value,
                'result_date': result_date.isoformat(),
                'formatted': formatted,
                'timestamp': result_date.timestamp()
            }

            if diff_result is not None:
                result_data['difference'] = diff_result

            # Store in context
            context.set(output_var, formatted)
            context.set(f'{output_var}_datetime', result_date.isoformat())
            if diff_result is not None:
                context.set(f'{output_var}_diff', diff_result)

            return ActionResult(
                success=True,
                message=f"日期计算: {operation} {value} = {formatted}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'value': 0,
            'input_date': None,
            'format': '%Y-%m-%d %H:%M:%S',
            'output_var': 'date_result'
        }