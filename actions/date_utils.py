"""Date utilities action module for RabAI AutoClick.

Provides date manipulation and formatting actions
including date arithmetic, range generation, and parsing.
"""

import sys
import os
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DateFormatAction(BaseAction):
    """Format date to string.
    
    Supports custom format strings and locale formatting.
    """
    action_type = "date_format"
    display_name = "日期格式化"
    description = "格式化日期为字符串"

    COMMON_FORMATS = {
        'iso': '%Y-%m-%dT%H:%M:%S',
        'date': '%Y-%m-%d',
        'time': '%H:%M:%S',
        'datetime': '%Y-%m-%d %H:%M:%S',
        'cn': '%Y年%m月%d日',
        'us': '%m/%d/%Y',
        'eu': '%d/%m/%Y',
        'timestamp': None  # Special handling
    }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format date.
        
        Args:
            context: Execution context.
            params: Dict with keys: date_value, format_str, format_name,
                   timezone, save_to_var.
        
        Returns:
            ActionResult with formatted date string.
        """
        date_value = params.get('date_value', 'now')
        format_str = params.get('format_str', '%Y-%m-%d')
        format_name = params.get('format_name', None)
        timezone_str = params.get('timezone', 'local')
        save_to_var = params.get('save_to_var', None)

        # Parse date_value
        if date_value == 'now':
            dt = datetime.now()
        elif isinstance(date_value, (int, float)):
            # Unix timestamp
            if date_value > 1e12:
                date_value = date_value / 1000
            dt = datetime.fromtimestamp(date_value)
        elif isinstance(date_value, str):
            # Try parsing
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d %H:%M:%S', '%Y/%m/%d'):
                try:
                    dt = datetime.strptime(date_value, fmt)
                    break
                except ValueError:
                    continue
            else:
                # Treat as ISO
                try:
                    dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"Cannot parse date: {date_value}"
                    )
        elif isinstance(date_value, datetime):
            dt = date_value
        elif isinstance(date_value, date):
            dt = datetime.combine(date_value, datetime.min.time())
        else:
            return ActionResult(
                success=False,
                message=f"Invalid date type: {type(date_value).__name__}"
            )

        # Apply format name
        if format_name and format_name in self.COMMON_FORMATS:
            fmt = self.COMMON_FORMATS[format_name]
            if fmt is None:  # timestamp
                result = str(int(dt.timestamp()))
            else:
                result = dt.strftime(fmt)
        else:
            result = dt.strftime(format_str)

        result_data = {
            'formatted': result,
            'original': str(dt),
            'timestamp': int(dt.timestamp()),
            'format': format_str
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"日期格式化: {result}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['date_value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format_str': '%Y-%m-%d',
            'format_name': None,
            'timezone': 'local',
            'save_to_var': None
        }


class DateArithmeticAction(BaseAction):
    """Perform date arithmetic.
    
    Supports adding/subtracting days, hours, minutes,
    and calculating date differences.
    """
    action_type = "date_arithmetic"
    display_name = "日期计算"
    description = "日期加减计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Calculate date arithmetic.
        
        Args:
            context: Execution context.
            params: Dict with keys: date_value, days, hours,
                   minutes, seconds, operation, save_to_var.
        
        Returns:
            ActionResult with calculated date.
        """
        date_value = params.get('date_value', 'now')
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        operation = params.get('operation', 'add')  # 'add' or 'subtract'
        save_to_var = params.get('save_to_var', None)

        # Parse base date
        if date_value == 'now':
            dt = datetime.now()
        elif isinstance(date_value, (int, float)):
            if date_value > 1e12:
                date_value = date_value / 1000
            dt = datetime.fromtimestamp(date_value)
        elif isinstance(date_value, str):
            try:
                dt = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
            except ValueError:
                try:
                    dt = datetime.strptime(date_value, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    return ActionResult(
                        success=False,
                        message=f"Cannot parse date: {date_value}"
                    )
        elif isinstance(date_value, datetime):
            dt = date_value
        else:
            return ActionResult(
                success=False,
                message=f"Invalid date type: {type(date_value).__name__}"
            )

        # Create delta
        delta = timedelta(
            days=int(days),
            hours=int(hours),
            minutes=int(minutes),
            seconds=int(seconds)
        )

        # Apply operation
        if operation == 'subtract':
            result_dt = dt - delta
        else:
            result_dt = dt + delta

        result_data = {
            'original': str(dt),
            'result': str(result_dt),
            'timestamp': int(result_dt.timestamp()),
            'date': result_dt.strftime('%Y-%m-%d'),
            'time': result_dt.strftime('%H:%M:%S'),
            'operation': operation,
            'delta': f"{days}d {hours}h {minutes}m {seconds}s"
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"日期计算: {str(dt)[:19]} -> {str(result_dt)[:19]}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'date_value': 'now',
            'days': 0,
            'hours': 0,
            'minutes': 0,
            'seconds': 0,
            'operation': 'add',
            'save_to_var': None
        }


class DateRangeAction(BaseAction):
    """Generate date range.
    
    Creates list of dates between start and end.
    """
    action_type = "date_range"
    display_name = "日期范围"
    description = "生成日期范围列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate date range.
        
        Args:
            context: Execution context.
            params: Dict with keys: start_date, end_date,
                   format_str, step_days, save_to_var.
        
        Returns:
            ActionResult with date list.
        """
        start_date = params.get('start_date', '')
        end_date = params.get('end_date', '')
        format_str = params.get('format_str', '%Y-%m-%d')
        step_days = params.get('step_days', 1)
        save_to_var = params.get('save_to_var', None)

        # Parse dates
        try:
            if isinstance(start_date, str):
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            else:
                start_dt = start_date

            if isinstance(end_date, str):
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            else:
                end_dt = end_date
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Date parsing error: {e}"
            )

        # Generate range
        dates = []
        current = start_dt
        while current <= end_dt:
            dates.append(current.strftime(format_str))
            current += timedelta(days=step_days)

        result_data = {
            'dates': dates,
            'count': len(dates),
            'start': start_date,
            'end': end_date
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"日期范围生成: {len(dates)} 个日期",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['start_date', 'end_date']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'format_str': '%Y-%m-%d',
            'step_days': 1,
            'save_to_var': None
        }
