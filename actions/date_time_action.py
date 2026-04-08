"""Date/time action module for RabAI AutoClick.

Provides date and time manipulation actions.
"""

import time
from datetime import datetime, timedelta
import sys
import os
from typing import Any, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DateTimeNowAction(BaseAction):
    """Get current datetime.
    
    Returns current date and time in various formats.
    """
    action_type = "datetime_now"
    display_name = "获取当前时间"
    description = "获取当前日期时间"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get current datetime.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: timezone, format.
        
        Returns:
            ActionResult with current datetime.
        """
        timezone = params.get('timezone', 'local')
        fmt = params.get('format', '%Y-%m-%d %H:%M:%S')
        
        try:
            now = datetime.now()
            
            return ActionResult(
                success=True,
                message=f"Current time: {now.strftime(fmt)}",
                data={
                    'datetime': now.isoformat(),
                    'timestamp': now.timestamp(),
                    'formatted': now.strftime(fmt),
                    'date': now.date().isoformat(),
                    'time': now.time().isoformat()
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Datetime error: {e}",
                data={'error': str(e)}
            )


class DateTimeParseAction(BaseAction):
    """Parse datetime string.
    
    Converts string to datetime object with format detection.
    """
    action_type = "datetime_parse"
    display_name = "解析日期时间"
    description = "解析日期时间字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse datetime string.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: date_string, format, timezone.
        
        Returns:
            ActionResult with parsed datetime.
        """
        date_string = params.get('date_string', '')
        fmt = params.get('format', None)
        timezone = params.get('timezone', 'local')
        
        if not date_string:
            return ActionResult(success=False, message="date_string required")
        
        try:
            if fmt:
                dt = datetime.strptime(date_string, fmt)
            else:
                # Try common formats
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y',
                    '%Y/%m/%d %H:%M:%S',
                    '%Y/%m/%d',
                    '%b %d, %Y',
                    '%B %d, %Y',
                ]
                dt = None
                for f in formats:
                    try:
                        dt = datetime.strptime(date_string, f)
                        break
                    except ValueError:
                        continue
                
                if dt is None:
                    # Try with dateutil if available
                    try:
                        from dateutil import parser
                        dt = parser.parse(date_string)
                    except ImportError:
                        return ActionResult(
                            success=False,
                            message=f"Could not parse date: {date_string}"
                        )
            
            return ActionResult(
                success=True,
                message=f"Parsed: {dt.isoformat()}",
                data={
                    'datetime': dt.isoformat(),
                    'timestamp': dt.timestamp(),
                    'date': dt.date().isoformat(),
                    'time': dt.time().isoformat()
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Parse error: {e}",
                data={'error': str(e), 'date_string': date_string}
            )


class DateTimeFormatAction(BaseAction):
    """Format datetime with custom format.
    
    Formats datetime objects with strftime-style formatting.
    """
    action_type = "datetime_format"
    display_name = "格式化日期时间"
    description = "格式化日期时间为字符串"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format datetime.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: datetime, format, timezone.
        
        Returns:
            ActionResult with formatted string.
        """
        dt_input = params.get('datetime', None)
        fmt = params.get('format', '%Y-%m-%d %H:%M:%S')
        timezone = params.get('timezone', 'local')
        
        try:
            if isinstance(dt_input, str):
                from dateutil import parser
                dt = parser.parse(dt_input)
            elif isinstance(dt_input, (int, float)):
                dt = datetime.fromtimestamp(dt_input)
            elif dt_input is None:
                dt = datetime.now()
            else:
                dt = dt_input
            
            formatted = dt.strftime(fmt)
            
            return ActionResult(
                success=True,
                message=f"Formatted: {formatted}",
                data={'formatted': formatted, 'datetime': dt.isoformat()}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Format error: {e}",
                data={'error': str(e)}
            )


class DateTimeDeltaAction(BaseAction):
    """Add/subtract time delta.
    
    Performs datetime arithmetic.
    """
    action_type = "datetime_delta"
    display_name = "日期时间计算"
    description = "日期时间加减运算"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate datetime delta.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: datetime, days, hours, minutes,
                   seconds, microseconds.
        
        Returns:
            ActionResult with calculated datetime.
        """
        dt_input = params.get('datetime', None)
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        microseconds = params.get('microseconds', 0)
        
        try:
            if isinstance(dt_input, str):
                from dateutil import parser
                dt = parser.parse(dt_input)
            elif isinstance(dt_input, (int, float)):
                dt = datetime.fromtimestamp(dt_input)
            elif dt_input is None:
                dt = datetime.now()
            else:
                dt = dt_input
            
            delta = timedelta(
                days=days,
                hours=hours,
                minutes=minutes,
                seconds=seconds,
                microseconds=microseconds
            )
            
            result = dt + delta
            
            return ActionResult(
                success=True,
                message=f"Result: {result.isoformat()}",
                data={
                    'datetime': result.isoformat(),
                    'timestamp': result.timestamp()
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delta error: {e}",
                data={'error': str(e)}
            )
