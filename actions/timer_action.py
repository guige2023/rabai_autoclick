"""Timer action module for RabAI AutoClick.

Provides timing and scheduling actions including delays, intervals, and wait operations.
"""

import time
import asyncio
import sys
import os
from typing import Any, Dict, Optional, Union
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DelayAction(BaseAction):
    """Wait for a specified duration.
    
    Provides precise delay with optional early exit on context cancellation.
    """
    action_type = "delay"
    display_name = "延时等待"
    description = "等待指定时间长度"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute a delay.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: duration (seconds), allow_interrupt.
        
        Returns:
            ActionResult with delay completion status.
        """
        duration = params.get('duration', 1.0)
        allow_interrupt = params.get('allow_interrupt', True)
        
        # Validate duration
        if not isinstance(duration, (int, float)):
            return ActionResult(
                success=False,
                message=f"Duration must be numeric, got {type(duration).__name__}"
            )
        
        if duration < 0:
            return ActionResult(
                success=False,
                message=f"Duration must be positive, got {duration}"
            )
        
        if duration > 3600:
            return ActionResult(
                success=False,
                message=f"Duration exceeds maximum (3600s), got {duration}"
            )
        
        start_time = time.time()
        
        try:
            if allow_interrupt and hasattr(context, 'cancel_event') and context.cancel_event:
                # Check periodically for cancellation
                step = min(duration, 0.1)
                elapsed = 0
                while elapsed < duration:
                    if context.cancel_event.is_set():
                        return ActionResult(
                            success=False,
                            message=f"Delay interrupted after {elapsed:.2f}s",
                            data={'elapsed': elapsed, 'requested': duration}
                        )
                    time.sleep(step)
                    elapsed += step
            else:
                time.sleep(duration)
            
            actual_duration = time.time() - start_time
            
            return ActionResult(
                success=True,
                message=f"Delayed {actual_duration:.3f}s",
                data={'requested': duration, 'actual': actual_duration}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Delay error: {e}",
                data={'error': str(e)}
            )


class IntervalAction(BaseAction):
    """Track time intervals and report elapsed time.
    
    Useful for measuring duration between checkpoints in a workflow.
    """
    action_type = "interval"
    display_name = "间隔计时"
    description = "测量时间间隔"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Track or report interval.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: label, reset, format (seconds/ms).
        
        Returns:
            ActionResult with elapsed time.
        """
        label = params.get('label', 'default')
        reset = params.get('reset', False)
        format_type = params.get('format', 'seconds')
        
        # Get or create interval tracker in context
        if not hasattr(context, '_interval_timers'):
            context._interval_timers = {}
        
        if label not in context._interval_timers or reset:
            context._interval_timers[label] = time.time()
            return ActionResult(
                success=True,
                message=f"Timer '{label}' started",
                data={'started': True, 'label': label}
            )
        
        elapsed = time.time() - context._interval_timers[label]
        
        if format_type == 'ms':
            formatted = elapsed * 1000
            unit = 'ms'
        elif format_type == 'ms_precise':
            formatted = elapsed * 1000
            unit = 'ms'
        else:
            formatted = elapsed
            unit = 's'
        
        return ActionResult(
            success=True,
            message=f"Timer '{label}': {formatted:.3f}{unit}",
            data={'label': label, 'elapsed': elapsed, 'formatted': formatted, 'unit': unit}
        )


class WaitForConditionAction(BaseAction):
    """Wait until a condition is met or timeout expires.
    
    Polls a condition function at intervals until it returns True
    or the maximum wait time is reached.
    """
    action_type = "wait_for_condition"
    display_name = "等待条件"
    description = "等待条件满足或超时"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Wait for condition to be true.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: timeout, interval, check_function,
                   expected_value.
        
        Returns:
            ActionResult with condition status and elapsed time.
        """
        timeout = params.get('timeout', 30.0)
        interval = params.get('interval', 0.5)
        expected_value = params.get('expected_value', True)
        check_function = params.get('check_function', None)
        
        if timeout < 0 or timeout > 3600:
            return ActionResult(
                success=False,
                message=f"Timeout must be 0-3600, got {timeout}"
            )
        
        if interval <= 0 or interval > timeout:
            return ActionResult(
                success=False,
                message=f"Interval must be >0 and <= timeout"
            )
        
        start_time = time.time()
        iterations = 0
        
        try:
            while True:
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return ActionResult(
                        success=False,
                        message=f"Timeout after {elapsed:.2f}s ({iterations} iterations)",
                        data={'timeout': timeout, 'elapsed': elapsed, 'iterations': iterations}
                    )
                
                # Check condition
                if check_function:
                    try:
                        result = check_function()
                        condition_met = (result == expected_value)
                    except Exception as e:
                        condition_met = False
                else:
                    # No check function, just wait for timeout
                    condition_met = False
                
                if condition_met:
                    return ActionResult(
                        success=True,
                        message=f"Condition met after {elapsed:.2f}s ({iterations} iterations)",
                        data={'elapsed': elapsed, 'iterations': iterations}
                    )
                
                # Wait before next check
                time.sleep(interval)
                iterations += 1
                
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Wait error: {e}",
                data={'error': str(e), 'elapsed': time.time() - start_time}
            )


class CronScheduleAction(BaseAction):
    """Check if current time matches a cron-like schedule.
    
    Evaluates simple cron expressions (minute, hour, day, month, weekday).
    """
    action_type = "cron_check"
    display_name = "Cron检查"
    description = "检查当前时间是否匹配Cron表达式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check cron schedule.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: minute, hour, day, month, weekday.
                   Use * for any, ranges (1-5), lists (1,3,5).
        
        Returns:
            ActionResult with match status.
        """
        minute = params.get('minute', '*')
        hour = params.get('hour', '*')
        day = params.get('day', '*')
        month = params.get('month', '*')
        weekday = params.get('weekday', '*')
        
        now = datetime.now()
        
        def matches(field: str, value: int) -> bool:
            """Check if a field matches the current value."""
            if field == '*':
                return True
            
            # Handle lists (comma-separated)
            if ',' in field:
                return any(matches(f.strip(), value) for f in field.split(','))
            
            # Handle ranges (e.g., 1-5)
            if '-' in field:
                start, end = field.split('-')
                return int(start) <= value <= int(end)
            
            # Handle step values (e.g., */5)
            if '/' in field:
                base, step = field.split('/')
                base = int(base) if base != '*' else 0
                step = int(step)
                return (value - base) % step == 0
            
            # Single value
            return int(field) == value
        
        try:
            matches_minute = matches(minute, now.minute)
            matches_hour = matches(hour, now.hour)
            matches_day = matches(day, now.day)
            matches_month = matches(month, now.month)
            matches_weekday = matches(weekday, now.weekday())
            
            matched = matches_minute and matches_hour and matches_day and matches_month and matches_weekday
            
            return ActionResult(
                success=True,
                message=f"{'Matches' if matched else 'Does not match'} schedule",
                data={
                    'matched': matched,
                    'current_time': now.isoformat(),
                    'checks': {
                        'minute': matches_minute,
                        'hour': matches_hour,
                        'day': matches_day,
                        'month': matches_month,
                        'weekday': matches_weekday
                    }
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron check error: {e}",
                data={'error': str(e)}
            )
