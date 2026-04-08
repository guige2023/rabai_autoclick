"""Scheduler action module for RabAI AutoClick.

Provides scheduling and cron-like execution
for delayed and periodic task execution.
"""

import sys
import os
import time
import threading
import re
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DelayAction(BaseAction):
    """Delay execution for a specified duration.
    
    Simple sleep-based delay with optional
    variable interpolation.
    """
    action_type = "delay_execute"
    display_name = "延时执行"
    description = "延时执行指定时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Delay execution.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - duration: float (seconds to delay)
                - jitter: float (random jitter to add, optional)
                - save_to_var: str
        
        Returns:
            ActionResult after delay.
        """
        duration = params.get('duration', 1.0)
        jitter = params.get('jitter', 0.0)
        save_to_var = params.get('save_to_var', 'delay_result')

        if jitter > 0:
            import random
            duration += random.uniform(-jitter, jitter)

        duration = max(0, duration)
        time.sleep(duration)

        result = {
            'delayed_seconds': duration,
            'finished_at': datetime.now().isoformat(),
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Delayed {duration:.2f}s"
        )


class CronScheduleAction(BaseAction):
    """Parse and evaluate cron expressions.
    
    Supports standard 5-field cron format and
    computes next run times.
    """
    action_type = "cron_schedule"
    display_name = "Cron调度"
    description = "解析Cron表达式并计算下次执行时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Parse and evaluate cron expression.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - expression: str (cron expression, 5 fields)
                - base_time: str (ISO timestamp, optional)
                - count: int (number of next runs to compute)
                - save_to_var: str
        
        Returns:
            ActionResult with next run times.
        """
        expression = params.get('expression', '')
        base_time = params.get('base_time', None)
        count = params.get('count', 1)
        save_to_var = params.get('save_to_var', 'cron_result')

        if not expression:
            return ActionResult(success=False, message="Cron expression required")

        try:
            if base_time:
                base = datetime.fromisoformat(base_time.replace('Z', '+00:00'))
            else:
                base = datetime.now()

            fields = expression.strip().split()
            if len(fields) != 5:
                return ActionResult(success=False, message=f"Expected 5 cron fields, got {len(fields)}")

            runs = []
            current = base

            for _ in range(count):
                next_run = self._find_next_run(current, fields)
                runs.append(next_run.strftime('%Y-%m-%d %H:%M:%S'))
                current = next_run + timedelta(seconds=1)

            result = {
                'expression': expression,
                'next_runs': runs,
                'count': len(runs),
            }

            if context and save_to_var:
                context.variables[save_to_var] = result

            return ActionResult(
                success=True,
                data=result,
                message=f"Next run: {runs[0] if runs else 'none'}"
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Cron parse error: {e}")

    def _find_next_run(self, base: datetime, fields: List[str]) -> datetime:
        """Find next matching time for cron fields."""
        minute_field, hour_field, day_field, month_field, dow_field = fields

        # Simplified cron parser
        current = base.replace(second=0, microsecond=0)

        for _ in range(366 * 24 * 60):  # Max iterations
            if self._matches(current.minute, minute_field) and \
               self._matches(current.hour, hour_field) and \
               self._matches(current.day, day_field) and \
               self._matches(current.month, month_field) and \
               self._matches(current.weekday(), dow_field):
                return current
            current += timedelta(minutes=1)

        return current

    def _matches(self, value: int, field: str) -> bool:
        """Check if value matches cron field."""
        if field == '*':
            return True

        for part in field.split(','):
            if '/' in part:
                start, step = part.split('/')
                start = int(start) if start != '*' else 0
                if value >= start and (value - start) % int(step) == 0:
                    return True
            elif '-' in part:
                start, end = part.split('-')
                if int(start) <= value <= int(end):
                    return True
            else:
                if int(part) == value:
                    return True

        return False


class PeriodicTaskAction(BaseAction):
    """Run a task periodically at fixed intervals.
    
    Executes a task repeatedly with specified interval,
    for a maximum count or duration.
    """
    action_type = "periodic_task"
    display_name = "周期任务"
    description = "按固定间隔周期性执行任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute task periodically.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - interval: float (seconds between runs)
                - count: int (max executions, 0=unlimited)
                - max_duration: float (stop after this many seconds)
                - task_func: callable (task to run)
                - task_params: dict
                - save_to_var: str
        
        Returns:
            ActionResult with periodic execution result.
        """
        interval = params.get('interval', 1.0)
        count = params.get('count', 0)
        max_duration = params.get('max_duration', 0)
        task_func = params.get('task_func', None)
        task_params = params.get('task_params', {})
        save_to_var = params.get('save_to_var', 'periodic_result')

        if interval <= 0:
            return ActionResult(success=False, message="Interval must be positive")

        start_time = time.time()
        executions = 0
        errors = []

        while True:
            if count > 0 and executions >= count:
                break
            if max_duration > 0 and (time.time() - start_time) >= max_duration:
                break

            exec_start = time.time()

            try:
                if task_func and callable(task_func):
                    task_func(task_params)
                else:
                    # Simulate task execution
                    pass
                executions += 1
            except Exception as e:
                errors.append({'execution': executions, 'error': str(e)})

            elapsed = time.time() - exec_start
            sleep_time = max(0, interval - elapsed)

            if sleep_time > 0:
                time.sleep(sleep_time)

        result = {
            'executions': executions,
            'errors': len(errors),
            'error_list': errors,
            'total_duration': time.time() - start_time,
            'interval': interval,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Periodic task: {executions} executions, {len(errors)} errors"
        )


class IntervalScheduleAction(BaseAction):
    """Schedule task at regular intervals (like setInterval).
    
    Compute whether current time falls on an interval schedule.
    """
    action_type = "interval_schedule"
    display_name = "间隔调度"
    description = "按固定间隔判断是否到达执行时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check if interval has elapsed.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - interval_seconds: float
                - last_run: float (timestamp of last run, or 0 for first)
                - save_to_var: str
        
        Returns:
            ActionResult with schedule check result.
        """
        interval_seconds = params.get('interval_seconds', 60.0)
        last_run = params.get('last_run', 0.0)
        save_to_var = params.get('save_to_var', 'schedule_result')

        now = time.time()

        if last_run == 0:
            should_run = True
            next_run = now + interval_seconds
        else:
            elapsed = now - last_run
            should_run = elapsed >= interval_seconds
            next_run = last_run + interval_seconds

        result = {
            'should_run': should_run,
            'elapsed': now - last_run if last_run > 0 else 0,
            'interval': interval_seconds,
            'next_run': next_run,
            'now': now,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=True,
            data=result,
            message=f"Interval check: {'RUN NOW' if should_run else f'wait {next_run - now:.1f}s'}"
        )
