"""Scheduler action module for RabAI AutoClick.

Provides scheduling operations:
- SchedulerCronAction: Cron-based scheduling
- SchedulerIntervalAction: Interval-based scheduling
- SchedulerDelayAction: Delayed execution
- SchedulerRetryAction: Retry scheduling
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchedulerCronAction(BaseAction):
    """Cron-based scheduling."""
    action_type = "scheduler_cron"
    display_name = "Cron调度"
    description = "Cron定时调度"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cron scheduling."""
        cron_expression = params.get('cron', '* * * * *')
        timezone = params.get('timezone', 'UTC')
        output_var = params.get('output_var', 'schedule_info')

        try:
            import croniter

            resolved_cron = context.resolve_value(cron_expression) if context else cron_expression

            now = datetime.now()
            cron = croniter.croniter(resolved_cron, now)
            next_run = cron.get_next(datetime)

            result = {
                'cron': resolved_cron,
                'timezone': timezone,
                'next_run': next_run.isoformat(),
                'next_run_timestamp': next_run.timestamp(),
                'scheduled': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Next run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except ImportError:
            return ActionResult(success=False, message="croniter not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"Cron error: {e}")


class SchedulerIntervalAction(BaseAction):
    """Interval-based scheduling."""
    action_type = "scheduler_interval"
    display_name = "间隔调度"
    description = "间隔定时调度"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute interval scheduling."""
        interval_seconds = params.get('interval', 60)
        start_time = params.get('start_time', None)
        output_var = params.get('output_var', 'schedule_info')

        try:
            resolved_interval = context.resolve_value(interval_seconds) if context else interval_seconds

            now = datetime.now()
            if start_time:
                resolved_start = context.resolve_value(start_time) if context else start_time
                next_run = datetime.fromisoformat(resolved_start)
            else:
                next_run = now + timedelta(seconds=resolved_interval)

            result = {
                'interval_seconds': resolved_interval,
                'next_run': next_run.isoformat(),
                'next_run_timestamp': next_run.timestamp(),
                'scheduled': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Scheduled every {resolved_interval}s, next: {next_run.strftime('%H:%M:%S')}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Interval scheduler error: {e}")


class SchedulerDelayAction(BaseAction):
    """Delayed execution."""
    action_type = "scheduler_delay"
    display_name = "延迟执行"
    description = "延迟执行任务"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute delay."""
        delay_seconds = params.get('delay', 0)
        execute_at = params.get('execute_at', None)
        output_var = params.get('output_var', 'delay_info')

        try:
            resolved_delay = context.resolve_value(delay_seconds) if context else delay_seconds

            if execute_at:
                resolved_execute = context.resolve_value(execute_at) if context else execute_at
                execute_time = datetime.fromisoformat(resolved_execute)
                delay = max(0, (execute_time - datetime.now()).total_seconds())
            else:
                delay = resolved_delay
                execute_time = datetime.now() + timedelta(seconds=delay)

            result = {
                'delay_seconds': delay,
                'execute_at': execute_time.isoformat(),
                'execute_timestamp': execute_time.timestamp(),
                'scheduled': True,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Scheduled to execute in {delay:.0f}s at {execute_time.strftime('%H:%M:%S')}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Delay scheduler error: {e}")


class SchedulerRetryAction(BaseAction):
    """Retry scheduling."""
    action_type = "scheduler_retry"
    display_name = "重试调度"
    description = "重试调度任务"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute retry scheduling."""
        max_retries = params.get('max_retries', 3)
        retry_delay = params.get('retry_delay', 5)
        backoff = params.get('backoff', 'exponential')
        attempt = params.get('attempt', 0)
        output_var = params.get('output_var', 'retry_info')

        try:
            resolved_max = context.resolve_value(max_retries) if context else max_retries
            resolved_delay = context.resolve_value(retry_delay) if context else retry_delay
            resolved_attempt = context.resolve_value(attempt) if context else attempt

            if resolved_attempt >= resolved_max:
                return ActionResult(
                    success=False,
                    data={output_var: {'should_retry': False, 'attempt': resolved_attempt, 'max_retries': resolved_max}},
                    message=f"Max retries ({resolved_max}) reached"
                )

            if backoff == 'exponential':
                delay = resolved_delay * (2 ** resolved_attempt)
            elif backoff == 'linear':
                delay = resolved_delay * (resolved_attempt + 1)
            else:
                delay = resolved_delay

            result = {
                'should_retry': True,
                'attempt': resolved_attempt + 1,
                'max_retries': resolved_max,
                'delay_seconds': delay,
                'backoff': backoff,
            }

            return ActionResult(
                success=True,
                data={output_var: result},
                message=f"Retry {resolved_attempt + 1}/{resolved_max} in {delay}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry scheduler error: {e}")
