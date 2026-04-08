"""Advanced scheduler action module for RabAI AutoClick.

Provides cron-based scheduling, delayed execution, and interval-based triggers.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import croniter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CronScheduleAction(BaseAction):
    """Schedule actions based on cron expressions.

    Evaluates cron expression and determines next run times.
    """
    action_type = "scheduler_cron"
    display_name = "Cron定时调度"
    description = "Cron表达式定时调度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate cron expression.

        Args:
            context: Execution context.
            params: Dict with keys:
                - cron_expr: Cron expression (e.g. '0 8 * * *')
                - base_time: Base time for calculation (ISO format, default: now)
                - count: Number of next run times to compute (default: 5)
                - timezone: Timezone (default: UTC)

        Returns:
            ActionResult with next run times.
        """
        cron_expr = params.get('cron_expr', '')
        base_time_str = params.get('base_time', '')
        count = params.get('count', 5)
        timezone = params.get('timezone', 'UTC')

        if not cron_expr:
            return ActionResult(success=False, message="cron_expr is required")

        start = time.time()
        try:
            if base_time_str:
                base_time = datetime.fromisoformat(base_time_str.replace('Z', '+00:00'))
            else:
                base_time = datetime.now()

            cron = croniter.croniter(cron_expr, base_time)
            next_runs = [cron.get_next(datetime) for _ in range(count)]

            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Computed {count} next run times",
                data={
                    'cron_expr': cron_expr,
                    'next_runs': [t.isoformat() for t in next_runs],
                    'timezone': timezone,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cron parse error: {str(e)}")


class IntervalScheduleAction(BaseAction):
    """Calculate interval-based scheduling."""
    action_type = "scheduler_interval"
    display_name = "间隔调度"
    description = "固定间隔重复调度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate interval schedule.

        Args:
            context: Execution context.
            params: Dict with keys:
                - interval_seconds: Interval in seconds
                - start_time: Start time (ISO format)
                - count: Number of executions to compute

        Returns:
            ActionResult with scheduled times.
        """
        interval_seconds = params.get('interval_seconds', 60)
        start_time_str = params.get('start_time', '')
        count = params.get('count', 5)

        if interval_seconds <= 0:
            return ActionResult(success=False, message="interval_seconds must be positive")

        start = time.time()
        try:
            if start_time_str:
                start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            else:
                start_time = datetime.now()

            scheduled_times = [start_time + timedelta(seconds=interval_seconds * i) for i in range(count)]

            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"Scheduled {count} executions every {interval_seconds}s",
                data={
                    'interval_seconds': interval_seconds,
                    'scheduled_times': [t.isoformat() for t in scheduled_times],
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Interval schedule error: {str(e)}")


class DelayAction(BaseAction):
    """Delay execution for specified duration."""
    action_type = "scheduler_delay"
    display_name = "延时执行"
    description = "延时等待执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Wait/delay execution.

        Args:
            context: Execution context.
            params: Dict with keys:
                - seconds: Number of seconds to wait
                - message: Optional message to return

        Returns:
            ActionResult after delay.
        """
        seconds = params.get('seconds', 1)
        message = params.get('message', 'Delay completed')

        if seconds < 0:
            return ActionResult(success=False, message="seconds must be non-negative")
        if seconds > 3600:
            return ActionResult(success=False, message="seconds exceeds maximum (3600)")

        start = time.time()
        time.sleep(seconds)
        duration = time.time() - start
        return ActionResult(
            success=True,
            message=message,
            data={'waited_seconds': duration, 'requested_seconds': seconds},
            duration=duration
        )
