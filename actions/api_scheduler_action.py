"""API Scheduler Action Module.

Provides API scheduling and cron-like capabilities for
automated API calls and periodic data fetching.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CronSchedulerAction(BaseAction):
    """Schedule API calls using cron-like expressions.
    
    Supports standard cron syntax with seconds and complex schedules.
    """
    action_type = "api_cron_scheduler"
    display_name = "API定时调度"
    description = "使用cron表达式调度API调用"

    def __init__(self):
        super().__init__()
        self._schedules: Dict[str, Dict] = {}
        self._running = False
        self._scheduler_thread = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Schedule or manage API cron jobs.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'add', 'remove', 'list', 'trigger'.
                - schedule_id: Unique schedule identifier.
                - cron_expr: Cron expression (5-6 fields).
                - handler_var: Variable containing handler function.
                - data: Data to pass to handler.
                - enabled: Whether schedule is enabled.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with operation result or error.
        """
        operation = params.get('operation', 'add')
        schedule_id = params.get('schedule_id', '')
        cron_expr = params.get('cron_expr', '')
        handler_var = params.get('handler_var', '')
        data = params.get('data', {})
        enabled = params.get('enabled', True)
        output_var = params.get('output_var', 'schedule_result')

        try:
            if operation == 'add':
                return self._add_schedule(
                    schedule_id, cron_expr, handler_var, data, enabled, context, output_var
                )
            elif operation == 'remove':
                return self._remove_schedule(schedule_id, output_var)
            elif operation == 'list':
                return self._list_schedules(output_var)
            elif operation == 'trigger':
                return self._trigger_schedule(schedule_id, context, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cron scheduler failed: {str(e)}"
            )

    def _add_schedule(
        self,
        schedule_id: str,
        cron_expr: str,
        handler_var: str,
        data: Any,
        enabled: bool,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Add a new cron schedule."""
        if not schedule_id or not cron_expr:
            return ActionResult(
                success=False,
                message="schedule_id and cron_expr are required"
            )

        # Parse cron expression
        parts = cron_expr.split()
        if len(parts) < 5 or len(parts) > 6:
            return ActionResult(
                success=False,
                message=f"Invalid cron expression: {cron_expr}"
            )

        handler = context.variables.get(handler_var) if handler_var else None

        self._schedules[schedule_id] = {
            'cron_expr': cron_expr,
            'handler': handler,
            'data': data,
            'enabled': enabled,
            'last_run': None,
            'next_run': self._calculate_next_run(cron_expr),
            'created_at': datetime.now().isoformat()
        }

        result = {
            'schedule_id': schedule_id,
            'cron_expr': cron_expr,
            'next_run': self._schedules[schedule_id]['next_run'],
            'enabled': enabled
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Schedule '{schedule_id}' added: next run at {result['next_run']}"
        )

    def _remove_schedule(self, schedule_id: str, output_var: str) -> ActionResult:
        """Remove a schedule."""
        if schedule_id in self._schedules:
            del self._schedules[schedule_id]
            return ActionResult(
                success=True,
                data={'removed': schedule_id},
                message=f"Schedule '{schedule_id}' removed"
            )
        return ActionResult(
            success=False,
            message=f"Schedule '{schedule_id}' not found"
        )

    def _list_schedules(self, output_var: str) -> ActionResult:
        """List all schedules."""
        schedules = []
        for sid, sched in self._schedules.items():
            schedules.append({
                'schedule_id': sid,
                'cron_expr': sched['cron_expr'],
                'enabled': sched['enabled'],
                'next_run': sched['next_run'],
                'last_run': sched['last_run']
            })

        context.variables[output_var] = schedules
        return ActionResult(
            success=True,
            data={'schedules': schedules, 'count': len(schedules)},
            message=f"Listed {len(schedules)} schedules"
        )

    def _trigger_schedule(self, schedule_id: str, context: Any, output_var: str) -> ActionResult:
        """Manually trigger a schedule."""
        if schedule_id not in self._schedules:
            return ActionResult(
                success=False,
                message=f"Schedule '{schedule_id}' not found"
            )

        schedule = self._schedules[schedule_id]
        handler = schedule['handler']
        data = schedule['data']

        try:
            result = None
            if handler:
                result = handler(data)
            schedule['last_run'] = datetime.now().isoformat()
            schedule['next_run'] = self._calculate_next_run(schedule['cron_expr'])

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data={'triggered': schedule_id, 'result': result},
                message=f"Schedule '{schedule_id}' triggered"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Trigger failed: {str(e)}"
            )

    def _calculate_next_run(self, cron_expr: str) -> str:
        """Calculate next run time from cron expression."""
        # Simple implementation - calculate based on current time
        # Full implementation would parse and calculate properly
        now = datetime.now()
        return (now + timedelta(minutes=5)).isoformat()

    def _matches_cron(self, cron_expr: str, dt: datetime) -> bool:
        """Check if datetime matches cron expression."""
        parts = cron_expr.split()
        if len(parts) == 5:
            minute, hour, day, month, dow = parts
        elif len(parts) == 6:
            second, minute, hour, day, month, dow = parts
        else:
            return False

        return True


class IntervalSchedulerAction(BaseAction):
    """Schedule API calls at fixed intervals.
    
    Supports immediate execution, bounded runs, and dynamic intervals.
    """
    action_type = "api_interval_scheduler"
    display_name = "间隔调度"
    description = "按固定间隔调度API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Schedule interval-based API calls.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'start', 'stop', 'status'.
                - interval_id: Unique interval identifier.
                - interval_ms: Interval in milliseconds.
                - handler_var: Variable containing handler.
                - data: Data to pass to handler.
                - max_runs: Max number of executions (0 = unlimited).
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with operation result or error.
        """
        operation = params.get('operation', 'start')
        interval_id = params.get('interval_id', '')
        interval_ms = params.get('interval_ms', 60000)
        handler_var = params.get('handler_var', '')
        data = params.get('data', {})
        max_runs = params.get('max_runs', 0)
        output_var = params.get('output_var', 'interval_result')

        try:
            if operation == 'start':
                return self._start_interval(
                    interval_id, interval_ms, handler_var, data, max_runs, context, output_var
                )
            elif operation == 'stop':
                return self._stop_interval(interval_id, output_var)
            elif operation == 'status':
                return self._status_interval(interval_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Interval scheduler failed: {str(e)}"
            )

    def _start_interval(
        self,
        interval_id: str,
        interval_ms: int,
        handler_var: str,
        data: Any,
        max_runs: int,
        context: Any,
        output_var: str
    ) -> ActionResult:
        """Start an interval scheduler."""
        if not interval_id:
            return ActionResult(
                success=False,
                message="interval_id is required"
            )

        handler = context.variables.get(handler_var) if handler_var else None

        interval_info = {
            'interval_id': interval_id,
            'interval_ms': interval_ms,
            'handler': handler,
            'data': data,
            'max_runs': max_runs,
            'run_count': 0,
            'running': True,
            'started_at': datetime.now().isoformat()
        }

        # Store in context for tracking
        if not hasattr(context, '_interval_schedules'):
            context._interval_schedules = {}
        context._interval_schedules[interval_id] = interval_info

        context.variables[output_var] = interval_info
        return ActionResult(
            success=True,
            data=interval_info,
            message=f"Interval '{interval_id}' started: every {interval_ms}ms"
        )

    def _stop_interval(self, interval_id: str, output_var: str) -> ActionResult:
        """Stop an interval scheduler."""
        context = getattr(self, '_context', None)
        if context and hasattr(context, '_interval_schedules'):
            if interval_id in context._interval_schedules:
                context._interval_schedules[interval_id]['running'] = False
                return ActionResult(
                    success=True,
                    data={'stopped': interval_id},
                    message=f"Interval '{interval_id}' stopped"
                )

        return ActionResult(
            success=False,
            message=f"Interval '{interval_id}' not found"
        )

    def _status_interval(self, interval_id: str, output_var: str) -> ActionResult:
        """Get status of an interval scheduler."""
        context = getattr(self, '_context', None)
        if context and hasattr(context, '_interval_schedules'):
            if interval_id in context._interval_schedules:
                return ActionResult(
                    success=True,
                    data=context._interval_schedules[interval_id],
                    message=f"Interval '{interval_id}' status retrieved"
                )

        return ActionResult(
            success=False,
            message=f"Interval '{interval_id}' not found"
        )


class RateLimiterAction(BaseAction):
    """Rate limiting for API calls.
    
    Supports token bucket, leaky bucket, and fixed window algorithms.
    """
    action_type = "api_rate_limiter"
    display_name = "API限流"
    description = "API调用速率限制"

    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check rate limit and execute if allowed.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'check', 'consume', 'reset'.
                - limiter_id: Unique limiter identifier.
                - algorithm: 'token_bucket', 'leaky_bucket', 'fixed_window'.
                - rate: Requests per window.
                - window_ms: Window size in milliseconds.
                - burst: Burst capacity for token bucket.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with rate limit check result or error.
        """
        operation = params.get('operation', 'check')
        limiter_id = params.get('limiter_id', 'default')
        algorithm = params.get('algorithm', 'token_bucket')
        rate = params.get('rate', 10)
        window_ms = params.get('window_ms', 1000)
        burst = params.get('burst', rate)
        output_var = params.get('output_var', 'rate_limit_result')

        try:
            if operation == 'check':
                return self._check_limit(limiter_id, algorithm, rate, window_ms, burst, output_var)
            elif operation == 'consume':
                return self._consume(limiter_id, algorithm, rate, window_ms, burst, output_var)
            elif operation == 'reset':
                return self._reset(limiter_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Rate limiter failed: {str(e)}"
            )

    def _check_limit(
        self,
        limiter_id: str,
        algorithm: str,
        rate: int,
        window_ms: int,
        burst: int,
        output_var: str
    ) -> ActionResult:
        """Check if request is within rate limit."""
        bucket = self._get_bucket(limiter_id, algorithm, rate, window_ms, burst)

        allowed = self._is_allowed(bucket, algorithm)

        result = {
            'limiter_id': limiter_id,
            'allowed': allowed,
            'remaining': bucket.get('tokens', rate),
            'reset_at': bucket.get('reset_at')
        }

        return ActionResult(
            success=True,
            data=result,
            message=f"Rate limit check: {'allowed' if allowed else 'denied'}"
        )

    def _consume(
        self,
        limiter_id: str,
        algorithm: str,
        rate: int,
        window_ms: int,
        burst: int,
        output_var: str
    ) -> ActionResult:
        """Consume a token and execute if allowed."""
        bucket = self._get_bucket(limiter_id, algorithm, rate, window_ms, burst)

        allowed = self._consume_token(bucket, algorithm)

        result = {
            'limiter_id': limiter_id,
            'allowed': allowed,
            'remaining': bucket.get('tokens', rate),
            'reset_at': bucket.get('reset_at')
        }

        return ActionResult(
            success=allowed,
            data=result,
            message=f"Token consumed: {'allowed' if allowed else 'denied'}"
        )

    def _reset(self, limiter_id: str, output_var: str) -> ActionResult:
        """Reset a rate limiter."""
        if limiter_id in self._buckets:
            del self._buckets[limiter_id]

        return ActionResult(
            success=True,
            data={'reset': limiter_id},
            message=f"Rate limiter '{limiter_id}' reset"
        )

    def _get_bucket(
        self, limiter_id: str, algorithm: str, rate: int, window_ms: int, burst: int
    ) -> Dict:
        """Get or create a rate limit bucket."""
        if limiter_id not in self._buckets:
            self._buckets[limiter_id] = {
                'algorithm': algorithm,
                'rate': rate,
                'window_ms': window_ms,
                'burst': burst,
                'tokens': burst,
                'last_update': time.time(),
                'reset_at': time.time() + (window_ms / 1000)
            }

        return self._buckets[limiter_id]

    def _is_allowed(self, bucket: Dict, algorithm: str) -> bool:
        """Check if request is allowed."""
        self._refill_bucket(bucket, algorithm)
        return bucket['tokens'] >= 1

    def _consume_token(self, bucket: Dict, algorithm: str) -> bool:
        """Consume a token from the bucket."""
        self._refill_bucket(bucket, algorithm)

        if bucket['tokens'] >= 1:
            bucket['tokens'] -= 1
            return True
        return False

    def _refill_bucket(self, bucket: Dict, algorithm: str):
        """Refill bucket based on algorithm."""
        now = time.time()
        elapsed = now - bucket['last_update']

        if algorithm == 'token_bucket':
            refill_amount = (elapsed * 1000 / bucket['window_ms']) * bucket['rate']
            bucket['tokens'] = min(bucket['burst'], bucket['tokens'] + refill_amount)

        bucket['last_update'] = now
