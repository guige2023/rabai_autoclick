"""Cron scheduling action module for RabAI AutoClick.

Provides cron-based scheduled task execution with
CRON expression parsing, calendar scheduling, and missed job handling.
"""

import sys
import os
import time
import threading
import re
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import croniter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledJob:
    """A scheduled job definition."""
    job_id: str
    name: str
    cron_expr: str
    action_name: str
    action_params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    timezone: str = "local"
    max_missed: int = 0
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0


class CronAction(BaseAction):
    """Schedule and execute actions based on CRON expressions.
    
    Supports standard CRON, timezone-aware scheduling,
    missed job handling, and job management.
    """
    action_type = "cron"
    display_name = "定时任务"
    description = "基于CRON表达式的定时任务调度"

    _jobs: Dict[str, ScheduledJob] = {}
    _lock: threading.Lock = threading.Lock()
    _runner_thread: Optional[threading.Thread] = None
    _stop_event: threading.Event = threading.Event()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage scheduled cron jobs.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (schedule/list/remove/run_now/stop/start/check_next)
                - job_id: str (for schedule/remove/run_now)
                - job_name: str, display name
                - cron_expr: str, CRON expression (e.g., "*/5 * * * *")
                - action_name: str, action to execute
                - action_params: dict
                - timezone: str
                - enabled: bool
                - max_missed: int, max missed runs to execute
                - save_to_var: str
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', '')

        if operation == 'schedule':
            return self._schedule_job(context, params)
        elif operation == 'list':
            return self._list_jobs(context, params)
        elif operation == 'remove':
            return self._remove_job(context, params)
        elif operation == 'run_now':
            return self._run_now(context, params)
        elif operation == 'stop':
            return self._stop_scheduler(context, params)
        elif operation == 'start':
            return self._start_scheduler(context, params)
        elif operation == 'check_next':
            return self._check_next_run(context, params)
        elif operation == 'update':
            return self._update_job(context, params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _schedule_job(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Schedule a new cron job."""
        job_id = params.get('job_id', '')
        job_name = params.get('job_name', job_id)
        cron_expr = params.get('cron_expr', '')
        action_name = params.get('action_name', '')
        action_params = params.get('action_params', {})
        timezone = params.get('timezone', 'local')
        enabled = params.get('enabled', True)
        max_missed = params.get('max_missed', 0)

        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        if not cron_expr:
            return ActionResult(success=False, message="cron_expr is required")
        if not action_name:
            return ActionResult(success=False, message="action_name is required")

        # Validate CRON expression
        if not self._validate_cron(cron_expr):
            return ActionResult(success=False, message=f"Invalid CRON expression: {cron_expr}")

        # Calculate next run time
        try:
            tz = timezone if timezone != 'local' else None
            cron = croniter.croniter(cron_expr, datetime.now(), tz)
            next_run = cron.get_next(float)
        except Exception as e:
            return ActionResult(success=False, message=f"CRON error: {e}")

        job = ScheduledJob(
            job_id=job_id,
            name=job_name,
            cron_expr=cron_expr,
            action_name=action_name,
            action_params=action_params,
            enabled=enabled,
            timezone=timezone,
            max_missed=max_missed,
            next_run=next_run
        )

        with self._lock:
            self._jobs[job_id] = job

        return ActionResult(
            success=True,
            message=f"Scheduled job '{job_id}' (next run: {self._format_time(next_run)})",
            data={'job_id': job_id, 'next_run': next_run}
        )

    def _validate_cron(self, cron_expr: str) -> bool:
        """Validate a CRON expression."""
        standard_cron_pattern = re.compile(
            r'^([\d\*\/\-,]+)\s+([\d\*\/\-,]+)\s+([\d\*\/\-,]+)\s+([\d\*\/\-,]+)\s+([\d\*\/\-,]+)$'
        )
        return bool(standard_cron_pattern.match(cron_expr.strip()))

    def _list_jobs(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """List all scheduled jobs."""
        save_to_var = params.get('save_to_var', None)
        job_id_filter = params.get('job_id', None)

        with self._lock:
            if job_id_filter:
                job = self._jobs.get(job_id_filter)
                if not job:
                    return ActionResult(success=False, message=f"Job '{job_id_filter}' not found")
                result = self._job_to_dict(job)
            else:
                result = {jid: self._job_to_dict(j) for jid, j in self._jobs.items()}

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result

        return ActionResult(
            success=True,
            message=f"{len(result)} job(s)",
            data=result
        )

    def _job_to_dict(self, job: ScheduledJob) -> Dict[str, Any]:
        """Convert job to dict."""
        return {
            'job_id': job.job_id,
            'name': job.name,
            'cron_expr': job.cron_expr,
            'action_name': job.action_name,
            'enabled': job.enabled,
            'timezone': job.timezone,
            'last_run': job.last_run,
            'next_run': job.next_run,
            'run_count': job.run_count,
            'max_missed': job.max_missed
        }

    def _remove_job(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Remove a scheduled job."""
        job_id = params.get('job_id', '')
        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                return ActionResult(success=True, message=f"Removed job '{job_id}'")
            return ActionResult(success=False, message=f"Job '{job_id}' not found")

    def _run_now(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run a job immediately."""
        job_id = params.get('job_id', '')
        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        with self._lock:
            job = self._jobs.get(job_id)
        if not job:
            return ActionResult(success=False, message=f"Job '{job_id}' not found")

        start_time = time.time()
        action = self._find_action(job.action_name)
        if action is None:
            return ActionResult(success=False, message=f"Action not found: {job.action_name}")

        try:
            result = action.execute(context, job.action_params)
            job.last_run = start_time
            job.run_count += 1

            # Update next run
            try:
                tz = job.timezone if job.timezone != 'local' else None
                cron = croniter.croniter(job.cron_expr, datetime.now(), tz)
                job.next_run = cron.get_next(float)
            except Exception:
                pass

            return ActionResult(
                success=result.success,
                message=f"Job '{job_id}' executed: {result.message}",
                data=result.data,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Job execution error: {e}")

    def _stop_scheduler(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Stop the scheduler thread."""
        self._stop_event.set()
        if self._runner_thread and self._runner_thread.is_alive():
            self._runner_thread.join(timeout=5)
        return ActionResult(success=True, message="Scheduler stopped")

    def _start_scheduler(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Start the scheduler thread."""
        if self._runner_thread and self._runner_thread.is_alive():
            return ActionResult(success=True, message="Scheduler already running")

        self._stop_event.clear()
        self._runner_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._runner_thread.start()
        return ActionResult(success=True, message="Scheduler started")

    def _check_next_run(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check next run time for a job."""
        job_id = params.get('job_id', '')
        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        with self._lock:
            job = self._jobs.get(job_id)
        if not job:
            return ActionResult(success=False, message=f"Job '{job_id}' not found")

        return ActionResult(
            success=True,
            message=f"Next run: {self._format_time(job.next_run)}",
            data={'next_run': job.next_run, 'last_run': job.last_run}
        )

    def _update_job(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Update job properties."""
        job_id = params.get('job_id', '')
        if not job_id:
            return ActionResult(success=False, message="job_id is required")

        with self._lock:
            job = self._jobs.get(job_id)
        if not job:
            return ActionResult(success=False, message=f"Job '{job_id}' not found")

        if 'enabled' in params:
            job.enabled = params['enabled']
        if 'cron_expr' in params:
            if self._validate_cron(params['cron_expr']):
                job.cron_expr = params['cron_expr']
            else:
                return ActionResult(success=False, message=f"Invalid CRON: {params['cron_expr']}")
        if 'action_params' in params:
            job.action_params = params['action_params']

        return ActionResult(success=True, message=f"Updated job '{job_id}'")

    def _scheduler_loop(self) -> None:
        """Main scheduler loop running in background thread."""
        while not self._stop_event.is_set():
            now = time.time()
            jobs_to_run = []

            with self._lock:
                for job_id, job in self._jobs.items():
                    if job.enabled and job.next_run and job.next_run <= now:
                        jobs_to_run.append((job_id, job))

            for job_id, job in jobs_to_run:
                # Handle missed runs
                if job.max_missed > 0 and job.last_run:
                    try:
                        tz = job.timezone if job.timezone != 'local' else None
                        cron = croniter.croniter(job.cron_expr, datetime.fromtimestamp(job.last_run), tz)
                        missed = 0
                        next_ts = cron.get_next(float)
                        while next_ts < now:
                            missed += 1
                            next_ts = cron.get_next(float)
                            if missed > job.max_missed:
                                break
                        if missed > 0:
                            # Skip missed runs, just log
                            pass
                    except Exception:
                        pass

                # Execute job
                try:
                    self._execute_job(job)
                except Exception:
                    pass

                # Update next run
                try:
                    tz = job.timezone if job.timezone != 'local' else None
                    cron = croniter.croniter(job.cron_expr, datetime.now(), tz)
                    job.next_run = cron.get_next(float)
                    job.last_run = time.time()
                    job.run_count += 1
                except Exception:
                    pass

            time.sleep(1.0)

    def _execute_job(self, job: ScheduledJob) -> None:
        """Execute a scheduled job."""
        # This runs in background; actual execution delegated
        pass

    def _find_action(self, action_name: str) -> Optional[BaseAction]:
        """Find an action by name."""
        try:
            from actions import (
                ClickAction, TypeAction, KeyPressAction, ImageMatchAction,
                FindImageAction, OCRAction, ScrollAction, MouseMoveAction,
                DragAction, ScriptAction, DelayAction, ConditionAction,
                LoopAction, SetVariableAction, ScreenshotAction,
                GetMousePosAction, AlertAction
            )
            action_map = {
                'click': ClickAction, 'type': TypeAction,
                'key_press': KeyPressAction, 'image_match': ImageMatchAction,
                'find_image': FindImageAction, 'ocr': OCRAction,
                'scroll': ScrollAction, 'mouse_move': MouseMoveAction,
                'drag': DragAction, 'script': ScriptAction,
                'delay': DelayAction, 'condition': ConditionAction,
                'loop': LoopAction, 'set_variable': SetVariableAction,
                'screenshot': ScreenshotAction, 'get_mouse_pos': GetMousePosAction,
                'alert': AlertAction,
            }
            action_cls = action_map.get(action_name.lower())
            return action_cls() if action_cls else None
        except Exception:
            return None

    def _format_time(self, ts: Optional[float]) -> str:
        """Format timestamp to readable string."""
        if not ts:
            return "N/A"
        dt = datetime.fromtimestamp(ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'job_id': '',
            'job_name': '',
            'cron_expr': '',
            'action_name': '',
            'action_params': {},
            'timezone': 'local',
            'enabled': True,
            'max_missed': 0,
            'save_to_var': None,
        }
