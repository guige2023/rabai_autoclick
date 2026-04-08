"""Scheduler cron action module for RabAI AutoClick.

Provides cron-based scheduling evaluation for workflow automation.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional
from datetime import datetime
import croniter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CronSchedulerAction(BaseAction):
    """Calculate cron schedule times and evaluate if trigger should fire."""
    action_type = "cron_scheduler"
    display_name = "Cron调度器"
    description = "Cron调度时间计算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate cron schedule.

        Args:
            context: Execution context.
            params: Dict with keys:
                - cron_expr: Standard cron expression
                - reference_time: Reference datetime for calculation
                - count: Number of run times to compute
                - timezone: Timezone

        Returns:
            ActionResult with schedule info.
        """
        cron_expr = params.get('cron_expr', '')
        ref_time_str = params.get('reference_time', '')
        count = params.get('count', 10)
        timezone = params.get('timezone', 'UTC')

        if not cron_expr:
            return ActionResult(success=False, message="cron_expr is required")

        start = time.time()
        try:
            ref_time = datetime.now() if not ref_time_str else datetime.fromisoformat(ref_time_str)
            cron = croniter.croniter(cron_expr, ref_time)
            next_times = [cron.get_next(datetime) for _ in range(count)]
            prev_time = cron.get_prev(datetime)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Scheduled {count} runs",
                data={
                    'cron_expr': cron_expr,
                    'previous_run': prev_time.isoformat(),
                    'next_runs': [t.isoformat() for t in next_times],
                    'timezone': timezone,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cron scheduler error: {str(e)}")
