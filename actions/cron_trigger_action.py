"""Cron trigger action module for RabAI AutoClick.

Provides cron-based workflow triggering and scheduling evaluation.
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


class CronTriggerAction(BaseAction):
    """Evaluate cron expressions and trigger workflows at scheduled times."""
    action_type = "cron_trigger"
    display_name = "Cron触发器"
    description = "Cron表达式工作流触发"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Evaluate cron and determine if should trigger.

        Args:
            context: Execution context.
            params: Dict with keys:
                - cron_expr: Cron expression (e.g. '*/5 * * * *')
                - base_time: Reference time for evaluation
                - timezone: Timezone name

        Returns:
            ActionResult with trigger evaluation.
        """
        cron_expr = params.get('cron_expr', '')
        base_time_str = params.get('base_time', '')
        timezone = params.get('timezone', 'UTC')

        if not cron_expr:
            return ActionResult(success=False, message="cron_expr is required")

        start = time.time()
        try:
            base_time = datetime.now() if not base_time_str else datetime.fromisoformat(base_time_str)
            cron = croniter.croniter(cron_expr, base_time)
            prev_run = cron.get_prev(datetime)
            next_run = cron.get_next(datetime)
            is_due = (base_time - prev_run) < timedelta(minutes=1)
            duration = time.time() - start
            return ActionResult(
                success=True,
                message=f"{'Trigger DUE' if is_due else 'Not yet due'}",
                data={
                    'cron_expr': cron_expr,
                    'previous_run': prev_run.isoformat(),
                    'next_run': next_run.isoformat(),
                    'is_due': is_due,
                    'timezone': timezone,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cron trigger error: {str(e)}")
