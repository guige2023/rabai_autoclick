"""API Retry V2 action module for RabAI AutoClick.

Advanced retry logic with exponential backoff, jitter,
and retry budget tracking.
"""

import time
import random
import sys
import os
from typing import Any, Dict, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiRetryV2Action(BaseAction):
    """Advanced retry with backoff, jitter, and budgets.

    Implements exponential backoff, jitter, retry budgets,
    and selective retry on specific errors.
    """
    action_type = "api_retry_v2"
    display_name = "API重试V2"
    description = "带退避和预算的高级重试"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retry with advanced backoff.

        Args:
            context: Execution context.
            params: Dict with keys: action_fn, max_attempts,
                   base_delay, max_delay, exponential_base,
                   jitter, retry_on, budget_requests, budget_window.

        Returns:
            ActionResult with retry result.
        """
        start_time = time.time()
        try:
            action_fn = params.get('action_fn')
            max_attempts = params.get('max_attempts', 3)
            base_delay = params.get('base_delay', 1.0)
            max_delay = params.get('max_delay', 60.0)
            exponential_base = params.get('exponential_base', 2)
            jitter = params.get('jitter', True)
            retry_on = params.get('retry_on', [Exception])
            budget_requests = params.get('budget_requests', 0)
            budget_window = params.get('budget_window', 60)

            retry_id = params.get('retry_id', 'default')
            if not hasattr(context, '_retry_budgets'):
                context._retry_budgets = {}
            budgets = context._retry_budgets

            # Check budget
            if budget_requests > 0:
                if retry_id not in budgets:
                    budgets[retry_id] = []
                now = time.time()
                recent = [t for t in budgets[retry_id] if now - t < budget_window]
                budgets[retry_id] = recent
                if len(recent) >= budget_requests:
                    return ActionResult(
                        success=False,
                        message=f"Retry budget exhausted: {budget_requests}/{budget_window}s",
                        data={'budget_exhausted': True},
                        duration=time.time() - start_time,
                    )

            last_error = None
            attempts = []

            for attempt in range(max_attempts):
                attempt_start = time.time()
                try:
                    if callable(action_fn):
                        result = action_fn(context, params)
                    else:
                        result = ActionResult(success=False, message="action_fn not callable")

                    if isinstance(result, ActionResult) and result.success:
                        return ActionResult(
                            success=True,
                            message=f"Succeeded on attempt {attempt + 1}",
                            data={'result': result.data, 'attempts': attempt + 1},
                            duration=time.time() - start_time,
                        )
                    last_error = result.message if isinstance(result, ActionResult) else "Unknown error"

                except Exception as e:
                    last_error = str(e)
                    should_retry = any(isinstance(e, t) for t in retry_on)
                    if not should_retry or attempt >= max_attempts - 1:
                        attempts.append({'attempt': attempt + 1, 'success': False, 'error': str(e), 'duration': time.time() - attempt_start, 'retryable': should_retry})
                        break

                attempts.append({'attempt': attempt + 1, 'success': False, 'error': last_error, 'duration': time.time() - attempt_start})

                if attempt < max_attempts - 1:
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    time.sleep(delay)

                if budget_requests > 0:
                    budgets[retry_id].append(time.time())

            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Failed after {len(attempts)} attempts: {last_error}",
                data={'attempts': attempts, 'last_error': last_error},
                duration=duration,
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Retry error: {str(e)}", duration=time.time() - start_time)
