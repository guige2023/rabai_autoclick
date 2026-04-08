"""Automation retry action module for RabAI AutoClick.

Provides retry logic:
- AutomationRetryAction: Retry failed operations
- RetryPolicyAction: Define retry policies
- BackoffStrategyAction: Configure backoff
"""

import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationRetryAction(BaseAction):
    """Retry failed operations."""
    action_type = "automation_retry"
    display_name = "自动化重试"
    description = "重试失败操作"

    def __init__(self):
        super().__init__()
        self._retry_counts = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation_id = params.get("operation_id", "default")
            max_retries = params.get("max_retries", 3)
            retry_delay = params.get("retry_delay", 1.0)
            action = params.get("action", None)

            current_attempt = self._retry_counts.get(operation_id, 0)

            if current_attempt < max_retries:
                self._retry_counts[operation_id] = current_attempt + 1

                if retry_delay > 0:
                    time.sleep(retry_delay)

                return ActionResult(
                    success=True,
                    data={
                        "operation_id": operation_id,
                        "attempt": current_attempt + 1,
                        "max_retries": max_retries,
                        "retrying": True
                    },
                    message=f"Retry {operation_id}: attempt {current_attempt + 1}/{max_retries}"
                )
            else:
                self._retry_counts[operation_id] = 0
                return ActionResult(
                    success=False,
                    data={
                        "operation_id": operation_id,
                        "attempts": current_attempt,
                        "exhausted": True
                    },
                    message=f"Retry exhausted for {operation_id} after {current_attempt} attempts"
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Automation retry error: {str(e)}")


class RetryPolicyAction(BaseAction):
    """Define retry policies."""
    action_type = "retry_policy"
    display_name = "重试策略"
    description = "定义重试策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            policy_type = params.get("policy_type", "fixed")
            max_attempts = params.get("max_attempts", 3)
            base_delay = params.get("base_delay", 1.0)

            if policy_type == "fixed":
                delays = [base_delay] * max_attempts
            elif policy_type == "linear":
                delays = [base_delay * (i + 1) for i in range(max_attempts)]
            elif policy_type == "exponential":
                delays = [base_delay * (2 ** i) for i in range(max_attempts)]
            elif policy_type == "fibonacci":
                delays = [base_delay * max(1, ((1.618 ** i) - (0.618 ** i)) / (5 ** 0.5)) for i in range(max_attempts)]
            else:
                delays = [base_delay] * max_attempts

            return ActionResult(
                success=True,
                data={
                    "policy_type": policy_type,
                    "max_attempts": max_attempts,
                    "delays": delays,
                    "total_delay": sum(delays)
                },
                message=f"Retry policy '{policy_type}': max_attempts={max_attempts}, total_delay={sum(delays):.1f}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry policy error: {str(e)}")


class BackoffStrategyAction(BaseAction):
    """Configure backoff."""
    action_type = "backoff_strategy"
    display_name = "退避策略"
    description = "配置退避策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strategy = params.get("strategy", "exponential")
            base_delay = params.get("base_delay", 1.0)
            max_delay = params.get("max_delay", 60.0)
            attempt = params.get("attempt", 1)

            if strategy == "fixed":
                delay = base_delay
            elif strategy == "linear":
                delay = base_delay * attempt
            elif strategy == "exponential":
                delay = min(base_delay * (2 ** attempt), max_delay)
            elif strategy == "exponential_with_jitter":
                import random
                delay = min(base_delay * (2 ** attempt) * random.uniform(0.5, 1.5), max_delay)
            else:
                delay = base_delay

            return ActionResult(
                success=True,
                data={
                    "strategy": strategy,
                    "attempt": attempt,
                    "delay": delay,
                    "max_delay": max_delay
                },
                message=f"Backoff: strategy={strategy}, attempt={attempt}, delay={delay:.2f}s"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Backoff strategy error: {str(e)}")
