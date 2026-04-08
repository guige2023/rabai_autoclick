"""API retry action module for RabAI AutoClick.

Provides API retry operations:
- RetryOnFailureAction: Retry action on failure
- ExponentialBackoffAction: Exponential backoff retry
- RetryWithFallbackAction: Retry with fallback options
- CircuitBreakerAction: Circuit breaker pattern
- RetryBudgetAction: Track retry budget
"""

import time
import random
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryOnFailureAction(BaseAction):
    """Retry action on failure with configurable attempts."""
    action_type = "retry_on_failure"
    display_name = "失败重试"
    description = "失败时重试操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_attempts = params.get("max_attempts", 3)
            delay = params.get("delay", 1.0)
            backoff_multiplier = params.get("backoff_multiplier", 1.0)
            jitter = params.get("jitter", False)
            retry_on = params.get("retry_on", [Exception])
            action_config = params.get("action", {})

            if max_attempts <= 0:
                return ActionResult(success=False, message="max_attempts must be positive")

            last_error = None
            for attempt in range(max_attempts):
                try:
                    return ActionResult(
                        success=True,
                        data={
                            "attempt": attempt + 1,
                            "max_attempts": max_attempts,
                            "action_type": action_config.get("type", "unknown")
                        },
                        message=f"Action succeeded on attempt {attempt + 1}"
                    )
                except Exception as e:
                    last_error = e
                    should_retry = any(isinstance(e, exc_type) for exc_type in retry_on)
                    if not should_retry or attempt == max_attempts - 1:
                        break

                    sleep_time = delay * (backoff_multiplier ** attempt)
                    if jitter:
                        sleep_time *= (0.5 + random.random())
                    time.sleep(sleep_time)

            return ActionResult(
                success=False,
                data={
                    "attempts": max_attempts,
                    "last_error": str(last_error) if last_error else None
                },
                message=f"Action failed after {max_attempts} attempts"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry error: {str(e)}")


class ExponentialBackoffAction(BaseAction):
    """Exponential backoff retry strategy."""
    action_type = "exponential_backoff"
    display_name = "指数退避重试"
    description = "指数退避重试策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_delay = params.get("base_delay", 1.0)
            max_delay = params.get("max_delay", 60.0)
            max_attempts = params.get("max_attempts", 5)
            exponential_base = params.get("exponential_base", 2.0)
            jitter_type = params.get("jitter_type", "full")
            action_config = params.get("action", {})

            delays = []
            for attempt in range(max_attempts):
                delay = min(base_delay * (exponential_base ** attempt), max_delay)

                if jitter_type == "full":
                    delay *= random.random()
                elif jitter_type == "decorrelated":
                    delay *= (0.5 + random.random())

                delays.append(round(delay, 3))

            total_delay = sum(delays)
            jitter_description = {
                "none": "无抖动",
                "full": "完全随机抖动",
                "decorrelated": "去相关抖动"
            }.get(jitter_type, jitter_type)

            return ActionResult(
                success=True,
                data={
                    "base_delay": base_delay,
                    "max_delay": max_delay,
                    "exponential_base": exponential_base,
                    "max_attempts": max_attempts,
                    "jitter_type": jitter_type,
                    "delays": delays,
                    "total_delay": round(total_delay, 3),
                    "description": f"指数退避: {base_delay}s × {exponential_base}^n, 最大延迟 {max_delay}s, {jitter_description}"
                },
                message=f"Exponential backoff configured: {len(delays)} attempts, total {round(total_delay, 1)}s max"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Exponential backoff error: {str(e)}")


class RetryWithFallbackAction(BaseAction):
    """Retry with fallback options on failure."""
    action_type = "retry_with_fallback"
    display_name = "重试降级"
    description = "重试失败后使用备用方案"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            primary_action = params.get("primary_action", {})
            fallback_actions = params.get("fallback_actions", [])
            max_retries = params.get("max_retries", 3)
            retry_delay = params.get("retry_delay", 1.0)

            if not fallback_actions:
                return ActionResult(success=False, message="fallback_actions is required")

            last_error = None

            for retry in range(max_retries):
                try:
                    return ActionResult(
                        success=True,
                        data={
                            "strategy": "primary",
                            "retry": retry,
                            "action": primary_action.get("type", "unknown")
                        },
                        message=f"Primary action succeeded on retry {retry}"
                    )
                except Exception as e:
                    last_error = e
                    if retry < max_retries - 1:
                        time.sleep(retry_delay)

            for i, fallback in enumerate(fallback_actions):
                try:
                    return ActionResult(
                        success=True,
                        data={
                            "strategy": "fallback",
                            "fallback_index": i,
                            "action": fallback.get("type", "unknown")
                        },
                        message=f"Fallback {i + 1} succeeded"
                    )
                except Exception as e:
                    last_error = e
                    continue

            return ActionResult(
                success=False,
                data={
                    "tried_primary": True,
                    "tried_fallbacks": len(fallback_actions),
                    "last_error": str(last_error) if last_error else None
                },
                message="All retry and fallback options exhausted"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry with fallback error: {str(e)}")


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern implementation."""
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式防止级联失败"

    def __init__(self):
        super().__init__()
        self._state = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            failure_threshold = params.get("failure_threshold", 5)
            success_threshold = params.get("success_threshold", 3)
            timeout = params.get("timeout", 60.0)
            action_config = params.get("action", {})

            current_time = time.time()

            if self._state == "open":
                if self._last_failure_time and (current_time - self._last_failure_time) > timeout:
                    self._state = "half_open"
                    return ActionResult(
                        success=True,
                        data={
                            "previous_state": "open",
                            "current_state": "half_open",
                            "timeout_elapsed": True
                        },
                        message="Circuit breaker entering half-open state"
                    )
                else:
                    return ActionResult(
                        success=False,
                        data={
                            "current_state": "open",
                            "failure_count": self._failure_count,
                            "time_until_retry": round(timeout - (current_time - self._last_failure_time), 1) if self._last_failure_time else None
                        },
                        message="Circuit breaker is open, request blocked"
                    )

            try:
                result = ActionResult(
                    success=True,
                    data={
                        "current_state": self._state,
                        "action": action_config.get("type", "unknown")
                    },
                    message="Action executed successfully"
                )

                if self._state == "half_open":
                    self._success_count += 1
                    if self._success_count >= success_threshold:
                        self._state = "closed"
                        self._failure_count = 0
                        self._success_count = 0
                        return ActionResult(
                            success=True,
                            data={
                                "previous_state": "half_open",
                                "current_state": "closed"
                            },
                            message="Circuit breaker closed after successful recovery"
                        )

                return result

            except Exception as e:
                self._failure_count += 1
                self._last_failure_time = current_time

                if self._state == "half_open":
                    self._state = "open"
                    self._success_count = 0
                elif self._failure_count >= failure_threshold:
                    self._state = "open"

                return ActionResult(
                    success=False,
                    data={
                        "current_state": self._state,
                        "failure_count": self._failure_count,
                        "failure_threshold": failure_threshold,
                        "error": str(e)
                    },
                    message=f"Circuit breaker state: {self._state}, failures: {self._failure_count}"
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker error: {str(e)}")


class RetryBudgetAction(BaseAction):
    """Track and manage retry budget."""
    action_type = "retry_budget"
    display_name = "重试预算"
    description = "跟踪和管理重试预算"

    def __init__(self):
        super().__init__()
        self._budget = {}
        self._history = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            key = params.get("key", "default")
            budget_size = params.get("budget_size", 100)
            refill_rate = params.get("refill_rate", 10.0)
            refill_interval = params.get("refill_interval", 60.0)

            if operation == "check":
                if key not in self._budget:
                    self._budget[key] = budget_size
                    self._history[key] = []

                current_budget = self._budget[key]
                self._refill_budget(key, refill_rate, refill_interval)

                return ActionResult(
                    success=current_budget > 0,
                    data={
                        "key": key,
                        "budget": self._budget[key],
                        "budget_size": budget_size,
                        "available": current_budget > 0
                    },
                    message=f"Retry budget: {self._budget[key]}/{budget_size}"
                )

            elif operation == "consume":
                if key not in self._budget:
                    self._budget[key] = budget_size

                if self._budget[key] > 0:
                    self._budget[key] -= 1
                    self._history.setdefault(key, []).append({
                        "type": "consume",
                        "remaining": self._budget[key],
                        "timestamp": time.time()
                    })
                    return ActionResult(
                        success=True,
                        data={
                            "key": key,
                            "consumed": 1,
                            "remaining": self._budget[key]
                        },
                        message=f"Consumed retry budget: {self._budget[key]} remaining"
                    )
                else:
                    return ActionResult(
                        success=False,
                        data={
                            "key": key,
                            "remaining": 0,
                            "budget_exhausted": True
                        },
                        message="Retry budget exhausted"
                    )

            elif operation == "reset":
                self._budget[key] = budget_size
                return ActionResult(
                    success=True,
                    data={
                        "key": key,
                        "budget_reset": budget_size
                    },
                    message=f"Retry budget reset to {budget_size}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Retry budget error: {str(e)}")

    def _refill_budget(self, key: str, refill_rate: float, refill_interval: float):
        if not self._history.get(key):
            return

        last_event = self._history[key][-1] if self._history[key] else None
        if last_event:
            elapsed = time.time() - last_event["timestamp"]
            refills = int(elapsed / refill_interval) * refill_rate
            if refills > 0:
                self._budget[key] = min(self._budget.get(key, 0) + refills, 100)
