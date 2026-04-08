"""Retry Policy Action Module.

Provides configurable retry logic with exponential backoff,
jitter, and circuit breaker integration.
"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class BackoffStrategy(Enum):
    """Backoff strategy."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class RetryPolicy:
    """Retry policy configuration."""
    max_attempts: int = 3
    initial_delay_ms: float = 100.0
    max_delay_ms: float = 30000.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    retryable_errors: List[str] = field(default_factory=lambda: ["timeout", "connection", "429", "500", "502", "503"])


@dataclass
class RetryAttempt:
    """Single retry attempt."""
    attempt: int
    delay_ms: float
    error: Optional[str] = None
    success: bool = False
    duration_ms: float = 0.0


@dataclass
class RetryResult:
    """Retry operation result."""
    success: bool
    total_attempts: int
    final_error: Optional[str]
    attempts: List[RetryAttempt]
    total_duration_ms: float
    retried: bool


class RetryEngine:
    """Retry execution engine."""

    def __init__(self, policy: RetryPolicy):
        self._policy = policy

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for attempt."""
        if self._policy.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = self._policy.initial_delay_ms * (2 ** (attempt - 1))
        elif self._policy.backoff_strategy == BackoffStrategy.LINEAR:
            delay = self._policy.initial_delay_ms * attempt
        else:
            delay = self._policy.initial_delay_ms

        delay = min(delay, self._policy.max_delay_ms)

        if self._policy.jitter:
            jitter_range = delay * self._policy.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    def should_retry(self, error: str, attempt: int) -> bool:
        """Check if should retry error."""
        if attempt >= self._policy.max_attempts:
            return False

        error_lower = error.lower()
        for retryable in self._policy.retryable_errors:
            if retryable.lower() in error_lower:
                return True

        return False

    def execute(self, func: Callable, *args, **kwargs) -> RetryResult:
        """Execute function with retry."""
        attempts = []
        start_time = time.time()

        for attempt in range(1, self._policy.max_attempts + 1):
            attempt_start = time.time()

            try:
                result = func(*args, **kwargs)
                duration = (time.time() - attempt_start) * 1000

                attempts.append(RetryAttempt(
                    attempt=attempt,
                    delay_ms=0,
                    success=True,
                    duration_ms=duration
                ))

                return RetryResult(
                    success=True,
                    total_attempts=attempt,
                    final_error=None,
                    attempts=attempts,
                    total_duration_ms=(time.time() - start_time) * 1000,
                    retried=attempt > 1
                )

            except Exception as e:
                duration = (time.time() - attempt_start) * 1000
                error_msg = str(e)

                if not self.should_retry(error_msg, attempt):
                    attempts.append(RetryAttempt(
                        attempt=attempt,
                        delay_ms=0,
                        error=error_msg,
                        success=False,
                        duration_ms=duration
                    ))

                    return RetryResult(
                        success=False,
                        total_attempts=attempt,
                        final_error=error_msg,
                        attempts=attempts,
                        total_duration_ms=(time.time() - start_time) * 1000,
                        retried=attempt > 1
                    )

                delay = self.calculate_delay(attempt)
                time.sleep(delay / 1000.0)

                attempts.append(RetryAttempt(
                    attempt=attempt,
                    delay_ms=delay,
                    error=error_msg,
                    success=False,
                    duration_ms=duration
                ))

        return RetryResult(
            success=False,
            total_attempts=self._policy.max_attempts,
            final_error=attempts[-1].error if attempts else "Unknown",
            attempts=attempts,
            total_duration_ms=(time.time() - start_time) * 1000,
            retried=True
        )


_global_policies: Dict[str, RetryPolicy] = {}


class RetryPolicyAction:
    """Retry policy action.

    Example:
        action = RetryPolicyAction()

        action.define("default", max_attempts=3, backoff="exponential")
        result = action.execute("default", lambda: call_api())
    """

    def __init__(self):
        self._engines: Dict[str, RetryEngine] = {}

    def define(self, name: str,
               max_attempts: int = 3,
               initial_delay_ms: float = 100.0,
               max_delay_ms: float = 30000.0,
               backoff: str = "exponential",
               jitter: bool = True,
               retryable_errors: Optional[List[str]] = None) -> Dict[str, Any]:
        """Define retry policy."""
        try:
            strategy = BackoffStrategy(backoff)
        except ValueError:
            return {"success": False, "message": f"Invalid backoff: {backoff}"}

        policy = RetryPolicy(
            max_attempts=max_attempts,
            initial_delay_ms=initial_delay_ms,
            max_delay_ms=max_delay_ms,
            backoff_strategy=strategy,
            jitter=jitter,
            retryable_errors=retryable_errors or []
        )

        _global_policies[name] = policy
        self._engines[name] = RetryEngine(policy)

        return {
            "success": True,
            "policy": name,
            "max_attempts": max_attempts,
            "backoff": strategy.value,
            "message": f"Defined retry policy: {name}"
        }

    def get_policy(self, name: str) -> Dict[str, Any]:
        """Get policy details."""
        policy = _global_policies.get(name)
        if policy:
            return {
                "success": True,
                "policy": {
                    "name": name,
                    "max_attempts": policy.max_attempts,
                    "initial_delay_ms": policy.initial_delay_ms,
                    "max_delay_ms": policy.max_delay_ms,
                    "backoff_strategy": policy.backoff_strategy.value,
                    "jitter": policy.jitter,
                    "retryable_errors": policy.retryable_errors
                }
            }
        return {"success": False, "message": "Policy not found"}

    def execute(self, policy_name: str,
               simulate_func: bool = True) -> Dict[str, Any]:
        """Execute with retry policy (simulated)."""
        policy = _global_policies.get(policy_name)
        if not policy:
            return {"success": False, "message": "Policy not found"}

        engine = RetryEngine(policy)
        attempts = []

        for i in range(policy.max_attempts):
            delay = engine.calculate_delay(i + 1)
            attempts.append({
                "attempt": i + 1,
                "delay_ms": delay,
                "success": i == policy.max_attempts - 1
            })

        return {
            "success": True,
            "policy": policy_name,
            "total_attempts": policy.max_attempts,
            "attempts": attempts,
            "message": f"Would retry with policy {policy_name}"
        }

    def calculate_delay(self, policy_name: str,
                       attempt: int) -> Dict[str, Any]:
        """Calculate delay for attempt."""
        policy = _global_policies.get(policy_name)
        if not policy:
            return {"success": False, "message": "Policy not found"}

        engine = RetryEngine(policy)
        delay = engine.calculate_delay(attempt)

        return {
            "success": True,
            "policy": policy_name,
            "attempt": attempt,
            "delay_ms": delay,
            "backoff_strategy": policy.backoff_strategy.value
        }

    def list_policies(self) -> Dict[str, Any]:
        """List all policies."""
        return {
            "success": True,
            "policies": list(_global_policies.keys()),
            "count": len(_global_policies)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute retry policy action."""
    operation = params.get("operation", "")
    action = RetryPolicyAction()

    try:
        if operation == "define":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.define(
                name=name,
                max_attempts=params.get("max_attempts", 3),
                initial_delay_ms=params.get("initial_delay_ms", 100.0),
                max_delay_ms=params.get("max_delay_ms", 30000.0),
                backoff=params.get("backoff", "exponential"),
                jitter=params.get("jitter", True),
                retryable_errors=params.get("retryable_errors")
            )

        elif operation == "get":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.get_policy(name)

        elif operation == "execute":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.execute(name)

        elif operation == "calculate_delay":
            name = params.get("name", "")
            attempt = params.get("attempt", 1)
            if not name:
                return {"success": False, "message": "name required"}
            return action.calculate_delay(name, attempt)

        elif operation == "list":
            return action.list_policies()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Retry policy error: {str(e)}"}
