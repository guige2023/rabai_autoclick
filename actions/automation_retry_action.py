"""Automation retry policy action module for RabAI AutoClick.

Provides retry policy and circuit breaker operations:
- RetryPolicyAction: Configurable retry policies
- CircuitBreakerAction: Circuit breaker pattern implementation
- TimeoutHandlerAction: Timeout management
- BackoffStrategyAction: Backoff strategies
"""

import time
import random
from typing import Any, Dict, List, Optional, Callable
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryPolicyState:
    """Shared retry state."""

    def __init__(self):
        self._lock = Lock()
        self._attempts: Dict[str, List[float]] = {}

    def record_attempt(self, key: str):
        with self._lock:
            if key not in self._attempts:
                self._attempts[key] = []
            self._attempts[key].append(time.time())

    def get_attempts(self, key: str, window: float = 60) -> List[float]:
        with self._lock:
            now = time.time()
            if key not in self._attempts:
                return []
            return [t for t in self._attempts[key] if now - t < window]


_retry_state = RetryPolicyState()


class RetryPolicyAction(BaseAction):
    """Configurable retry policies."""
    action_type = "retry_policy"
    display_name = "重试策略"
    description = "可配置的重试策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "retry")
            policy_name = params.get("policy_name", "default")
            max_attempts = params.get("max_attempts", 3)
            base_delay = params.get("base_delay", 1.0)
            max_delay = params.get("max_delay", 60.0)
            exponential = params.get("exponential", True)
            jitter = params.get("jitter", True)
            retry_on = params.get("retry_on", ["exception", "timeout", "rate_limit"])

            if action == "compute_delay":
                attempt = params.get("attempt", 1)

                if exponential:
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                else:
                    delay = base_delay

                if jitter:
                    delay = delay * (0.5 + random.random() * 0.5)

                return ActionResult(
                    success=True,
                    message=f"Retry delay for attempt {attempt}: {delay:.2f}s",
                    data={"delay": delay, "attempt": attempt, "policy": policy_name},
                )

            elif action == "should_retry":
                attempt = params.get("attempt", 1)
                error_type = params.get("error_type", "exception")

                should_retry = attempt < max_attempts and error_type in retry_on
                delay = 0
                if should_retry:
                    if exponential:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    else:
                        delay = base_delay
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)

                return ActionResult(
                    success=True,
                    message=f"Should retry: {should_retry}",
                    data={
                        "should_retry": should_retry,
                        "delay": delay,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                    },
                )

            elif action == "get_backoff_sequence":
                sequence = []
                for attempt in range(1, max_attempts + 1):
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    sequence.append(round(delay, 2))

                return ActionResult(
                    success=True,
                    message=f"Backoff sequence: {sequence}",
                    data={"sequence": sequence, "max_attempts": max_attempts},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"RetryPolicy error: {e}")


class CircuitBreakerState:
    """Circuit breaker shared state."""

    def __init__(self):
        self._lock = Lock()
        self._circuits: Dict[str, Dict] = {}

    def get_circuit(self, name: str) -> Dict:
        with self._lock:
            if name not in self._circuits:
                self._circuits[name] = {
                    "state": "closed",
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_time": None,
                    "last_success_time": None,
                }
            return self._circuits[name]


_cb_state = CircuitBreakerState()


class CircuitBreakerAction(BaseAction):
    """Circuit breaker pattern implementation."""
    action_type = "circuit_breaker"
    display_name = "熔断器"
    description = "熔断器模式实现"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "call")
            circuit_name = params.get("circuit_name", "default")
            failure_threshold = params.get("failure_threshold", 5)
            success_threshold = params.get("success_threshold", 3)
            timeout = params.get("timeout", 60)

            circuit = _cb_state.get_circuit(circuit_name)

            if action == "call":
                current_state = circuit["state"]

                if current_state == "open":
                    if circuit["last_failure_time"] and (time.time() - circuit["last_failure_time"]) > timeout:
                        circuit["state"] = "half_open"
                        return ActionResult(
                            success=False,
                            message="Circuit transitioning to half-open",
                            data={"circuit": circuit_name, "state": "half_open", "should_allow_request": True},
                        )
                    return ActionResult(
                        success=False,
                        message="Circuit is open",
                        data={"circuit": circuit_name, "state": "open", "should_allow_request": False},
                    )

                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_name}: {current_state}",
                    data={"circuit": circuit_name, "state": current_state, "should_allow_request": True},
                )

            elif action == "record_success":
                circuit["success_count"] += 1
                circuit["last_success_time"] = time.time()

                if circuit["state"] == "half_open":
                    if circuit["success_count"] >= success_threshold:
                        circuit["state"] = "closed"
                        circuit["failure_count"] = 0
                        circuit["success_count"] = 0

                return ActionResult(
                    success=True,
                    message=f"Success recorded for {circuit_name}",
                    data={"circuit": circuit_name, "state": circuit["state"], "success_count": circuit["success_count"]},
                )

            elif action == "record_failure":
                circuit["failure_count"] += 1
                circuit["last_failure_time"] = time.time()

                if circuit["failure_count"] >= failure_threshold:
                    circuit["state"] = "open"

                return ActionResult(
                    success=True,
                    message=f"Failure recorded for {circuit_name} (count: {circuit['failure_count']})",
                    data={"circuit": circuit_name, "state": circuit["state"], "failure_count": circuit["failure_count"]},
                )

            elif action == "status":
                return ActionResult(
                    success=True,
                    message=f"Circuit {circuit_name}: {circuit['state']}",
                    data={
                        "circuit": circuit_name,
                        "state": circuit["state"],
                        "failure_count": circuit["failure_count"],
                        "success_count": circuit["success_count"],
                    },
                )

            elif action == "reset":
                circuit["state"] = "closed"
                circuit["failure_count"] = 0
                circuit["success_count"] = 0
                return ActionResult(success=True, message=f"Circuit {circuit_name} reset")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"CircuitBreaker error: {e}")


class TimeoutHandlerAction(BaseAction):
    """Timeout management."""
    action_type = "timeout_handler"
    display_name = "超时处理"
    description = "超时管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "check")
            timeout_value = params.get("timeout", 30)
            start_time = params.get("start_time", None)

            if action == "check":
                if start_time is None:
                    start_time = time.time()

                elapsed = time.time() - start_time
                remaining = max(0, timeout_value - elapsed)
                is_timed_out = elapsed >= timeout_value

                return ActionResult(
                    success=not is_timed_out,
                    message=f"Timeout check: elapsed={elapsed:.2f}s, remaining={remaining:.2f}s",
                    data={
                        "elapsed": round(elapsed, 2),
                        "remaining": round(remaining, 2),
                        "is_timed_out": is_timed_out,
                        "timeout": timeout_value,
                    },
                )

            elif action == "remaining":
                if start_time is None:
                    start_time = time.time()
                elapsed = time.time() - start_time
                remaining = max(0, timeout_value - elapsed)
                return ActionResult(success=True, message=f"Remaining time: {remaining:.2f}s", data={"remaining": round(remaining, 2)})

            elif action == "is_expired":
                if start_time is None:
                    start_time = time.time()
                elapsed = time.time() - start_time
                is_expired = elapsed >= timeout_value
                return ActionResult(success=not is_expired, message=f"Expired: {is_expired}", data={"is_expired": is_expired})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"TimeoutHandler error: {e}")


class BackoffStrategyAction(BaseAction):
    """Backoff strategies."""
    action_type = "backoff_strategy"
    display_name = "退避策略"
    description = "退避重试策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strategy = params.get("strategy", "exponential")
            attempt = params.get("attempt", 1)
            base_delay = params.get("base_delay", 1.0)
            max_delay = params.get("max_delay", 60.0)
            jitter = params.get("jitter", True)

            if strategy == "fixed":
                delay = base_delay
            elif strategy == "linear":
                delay = base_delay * attempt
            elif strategy == "exponential":
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            elif strategy == "fibonacci":
                a, b = 1, 1
                for _ in range(attempt - 1):
                    a, b = b, a + b
                delay = min(base_delay * a, max_delay)
            elif strategy == "polynomial":
                delay = min(base_delay * (attempt ** 2), max_delay)
            else:
                delay = base_delay

            if jitter:
                if strategy == "exponential":
                    delay = delay * (0.5 + random.random() * 0.5)
                else:
                    delay = delay * (0.8 + random.random() * 0.4)

            sequence = []
            for i in range(1, min(attempt + 5, 11)):
                d = base_delay
                if strategy == "fixed":
                    d = base_delay
                elif strategy == "linear":
                    d = base_delay * i
                elif strategy == "exponential":
                    d = min(base_delay * (2 ** (i - 1)), max_delay)
                elif strategy == "fibonacci":
                    a, b = 1, 1
                    for _ in range(i - 1):
                        a, b = b, a + b
                    d = min(base_delay * a, max_delay)
                sequence.append(round(d, 2))

            return ActionResult(
                success=True,
                message=f"Backoff ({strategy}): attempt {attempt} = {delay:.2f}s",
                data={"delay": round(delay, 2), "attempt": attempt, "strategy": strategy, "sequence": sequence[:10]},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BackoffStrategy error: {e}")
