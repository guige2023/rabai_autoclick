"""API retry and resilience action module for RabAI AutoClick.

Provides:
- ApiRetryStrategyAction: Configurable retry strategies
- ApiBackoffAction: Exponential and linear backoff
- ApiTimeoutAction: Request timeout management
- ApiCircuitBreakerAction: Circuit breaker pattern
- ApiFallbackAction: Fallback mechanisms
"""

import time
import random
import hashlib
from typing import Any, Dict, List, Optional, Callable
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RetryStrategy(str, Enum):
    """Retry strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"
    FIBONACCI = "fibonacci"


class CircuitState(str, Enum):
    """Circuit states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ApiRetryStrategyAction(BaseAction):
    """Configurable retry strategies for API calls."""
    action_type = "api_retry_strategy"
    display_name = "API重试策略"
    description = "可配置API重试策略"

    def __init__(self):
        super().__init__()
        self._retry_stats: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "retry")
            endpoint = params.get("endpoint", "")

            if operation == "retry":
                max_attempts = params.get("max_attempts", 3)
                strategy = params.get("strategy", RetryStrategy.EXPONENTIAL.value)
                base_delay = params.get("base_delay", 1.0)
                max_delay = params.get("max_delay", 60.0)
                jitter = params.get("jitter", True)
                retriable_errors = params.get("retriable_errors", [429, 500, 502, 503, 504])

                attempt = 0
                last_error = None

                while attempt < max_attempts:
                    attempt += 1

                    attempt_result = params.get("simulate_result", True)
                    error_code = params.get("simulate_error_code", None)

                    if error_code and error_code in retriable_errors:
                        last_error = f"HTTP {error_code}"
                        delay = self._calculate_delay(attempt, strategy, base_delay, max_delay, jitter)
                        if attempt < max_attempts:
                            time.sleep(delay)
                        continue

                    if not attempt_result:
                        last_error = "Simulated failure"
                        delay = self._calculate_delay(attempt, strategy, base_delay, max_delay, jitter)
                        if attempt < max_attempts:
                            time.sleep(delay)
                        continue

                    return ActionResult(
                        success=True,
                        data={
                            "endpoint": endpoint,
                            "attempts": attempt,
                            "strategy": strategy,
                            "total_delay": self._sum_delays(attempt - 1, strategy, base_delay, max_delay, jitter)
                        },
                        message=f"Success on attempt {attempt}/{max_attempts}"
                    )

                return ActionResult(
                    success=False,
                    data={
                        "endpoint": endpoint,
                        "attempts": attempt,
                        "strategy": strategy,
                        "error": last_error
                    },
                    message=f"Failed after {attempt} attempts"
                )

            elif operation == "simulate":
                results = []
                for delay_type in [RetryStrategy.FIXED, RetryStrategy.LINEAR, RetryStrategy.EXPONENTIAL, RetryStrategy.EXPONENTIAL_WITH_JITTER]:
                    delays = [self._calculate_delay(i, delay_type.value, 1.0, 60.0, True) for i in range(1, 6)]
                    results.append({"strategy": delay_type.value, "delays": [round(d, 2) for d in delays]})

                return ActionResult(success=True, data={"strategies": results})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Retry strategy error: {str(e)}")

    def _calculate_delay(self, attempt: int, strategy: str, base_delay: float, max_delay: float, jitter: bool) -> float:
        if strategy == RetryStrategy.FIXED.value:
            delay = base_delay
        elif strategy == RetryStrategy.LINEAR.value:
            delay = base_delay * attempt
        elif strategy == RetryStrategy.EXPONENTIAL.value:
            delay = base_delay * (2 ** (attempt - 1))
        elif strategy == RetryStrategy.EXPONENTIAL_WITH_JITTER.value:
            delay = base_delay * (2 ** (attempt - 1))
        elif strategy == RetryStrategy.FIBONACCI.value:
            fib = [1, 1, 2, 3, 5, 8]
            delay = base_delay * (fib[min(attempt - 1, len(fib) - 1)])
        else:
            delay = base_delay

        delay = min(delay, max_delay)

        if jitter and strategy == RetryStrategy.EXPONENTIAL_WITH_JITTER.value:
            delay = delay * (0.5 + random.random())

        return delay

    def _sum_delays(self, attempts: int, strategy: str, base_delay: float, max_delay: float, jitter: bool) -> float:
        total = 0
        for i in range(1, attempts + 1):
            total += self._calculate_delay(i, strategy, base_delay, max_delay, jitter)
        return round(total, 2)


class ApiBackoffAction(BaseAction):
    """Exponential and linear backoff calculation."""
    action_type = "api_backoff"
    display_name = "API退避策略"
    description = "退避时间计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "calculate")
            attempt = params.get("attempt", 1)
            base = params.get("base_delay", 1.0)
            cap = params.get("cap", 60.0)

            if operation == "calculate":
                method = params.get("method", "exponential")
                multiplier = params.get("multiplier", 1.0)

                if method == "exponential":
                    delay = min(cap, base * (2 ** (attempt - 1)) * multiplier)
                elif method == "linear":
                    delay = min(cap, base * attempt * multiplier)
                elif method == "fibonacci":
                    fib = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
                    fib_val = fib[min(attempt - 1, len(fib) - 1)]
                    delay = min(cap, base * fib_val * multiplier)
                elif method == "constant":
                    delay = min(cap, base * multiplier)
                else:
                    delay = base

                return ActionResult(
                    success=True,
                    data={"attempt": attempt, "delay": round(delay, 3), "method": method},
                    message=f"Backoff for attempt {attempt}: {delay:.2f}s ({method})"
                )

            elif operation == "sequence":
                max_attempts = params.get("max_attempts", 10)
                method = params.get("method", "exponential")
                sequence = []
                for i in range(1, max_attempts + 1):
                    d = min(cap, base * (2 ** (i - 1)))
                    sequence.append(round(d, 2))

                return ActionResult(
                    success=True,
                    data={"sequence": sequence, "method": method, "total": round(sum(sequence), 2)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Backoff error: {str(e)}")


class ApiTimeoutAction(BaseAction):
    """Request timeout management."""
    action_type = "api_timeout"
    display_name = "API超时管理"
    description = "请求超时处理"

    def __init__(self):
        super().__init__()
        self._timeouts: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "manage")
            endpoint = params.get("endpoint", "")

            if operation == "set":
                if not endpoint:
                    return ActionResult(success=False, message="endpoint required")

                connect_timeout = params.get("connect_timeout", 5.0)
                read_timeout = params.get("read_timeout", 30.0)
                write_timeout = params.get("write_timeout", 30.0)
                total_timeout = params.get("total_timeout", 60.0)

                self._timeouts[endpoint] = {
                    "connect": connect_timeout,
                    "read": read_timeout,
                    "write": write_timeout,
                    "total": total_timeout,
                    "set_at": time.time()
                }

                return ActionResult(
                    success=True,
                    data={"endpoint": endpoint, "timeouts": self._timeouts[endpoint]},
                    message=f"Timeouts set for '{endpoint}'"
                )

            elif operation == "get":
                if endpoint not in self._timeouts:
                    default_timeouts = {"connect": 5.0, "read": 30.0, "write": 30.0, "total": 60.0}
                    return ActionResult(
                        success=True,
                        data={"endpoint": endpoint, "timeouts": default_timeouts, "source": "default"}
                    )
                return ActionResult(success=True, data={"endpoint": endpoint, "timeouts": self._timeouts[endpoint], "source": "custom"})

            elif operation == "check":
                timeout_config = self._timeouts.get(endpoint, {"connect": 5.0, "read": 30.0})
                elapsed = params.get("elapsed", 0)

                would_timeout = (
                    elapsed > timeout_config.get("connect", 5.0) or
                    elapsed > timeout_config.get("read", 30.0) or
                    elapsed > timeout_config.get("total", 60.0)
                )

                return ActionResult(
                    success=not would_timeout,
                    data={"would_timeout": would_timeout, "elapsed": elapsed, "config": timeout_config}
                )

            elif operation == "list":
                return ActionResult(success=True, data={"endpoints": list(self._timeouts.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Timeout error: {str(e)}")


class ApiCircuitBreakerAction(BaseAction):
    """Circuit breaker pattern for API resilience."""
    action_type = "api_circuit_breaker"
    display_name = "API断路器"
    description = "断路器模式"

    def __init__(self):
        super().__init__()
        self._circuits: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "call")
            service = params.get("service", "default")

            if operation == "setup":
                self._circuits[service] = {
                    "state": CircuitState.CLOSED,
                    "failure_count": 0,
                    "success_count": 0,
                    "last_failure_time": None,
                    "failure_threshold": params.get("failure_threshold", 5),
                    "timeout_seconds": params.get("timeout_seconds", 60),
                    "half_open_max_calls": params.get("half_open_max_calls", 3),
                    "half_open_calls": 0,
                    "total_calls": 0,
                    "total_failures": 0
                }
                return ActionResult(success=True, data={"service": service}, message=f"Circuit '{service}' set up")

            elif operation == "call":
                if service not in self._circuits:
                    self._circuits[service] = {
                        "state": CircuitState.CLOSED,
                        "failure_count": 0,
                        "success_count": 0,
                        "last_failure_time": None,
                        "failure_threshold": 5,
                        "timeout_seconds": 60,
                        "half_open_max_calls": 3,
                        "half_open_calls": 0,
                        "total_calls": 0,
                        "total_failures": 0
                    }

                circuit = self._circuits[service]
                circuit["total_calls"] += 1

                if circuit["state"] == CircuitState.OPEN:
                    if circuit["last_failure_time"]:
                        elapsed = time.time() - circuit["last_failure_time"]
                        if elapsed >= circuit["timeout_seconds"]:
                            circuit["state"] = CircuitState.HALF_OPEN
                            circuit["half_open_calls"] = 0
                            return ActionResult(
                                success=False,
                                data={"state": CircuitState.HALF_OPEN.value, "reason": "timeout_expired"},
                                message="Circuit half-open after timeout"
                            )
                    return ActionResult(
                        success=False,
                        data={"state": CircuitState.OPEN.value, "reason": "circuit_open"},
                        message="Circuit open - call rejected"
                    )

                if circuit["state"] == CircuitState.HALF_OPEN:
                    if circuit["half_open_calls"] >= circuit["half_open_max_calls"]:
                        return ActionResult(
                            success=False,
                            data={"state": CircuitState.HALF_OPEN.value, "reason": "max_calls_reached"},
                            message="Half-open calls exhausted"
                        )
                    circuit["half_open_calls"] += 1

                success = params.get("success", True)
                if success:
                    circuit["success_count"] += 1
                    if circuit["state"] == CircuitState.HALF_OPEN:
                        circuit["state"] = CircuitState.CLOSED
                        circuit["failure_count"] = 0
                    return ActionResult(success=True, data={"state": circuit["state"].value}, message="Call succeeded")
                else:
                    circuit["failure_count"] += 1
                    circuit["total_failures"] += 1
                    circuit["last_failure_time"] = time.time()

                    if circuit["failure_count"] >= circuit["failure_threshold"]:
                        circuit["state"] = CircuitState.OPEN
                    return ActionResult(success=False, data={"state": circuit["state"].value}, message="Call failed")

            elif operation == "status":
                if service not in self._circuits:
                    return ActionResult(success=False, message=f"Circuit '{service}' not found")

                circuit = self._circuits[service]
                success_rate = circuit["total_calls"] - circuit["total_failures"]
                success_rate = success_rate / circuit["total_calls"] if circuit["total_calls"] > 0 else 1.0

                return ActionResult(
                    success=True,
                    data={
                        "service": service,
                        "state": circuit["state"].value,
                        "total_calls": circuit["total_calls"],
                        "total_failures": circuit["total_failures"],
                        "success_rate": round(success_rate, 4),
                        "failure_count": circuit["failure_count"],
                        "last_failure": circuit["last_failure_time"]
                    }
                )

            elif operation == "reset":
                if service in self._circuits:
                    self._circuits[service]["state"] = CircuitState.CLOSED
                    self._circuits[service]["failure_count"] = 0
                return ActionResult(success=True, message=f"Circuit '{service}' reset")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Circuit breaker error: {str(e)}")


class ApiFallbackAction(BaseAction):
    """Fallback mechanisms for API failures."""
    action_type = "api_fallback"
    display_name = "API降级策略"
    description = "API降级处理"

    def __init__(self):
        super().__init__()
        self._fallback_map: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "handle")
            endpoint = params.get("endpoint", "")

            if operation == "register":
                if not endpoint:
                    return ActionResult(success=False, message="endpoint required")

                self._fallback_map[endpoint] = {
                    "fallback_type": params.get("fallback_type", "static"),
                    "fallback_value": params.get("fallback_value"),
                    "fallback_endpoint": params.get("fallback_endpoint"),
                    "cache_ttl": params.get("cache_ttl", 300),
                    "last_cached": None,
                    "cached_value": None,
                    "created_at": time.time()
                }

                return ActionResult(success=True, data={"endpoint": endpoint}, message=f"Fallback registered for '{endpoint}'")

            elif operation == "handle":
                if endpoint not in self._fallback_map:
                    return ActionResult(success=False, message=f"No fallback for '{endpoint}'")

                fb = self._fallback_map[endpoint]
                primary_success = params.get("primary_success", False)

                if primary_success:
                    result = params.get("primary_result")
                    fb["cached_value"] = result
                    fb["last_cached"] = time.time()
                    return ActionResult(
                        success=True,
                        data={"source": "primary", "cached": False},
                        message="Primary succeeded, result cached"
                    )

                fb_type = fb["fallback_type"]
                if fb_type == "static":
                    return ActionResult(
                        success=True,
                        data={"source": "fallback", "fallback_type": "static", "value": fb["fallback_value"]},
                        message="Fallback: returning static value"
                    )
                elif fb_type == "cached":
                    if fb["cached_value"] and fb["last_cached"]:
                        age = time.time() - fb["last_cached"]
                        if age < fb["cache_ttl"]:
                            return ActionResult(
                                success=True,
                                data={"source": "fallback", "fallback_type": "cached", "value": fb["cached_value"], "cache_age": round(age, 1)},
                                message=f"Fallback: returning cached value (age={age:.1f}s)"
                            )
                    return ActionResult(success=False, data={"source": "fallback", "fallback_type": "cached", "reason": "cache_expired_or_empty"}, message="Fallback: cache expired")
                elif fb_type == "alternative":
                    return ActionResult(
                        success=True,
                        data={"source": "fallback", "fallback_type": "alternative", "alt_endpoint": fb["fallback_endpoint"]},
                        message=f"Fallback: redirect to {fb['fallback_endpoint']}"
                    )

                return ActionResult(success=False, message="Fallback exhausted")

            elif operation == "list":
                return ActionResult(success=True, data={"endpoints": list(self._fallback_map.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Fallback error: {str(e)}")
