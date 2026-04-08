"""
Retry and backoff utilities - retry with backoff, circuit breaker, timeout handling.
"""
from typing import Any, Dict, Optional, Callable
import time
import logging
import threading

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    STATES = {"closed": 0, "open": 1, "half_open": 2}

    def __init__(self, failure_threshold: int = 5, timeout: float = 60.0) -> None:
        self._failure_threshold = failure_threshold
        self._timeout = timeout
        self._state = self.STATES["closed"]
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        with self._lock:
            if self._state == self.STATES["open"]:
                if time.time() - self._last_failure_time >= self._timeout:
                    self._state = self.STATES["half_open"]
                else:
                    raise Exception("Circuit breaker is OPEN")
        result = fn(*args, **kwargs)
        with self._lock:
            self._failure_count = 0
            self._state = self.STATES["closed"]
        return result

    def record_failure(self) -> None:
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self._failure_threshold:
                self._state = self.STATES["open"]

    def state(self) -> str:
        with self._lock:
            return {v: k for k, v in self.STATES.items()}[self._state]


class RetryAction(BaseAction):
    """Retry and backoff operations.

    Provides retry with exponential backoff, circuit breaker, timeout simulation.
    """

    def __init__(self) -> None:
        self._breakers: Dict[str, CircuitBreaker] = {}

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "backoff")
        name = params.get("name", "default")

        try:
            if operation == "backoff":
                attempt = int(params.get("attempt", 0))
                base_delay = float(params.get("base_delay", 1.0))
                max_delay = float(params.get("max_delay", 60.0))
                exponential = params.get("exponential", True)
                jitter = params.get("jitter", True)
                if exponential:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                else:
                    delay = min(base_delay * (attempt + 1), max_delay)
                if jitter:
                    import random
                    delay = delay * (0.5 + random.random())
                return {"success": True, "delay_seconds": round(delay, 3), "attempt": attempt}

            elif operation == "retry_config":
                max_attempts = int(params.get("max_attempts", 3))
                base_delay = float(params.get("base_delay", 1.0))
                return {"success": True, "max_attempts": max_attempts, "base_delay": base_delay}

            elif operation == "circuit_breaker_create":
                failure_threshold = int(params.get("failure_threshold", 5))
                timeout = float(params.get("timeout", 60.0))
                self._breakers[name] = CircuitBreaker(failure_threshold, timeout)
                return {"success": True, "name": name, "state": "closed"}

            elif operation == "circuit_breaker_state":
                if name not in self._breakers:
                    return {"success": False, "error": f"Circuit breaker {name} not found"}
                state = self._breakers[name].state()
                return {"success": True, "name": name, "state": state}

            elif operation == "circuit_breaker_record":
                if name not in self._breakers:
                    return {"success": False, "error": f"Circuit breaker {name} not found"}
                self._breakers[name].record_failure()
                return {"success": True, "name": name, "state": self._breakers[name].state()}

            elif operation == "should_retry":
                attempt = int(params.get("attempt", 0))
                max_attempts = int(params.get("max_attempts", 3))
                error_type = params.get("error_type", "transient")
                retryable = error_type in ("transient", "timeout", "network")
                should_retry = attempt < max_attempts - 1 and retryable
                return {"success": True, "should_retry": should_retry, "attempt": attempt, "max_attempts": max_attempts}

            elif operation == "timeout":
                timeout_seconds = float(params.get("timeout_seconds", 5))
                start_time = time.time()
                return {"success": True, "timeout_seconds": timeout_seconds, "started_at": start_time}

            elif operation == "check_timeout":
                started_at = float(params.get("started_at", time.time()))
                timeout_seconds = float(params.get("timeout_seconds", 5))
                elapsed = time.time() - started_at
                exceeded = elapsed > timeout_seconds
                remaining = max(0, timeout_seconds - elapsed)
                return {"success": True, "elapsed": round(elapsed, 3), "remaining": round(remaining, 3), "exceeded": exceeded}

            elif operation == "delayed_retry":
                attempt = int(params.get("attempt", 0))
                base_delay = float(params.get("base_delay", 1.0))
                max_delay = float(params.get("max_delay", 30.0))
                delay = min(base_delay * (2 ** attempt), max_delay)
                import random
                delay = delay * (0.5 + random.random())
                return {"success": True, "delay_seconds": round(delay, 3), "attempt": attempt, "can_retry": True}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"RetryAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return RetryAction().execute(context, params)
