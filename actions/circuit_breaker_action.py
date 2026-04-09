"""
Circuit breaker action for fault tolerance and degradation handling.

Provides failure tracking with half-open state for recovery attempts.
"""

from typing import Any, Optional
import time
import threading


class CircuitBreakerAction:
    """Circuit breaker for fault tolerance."""

    STATES = ("closed", "open", "half_open")

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 3,
        half_open_max_calls: int = 3,
    ) -> None:
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Failures before opening circuit
            recovery_timeout: Seconds before attempting recovery
            success_threshold: Successes in half-open to close
            half_open_max_calls: Max calls in half-open state
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.half_open_max_calls = half_open_max_calls

        self._state = "closed"
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute circuit breaker operation.

        Args:
            params: Dictionary containing:
                - operation: 'call', 'status', 'reset', 'force_open'
                - service: Service name
                - action: Action to execute (for call)

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "call")

        if operation == "call":
            return self._execute_call(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "reset":
            return self._reset(params)
        elif operation == "force_open":
            return self._force_open(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _execute_call(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute call through circuit breaker."""
        service = params.get("service", "default")
        action = params.get("action")
        fallback = params.get("fallback")

        with self._lock:
            if self._state == "open":
                if self._should_attempt_recovery():
                    self._to_half_open()
                else:
                    return {
                        "success": False,
                        "error": "Circuit is open",
                        "state": self._state,
                        "service": service,
                        "fallback_used": fallback is not None,
                    }

            if self._state == "half_open":
                if self._half_open_calls >= self.half_open_max_calls:
                    return {
                        "success": False,
                        "error": "Half-open call limit reached",
                        "state": self._state,
                        "service": service,
                    }
                self._half_open_calls += 1

        try:
            result = self._execute_action(action, params)
            self._on_success()
            return {
                "success": True,
                "result": result,
                "state": self._state,
                "service": service,
            }
        except Exception as e:
            self._on_failure()
            fallback_result = self._execute_fallback(fallback, params)
            return {
                "success": False,
                "error": str(e),
                "fallback_used": fallback_result is not None,
                "fallback_result": fallback_result,
                "state": self._state,
                "service": service,
            }

    def _execute_action(self, action: Any, params: dict[str, Any]) -> Any:
        """Execute the actual action."""
        if callable(action):
            return action(params)
        return {"message": "Action executed", "params": params}

    def _execute_fallback(self, fallback: Any, params: dict[str, Any]) -> Any:
        """Execute fallback if provided."""
        if callable(fallback):
            return fallback(params)
        return None

    def _should_attempt_recovery(self) -> bool:
        """Check if enough time has passed for recovery attempt."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.recovery_timeout

    def _to_half_open(self) -> None:
        """Transition to half-open state."""
        self._state = "half_open"
        self._half_open_calls = 0
        self._success_count = 0

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            if self._state == "half_open":
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = "closed"
                    self._failure_count = 0
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == "half_open":
                self._state = "open"
            elif self._failure_count >= self.failure_threshold:
                self._state = "open"

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get circuit breaker status."""
        with self._lock:
            return {
                "success": True,
                "state": self._state,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "recovery_timeout": self.recovery_timeout,
                "failure_threshold": self.failure_threshold,
            }

    def _reset(self, params: dict[str, Any]) -> dict[str, Any]:
        """Reset circuit breaker to closed state."""
        with self._lock:
            self._state = "closed"
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._half_open_calls = 0
        return {"success": True, "state": self._state}

    def _force_open(self, params: dict[str, Any]) -> dict[str, Any]:
        """Force circuit breaker to open state."""
        with self._lock:
            self._state = "open"
            self._last_failure_time = time.time()
        return {"success": True, "state": self._state}
