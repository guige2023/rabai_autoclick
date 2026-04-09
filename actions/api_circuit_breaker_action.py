"""API Circuit Breaker Action Module.

Implements the circuit breaker pattern for API calls to prevent
cascading failures and provide graceful degradation.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"         # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3
    excluded_status_codes: List[int] = field(default_factory=list)


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker monitoring."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    state_transitions: int = 0
    total_open_time: float = 0.0
    last_state_change: float = 0.0


class CircuitBreaker:
    """Circuit breaker implementation thread-safe."""

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._stats = CircuitBreakerStats()
        self._lock = threading.RLock()
        self._last_failure: float = 0.0
        self._half_open_calls: int = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() >= self._last_failure + self.config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._stats.successful_calls += 1
            self._stats.consecutive_failures = 0
            self._stats.consecutive_successes += 1
            self._stats.last_success_time = time.time()
            self._stats.total_calls += 1

            if self._state == CircuitState.HALF_OPEN:
                if self._stats.consecutive_successes >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)

    def record_failure(self, status_code: Optional[int] = None) -> None:
        """Record a failed call."""
        with self._lock:
            self._stats.failed_calls += 1
            self._stats.consecutive_successes = 0
            self._stats.consecutive_failures += 1
            self._stats.last_failure_time = time.time()
            self._stats.total_calls += 1

            # Check if failure should be excluded
            if status_code and status_code in self.config.excluded_status_codes:
                return

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif (self._state == CircuitState.CLOSED and
                  self._stats.consecutive_failures >= self.config.failure_threshold):
                self._transition_to(CircuitState.OPEN)

    def can_execute(self) -> bool:
        """Check if a call can be executed."""
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                return False
            # HALF_OPEN
            return self._half_open_calls < self.config.half_open_max_calls

    def on_execute(self) -> None:
        """Called when a call starts in half-open state."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1

    def get_stats(self) -> CircuitBreakerStats:
        """Get current statistics."""
        with self._lock:
            return CircuitBreakerStats(
                total_calls=self._stats.total_calls,
                successful_calls=self._stats.successful_calls,
                failed_calls=self._stats.failed_calls,
                rejected_calls=self._stats.rejected_calls,
                consecutive_failures=self._stats.consecutive_failures,
                consecutive_successes=self._stats.consecutive_successes,
                last_failure_time=self._stats.last_failure_time,
                last_success_time=self._stats.last_success_time,
                state_transitions=self._stats.state_transitions,
                last_state_change=self._stats.last_state_change,
            )

    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state == new_state:
            return
        old_state = self._state
        self._state = new_state
        self._stats.state_transitions += 1
        self._stats.last_state_change = time.time()
        self._half_open_calls = 0
        self._stats.consecutive_successes = 0
        self._stats.consecutive_failures = 0

        if old_state == CircuitState.OPEN and new_state != CircuitState.OPEN:
            # Was open, now closing
            pass
        elif new_state == CircuitState.OPEN:
            self._last_failure = time.time()

        logger.info(f"Circuit '{self.name}': {old_state.value} -> {new_state.value}")

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._stats = CircuitBreakerStats()
            self._half_open_calls = 0


class APICircuitBreakerAction(BaseAction):
    """API Circuit Breaker Action for fault tolerance.

    Wraps API calls with circuit breaker pattern to prevent
    cascading failures and enable graceful degradation.

    Examples:
        >>> action = APICircuitBreakerAction()
        >>> result = action.execute(ctx, {
        ...     "url": "https://api.example.com/data",
        ...     "failure_threshold": 5,
        ...     "timeout_seconds": 30.0,
        ... })
    """

    action_type = "api_circuit_breaker"
    display_name = "API熔断器"
    description = "防止API级联失败，提供熔断和半开状态"

    # Class-level circuit breakers shared across instances
    _breakers: Dict[str, CircuitBreaker] = {}
    _breakers_lock = threading.Lock()

    def __init__(self):
        super().__init__()

    @classmethod
    def get_breaker(cls, name: str) -> Optional[CircuitBreaker]:
        """Get existing circuit breaker by name."""
        with cls._breakers_lock:
            return cls._breakers.get(name)

    @classmethod
    def get_or_create_breaker(
        cls, name: str, config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker."""
        with cls._breakers_lock:
            if name not in cls._breakers:
                cls._breakers[name] = CircuitBreaker(name, config)
            return cls._breakers[name]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute API call with circuit breaker protection.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Target API URL
                - breaker_name: Name for this circuit (default: url host)
                - failure_threshold: Failures before opening (default: 5)
                - success_threshold: Successes to close (default: 3)
                - timeout_seconds: Time before half-open (default: 30)
                - excluded_status_codes: Status codes that don't count as failure

        Returns:
            ActionResult with circuit status and call result.
        """
        import urllib.request
        import urllib.error

        url = params.get("url")
        if not url:
            return ActionResult(success=False, message="Missing 'url' parameter")

        from urllib.parse import urlparse
        host = urlparse(url).netloc
        breaker_name = params.get("breaker_name", f"api_circuit_{host}")

        # Get or create circuit breaker
        config = CircuitBreakerConfig(
            failure_threshold=params.get("failure_threshold", 5),
            success_threshold=params.get("success_threshold", 3),
            timeout_seconds=params.get("timeout_seconds", 30.0),
            half_open_max_calls=params.get("half_open_max_calls", 3),
            excluded_status_codes=params.get("excluded_status_codes", []),
        )

        breaker = self.get_or_create_breaker(breaker_name, config)

        # Check if circuit allows execution
        if not breaker.can_execute():
            breaker._stats.rejected_calls += 1
            return ActionResult(
                success=False,
                message=f"Circuit breaker OPEN for '{breaker_name}'. "
                        f"Retry after {breaker.config.timeout_seconds}s.",
                data={
                    "circuit_state": CircuitState.OPEN.value,
                    "breaker_name": breaker_name,
                    "wait_seconds": breaker.config.timeout_seconds,
                    "stats": self._stats_to_dict(breaker.get_stats()),
                }
            )

        # Track half-open call
        breaker.on_execute()

        # Execute the actual API call
        try:
            headers = params.get("headers", {})
            data = params.get("data")
            method = params.get("method", "GET").upper()

            req = urllib.request.Request(url, headers=headers, method=method)
            if data:
                if isinstance(data, dict):
                    import urllib.parse
                    req.data = urllib.parse.urlencode(data).encode()
                elif isinstance(data, (str, bytes)):
                    req.data = data.encode() if isinstance(data, str) else data

            timeout = params.get("timeout", 30.0)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                content = response.read()
                status = response.status

                breaker.record_success()

                return ActionResult(
                    success=True,
                    message=f"API call succeeded (circuit: {breaker.state.value})",
                    data={
                        "status_code": status,
                        "content_length": len(content),
                        "circuit_state": breaker.state.value,
                        "breaker_name": breaker_name,
                        "stats": self._stats_to_dict(breaker.get_stats()),
                    }
                )

        except urllib.error.HTTPError as e:
            breaker.record_failure(e.code)
            return ActionResult(
                success=False,
                message=f"HTTP error {e.code}: {e.reason}",
                data={
                    "status_code": e.code,
                    "circuit_state": breaker.state.value,
                    "breaker_name": breaker_name,
                    "stats": self._stats_to_dict(breaker.get_stats()),
                }
            )

        except urllib.error.URLError as e:
            breaker.record_failure()
            return ActionResult(
                success=False,
                message=f"URL error: {e.reason}",
                data={
                    "circuit_state": breaker.state.value,
                    "breaker_name": breaker_name,
                    "stats": self._stats_to_dict(breaker.get_stats()),
                }
            )

        except Exception as e:
            breaker.record_failure()
            logger.exception("Circuit breaker call failed")
            return ActionResult(
                success=False,
                message=f"Call failed: {str(e)}",
                data={
                    "circuit_state": breaker.state.value,
                    "breaker_name": breaker_name,
                    "stats": self._stats_to_dict(breaker.get_stats()),
                }
            )

    def _stats_to_dict(self, stats: CircuitBreakerStats) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            "total_calls": stats.total_calls,
            "successful_calls": stats.successful_calls,
            "failed_calls": stats.failed_calls,
            "rejected_calls": stats.rejected_calls,
            "consecutive_failures": stats.consecutive_failures,
            "consecutive_successes": stats.consecutive_successes,
            "state_transitions": stats.state_transitions,
            "last_failure_time": stats.last_failure_time,
            "last_success_time": stats.last_success_time,
        }

    def get_required_params(self) -> List[str]:
        return ["url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "breaker_name": "",
            "failure_threshold": 5,
            "success_threshold": 3,
            "timeout_seconds": 30.0,
            "half_open_max_calls": 3,
            "excluded_status_codes": [],
            "method": "GET",
            "headers": {},
            "data": None,
            "timeout": 30.0,
        }
