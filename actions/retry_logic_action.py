"""Retry logic action module for RabAI AutoClick.

Provides retry mechanisms:
- RetryExecutor: Execute with retry logic
- ExponentialBackoff: Exponential backoff calculator
- JitteredRetry: Random jitter for retries
- CircuitBreaker: Circuit breaker pattern
- FallbackChain: Chain of fallback actions
"""

import time
import random
import threading
from typing import Any, Callable, Dict, List, Optional, Type
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class BackoffType(Enum):
    """Backoff strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    JITTER = "jitter"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_type: BackoffType = BackoffType.EXPONENTIAL
    multiplier: float = 2.0
    jitter: bool = True
    jitter_max: float = 1.0
    retryable_exceptions: List[Type[Exception]] = None
    on_retry: Optional[Callable[[Exception, int], None]] = None

    def __post_init__(self):
        if self.retryable_exceptions is None:
            self.retryable_exceptions = [Exception]


@dataclass
class CircuitState:
    """Circuit breaker state."""
    state: str = "closed"  # closed, open, half_open
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0
    last_state_change: float = 0.0


class ExponentialBackoff:
    """Calculate exponential backoff delays."""

    @staticmethod
    def calculate(
        attempt: int,
        initial_delay: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        jitter_max: float = 1.0,
    ) -> float:
        """Calculate delay with exponential backoff."""
        delay = min(initial_delay * (multiplier ** attempt), max_delay)

        if jitter:
            jitter_amount = delay * random.uniform(0, jitter_max)
            delay += jitter_amount

        return delay

    @staticmethod
    def linear(attempt: int, delay: float = 1.0, max_delay: float = 60.0) -> float:
        """Linear increasing delay."""
        return min(delay * (attempt + 1), max_delay)

    @staticmethod
    def fibonacci(attempt: int, initial_delay: float = 1.0, max_delay: float = 60.0) -> float:
        """Fibonacci-based backoff."""
        a, b = 0, 1
        for _ in range(attempt):
            a, b = b, a + b
        delay = min(initial_delay * a, max_delay)
        return delay


class RetryExecutor:
    """Execute functions with retry logic."""

    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()

    def execute(self, func: Callable[[], Any], *args, **kwargs) -> Any:
        """Execute function with retry logic."""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                last_exception = e

                if not self._is_retryable(e):
                    raise

                if attempt < self.config.max_attempts - 1:
                    delay = ExponentialBackoff.calculate(
                        attempt=attempt,
                        initial_delay=self.config.initial_delay,
                        multiplier=self.config.multiplier,
                        max_delay=self.config.max_delay,
                        jitter=self.config.jitter,
                        jitter_max=self.config.jitter_max,
                    )

                    if self.config.on_retry:
                        self.config.on_retry(e, attempt)

                    time.sleep(delay)

        if last_exception:
            raise last_exception

    def _is_retryable(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        for exc_type in self.config.retryable_exceptions:
            if isinstance(exception, exc_type):
                return True
        return False


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(
        self,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.state = CircuitState()
        self._lock = threading.Lock()

    def call(self, func: Callable[[], Any], *args, **kwargs) -> Any:
        """Execute with circuit breaker."""
        with self._lock:
            if self._is_open():
                if self._should_attempt_reset():
                    self._set_half_open()
                else:
                    raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise

    def _is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state.state == "open"

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset."""
        return time.time() - self.state.last_failure_time >= self.timeout

    def _set_half_open(self):
        """Set circuit to half-open state."""
        self.state.state = "half_open"
        self.state.last_state_change = time.time()

    def _on_success(self):
        """Handle successful call."""
        with self._lock:
            if self.state.state == "half_open":
                self.state.success_count += 1
                if self.state.success_count >= self.success_threshold:
                    self.state.state = "closed"
                    self.state.failure_count = 0
                    self.state.success_count = 0
                    self.state.last_state_change = time.time()

    def _on_failure(self):
        """Handle failed call."""
        with self._lock:
            self.state.failure_count += 1
            self.state.last_failure_time = time.time()

            if self.state.state == "half_open":
                self.state.state = "open"
                self.state.last_state_change = time.time()
            elif self.state.failure_count >= self.failure_threshold:
                self.state.state = "open"
                self.state.last_state_change = time.time()


class FallbackChain:
    """Chain of fallback actions."""

    def __init__(self):
        self._chain: List[tuple[Callable, Dict]] = []

    def add_fallback(self, func: Callable, priority: int = 0, params: Dict = None) -> "FallbackChain":
        """Add a fallback to the chain."""
        self._chain.append((func, params or {}))
        self._chain.sort(key=lambda x: x[1].get("priority", 0))
        return self

    def execute(self, *args, **kwargs) -> Any:
        """Execute chain until one succeeds."""
        last_error = None

        for func, params in self._chain:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise last_error


class RetryLogicAction(BaseAction):
    """Retry logic action with multiple strategies."""
    action_type = "retry_logic"
    display_name = "重试逻辑"
    description = "多种重试策略的执行器"

    def __init__(self):
        super().__init__()
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_type = params.get("type", "retry")
            func = params.get("func")
            args = params.get("args", [])
            kwargs = params.get("kwargs", {})

            if action_type == "retry":
                return self._execute_retry(func, args, kwargs, params)
            elif action_type == "circuit_breaker":
                return self._execute_circuit_breaker(func, args, kwargs, params)
            elif action_type == "fallback":
                return self._execute_fallback(func, args, kwargs, params)
            else:
                return ActionResult(success=False, message=f"Unknown type: {action_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Retry logic error: {str(e)}")

    def _execute_retry(self, func: Callable, args: List, kwargs: Dict, params: Dict) -> ActionResult:
        """Execute with retry."""
        config = RetryConfig(
            max_attempts=params.get("max_attempts", 3),
            initial_delay=params.get("initial_delay", 1.0),
            max_delay=params.get("max_delay", 60.0),
            multiplier=params.get("multiplier", 2.0),
            jitter=params.get("jitter", True),
        )

        executor = RetryExecutor(config)
        start_time = time.time()

        try:
            result = executor.execute(func, *args, **kwargs)
            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Success after retry",
                data={"duration": duration, "result": result},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed after retries: {str(e)}")

    def _execute_circuit_breaker(self, func: Callable, args: List, kwargs: Dict, params: Dict) -> ActionResult:
        """Execute with circuit breaker."""
        name = params.get("name", "default")
        failure_threshold = params.get("failure_threshold", 5)
        success_threshold = params.get("success_threshold", 2)
        timeout = params.get("timeout", 60.0)

        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(
                failure_threshold=failure_threshold,
                success_threshold=success_threshold,
                timeout=timeout,
            )

        cb = self._circuit_breakers[name]

        try:
            result = cb.call(func, *args, **kwargs)
            return ActionResult(
                success=True,
                message="Success with circuit breaker",
                data={"result": result, "state": cb.state.state},
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Circuit breaker error: {str(e)}",
                data={"state": cb.state.state},
            )

    def _execute_fallback(self, func: Callable, args: List, kwargs: Dict, params: Dict) -> ActionResult:
        """Execute with fallback chain."""
        fallbacks = params.get("fallbacks", [])

        chain = FallbackChain()
        for fb in fallbacks:
            chain.add_fallback(fb.get("func"), priority=fb.get("priority", 0))

        chain.add_fallback(func)

        try:
            result = chain.execute(*args, **kwargs)
            return ActionResult(success=True, message="Success with fallback", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"All fallbacks failed: {str(e)}")
