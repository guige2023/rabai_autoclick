"""Error recovery utilities for automation workflows."""

from typing import Callable, TypeVar, Optional, Any, List, Tuple
from dataclasses import dataclass
import time
import logging


T = TypeVar("T")


@dataclass
class RecoveryAction:
    """Defines a recovery action to take on error."""
    name: str
    action: Callable[[], Any]
    max_attempts: int = 3
    delay: float = 1.0


class ErrorRecoveryManager:
    """Manages error recovery strategies."""

    def __init__(self):
        """Initialize error recovery manager."""
        self._strategies: dict = {}
        self._logger = logging.getLogger("error_recovery")

    def register_strategy(
        self,
        error_type: type,
        strategy: RecoveryAction
    ) -> None:
        """Register a recovery strategy for an error type.
        
        Args:
            error_type: Exception type to handle.
            strategy: Recovery action to take.
        """
        self._strategies[error_type] = strategy

    def recover(
        self,
        error: Exception,
        context: Optional[dict] = None
    ) -> Tuple[bool, Any]:
        """Attempt to recover from an error.
        
        Args:
            error: The exception that occurred.
            context: Optional context for recovery.
        
        Returns:
            Tuple of (recovered, result).
        """
        error_type = type(error)
        for exc_type, strategy in self._strategies.items():
            if isinstance(error, exc_type):
                self._logger.info(f"Attempting recovery: {strategy.name}")
                for attempt in range(strategy.max_attempts):
                    try:
                        if strategy.delay > 0 and attempt > 0:
                            time.sleep(strategy.delay)
                        result = strategy.action()
                        self._logger.info(f"Recovery succeeded: {strategy.name}")
                        return True, result
                    except Exception as e:
                        self._logger.warning(
                            f"Recovery attempt {attempt + 1} failed: {e}"
                        )
                self._logger.error(f"All recovery attempts exhausted for {strategy.name}")
        return False, None

    def unregister(self, error_type: type) -> bool:
        """Unregister a recovery strategy."""
        if error_type in self._strategies:
            del self._strategies[error_type]
            return True
        return False


def with_recovery(
    recovery_actions: List[RecoveryAction],
    fallback: Optional[Callable[[Exception], Any]] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to add error recovery to a function.
    
    Args:
        recovery_actions: List of recovery actions to try.
        fallback: Optional fallback if all recoveries fail.
    
    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args, **kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                for action in recovery_actions:
                    for attempt in range(action.max_attempts):
                        try:
                            if action.delay > 0 and attempt > 0:
                                time.sleep(action.delay)
                            return action.action()
                        except Exception:
                            continue
                if fallback:
                    return fallback(e)
                raise
        return wrapper
    return decorator


class CircuitBreaker:
    """Simple circuit breaker for preventing repeated failures."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Failures before opening.
            recovery_timeout: Seconds before attempting recovery.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._last_failure_time: Optional[float] = None
        self._is_open = False

    def record_success(self) -> None:
        """Record a successful call."""
        self._failures = 0
        self._is_open = False

    def record_failure(self) -> None:
        """Record a failed call."""
        self._failures += 1
        self._last_failure_time = time.time()
        if self._failures >= self.failure_threshold:
            self._is_open = True

    def is_open(self) -> bool:
        """Check if circuit is open."""
        if not self._is_open:
            return False
        if self._last_failure_time and \
           time.time() - self._last_failure_time >= self.recovery_timeout:
            self._is_open = False
            self._failures = 0
            return False
        return True

    def call(self, func: Callable[[], T]) -> T:
        """Execute function through circuit breaker."""
        if self.is_open():
            raise RuntimeError("Circuit breaker is open")
        try:
            result = func()
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise e
