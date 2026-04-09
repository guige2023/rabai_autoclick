"""
Automation Recovery Action Module.

Fault recovery and retry orchestration with exponential
backoff, jitter, and failure classification.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar
from enum import Enum
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")


class FailureType(Enum):
    """Classified failure types."""
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    TIMEOUT = "timeout"
    RESOURCE = "resource"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.3


@dataclass
class RecoveryAction:
    """
    Recovery action definition.

    Attributes:
        name: Action identifier.
        action_type: Type of recovery action.
        func: Function to execute.
        timeout: Action timeout.
        on_failure: Action to run on failure.
    """
    name: str
    action_type: str
    func: Callable
    timeout: float = 30.0
    on_failure: Optional[Callable] = None


@dataclass
class ExecutionResult:
    """Result of recovery execution."""
    success: bool
    result: Any
    attempts: int
    total_duration: float
    errors: list[str]
    recovery_actions_triggered: list[str]


class AutomationRecoveryAction:
    """
    Recovery orchestration for automation failures.

    Example:
        recovery = AutomationRecoveryAction()
        recovery.configure(max_attempts=5, base_delay=2.0)
        result = await recovery.execute_with_recovery(faulty_operation, args)
    """

    def __init__(self, retry_config: Optional[RetryConfig] = None):
        """
        Initialize recovery action.

        Args:
            retry_config: Retry configuration.
        """
        self.retry_config = retry_config or RetryConfig()
        self._recovery_actions: dict[str, RecoveryAction] = {}
        self._failure_classifiers: list[Callable[[Exception], FailureType]] = []

    def configure(
        self,
        max_attempts: Optional[int] = None,
        base_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
        exponential_base: Optional[float] = None,
        jitter: Optional[bool] = None
    ) -> None:
        """Update retry configuration."""
        if max_attempts is not None:
            self.retry_config.max_attempts = max_attempts
        if base_delay is not None:
            self.retry_config.base_delay = base_delay
        if max_delay is not None:
            self.retry_config.max_delay = max_delay
        if exponential_base is not None:
            self.retry_config.exponential_base = exponential_base
        if jitter is not None:
            self.retry_config.jitter = jitter

    def register_recovery_action(
        self,
        name: str,
        action_type: str,
        func: Callable,
        timeout: float = 30.0,
        on_failure: Optional[Callable] = None
    ) -> RecoveryAction:
        """
        Register a recovery action.

        Args:
            name: Action identifier.
            action_type: Type of recovery.
            func: Recovery function.
            timeout: Action timeout.
            on_failure: Callback on failure.

        Returns:
            Created RecoveryAction.
        """
        action = RecoveryAction(
            name=name,
            action_type=action_type,
            func=func,
            timeout=timeout,
            on_failure=on_failure
        )

        self._recovery_actions[name] = action
        logger.debug(f"Registered recovery action: {name}")
        return action

    def register_failure_classifier(
        self,
        classifier: Callable[[Exception], FailureType]
    ) -> None:
        """
        Register a failure classifier.

        Args:
            classifier: Function that classifies exception type.
        """
        self._failure_classifiers.append(classifier)

    def classify_failure(self, exception: Exception) -> FailureType:
        """
        Classify failure type.

        Args:
            exception: Exception to classify.

        Returns:
            FailureType classification.
        """
        for classifier in self._failure_classifiers:
            try:
                result = classifier(exception)
                if result:
                    return result
            except Exception:
                pass

        if isinstance(exception, asyncio.TimeoutError):
            return FailureType.TIMEOUT

        if "timeout" in str(exception).lower():
            return FailureType.TIMEOUT

        if isinstance(exception, (ConnectionError, OSError)):
            return FailureType.TRANSIENT

        if isinstance(exception, MemoryError, KeyboardInterrupt):
            return FailureType.PERMANENT

        return FailureType.UNKNOWN

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate retry delay with exponential backoff.

        Args:
            attempt: Current attempt number (1-indexed).

        Returns:
            Delay in seconds.
        """
        delay = self.retry_config.base_delay * (
            self.retry_config.exponential_base ** (attempt - 1)
        )

        delay = min(delay, self.retry_config.max_delay)

        if self.retry_config.jitter:
            jitter_range = delay * self.retry_config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0.1, delay)

    async def execute_with_recovery(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> ExecutionResult:
        """
        Execute function with retry and recovery.

        Args:
            func: Async function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            ExecutionResult with outcome details.
        """
        start_time = time.time()
        errors = []
        recovery_triggered = []
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=getattr(self, '_timeout', 300.0)
                    )
                else:
                    result = await asyncio.to_thread(func, *args, **kwargs)

                duration = time.time() - start_time

                return ExecutionResult(
                    success=True,
                    result=result,
                    attempts=attempt,
                    total_duration=duration,
                    errors=errors,
                    recovery_actions_triggered=recovery_triggered
                )

            except asyncio.TimeoutError as e:
                last_exception = e
                failure_type = FailureType.TIMEOUT
                errors.append(f"Attempt {attempt}: Timeout - {e}")

            except Exception as e:
                last_exception = e
                failure_type = self.classify_failure(e)
                errors.append(f"Attempt {attempt}: {failure_type.value} - {e}")

            if failure_type == FailureType.PERMANENT:
                logger.error(f"Permanent failure, stopping retries: {e}")
                break

            if attempt < self.retry_config.max_attempts:
                delay = self.calculate_delay(attempt)
                logger.info(f"Retrying in {delay:.2f}s (attempt {attempt + 1}/{self.retry_config.max_attempts})")
                await asyncio.sleep(delay)

                triggered = await self._run_recovery_actions(failure_type, e)
                recovery_triggered.extend(triggered)

        duration = time.time() - start_time

        await self._run_failure_handlers(last_exception)

        return ExecutionResult(
            success=False,
            result=None,
            attempts=self.retry_config.max_attempts,
            total_duration=duration,
            errors=errors,
            recovery_actions_triggered=recovery_triggered
        )

    async def _run_recovery_actions(
        self,
        failure_type: FailureType,
        exception: Exception
    ) -> list[str]:
        """Run applicable recovery actions."""
        triggered = []

        for name, action in self._recovery_actions.items():
            if action.action_type == failure_type.value or action.action_type == "all":
                try:
                    logger.info(f"Running recovery action: {name}")

                    if asyncio.iscoroutinefunction(action.func):
                        await asyncio.wait_for(action.func(exception), timeout=action.timeout)
                    else:
                        action.func(exception)

                    triggered.append(name)

                except Exception as e:
                    logger.error(f"Recovery action {name} failed: {e}")
                    if action.on_failure:
                        try:
                            action.on_failure(e)
                        except Exception:
                            pass

        return triggered

    async def _run_failure_handlers(self, exception: Optional[Exception]) -> None:
        """Run failure handlers after all retries exhausted."""
        if not exception:
            return

        logger.error(f"All retries exhausted. Final error: {exception}")

    def execute_sync_with_recovery(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> ExecutionResult:
        """
        Synchronous version of execute_with_recovery.

        Args:
            func: Sync function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            ExecutionResult.
        """
        import threading
        return asyncio.run(self.execute_with_recovery(func, *args, **kwargs))

    def get_stats(self) -> dict:
        """Get recovery statistics."""
        return {
            "max_attempts": self.retry_config.max_attempts,
            "base_delay": self.retry_config.base_delay,
            "registered_actions": len(self._recovery_actions),
            "failure_classifiers": len(self._failure_classifiers)
        }
