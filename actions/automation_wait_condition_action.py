"""Automation Wait Condition Action Module.

Waits for arbitrary conditions to be met before proceeding,
with timeout, polling interval, and maximum attempts configuration.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
import time
import logging

logger = logging.getLogger(__name__)


class WaitTimeoutError(Exception):
    """Raised when wait condition times out."""
    pass


@dataclass
class WaitAttempt:
    """Record of a single wait attempt."""
    attempt: int
    elapsed_sec: float
    condition_value: Any
    satisfied: bool


class AutomationWaitConditionAction:
    """Wait for conditions before proceeding in automation.
    
    Polls a condition function at regular intervals until it returns
    True (or a non-None/non-False value), or timeout is reached.
    """

    def __init__(
        self,
        default_interval_sec: float = 1.0,
        default_timeout_sec: float = 30.0,
    ) -> None:
        self.default_interval_sec = default_interval_sec
        self.default_timeout_sec = default_timeout_sec

    def wait(
        self,
        condition: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        interval: Optional[float] = None,
        description: str = "condition",
        **kwargs: Any,
    ) -> Any:
        """Wait for a condition to be satisfied.
        
        Args:
            condition: Callable that returns a value. Truthy = satisfied.
            *args: Positional args passed to condition.
            timeout: Max seconds to wait (uses default if None).
            interval: Seconds between polls (uses default if None).
            description: Human-readable description for logging.
            **kwargs: Keyword args passed to condition.
        
        Returns:
            The truthy value returned by condition.
        
        Raises:
            WaitTimeoutError: If condition not satisfied within timeout.
        """
        timeout = timeout if timeout is not None else self.default_timeout_sec
        interval = interval if interval is not None else self.default_interval_sec
        start = time.time()
        attempt = 0
        history: List[WaitAttempt] = []

        logger.debug("Waiting for %s (timeout=%.1fs, interval=%.1f)", description, timeout, interval)

        while True:
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise WaitTimeoutError(
                    f"Timed out after {timeout}s waiting for {description} "
                    f"(attempts={attempt}, last_value={history[-1].condition_value if history else None})"
                )

            attempt += 1
            try:
                value = condition(*args, **kwargs)
            except Exception as exc:
                value = False
                logger.debug("Condition %s raised exception: %s", description, exc)

            satisfied = bool(value)
            record = WaitAttempt(
                attempt=attempt, elapsed_sec=elapsed,
                condition_value=value, satisfied=satisfied,
            )
            history.append(record)

            if satisfied:
                logger.info(
                    "Condition '%s' satisfied after %.2fs (attempt %d)",
                    description, elapsed, attempt,
                )
                return value

            remaining = timeout - elapsed
            sleep_time = min(interval, remaining)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def wait_any(
        self,
        conditions: List[Tuple[str, Callable[..., Any]]],
        timeout: Optional[float] = None,
        interval: Optional[float] = None,
    ) -> Tuple[str, Any]:
        """Wait for any of several conditions to be satisfied.
        
        Args:
            conditions: List of (name, condition_fn) tuples.
            timeout: Max seconds to wait.
            interval: Seconds between polls.
        
        Returns:
            Tuple of (condition_name, condition_value) that satisfied first.
        
        Raises:
            WaitTimeoutError: If all conditions time out.
        """
        timeout = timeout if timeout is not None else self.default_timeout_sec
        interval = interval if interval is not None else self.default_interval_sec
        start = time.time()
        remaining = timeout

        while True:
            elapsed = time.time() - start
            if elapsed >= timeout:
                raise WaitTimeoutError(f"All {len(conditions)} conditions timed out")
            for name, cond in conditions:
                try:
                    value = cond()
                except Exception:
                    value = False
                if bool(value):
                    logger.info("Condition '%s' satisfied first", name)
                    return name, value
            time.sleep(min(interval, timeout - elapsed))

    def get_attempt_history(self) -> List[WaitAttempt]:
        """Get last wait attempt history (if any)."""
        return getattr(self, "_last_history", [])
