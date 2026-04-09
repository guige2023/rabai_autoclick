"""
API Polling Action Module.

Provides polling with exponential backoff, condition-based
stop, and async support.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from .api_retry_action import RetryConfig, RetryStrategy


class PollStatus(Enum):
    """Polling status."""
    PENDING = "pending"
    POLLING = "polling"
    COMPLETED = "completed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class PollConfig:
    """Polling configuration."""
    interval: float = 1.0
    max_attempts: int = 60
    timeout: float = 60.0
    backoff: bool = True
    backoff_multiplier: float = 1.5
    max_interval: float = 30.0
    jitter: bool = True


@dataclass
class PollResult:
    """Polling result."""
    status: PollStatus
    result: Any = None
    attempts: int = 0
    elapsed: float = 0.0
    last_error: Optional[Exception] = None


class APIPollingAction:
    """
    API polling with backoff.

    Example:
        poller = APIPollingAction(
            interval=2.0,
            timeout=60.0,
            backoff=True
        )

        result = await poller.poll(
            check_func=lambda: api.get_status(),
            desired_condition=lambda r: r.status == "ready"
        )
    """

    def __init__(
        self,
        interval: float = 1.0,
        max_attempts: int = 60,
        timeout: float = 60.0,
        backoff: bool = True
    ):
        self.config = PollConfig(
            interval=interval,
            max_attempts=max_attempts,
            timeout=timeout,
            backoff=backoff
        )

    async def poll(
        self,
        check_func: Callable[[], Any],
        desired_condition: Callable[[Any], bool],
        on_poll: Optional[Callable[[int, Any], None]] = None
    ) -> PollResult:
        """Poll until condition is met."""
        start_time = time.monotonic()
        result = PollResult(status=PollStatus.POLLING)
        current_interval = self.config.interval

        for attempt in range(1, self.config.max_attempts + 1):
            elapsed = time.monotonic() - start_time

            if elapsed >= self.config.timeout:
                result.status = PollStatus.TIMEOUT
                result.attempts = attempt
                result.elapsed = elapsed
                return result

            try:
                check_result = check_func()
                if asyncio.iscoroutinefunction(check_func):
                    check_result = await check_result

                if on_poll:
                    on_poll(attempt, check_result)

                if desired_condition(check_result):
                    result.status = PollStatus.COMPLETED
                    result.result = check_result
                    result.attempts = attempt
                    result.elapsed = elapsed
                    return result

            except Exception as e:
                result.last_error = e

            result.attempts = attempt
            await asyncio.sleep(current_interval)

            if self.config.backoff:
                current_interval = min(
                    current_interval * self.config.backoff_multiplier,
                    self.config.max_interval
                )

                if self.config.jitter:
                    import random
                    current_interval *= (0.5 + random.random())

        result.status = PollStatus.TIMEOUT
        result.elapsed = time.monotonic() - start_time
        return result

    async def poll_until_value(
        self,
        check_func: Callable[[], Any],
        value: Any,
        max_attempts: Optional[int] = None
    ) -> PollResult:
        """Poll until check returns specific value."""
        return await self.poll(
            check_func,
            lambda r: r == value,
        )

    def cancel(self) -> None:
        """Cancel polling."""
        pass
