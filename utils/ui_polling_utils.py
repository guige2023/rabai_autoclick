"""UI Polling Utilities.

Polling utilities for waiting on UI conditions.

Example:
    >>> from ui_polling_utils import poll_until
    >>> element = poll_until(lambda: find_element("btn"), timeout=5)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")


@dataclass
class PollResult:
    """Result of a polling operation."""
    success: bool
    value: Any
    attempts: int
    elapsed: float
    error: Optional[str] = None


class PollOptions:
    """Options for polling operations."""

    def __init__(
        self,
        timeout: float = 10.0,
        interval: float = 0.1,
        max_attempts: Optional[int] = None,
        ignored_exceptions: tuple = (),
    ):
        """Initialize poll options.

        Args:
            timeout: Maximum time to poll in seconds.
            interval: Time between polls in seconds.
            max_attempts: Maximum number of attempts.
            ignored_exceptions: Exceptions to ignore and retry.
        """
        self.timeout = timeout
        self.interval = interval
        self.max_attempts = max_attempts
        self.ignored_exceptions = ignored_exceptions


def poll_until(
    condition: Callable[[], T],
    timeout: float = 10.0,
    interval: float = 0.1,
    default: Optional[T] = None,
) -> T:
    """Poll until condition is met or timeout.

    Args:
        condition: Callable that returns truthy value when ready.
        timeout: Maximum time to wait in seconds.
        interval: Time between polls in seconds.
        default: Default value if timeout.

    Returns:
        Condition result or default.
    """
    start = time.time()
    attempts = 0
    while time.time() - start < timeout:
        attempts += 1
        try:
            result = condition()
            if result:
                return result
        except Exception:
            pass
        time.sleep(interval)
    return default


def poll_until_not(
    condition: Callable[[], Any],
    timeout: float = 10.0,
    interval: float = 0.1,
    default: Optional[Any] = None,
) -> Any:
    """Poll until condition is falsy or timeout.

    Args:
        condition: Callable that returns falsy value when ready.
        timeout: Maximum time to wait in seconds.
        interval: Time between polls in seconds.
        default: Default value if timeout.

    Returns:
        Condition result or default.
    """
    return poll_until(lambda: not condition(), timeout, interval, default)


def poll_with_result(
    condition: Callable[[], tuple[bool, T]],
    timeout: float = 10.0,
    interval: float = 0.1,
) -> PollResult:
    """Poll with a condition returning (success, value).

    Args:
        condition: Callable returning (done, value).
        timeout: Maximum time to wait.
        interval: Time between polls.

    Returns:
        PollResult with details.
    """
    start = time.time()
    attempts = 0
    last_error: Optional[str] = None

    while time.time() - start < timeout:
        attempts += 1
        try:
            done, value = condition()
            if done:
                return PollResult(
                    success=True,
                    value=value,
                    attempts=attempts,
                    elapsed=time.time() - start,
                )
        except Exception as e:
            last_error = str(e)
        time.sleep(interval)

    return PollResult(
        success=False,
        value=None,
        attempts=attempts,
        elapsed=time.time() - start,
        error=last_error,
    )
