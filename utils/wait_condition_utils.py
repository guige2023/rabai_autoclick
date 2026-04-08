"""
Wait Condition Utilities

Provides utilities for waiting on conditions
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
import time


class WaitCondition:
    """
    Waits for a condition to be met.
    
    Polls a condition function until it
    returns True or timeout expires.
    """

    def __init__(
        self,
        timeout_seconds: float = 30.0,
        poll_interval_seconds: float = 0.5,
    ) -> None:
        self._timeout = timeout_seconds
        self._poll_interval = poll_interval_seconds

    def wait_for(
        self,
        condition: Callable[[], bool],
        timeout: float | None = None,
    ) -> bool:
        """
        Wait for condition to be true.
        
        Args:
            condition: Function that returns bool.
            timeout: Optional override timeout.
            
        Returns:
            True if condition met, False if timeout.
        """
        timeout = timeout or self._timeout
        deadline = time.time() + timeout
        while time.time() < deadline:
            if condition():
                return True
            time.sleep(self._poll_interval)
        return False

    def wait_for_value(
        self,
        getter: Callable[[], Any],
        expected: Any,
        timeout: float | None = None,
    ) -> bool:
        """Wait for getter to return expected value."""
        return self.wait_for(
            lambda: getter() == expected,
            timeout,
        )


def wait_for_element_visible(
    element_getter: Callable[[], dict[str, Any]],
    timeout: float = 30.0,
) -> bool:
    """Wait for element to be visible."""
    wait = WaitCondition(timeout_seconds=timeout)
    return wait.wait_for(lambda: element_getter().get("visible", False))
