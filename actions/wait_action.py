"""Wait action for timed delays and condition waiting.

This module provides waiting capabilities including
fixed delays, condition polling, and timeout handling.

Example:
    >>> action = WaitAction()
    >>> result = action.execute(seconds=5)
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class WaitConfig:
    """Configuration for waiting."""
    timeout: float = 30.0
    interval: float = 0.5
    max_retries: int = 60


class WaitAction:
    """Wait and delay action.

    Provides timed delays, condition polling,
    and timeout handling for automation.

    Example:
        >>> action = WaitAction()
        >>> result = action.execute(
        ...     command="for_condition",
        ...     condition=lambda: check_something()
        ... )
    """

    def __init__(self, config: Optional[WaitConfig] = None) -> None:
        """Initialize wait action.

        Args:
            config: Optional wait configuration.
        """
        self.config = config or WaitConfig()

    def execute(
        self,
        command: str = "sleep",
        seconds: float = 1.0,
        condition: Optional[Callable[[], bool]] = None,
        timeout: Optional[float] = None,
        interval: Optional[float] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute wait command.

        Args:
            command: Wait command (sleep, for_element, for_condition, poll).
            seconds: Seconds to wait.
            condition: Condition function to poll.
            timeout: Maximum wait time.
            interval: Poll interval.
            **kwargs: Additional parameters.

        Returns:
            Wait result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        timeout = timeout or self.config.timeout
        interval = interval or self.config.interval

        if cmd in ("sleep", "wait", "delay"):
            time.sleep(seconds)
            result["waited"] = seconds

        elif cmd in ("for_condition", "until"):
            if not condition:
                raise ValueError("condition required for 'for_condition'")
            result.update(self._wait_for_condition(condition, timeout, interval))

        elif cmd == "poll":
            max_attempts = kwargs.get("max_attempts", int(timeout / interval))
            result.update(self._poll_condition(condition, max_attempts, interval))

        elif cmd == "for_element":
            selector = kwargs.get("selector")
            result.update(self._wait_for_element(selector, timeout))

        elif cmd == "for_text":
            text = kwargs.get("text")
            result.update(self._wait_for_text(text, timeout))

        elif cmd == "for_url":
            url = kwargs.get("url")
            contains = kwargs.get("contains")
            result.update(self._wait_for_url(url, contains, timeout))

        elif cmd == "for_change":
            initial = kwargs.get("initial_value")
            getter = kwargs.get("getter")
            result.update(self._wait_for_change(initial, getter, timeout, interval))

        elif cmd == "retry":
            func = kwargs.get("func")
            max_attempts = kwargs.get("max_attempts", 3)
            result.update(self._retry_operation(func, max_attempts, interval))

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def _wait_for_condition(
        self,
        condition: Callable[[], bool],
        timeout: float,
        interval: float,
    ) -> dict[str, Any]:
        """Wait for condition to become true.

        Args:
            condition: Condition function.
            timeout: Maximum wait time.
            interval: Check interval.

        Returns:
            Result dictionary.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                if condition():
                    elapsed = time.time() - start_time
                    return {
                        "success": True,
                        "met": True,
                        "wait_time": elapsed,
                        "attempts": int(elapsed / interval),
                    }
            except Exception:
                pass

            time.sleep(interval)

        return {
            "success": True,
            "met": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _poll_condition(
        self,
        condition: Optional[Callable[[], bool]],
        max_attempts: int,
        interval: float,
    ) -> dict[str, Any]:
        """Poll condition with max attempts.

        Args:
            condition: Condition function.
            max_attempts: Maximum poll attempts.
            interval: Poll interval.

        Returns:
            Result dictionary.
        """
        if not condition:
            return {"success": False, "error": "condition required"}

        attempts = 0
        for i in range(max_attempts):
            attempts = i + 1
            try:
                if condition():
                    return {
                        "success": True,
                        "met": True,
                        "attempts": attempts,
                    }
            except Exception:
                pass
            time.sleep(interval)

        return {
            "success": True,
            "met": False,
            "attempts": attempts,
            "timeout": True,
        }

    def _wait_for_element(
        self,
        selector: Optional[str],
        timeout: float,
    ) -> dict[str, Any]:
        """Wait for element to appear.

        Args:
            selector: Element selector.
            timeout: Maximum wait time.

        Returns:
            Result dictionary.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            # In real impl, would check for element in browser
            # For now, just wait
            time.sleep(0.5)

            # Check element (simplified)
            # if find_element(selector):
            #     return {"found": True, "wait_time": elapsed}

        return {
            "success": True,
            "found": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _wait_for_text(
        self,
        text: Optional[str],
        timeout: float,
    ) -> dict[str, Any]:
        """Wait for text to appear.

        Args:
            text: Text to wait for.
            timeout: Maximum wait time.

        Returns:
            Result dictionary.
        """
        if not text:
            return {"success": False, "error": "text required"}

        start_time = time.time()

        while time.time() - start_time < timeout:
            time.sleep(0.5)
            # Would check for text in page content

        return {
            "success": True,
            "found": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _wait_for_url(
        self,
        url: Optional[str],
        contains: Optional[str],
        timeout: float,
    ) -> dict[str, Any]:
        """Wait for URL condition.

        Args:
            url: Exact URL to wait for.
            contains: URL substring to wait for.
            timeout: Maximum wait time.

        Returns:
            Result dictionary.
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            time.sleep(0.5)
            # Would check current URL
            # current = get_current_url()
            # if url and current == url: return {"found": True}
            # if contains and contains in current: return {"found": True}

        return {
            "success": True,
            "found": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _wait_for_change(
        self,
        initial: Any,
        getter: Optional[Callable[[], Any]],
        timeout: float,
        interval: float,
    ) -> dict[str, Any]:
        """Wait for value to change.

        Args:
            initial: Initial value.
            getter: Function to get current value.
            timeout: Maximum wait time.
            interval: Check interval.

        Returns:
            Result dictionary.
        """
        if not getter:
            return {"success": False, "error": "getter required"}

        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                current = getter()
                if current != initial:
                    return {
                        "success": True,
                        "changed": True,
                        "initial": initial,
                        "current": current,
                        "wait_time": time.time() - start_time,
                    }
            except Exception:
                pass

            time.sleep(interval)

        return {
            "success": True,
            "changed": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _retry_operation(
        self,
        func: Optional[Callable[[], Any]],
        max_attempts: int,
        interval: float,
    ) -> dict[str, Any]:
        """Retry operation until success or max attempts.

        Args:
            func: Function to retry.
            max_attempts: Maximum attempts.
            interval: Interval between attempts.

        Returns:
            Result dictionary.
        """
        if not func:
            return {"success": False, "error": "func required"}

        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                result = func()
                return {
                    "success": True,
                    "attempt": attempt,
                    "result": result,
                }
            except Exception as e:
                last_error = e
                if attempt < max_attempts:
                    time.sleep(interval)

        return {
            "success": False,
            "attempts": max_attempts,
            "error": str(last_error),
        }
