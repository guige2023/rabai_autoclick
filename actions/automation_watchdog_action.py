"""Automation watchdog and timeout enforcement action."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class WatchdogState(str, Enum):
    """State of the watchdog."""

    IDLE = "idle"
    WATCHING = "watching"
    TRIGGERED = "triggered"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class WatchdogConfig:
    """Configuration for a watchdog."""

    name: str
    timeout_seconds: float
    check_interval_seconds: float = 1.0
    on_timeout: Optional[Callable[[], None]] = None
    on_warning: Optional[Callable[[float], None]] = None
    warning_threshold_seconds: Optional[float] = None


@dataclass
class WatchdogResult:
    """Result of watchdog execution."""

    name: str
    state: WatchdogState
    started_at: datetime
    triggered_at: Optional[datetime] = None
    duration_seconds: float = 0
    iterations: int = 0


class AutomationWatchdogAction:
    """Watches automation tasks and enforces timeouts."""

    def __init__(
        self,
        default_timeout: float = 300.0,
    ):
        """Initialize watchdog.

        Args:
            default_timeout: Default timeout in seconds.
        """
        self._default_timeout = default_timeout
        self._watchdogs: dict[str, asyncio.Task] = {}
        self._states: dict[str, WatchdogState] = {}
        self._results: dict[str, WatchdogResult] = {}

    async def watch(
        self,
        name: str,
        coro: Coroutine[Any, Any, Any],
        config: Optional[WatchdogConfig] = None,
    ) -> Any:
        """Watch an async operation with timeout.

        Args:
            name: Watchdog name.
            coro: Coroutine to watch.
            config: Watchdog configuration.

        Returns:
            Result of the coroutine.
        """
        config = config or WatchdogConfig(name=name, timeout_seconds=self._default_timeout)
        timeout = config.timeout_seconds
        warning_threshold = config.warning_threshold_seconds

        self._states[name] = WatchdogState.WATCHING
        start_time = datetime.now()
        result = None
        error = None
        triggered = False

        async def watchdog_loop():
            nonlocal triggered
            iterations = 0
            while self._states.get(name) == WatchdogState.WATCHING:
                iterations += 1
                elapsed = (datetime.now() - start_time).total_seconds()

                if warning_threshold and elapsed >= warning_threshold:
                    if config.on_warning:
                        config.on_warning(elapsed)

                if elapsed >= timeout:
                    self._states[name] = WatchdogState.TRIGGERED
                    triggered = True
                    if config.on_timeout:
                        config.on_timeout()
                    return

                await asyncio.sleep(config.check_interval_seconds)

        watchdog_task = asyncio.create_task(watchdog_loop())
        self._watchdogs[name] = watchdog_task

        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            error = "Operation timed out"
            self._states[name] = WatchdogState.EXPIRED
        except Exception as e:
            error = str(e)
        finally:
            if not triggered:
                self._states[name] = WatchdogState.CANCELLED
            watchdog_task.cancel()
            try:
                await asyncio.gather(watchdog_task, return_exceptions=True)
            except Exception:
                pass

        duration = (datetime.now() - start_time).total_seconds()
        self._results[name] = WatchdogResult(
            name=name,
            state=self._states[name],
            started_at=start_time,
            triggered_at=datetime.now() if triggered else None,
            duration_seconds=duration,
            iterations=0,
        )

        if error:
            raise TimeoutError(error) from None

        return result

    async def watch_with_heartbeat(
        self,
        name: str,
        coro: Coroutine[Any, Any, Any],
        heartbeat_interval: float = 10.0,
        timeout: float = 300.0,
    ) -> Any:
        """Watch with heartbeat/ping mechanism.

        Args:
            name: Watchdog name.
            coro: Coroutine to watch.
            heartbeat_interval: Interval to call heartbeat.
            timeout: Total timeout.

        Returns:
            Result of the coroutine.
        """
        heartbeat_received = asyncio.Event()
        last_heartbeat = datetime.now()

        async def heartbeat_waiter():
            nonlocal last_heartbeat
            while self._states.get(name) == WatchdogState.WATCHING:
                try:
                    await asyncio.wait_for(
                        heartbeat_received.wait(),
                        timeout=heartbeat_interval,
                    )
                    heartbeat_received.clear()
                    last_heartbeat = datetime.now()
                except asyncio.TimeoutError:
                    elapsed = (datetime.now() - last_heartbeat).total_seconds()
                    if elapsed >= heartbeat_interval * 2:
                        self._states[name] = WatchdogState.TRIGGERED
                        return

        async def heartbeat_setter():
            nonlocal heartbeat_received
            heartbeat_received.set()

        self._states[name] = WatchdogState.WATCHING
        start_time = datetime.now()

        heartbeat_task = asyncio.create_task(heartbeat_waiter())
        self._watchdogs[name] = heartbeat_task

        try:
            result = await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Operation {name} timed out after {timeout}s")
        finally:
            self._states[name] = WatchdogState.CANCELLED
            heartbeat_task.cancel()
            try:
                await asyncio.gather(heartbeat_task, return_exceptions=True)
            except Exception:
                pass

        return result

    def get_state(self, name: str) -> WatchdogState:
        """Get current state of a watchdog."""
        return self._states.get(name, WatchdogState.IDLE)

    def get_result(self, name: str) -> Optional[WatchdogResult]:
        """Get result of a completed watchdog."""
        return self._results.get(name)

    def cancel(self, name: str) -> bool:
        """Cancel a watchdog."""
        if name in self._watchdogs:
            self._watchdogs[name].cancel()
            self._states[name] = WatchdogState.CANCELLED
            return True
        return False

    def cancel_all(self) -> None:
        """Cancel all watchdogs."""
        for name in list(self._watchdogs.keys()):
            self.cancel(name)

    def get_active_watchdogs(self) -> list[str]:
        """Get list of active watchdog names."""
        return [
            name
            for name, state in self._states.items()
            if state == WatchdogState.WATCHING
        ]
