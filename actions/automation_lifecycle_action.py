"""
Automation Lifecycle Action Module.

Lifecycle management for automation workflows with
startup, shutdown, pause, resume, and state persistence.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class LifecycleState(Enum):
    """Lifecycle states."""
    INITIAL = "initial"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class LifecycleHook:
    """Lifecycle hook definition."""
    name: str
    func: Callable
    timeout: float = 30.0
    retry_count: int = 0


@dataclass
class LifecycleTransition:
    """Lifecycle state transition."""
    from_state: LifecycleState
    to_state: LifecycleState
    hooks: list[LifecycleHook] = field(default_factory=list)


@dataclass
class LifecycleMetrics:
    """Lifecycle metrics."""
    start_time: float = 0.0
    uptime_seconds: float = 0.0
    pause_count: int = 0
    restart_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None


class AutomationLifecycleAction:
    """
    Manages lifecycle states and hooks for automation workflows.

    Example:
        lifecycle = AutomationLifecycleAction("my_automation")

        @lifecycle.on_startup
        async def initialize():
            await setup_connections()

        @lifecycle.on_shutdown
        async def cleanup():
            await close_connections()

        await lifecycle.start()
    """

    def __init__(self, name: str = "automation"):
        """
        Initialize lifecycle manager.

        Args:
            name: Automation identifier.
        """
        self.name = name
        self.state = LifecycleState.INITIAL
        self._hooks: dict[str, list[LifecycleHook]] = {
            "startup": [],
            "shutdown": [],
            "pause": [],
            "resume": [],
            "error": []
        }
        self._metrics = LifecycleMetrics()
        self._startup_time: Optional[float] = None
        self._paused_duration: float = 0.0
        self._pause_start: Optional[float] = None
        self._subscribers: list[Callable] = []

    def on_startup(self, timeout: float = 30.0, retries: int = 0):
        """Decorator for startup hook."""
        def decorator(func: Callable) -> Callable:
            self.add_hook("startup", func, timeout=timeout, retries=retries)
            return func
        return decorator

    def on_shutdown(self, timeout: float = 30.0, retries: int = 0):
        """Decorator for shutdown hook."""
        def decorator(func: Callable) -> Callable:
            self.add_hook("shutdown", func, timeout=timeout, retries=retries)
            return func
        return decorator

    def on_pause(self, timeout: float = 30.0, retries: int = 0):
        """Decorator for pause hook."""
        def decorator(func: Callable) -> Callable:
            self.add_hook("pause", func, timeout=timeout, retries=retries)
            return func
        return decorator

    def on_resume(self, timeout: float = 30.0, retries: int = 0):
        """Decorator for resume hook."""
        def decorator(func: Callable) -> Callable:
            self.add_hook("resume", func, timeout=timeout, retries=retries)
            return func
        return decorator

    def on_error(self, timeout: float = 30.0, retries: int = 0):
        """Decorator for error hook."""
        def decorator(func: Callable) -> Callable:
            self.add_hook("error", func, timeout=timeout, retries=retries)
            return func
        return decorator

    def add_hook(
        self,
        event: str,
        func: Callable,
        timeout: float = 30.0,
        retries: int = 0
    ) -> None:
        """
        Add a lifecycle hook.

        Args:
            event: Event type (startup/shutdown/pause/resume/error).
            func: Async function to execute.
            timeout: Hook execution timeout.
            retries: Number of retries on failure.
        """
        hook = LifecycleHook(name=func.__name__, func=func, timeout=timeout, retry_count=retries)
        self._hooks[event].append(hook)
        logger.debug(f"Added {event} hook: {func.__name__}")

    async def start(self) -> bool:
        """
        Start the automation.

        Returns:
            True if started successfully.
        """
        if self.state not in (LifecycleState.INITIAL, LifecycleState.STOPPED):
            logger.warning(f"Cannot start from state: {self.state.value}")
            return False

        self.state = LifecycleState.STARTING
        logger.info(f"Starting automation: {self.name}")

        try:
            await self._execute_hooks("startup")
            self.state = LifecycleState.RUNNING
            self._metrics.start_time = time.time()
            self._startup_time = time.time()
            self._notify_subscribers()
            logger.info(f"Automation started: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Startup failed: {e}")
            self.state = LifecycleState.ERROR
            self._metrics.error_count += 1
            self._metrics.last_error = str(e)
            await self._execute_hooks("error", error=e)
            return False

    async def stop(self) -> bool:
        """
        Stop the automation gracefully.

        Returns:
            True if stopped successfully.
        """
        if self.state in (LifecycleState.STOPPED, LifecycleState.INITIAL):
            return True

        self.state = LifecycleState.STOPPING
        logger.info(f"Stopping automation: {self.name}")

        try:
            await self._execute_hooks("shutdown")
            self.state = LifecycleState.STOPPED
            self._metrics.uptime_seconds = time.time() - self._metrics.start_time
            self._notify_subscribers()
            logger.info(f"Automation stopped: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Shutdown failed: {e}")
            self.state = LifecycleState.ERROR
            self._metrics.error_count += 1
            self._metrics.last_error = str(e)
            return False

    async def pause(self) -> bool:
        """
        Pause the automation.

        Returns:
            True if paused successfully.
        """
        if self.state != LifecycleState.RUNNING:
            logger.warning(f"Cannot pause from state: {self.state.value}")
            return False

        try:
            await self._execute_hooks("pause")
            self.state = LifecycleState.PAUSED
            self._pause_start = time.time()
            self._metrics.pause_count += 1
            self._notify_subscribers()
            logger.info(f"Automation paused: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Pause failed: {e}")
            return False

    async def resume(self) -> bool:
        """
        Resume the automation.

        Returns:
            True if resumed successfully.
        """
        if self.state != LifecycleState.PAUSED:
            logger.warning(f"Cannot resume from state: {self.state.value}")
            return False

        try:
            await self._execute_hooks("resume")
            if self._pause_start:
                self._paused_duration += time.time() - self._pause_start
                self._pause_start = None
            self.state = LifecycleState.RUNNING
            self._notify_subscribers()
            logger.info(f"Automation resumed: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Resume failed: {e}")
            return False

    async def restart(self) -> bool:
        """
        Restart the automation.

        Returns:
            True if restarted successfully.
        """
        logger.info(f"Restarting automation: {self.name}")
        self._metrics.restart_count += 1
        await self.stop()
        return await self.start()

    async def _execute_hooks(self, event: str, error: Optional[Exception] = None) -> None:
        """Execute hooks for an event."""
        hooks = self._hooks.get(event, [])

        for hook in hooks:
            for attempt in range(hook.retry_count + 1):
                try:
                    if asyncio.iscoroutinefunction(hook.func):
                        await asyncio.wait_for(hook.func(), timeout=hook.timeout)
                    else:
                        hook.func()
                    break

                except Exception as e:
                    if attempt == hook.retry_count:
                        logger.error(f"Hook {hook.name} failed after {attempt + 1} attempts: {e}")
                        raise
                    await asyncio.sleep(2 ** attempt)

    def subscribe(self, callback: Callable) -> None:
        """Subscribe to state change notifications."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from notifications."""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def _notify_subscribers(self) -> None:
        """Notify all subscribers of state change."""
        for callback in self._subscribers:
            try:
                callback(self.state, self.name)
            except Exception as e:
                logger.error(f"Subscriber notification failed: {e}")

    def get_state(self) -> LifecycleState:
        """Get current lifecycle state."""
        return self.state

    def get_uptime(self) -> float:
        """Get uptime in seconds (excludes paused time)."""
        if self._metrics.start_time == 0:
            return 0.0

        uptime = time.time() - self._metrics.start_time

        if self.state == LifecycleState.PAUSED and self._pause_start:
            uptime -= (time.time() - self._pause_start)

        uptime -= self._paused_duration

        return max(0.0, uptime)

    def get_metrics(self) -> LifecycleMetrics:
        """Get lifecycle metrics."""
        metrics = self._metrics.copy() if hasattr(self._metrics, 'copy') else self._metrics
        metrics.uptime_seconds = self.get_uptime()
        return metrics

    def save_state(self, path: str) -> None:
        """Save lifecycle state to file."""
        state_data = {
            "name": self.name,
            "state": self.state.value,
            "metrics": {
                "start_time": self._metrics.start_time,
                "pause_count": self._metrics.pause_count,
                "restart_count": self._metrics.restart_count,
                "error_count": self._metrics.error_count,
                "last_error": self._metrics.last_error
            }
        }

        with open(path, "w") as f:
            json.dump(state_data, f, indent=2)

        logger.info(f"Saved lifecycle state to: {path}")

    def load_state(self, path: str) -> bool:
        """Load lifecycle state from file."""
        try:
            with open(path, "r") as f:
                state_data = json.load(f)

            self.state = LifecycleState(state_data.get("state", "initial"))

            metrics = state_data.get("metrics", {})
            self._metrics.start_time = metrics.get("start_time", 0.0)
            self._metrics.pause_count = metrics.get("pause_count", 0)
            self._metrics.restart_count = metrics.get("restart_count", 0)
            self._metrics.error_count = metrics.get("error_count", 0)
            self._metrics.last_error = metrics.get("last_error")

            logger.info(f"Loaded lifecycle state from: {path}")
            return True

        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False
