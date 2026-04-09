"""
Automation Poller Action Module.

Provides configurable polling mechanisms for monitoring resources
and triggering actions based on condition evaluation.
"""

import asyncio
import time
import threading
from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class PollingStrategy(Enum):
    """Polling strategy types."""
    FIXED = "fixed"
    ADAPTIVE = "adaptive"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    Jittered = "jittered"


class StopCondition(Enum):
    """Conditions for stopping the poller."""
    TIMEOUT = "timeout"
    CONDITION_MET = "condition_met"
    MAX_ATTEMPTS = "max_attempts"
    MANUAL = "manual"


@dataclass
class PollerConfig:
    """Configuration for the poller."""
    strategy: PollingStrategy = PollingStrategy.FIXED
    interval: float = 1.0  # base interval in seconds
    timeout: float = 60.0  # max time to poll
    max_attempts: int = 100  # max number of polling attempts
    min_interval: float = 0.1  # minimum interval for adaptive
    max_interval: float = 30.0  # maximum interval cap
    backoff_factor: float = 2.0  # exponential backoff multiplier
    jitter_range: float = 0.1  # jitter as fraction of interval


@dataclass
class PollResult:
    """Result of a single poll attempt."""
    attempt: int
    timestamp: float
    value: Any
    success: bool
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class PollerStats:
    """Statistics for poller execution."""
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    total_duration: float = 0.0
    last_attempt_time: Optional[float] = None
    condition_met_at: Optional[float] = None


class AutomationPollerAction:
    """
    Configurable polling action for monitoring and condition checking.

    Supports multiple polling strategies including fixed interval,
    adaptive polling, exponential backoff, and jittered polling.
    """

    def __init__(self, config: Optional[PollerConfig] = None):
        self.config = config or PollerConfig()
        self._running = False
        self._stopped = False
        self._lock = threading.RLock()
        self._stats = PollerStats()
        self._results: deque = deque(maxlen=1000)
        self._callbacks: List[Callable] = []

    def _calculate_interval(self, attempt: int) -> float:
        """Calculate polling interval based on strategy."""
        base = self.config.interval

        if self.config.strategy == PollingStrategy.FIXED:
            return base

        elif self.config.strategy == PollingStrategy.ADAPTIVE:
            # Adaptive: speed up on failures, slow down on successes
            return base

        elif self.config.strategy == PollingStrategy.EXPONENTIAL_BACKOFF:
            interval = base * (self.config.backoff_factor ** min(attempt, 10))
            return min(interval, self.config.max_interval)

        elif self.config.strategy == PollingStrategy.Jittered:
            import random
            jitter = base * self.config.jitter_range
            return base + random.uniform(-jitter, jitter)

        return base

    async def _poll_once(
        self,
        check_func: Callable[[], Any],
        attempt: int,
    ) -> PollResult:
        """Execute a single poll attempt."""
        start = time.time()
        try:
            value = check_func()
            if asyncio.iscoroutine(value):
                value = await value
            duration_ms = (time.time() - start) * 1000
            result = PollResult(
                attempt=attempt,
                timestamp=start,
                value=value,
                success=True,
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = (time.time() - start) * 1000
            result = PollResult(
                attempt=attempt,
                timestamp=start,
                value=None,
                success=False,
                error=str(e),
                duration_ms=duration_ms,
            )
        self._results.append(result)
        return result

    def _evaluate_condition(
        self,
        result: PollResult,
        condition_func: Optional[Callable[[Any], bool]] = None,
    ) -> bool:
        """Evaluate if stop condition is met."""
        if condition_func is None:
            return result.success
        try:
            if asyncio.iscoroutinefunction(condition_func):
                return asyncio.run(condition_func(result.value))
            return condition_func(result.value)
        except Exception:
            return False

    async def poll_async(
        self,
        check_func: Callable[[], Any],
        condition_func: Optional[Callable[[Any], bool]] = None,
        on_result: Optional[Callable[[PollResult], None]] = None,
    ) -> Optional[PollResult]:
        """
        Poll until condition is met or timeout occurs.

        Args:
            check_func: Function to call for each poll attempt
            condition_func: Optional function to evaluate condition
            on_result: Optional callback for each poll result

        Returns:
            PollResult if condition met, None if timeout/stopped
        """
        self._running = True
        self._stopped = False
        self._stats = PollerStats()
        start_time = time.time()
        attempt = 0

        try:
            while not self._stopped:
                # Check timeout
                if time.time() - start_time >= self.config.timeout:
                    break

                # Check max attempts
                if attempt >= self.config.max_attempts:
                    break

                attempt += 1
                self._stats.total_attempts = attempt
                self._stats.last_attempt_time = time.time()

                # Execute poll
                result = await self._poll_once(check_func, attempt)

                if result.success:
                    self._stats.successful_attempts += 1
                else:
                    self._stats.failed_attempts += 1

                # Callback
                if on_result:
                    if asyncio.iscoroutinefunction(on_result):
                        await on_result(result)
                    else:
                        on_result(result)

                # Fire registered callbacks
                for callback in self._callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(result)
                        else:
                            callback(result)
                    except Exception:
                        pass

                # Check condition
                if self._evaluate_condition(result, condition_func):
                    self._stats.condition_met_at = time.time()
                    self._stats.total_duration = time.time() - start_time
                    return result

                # Wait before next poll
                interval = self._calculate_interval(attempt)
                await asyncio.sleep(interval)

        finally:
            self._running = False
            self._stats.total_duration = time.time() - start_time

        return None

    def poll(
        self,
        check_func: Callable[[], Any],
        condition_func: Optional[Callable[[Any], bool]] = None,
        on_result: Optional[Callable[[PollResult], None]] = None,
    ) -> Optional[PollResult]:
        """Poll until condition is met (sync version)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.poll_async(check_func, condition_func, on_result), loop
                )
                return future.result(timeout=self.config.timeout + 10)
            return asyncio.run(self.poll_async(check_func, condition_func, on_result))
        except Exception:
            return None

    def stop(self) -> None:
        """Stop the polling loop."""
        with self._lock:
            self._stopped = True

    def is_running(self) -> bool:
        """Check if poller is currently running."""
        return self._running

    def register_callback(self, callback: Callable[["PollResult"], None]) -> None:
        """Register callback to be called on each poll result."""
        self._callbacks.append(callback)

    def get_stats(self) -> PollerStats:
        """Get poller statistics."""
        return self._stats

    def get_results(self, limit: int = 100) -> List[PollResult]:
        """Get recent poll results."""
        return list(self._results)[-limit:]

    def reset(self) -> None:
        """Reset poller state and statistics."""
        with self._lock:
            self._running = False
            self._stopped = False
            self._stats = PollerStats()
            self._results.clear()


class BatchPoller:
    """Poll multiple resources concurrently."""

    def __init__(self, config: Optional[PollerConfig] = None):
        self.config = config or PollerConfig()
        self._pollers: Dict[str, AutomationPollerAction] = {}

    async def poll_multiple_async(
        self,
        resources: Dict[str, Callable[[], Any]],
        condition_func: Optional[Callable[[Any], bool]] = None,
    ) -> Dict[str, Optional[PollResult]]:
        """Poll multiple resources concurrently."""
        tasks = {}
        for name, check_func in resources.items():
            poller = AutomationPollerAction(self.config)
            self._pollers[name] = poller
            tasks[name] = asyncio.create_task(
                poller.poll_async(check_func, condition_func)
            )

        results = {}
        for name, task in tasks.items():
            try:
                results[name] = await task
            except Exception:
                results[name] = None

        return results

    def poll_multiple(
        self,
        resources: Dict[str, Callable[[], Any]],
        condition_func: Optional[Callable[[Any], bool]] = None,
    ) -> Dict[str, Optional[PollResult]]:
        """Poll multiple resources (sync version)."""
        return asyncio.run(self.poll_multiple_async(resources, condition_func))
