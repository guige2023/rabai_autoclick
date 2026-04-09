"""
Automation Timer Action Module.

Provides timer-based automation with precise scheduling,
interval execution, debouncing, and throttling capabilities.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class TimerType(Enum):
    """Timer types."""
    INTERVAL = "interval"
    TIMEOUT = "timeout"
    DEBOUNCE = "debounce"
    THROTTLE = "throttle"
    CRON = "cron"


@dataclass
class TimerConfig:
    """Timer configuration."""
    timer_type: TimerType = TimerType.INTERVAL
    interval_seconds: float = 1.0
    max_executions: int = 0
    immediate: bool = False
    name: str = ""


@dataclass
class TimerTask:
    """Scheduled timer task."""
    task_id: str
    callback: Callable
    config: TimerConfig
    next_run: float = 0.0
    executions: int = 0
    active: bool = True


@dataclass
class ExecutionRecord:
    """Record of timer execution."""
    task_id: str
    execution_time: float
    duration_ms: float
    success: bool
    error: Optional[str] = None


class TimerManager:
    """Manages multiple timer tasks."""

    def __init__(self):
        self._tasks: dict[str, TimerTask] = {}
        self._execution_history: list[ExecutionRecord] = []
        self._running = False
        self._main_loop: Optional[asyncio.Task] = None

    def schedule(
        self,
        callback: Callable,
        config: TimerConfig,
    ) -> str:
        """Schedule a new timer task."""
        task_id = str(uuid.uuid4())
        interval = config.interval_seconds
        next_run = time.time() + (0 if config.immediate else interval)

        task = TimerTask(
            task_id=task_id,
            callback=callback,
            config=config,
            next_run=next_run,
        )
        self._tasks[task_id] = task
        return task_id

    def interval(
        self,
        callback: Callable,
        seconds: float,
        immediate: bool = False,
        max_executions: int = 0,
    ) -> str:
        """Schedule an interval timer."""
        config = TimerConfig(
            timer_type=TimerType.INTERVAL,
            interval_seconds=seconds,
            immediate=immediate,
            max_executions=max_executions,
        )
        return self.schedule(callback, config)

    def timeout(self, callback: Callable, seconds: float) -> str:
        """Schedule a one-time timeout."""
        config = TimerConfig(
            timer_type=TimerType.TIMEOUT,
            interval_seconds=seconds,
            max_executions=1,
        )
        return self.schedule(callback, config)

    def cancel(self, task_id: str) -> bool:
        """Cancel a timer task."""
        if task_id in self._tasks:
            self._tasks[task_id].active = False
            return True
        return False

    async def start(self) -> None:
        """Start the timer manager."""
        if self._running:
            return
        self._running = True
        self._main_loop = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop the timer manager."""
        self._running = False
        if self._main_loop:
            self._main_loop.cancel()
            try:
                await self._main_loop
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        """Main timer loop."""
        while self._running:
            now = time.time()
            for task in list(self._tasks.values()):
                if not task.active:
                    continue
                if task.config.timer_type == TimerType.TIMEOUT and task.executions > 0:
                    continue
                if now >= task.next_run:
                    await self._execute_task(task)
                    task.executions += 1
                    if task.config.max_executions > 0 and task.executions >= task.config.max_executions:
                        task.active = False
                    elif task.config.timer_type == TimerType.INTERVAL:
                        task.next_run = time.time() + task.config.interval_seconds
                    elif task.config.timer_type == TimerType.TIMEOUT:
                        task.active = False
            await asyncio.sleep(0.01)

    async def _execute_task(self, task: TimerTask) -> None:
        """Execute a timer task."""
        start = time.time()
        try:
            result = task.callback()
            if asyncio.iscoroutine(result):
                await result
            success = True
            error = None
        except Exception as e:
            success = False
            error = str(e)
        duration_ms = (time.time() - start) * 1000

        self._execution_history.append(ExecutionRecord(
            task_id=task.task_id,
            execution_time=time.time(),
            duration_ms=duration_ms,
            success=success,
            error=error,
        ))


class Debouncer:
    """Debounce utility - waits for silence before executing."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self._pending: Optional[asyncio.Task] = None
        self._last_call: float = 0

    async def call(self, callback: Callable) -> None:
        """Debounced call."""
        self._last_call = time.time()
        if self._pending:
            self._pending.cancel()
        self._pending = asyncio.create_task(self._wait_and_execute(callback))

    async def _wait_and_execute(self, callback: Callable) -> None:
        """Wait for delay then execute if no new calls."""
        await asyncio.sleep(self.delay)
        if time.time() - self._last_call >= self.delay:
            result = callback()
            if asyncio.iscoroutine(result):
                await result


class Throttler:
    """Throttle utility - limits execution rate."""

    def __init__(self, rate: float, burst: int = 1):
        self.rate = rate
        self.burst = burst
        self._tokens = float(burst)
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Acquire permission to execute."""
        async with self._lock:
            self._refill()
            if self._tokens >= 1:
                self._tokens -= 1
                return True
            return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        new_tokens = elapsed * self.rate
        self._tokens = min(self.burst, self._tokens + new_tokens)
        self._last_refill = now

    async def run(self, callback: Callable) -> Any:
        """Execute callback if throttler permits."""
        if await self.acquire():
            result = callback()
            if asyncio.iscoroutine(result):
                return await result
            return result
        return None


class CountdownTimer:
    """Countdown timer with progress callbacks."""

    def __init__(self, duration: float):
        self.duration = duration
        self._remaining = duration
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def remaining(self) -> float:
        """Remaining time in seconds."""
        return self._remaining

    async def start(
        self,
        on_tick: Optional[Callable[[float], None]] = None,
        on_complete: Optional[Callable] = None,
    ) -> None:
        """Start the countdown."""
        self._running = True
        start = time.time()

        while self._running and self._remaining > 0:
            elapsed = time.time() - start
            self._remaining = max(0, self.duration - elapsed)
            if on_tick:
                result = on_tick(self._remaining)
                if asyncio.iscoroutine(result):
                    await result
            await asyncio.sleep(0.1)

        if self._remaining <= 0 and on_complete:
            result = on_complete()
            if asyncio.iscoroutine(result):
                await result

    def stop(self) -> None:
        """Stop the countdown."""
        self._running = False


class Stopwatch:
    """High-precision stopwatch."""

    def __init__(self):
        self._start_time: Optional[float] = None
        self._stop_time: Optional[float] = None
        self._lap_times: list[float] = []

    def start(self) -> None:
        """Start the stopwatch."""
        self._start_time = time.perf_counter()
        self._stop_time = None
        self._lap_times.clear()

    def stop(self) -> float:
        """Stop and return elapsed time."""
        if self._start_time is None:
            return 0.0
        self._stop_time = time.perf_counter()
        return self.elapsed

    def reset(self) -> None:
        """Reset the stopwatch."""
        self._start_time = None
        self._stop_time = None
        self._lap_times.clear()

    def lap(self) -> float:
        """Record a lap time and return elapsed."""
        if self._start_time is None:
            return 0.0
        lap_time = time.perf_counter() - self._start_time
        self._lap_times.append(lap_time)
        return lap_time

    @property
    def elapsed(self) -> float:
        """Get elapsed time."""
        if self._start_time is None:
            return 0.0
        if self._stop_time is not None:
            return self._stop_time - self._start_time
        return time.perf_counter() - self._start_time

    @property
    def laps(self) -> list[float]:
        """Get lap times."""
        return self._lap_times.copy()


async def demo():
    """Demo timer operations."""
    manager = TimerManager()
    counter = {"value": 0}

    def increment():
        counter["value"] += 1
        print(f"Counter: {counter['value']}")

    task_id = manager.interval(increment, 0.5, immediate=True, max_executions=5)
    await manager.start()
    await asyncio.sleep(3)
    await manager.stop()
    print(f"Final count: {counter['value']}")

    sw = Stopwatch()
    sw.start()
    await asyncio.sleep(0.1)
    sw.stop()
    print(f"Elapsed: {sw.elapsed * 1000:.2f}ms")


if __name__ == "__main__":
    asyncio.run(demo())
