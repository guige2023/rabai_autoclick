"""
Latch Action Module.

Provides CountDownLatch and other latch primitives for
coordinating concurrent operations.
"""

import asyncio
import threading
import time
from typing import Optional, Callable, List, Any
from dataclasses import dataclass, field
from enum import Enum


class LatchState(Enum):
    """Latch states."""
    WAITING = "waiting"
    RELEASED = "released"
    TIMED_OUT = "timed_out"


@dataclass
class LatchConfig:
    """Configuration for latch behavior."""
    timeout: Optional[float] = None
    default_count: int = 1


class CountDownLatch:
    """
    CountDownLatch implementation for coordinating concurrent tasks.

    Example:
        latch = CountDownLatch(count=3)
        # In worker threads:
        latch.count_down()
        # In main thread:
        latch.await_timeout(30.0)
    """

    def __init__(self, count: int):
        if count < 0:
            raise ValueError("Count cannot be negative")
        self._count = count
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._state = LatchState.WAITING

    @property
    def count(self) -> int:
        """Current count value."""
        with self._lock:
            return self._count

    def count_down(self) -> None:
        """Decrement the count and release if reaching zero."""
        with self._lock:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._state = LatchState.RELEASED
                    self._condition.notify_all()

    def await_timeout(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for count to reach zero with optional timeout.

        Args:
            timeout: Maximum seconds to wait (None = forever)

        Returns:
            True if count reached zero, False if timed out
        """
        with self._lock:
            if self._count == 0:
                return True

            if timeout is None:
                self._condition.wait_for(lambda: self._count == 0)
                return True

            end_time = time.time() + timeout
            while self._count > 0:
                remaining = end_time - time.time()
                if remaining <= 0:
                    self._state = LatchState.TIMED_OUT
                    return False

                self._condition.wait(timeout=remaining)
                if self._count == 0:
                    self._state = LatchState.RELEASED
                    return True

            return False

    def reset(self, count: Optional[int] = None) -> None:
        """Reset the latch to a new count."""
        with self._lock:
            if count is not None:
                self._count = max(0, count)
            else:
                self._count = 0
            self._state = LatchState.WAITING
            self._condition.notify_all()

    @property
    def state(self) -> LatchState:
        """Current latch state."""
        with self._lock:
            return self._state


class AsyncCountDownLatch:
    """Async version of CountDownLatch."""

    def __init__(self, count: int):
        if count < 0:
            raise ValueError("Count cannot be negative")
        self._count = count
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition(self._lock)
        self._state = LatchState.WAITING

    @property
    def count(self) -> int:
        return self._count

    async def count_down(self) -> None:
        async with self._lock:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._state = LatchState.RELEASED
                    self._condition.notify_all()

    async def wait(self, timeout: Optional[float] = None) -> bool:
        """Wait for count to reach zero."""
        async with self._lock:
            if self._count == 0:
                return True

            if timeout is None:
                await self._condition.wait_for(lambda: self._count == 0)
                return True

            try:
                await asyncio.wait_for(
                    self._condition.wait_for(lambda: self._count == 0),
                    timeout=timeout,
                )
                return True
            except asyncio.TimeoutError:
                self._state = LatchState.TIMED_OUT
                return False


class LatchAction:
    """
    Action wrapper that uses latch coordination.

    Example:
        action = LatchAction("coordinated_task")
        latch = action.create_latch(3)
        # Schedule 3 workers...
        action.execute_coordinated(lambda: work(), latch)
    """

    def __init__(self, name: str):
        self.name = name
        self._latches: dict = {}
        self._lock = threading.RLock()

    def create_latch(self, count: int, key: str = "default") -> CountDownLatch:
        """Create a named latch."""
        with self._lock:
            latch = CountDownLatch(count)
            self._latches[key] = latch
            return latch

    def create_async_latch(
        self,
        count: int,
        key: str = "default",
    ) -> AsyncCountDownLatch:
        """Create a named async latch."""
        with self._lock:
            latch = AsyncCountDownLatch(count)
            self._latches[f"async_{key}"] = latch
            return latch

    def get_latch(self, key: str) -> Optional[CountDownLatch]:
        """Get a latch by name."""
        with self._lock:
            return self._latches.get(key)

    def count_down(self, key: str = "default") -> None:
        """Count down a latch."""
        latch = self.get_latch(key)
        if latch:
            latch.count_down()

    def await_latch(
        self,
        key: str = "default",
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait for a latch to be released."""
        latch = self.get_latch(key)
        if latch:
            return latch.await_timeout(timeout)
        return True

    def execute_coordinated(
        self,
        func: Callable,
        latch: CountDownLatch,
    ) -> Any:
        """Execute function and count down latch on completion."""
        try:
            result = func()
            latch.count_down()
            return result
        except Exception as e:
            latch.count_down()
            raise

    async def execute_coordinated_async(
        self,
        func: Callable,
        latch: AsyncCountDownLatch,
    ) -> Any:
        """Execute async function and count down latch on completion."""
        try:
            result = await func()
            await latch.count_down()
            return result
        except Exception as e:
            await latch.count_down()
            raise

    def barrier(self, timeout: Optional[float] = None) -> bool:
        """Wait for all registered latches to reach zero."""
        with self._lock:
            latches = list(self._latches.values())

        for latch in latches:
            if not latch.await_timeout(timeout):
                return False
        return True

    def remove_latch(self, key: str) -> bool:
        """Remove a latch."""
        with self._lock:
            if key in self._latches:
                del self._latches[key]
                return True
            return False

    def get_latch_states(self) -> dict:
        """Get states of all latches."""
        with self._lock:
            return {k: v.state.value for k, v in self._latches.items()}


class PhaserLatch:
    """Phaser-based latch that supports dynamic party registration."""

    def __init__(self):
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._parties = 0
        self._arrived = 0
        self._phase = 0

    def register(self) -> None:
        """Register a party."""
        with self._lock:
            self._parties += 1

    def arrive_and_wait(self, timeout: Optional[float] = None) -> bool:
        """Arrive at phaser and wait for others."""
        with self._lock:
            self._arrived += 1

            if self._arrived == self._parties:
                self._arrived = 0
                self._phase += 1
                self._condition.notify_all()
                return True

            if timeout is None:
                self._condition.wait_for(
                    lambda: self._arrived == 0 or self._phase > 0
                )
                return True

            end_time = time.time() + timeout
            while self._arrived != 0:
                remaining = end_time - time.time()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)

            return True
