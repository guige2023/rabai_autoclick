"""
API Debounce Action Module.

Implements debouncing for API calls to prevent server overload.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, Hashable, Optional, Tuple


@dataclass
class DebounceConfig:
    """Configuration for debounce behavior."""
    wait_ms: int = 300
    max_wait_ms: int = 5000
    leading_edge: bool = False
    trailing_edge: bool = True


@dataclass
class PendingCall:
    """Represents a pending debounced call."""
    key: Hashable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    timer: Optional[asyncio.Task] = None


class ApiDebounceAction:
    """
    Debounces API calls to prevent excessive requests.

    Supports both leading and trailing edge execution.
    """

    def __init__(
        self,
        config: Optional[DebounceConfig] = None,
    ) -> None:
        self.config = config or DebounceConfig()
        self._pending: Dict[Hashable, PendingCall] = {}
        self._locks: Dict[Hashable, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._results: Dict[Hashable, Any] = {}

    async def execute(
        self,
        key: Hashable,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a debounced API call.

        Args:
            key: Unique identifier for this debounce group
            func: Async function to call
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from the debounced function call
        """
        lock = self._locks[key]

        async with lock:
            self._pending[key] = PendingCall(
                key=key,
                args=args,
                kwargs=kwargs,
            )

            call = self._pending[key]

            if call.timer is None:
                call.timer = asyncio.create_task(self._wait_and_execute(key, func))

            if self.config.leading_edge:
                return await self._execute_now(key, func)

            while key in self._pending:
                await asyncio.sleep(0.01)

            return self._results.get(key)

    async def _wait_and_execute(
        self,
        key: Hashable,
        func: Callable[..., Coroutine[Any, Any, Any]],
    ) -> None:
        """Wait for debounce period then execute."""
        wait_s = self.config.wait_ms / 1000.0
        await asyncio.sleep(wait_s)

        async with self._locks[key]:
            if key in self._pending:
                call = self._pending.pop(key)
                call.timer = None

                try:
                    self._results[key] = await func(*call.args, **call.kwargs)
                except Exception as e:
                    self._results[key] = e

    async def _execute_now(
        self,
        key: Hashable,
        func: Callable[..., Coroutine[Any, Any, Any]],
    ) -> Any:
        """Execute immediately on leading edge."""
        if key in self._pending:
            call = self._pending.pop(key)
            if call.timer:
                call.timer.cancel()

            try:
                return await func(*call.args, **call.kwargs)
            except Exception as e:
                return e
        return None

    def cancel(self, key: Hashable) -> bool:
        """
        Cancel a pending debounced call.

        Returns:
            True if a call was cancelled, False otherwise
        """
        if key in self._pending:
            call = self._pending.pop(key)
            if call.timer:
                call.timer.cancel()
            return True
        return False

    def cancel_all(self) -> int:
        """
        Cancel all pending debounced calls.

        Returns:
            Number of calls cancelled
        """
        count = len(self._pending)
        for call in self._pending.values():
            if call.timer:
                call.timer.cancel()
        self._pending.clear()
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get debounce statistics."""
        return {
            "pending_count": len(self._pending),
            "pending_keys": list(self._pending.keys()),
            "config": {
                "wait_ms": self.config.wait_ms,
                "max_wait_ms": self.config.max_wait_ms,
                "leading_edge": self.config.leading_edge,
                "trailing_edge": self.config.trailing_edge,
            },
        }
