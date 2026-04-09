"""API debounce action for debouncing rapid API requests.

Provides debounce functionality for preventing excessive
API calls during burst traffic.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class DebounceRequest:
    request_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    scheduled_at: float = field(default_factory=time.time)
    executed: bool = False


class APIDebounceAction:
    """Debounce rapid API requests to prevent flooding.

    Args:
        default_delay: Default debounce delay in seconds.
        max_wait: Maximum time to wait before executing.
    """

    def __init__(
        self,
        default_delay: float = 0.5,
        max_wait: float = 5.0,
    ) -> None:
        self._default_delay = default_delay
        self._max_wait = max_wait
        self._pending_requests: dict[str, DebounceRequest] = {}
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()
        self._execution_count = 0
        self._debounced_count = 0

    def debounce(
        self,
        request_id: str,
        func: Callable,
        delay: Optional[float] = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Schedule a debounced request.

        Args:
            request_id: Unique request identifier.
            func: Function to execute.
            delay: Debounce delay (uses default if None).
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
        """
        with self._lock:
            if request_id in self._pending_requests:
                self._debounced_count += 1

            if request_id in self._timers:
                self._timers[request_id].cancel()

            debounce_request = DebounceRequest(
                request_id=request_id,
                func=func,
                args=args,
                kwargs=kwargs,
            )

            self._pending_requests[request_id] = debounce_request

            delay_time = delay or self._default_delay

            timer = threading.Timer(delay_time, self._execute_request, args=(request_id,))
            self._timers[request_id] = timer
            timer.start()

            logger.debug(f"Debounced request {request_id} with delay {delay_time}s")

    def _execute_request(self, request_id: str) -> None:
        """Execute a debounced request.

        Args:
            request_id: Request identifier.
        """
        with self._lock:
            request = self._pending_requests.pop(request_id, None)
            timer = self._timers.pop(request_id, None)

        if not request or request.executed:
            return

        try:
            request.func(*request.args, **request.kwargs)
            request.executed = True
            self._execution_count += 1
            logger.debug(f"Executed debounced request: {request_id}")
        except Exception as e:
            logger.error(f"Debounced request error: {request_id}: {e}")

    def flush(self, request_id: Optional[str] = None) -> bool:
        """Flush pending requests immediately.

        Args:
            request_id: Specific request to flush (all if None).

        Returns:
            True if flush was executed.
        """
        if request_id:
            with self._lock:
                if request_id in self._timers:
                    self._timers[request_id].cancel()
                    del self._timers[request_id]

            self._execute_request(request_id)
            return True
        else:
            with self._lock:
                for timer in self._timers.values():
                    timer.cancel()
                request_ids = list(self._pending_requests.keys())

            for rid in request_ids:
                self._execute_request(rid)

            return True

    def cancel(self, request_id: str) -> bool:
        """Cancel a pending debounced request.

        Args:
            request_id: Request to cancel.

        Returns:
            True if cancelled.
        """
        with self._lock:
            if request_id in self._timers:
                self._timers[request_id].cancel()
                del self._timers[request_id]

            if request_id in self._pending_requests:
                del self._pending_requests[request_id]

            return True

        return False

    def get_pending_count(self) -> int:
        """Get number of pending requests.

        Returns:
            Number of pending requests.
        """
        with self._lock:
            return len(self._pending_requests)

    def is_pending(self, request_id: str) -> bool:
        """Check if a request is pending.

        Args:
            request_id: Request identifier.

        Returns:
            True if request is pending.
        """
        with self._lock:
            return request_id in self._pending_requests

    def get_stats(self) -> dict[str, Any]:
        """Get debounce statistics.

        Returns:
            Dictionary with stats.
        """
        with self._lock:
            return {
                "pending_requests": len(self._pending_requests),
                "active_timers": len(self._timers),
                "total_executed": self._execution_count,
                "total_debounced": self._debounced_count,
                "default_delay": self._default_delay,
                "max_wait": self._max_wait,
            }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        with self._lock:
            self._execution_count = 0
            self._debounced_count = 0
