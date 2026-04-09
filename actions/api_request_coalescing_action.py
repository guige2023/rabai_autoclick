"""API Request Coalescing Action Module.

Coalesces multiple concurrent requests for the same resource
into a single outbound call, deduplicating redundant requests.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import threading
import time
import logging
from concurrent.futures import Future

logger = logging.getLogger(__name__)


@dataclass
class PendingRequest:
    """A pending request awaiting coalescing."""
    key: str
    future: Future[Any]
    created_at: float
    callback: Optional[Callable[..., Any]] = None


class APIRequestCoalescingAction:
    """Coalesces concurrent duplicate API requests.
    
    When multiple callers request the same resource simultaneously,
    only one actual API call is made; all callers share the result.
    """

    def __init__(self, ttl_sec: float = 5.0) -> None:
        self.ttl_sec = ttl_sec
        self._pending: Dict[str, PendingRequest] = {}
        self._lock = threading.Lock()
        self._stats: Dict[str, int] = {
            "coalesced": 0,
            "direct": 0,
            "total": 0,
        }

    def make_request(
        self,
        key: str,
        fetcher: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future[Any]:
        """Make a coalesced request.
        
        Args:
            key: Unique key identifying this request (shared requests use same key).
            fetcher: Function that actually fetches data.
            *args: Args passed to fetcher.
            **kwargs: Kwargs passed to fetcher.
        
        Returns:
            Future that will contain the result when ready.
        """
        self._stats["total"] += 1
        with self._lock:
            if key in self._pending:
                self._stats["coalesced"] += 1
                logger.debug("Coalescing request: %s", key)
                return self._pending[key].future

            future: Future[Any] = Future()
            pending = PendingRequest(key=key, future=future, created_at=time.time())
            self._pending[key] = pending

        def _execute() -> None:
            try:
                result = fetcher(*args, **kwargs)
                future.set_result(result)
                logger.debug("Request completed: %s", key)
            except Exception as exc:
                future.set_exception(exc)
                logger.warning("Request failed: %s -> %s", key, exc)
            finally:
                self._cleanup(key)

        threading.Thread(target=_execute, daemon=True).start()
        return future

    def _cleanup(self, key: str) -> None:
        with self._lock:
            if key in self._pending:
                del self._pending[key]
        now = time.time()
        expired = [
            k for k, p in list(self._pending.items())
            if now - p.created_at > self.ttl_sec
        ]
        for k in expired:
            del self._pending[k]

    def cancel_pending(self, key: str) -> bool:
        """Cancel a pending request if it exists.
        
        Args:
            key: The request key to cancel.
        
        Returns:
            True if a pending request was found and cancelled.
        """
        with self._lock:
            if key in self._pending:
                self._pending[key].future.cancel()
                del self._pending[key]
                return True
        return False

    def get_pending_count(self) -> int:
        """Get number of currently pending requests."""
        with self._lock:
            return len(self._pending)

    def get_stats(self) -> Dict[str, Any]:
        """Get coalescing statistics."""
        total = self._stats["total"]
        return {
            "total_requests": total,
            "coalesced": self._stats["coalesced"],
            "direct": self._stats["direct"],
            "coalescing_rate": round(self._stats["coalesced"] / total, 4) if total > 0 else 0.0,
            "pending": self.get_pending_count(),
        }
