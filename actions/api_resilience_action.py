"""API Resilience Action Module.

Implements bulkhead isolation, timeouts, and graceful degradation
patterns for API clients to handle partial failures.
"""

import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BulkheadConfig:
    max_concurrent: int = 10
    max_queue_size: int = 100
    timeout_sec: float = 30.0


@dataclass
class ResilienceResult:
    success: bool
    duration_ms: float
    output: Any = None
    error: Optional[str] = None
    fallback_used: bool = False


class APIResilienceAction:
    """Bulkhead isolation and resilience patterns for API calls."""

    def __init__(self) -> None:
        self._bulkhead_semaphore = threading.Semaphore(10)
        self._bulkhead_queue: List = []
        self._bulkhead_lock = threading.Lock()
        self._stats = {
            "calls_accepted": 0,
            "calls_rejected": 0,
            "calls_timeout": 0,
            "calls_success": 0,
            "calls_failed": 0,
            "fallback_used": 0,
        }

    def call_with_bulkhead(
        self,
        call_fn: Callable[[], Any],
        max_concurrent: int = 10,
        timeout_sec: float = 30.0,
    ) -> Tuple[bool, Any, Optional[str]]:
        acquired = False
        try:
            if not self._bulkhead_semaphore.acquire(timeout=timeout_sec):
                self._stats["calls_rejected"] += 1
                return False, None, "Bulkhead capacity exceeded"
            acquired = True
            self._stats["calls_accepted"] += 1
            start = time.time()
            result = call_fn()
            self._stats["calls_success"] += 1
            return True, result, None
        except TimeoutError:
            self._stats["calls_timeout"] += 1
            return False, None, "Call timeout"
        except Exception as e:
            self._stats["calls_failed"] += 1
            return False, None, str(e)
        finally:
            if acquired:
                self._bulkhead_semaphore.release()

    def call_with_timeout(
        self,
        call_fn: Callable[[], Any],
        timeout_sec: float = 30.0,
        fallback_fn: Optional[Callable[[], Any]] = None,
    ) -> ResilienceResult:
        start = time.time()
        try:
            result = call_fn()
            return ResilienceResult(
                success=True,
                duration_ms=(time.time() - start) * 1000,
                output=result,
            )
        except TimeoutError:
            self._stats["calls_timeout"] += 1
            if fallback_fn:
                try:
                    fallback_result = fallback_fn()
                    self._stats["fallback_used"] += 1
                    return ResilienceResult(
                        success=True,
                        duration_ms=(time.time() - start) * 1000,
                        output=fallback_result,
                        fallback_used=True,
                    )
                except Exception as e:
                    return ResilienceResult(
                        success=False,
                        duration_ms=(time.time() - start) * 1000,
                        error=f"Fallback failed: {e}",
                        fallback_used=True,
                    )
            return ResilienceResult(
                success=False,
                duration_ms=(time.time() - start) * 1000,
                error="Timeout",
            )
        except Exception as e:
            self._stats["calls_failed"] += 1
            return ResilienceResult(
                success=False,
                duration_ms=(time.time() - start) * 1000,
                error=str(e),
            )

    def call_with_circuit_breaker(
        self,
        call_fn: Callable[[], Any],
        failure_threshold: int = 5,
        timeout_sec: float = 60.0,
        half_open_requests: int = 3,
    ) -> Tuple[bool, Any, Optional[str]]:
        if not hasattr(self, "_circuit_state"):
            self._circuit_state = {
                "state": "closed",
                "failures": 0,
                "last_failure": 0.0,
                "half_open_count": 0,
            }
        state = self._circuit_state
        if state["state"] == "open":
            if time.time() - state["last_failure"] > timeout_sec:
                state["state"] = "half_open"
                state["half_open_count"] = 0
                logger.info("Circuit breaker entering half-open state")
            else:
                return False, None, "Circuit breaker open"
        try:
            result = call_fn()
            if state["state"] == "half_open":
                state["half_open_count"] += 1
                if state["half_open_count"] >= half_open_requests:
                    state["state"] = "closed"
                    state["failures"] = 0
                    logger.info("Circuit breaker closed")
            return True, result, None
        except Exception as e:
            state["failures"] += 1
            state["last_failure"] = time.time()
            if state["failures"] >= failure_threshold:
                state["state"] = "open"
                logger.warning("Circuit breaker opened due to failures")
            return False, None, str(e)

    def get_stats(self) -> Dict[str, Any]:
        total = self._stats["calls_success"] + self._stats["calls_failed"]
        return {
            **self._stats,
            "total_calls": total,
            "success_rate": self._stats["calls_success"] / total if total > 0 else 0,
        }

    def reset_stats(self) -> None:
        self._stats = {
            "calls_accepted": 0,
            "calls_rejected": 0,
            "calls_timeout": 0,
            "calls_success": 0,
            "calls_failed": 0,
            "fallback_used": 0,
        }
