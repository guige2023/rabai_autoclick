"""
Bulkhead isolation action for resource isolation.

Provides thread/connection pool isolation per service.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading


class BulkheadIsolationAction:
    """Bulkhead pattern for resource isolation."""

    def __init__(
        self,
        max_concurrent: int = 100,
        max_queue_size: int = 50,
        timeout: float = 30.0,
    ) -> None:
        """
        Initialize bulkhead isolation.

        Args:
            max_concurrent: Maximum concurrent executions
            max_queue_size: Maximum queue size for waiting
            timeout: Execution timeout in seconds
        """
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self.timeout = timeout

        self._semaphore = threading.Semaphore(max_concurrent)
        self._active_count = 0
        self._queue_count = 0
        self._rejected_count = 0
        self._completed_count = 0
        self._lock = threading.Lock()

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute bulkhead operation.

        Args:
            params: Dictionary containing:
                - operation: 'execute', 'status', 'reset'
                - action: Action to execute
                - wait: Whether to wait if queue is full

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "execute")

        if operation == "execute":
            return self._execute_isolated(params)
        elif operation == "status":
            return self._get_status(params)
        elif operation == "reset":
            return self._reset(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _execute_isolated(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute action with bulkhead isolation."""
        action = params.get("action")
        wait = params.get("wait", True)

        start_time = time.time()

        acquired = self._semaphore.acquire(timeout=self.timeout if wait else 0)

        if not acquired:
            with self._lock:
                self._rejected_count += 1
            return {
                "success": False,
                "error": "Bulkhead capacity exceeded",
                "rejected": True,
                "active_count": self._active_count,
                "queue_size": self._queue_count,
            }

        try:
            with self._lock:
                self._active_count += 1

            if callable(action):
                result = action()
            else:
                result = {"success": True, "message": "Action completed"}

            elapsed = time.time() - start_time

            with self._lock:
                self._completed_count += 1

            return {
                "success": True,
                "result": result,
                "elapsed_seconds": elapsed,
                "active_count": self._active_count,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "active_count": self._active_count,
            }

        finally:
            with self._lock:
                self._active_count = max(0, self._active_count - 1)
            self._semaphore.release()

    def _get_status(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get bulkhead status."""
        with self._lock:
            return {
                "success": True,
                "max_concurrent": self.max_concurrent,
                "current_active": self._active_count,
                "available_capacity": self.max_concurrent - self._active_count,
                "total_completed": self._completed_count,
                "total_rejected": self._rejected_count,
            }

    def _reset(self, params: dict[str, Any]) -> dict[str, Any]:
        """Reset bulkhead counters."""
        with self._lock:
            self._active_count = 0
            self._queue_count = 0
            self._rejected_count = 0
            self._completed_count = 0

        return {"success": True, "message": "Bulkhead reset"}
