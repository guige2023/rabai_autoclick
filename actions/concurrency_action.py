"""
Concurrency utilities - async execution, parallel processing, thread pool simulation, future.
"""
from typing import Any, Dict, List, Optional, Callable
import logging
import time
import threading
from collections import deque

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class SimpleFuture:
    def __init__(self) -> None:
        self._value: Any = None
        self._done = False
        self._error: Optional[Exception] = None
        self._lock = threading.Lock()

    def resolve(self, value: Any) -> None:
        with self._lock:
            self._value = value
            self._done = True

    def reject(self, error: Exception) -> None:
        with self._lock:
            self._error = error
            self._done = True

    def get(self, timeout: Optional[float] = None) -> Any:
        start = time.time()
        while True:
            with self._lock:
                if self._done:
                    if self._error:
                        raise self._error
                    return self._value
            if timeout and (time.time() - start) > timeout:
                raise TimeoutError("Future timed out")
            time.sleep(0.01)


class WorkerPool:
    def __init__(self, size: int = 4) -> None:
        self._size = size
        self._queue: deque = deque()
        self._running = False
        self._threads: List[threading.Thread] = []
        self._lock = threading.Lock()

    def submit(self, fn: Callable, *args, **kwargs) -> SimpleFuture:
        future = SimpleFuture()
        with self._lock:
            self._queue.append((fn, args, kwargs, future))
        return future

    def start(self) -> None:
        self._running = True
        for _ in range(self._size):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._threads.append(t)

    def _worker(self) -> None:
        while self._running:
            item = None
            with self._lock:
                if self._queue:
                    item = self._queue.popleft()
            if item:
                fn, args, kwargs, future = item
                try:
                    result = fn(*args, **kwargs)
                    future.resolve(result)
                except Exception as e:
                    future.reject(e)
            else:
                time.sleep(0.01)

    def stop(self) -> None:
        self._running = False
        for t in self._threads:
            t.join(timeout=1.0)


class ConcurrencyAction(BaseAction):
    """Concurrency operations.

    Provides future simulation, worker pool, parallel map, async coordination.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "future_create")

        try:
            if operation == "future_create":
                future_id = str(time.time())
                return {"success": True, "future_id": future_id, "done": False}

            elif operation == "future_resolve":
                value = params.get("value")
                return {"success": True, "resolved": True, "value": value}

            elif operation == "future_reject":
                error = params.get("error", "Unknown error")
                return {"success": True, "rejected": True, "error": error}

            elif operation == "parallel_map":
                fn_name = params.get("fn", "upper")
                items = params.get("items", [])
                results = []
                for item in items:
                    if fn_name == "upper":
                        results.append(str(item).upper())
                    elif fn_name == "lower":
                        results.append(str(item).lower())
                    elif fn_name == "double":
                        try:
                            results.append(float(item) * 2)
                        except (ValueError, TypeError):
                            results.append(item)
                    else:
                        results.append(item)
                return {"success": True, "results": results, "count": len(results)}

            elif operation == "batch_execute":
                tasks = params.get("tasks", [])
                results = []
                for task in tasks:
                    task_name = task.get("name", "unnamed")
                    task_type = task.get("type", "pass")
                    if task_type == "delay":
                        time.sleep(float(task.get("duration", 0.01)))
                        results.append({"name": task_name, "success": True})
                    else:
                        results.append({"name": task_name, "success": True})
                return {"success": True, "results": results, "count": len(results), "all_success": all(r["success"] for r in results)}

            elif operation == "sleep":
                duration = float(params.get("duration", 0.1))
                time.sleep(duration)
                return {"success": True, "slept": duration}

            elif operation == "wait_all":
                futures = params.get("futures", [])
                return {"success": True, "waiting_on": len(futures)}

            elif operation == "gather":
                results = params.get("results", [])
                return {"success": True, "gathered": results, "count": len(results)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"ConcurrencyAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return ConcurrencyAction().execute(context, params)
