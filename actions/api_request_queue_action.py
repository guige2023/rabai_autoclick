"""API Request Queue Action Module.

Provides persistent queue-based API request management with retry logic,
priority handling, and batch processing support.
"""

from __future__ import annotations

import sys
import os
import time
import json
import threading
import queue
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RequestPriority(Enum):
    """Priority levels for queued requests."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class RequestStatus(Enum):
    """Status of a queued request."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class QueuedRequest:
    """A queued API request."""
    request_id: str
    url: str
    method: str
    headers: Dict[str, str] = field(default_factory=dict)
    data: Any = None
    params: Dict[str, Any] = field(default_factory=dict)
    priority: RequestPriority = RequestPriority.NORMAL
    status: RequestStatus = RequestStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    attempts: int = 0
    max_attempts: int = 3
    timeout: float = 30.0
    retry_delay: float = 1.0
    last_error: Optional[str] = None
    response_data: Any = None


class PriorityQueue:
    """Thread-safe priority queue implementation."""

    def __init__(self):
        self._queues: Dict[RequestPriority, queue.Queue] = {
            p: queue.Queue() for p in RequestPriority
        }
        self._priority_order = list(RequestPriority)
        self._total_size = 0
        self._lock = threading.Lock()

    def put(self, request: QueuedRequest) -> None:
        """Add a request to the queue."""
        with self._lock:
            self._queues[request.priority].put(request)
            self._total_size += 1

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[QueuedRequest]:
        """Get the highest priority request."""
        with self._lock:
            for priority in self._priority_order:
                try:
                    item = self._queues[priority].get_nowait()
                    self._total_size -= 1
                    return item
                except queue.Empty:
                    continue
        if block:
            time.sleep(0.01)
            return self.get(block=False)
        return None

    def size(self) -> int:
        """Get total queue size."""
        with self._lock:
            return self._total_size

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return self.size() == 0


class ApiRequestQueueAction(BaseAction):
    """Queue and manage API requests with retry and priority.

    Provides persistent queuing, automatic retry with exponential backoff,
    priority-based processing, and batch execution support.
    """
    action_type = "api_request_queue"
    display_name = "API请求队列"
    description = "管理API请求队列，支持重试、优先级和批处理"

    def __init__(self):
        super().__init__()
        self._queue = PriorityQueue()
        self._requests: Dict[str, QueuedRequest] = {}
        self._lock = threading.Lock()
        self._stats = defaultdict(lambda: {
            "enqueued": 0, "completed": 0, "failed": 0, "processing": 0
        })

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute queue operation.

        Args:
            context: Execution context.
            params: Dict with keys: action (enqueue/process/status/clear),
                   url, method, priority, max_attempts, etc.

        Returns:
            ActionResult with operation result.
        """
        action = params.get("action", "enqueue")

        if action == "enqueue":
            return self._enqueue_request(context, params)
        elif action == "process":
            return self._process_queue(context, params)
        elif action == "status":
            return self._get_status(params)
        elif action == "clear":
            return self._clear_queue(params)
        elif action == "cancel":
            return self._cancel_request(params)
        elif action == "batch":
            return self._batch_enqueue(context, params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _enqueue_request(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Add a request to the queue."""
        import uuid

        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        priority_str = params.get("priority", "normal").upper()
        headers = params.get("headers", {})
        data = params.get("data", None)
        query_params = params.get("params", {})
        max_attempts = int(params.get("max_attempts", 3))
        timeout = float(params.get("timeout", 30.0))
        retry_delay = float(params.get("retry_delay", 1.0))
        save_to_var = params.get("save_to_var", None)

        if not url:
            return ActionResult(success=False, message="Parameter 'url' is required")

        try:
            priority = RequestPriority[priority_str]
        except KeyError:
            priority = RequestPriority.NORMAL

        request_id = str(uuid.uuid4())[:8]

        request = QueuedRequest(
            request_id=request_id,
            url=url,
            method=method,
            headers=headers,
            data=data,
            params=query_params,
            priority=priority,
            max_attempts=max_attempts,
            timeout=timeout,
            retry_delay=retry_delay
        )

        with self._lock:
            self._requests[request_id] = request
            self._stats["enqueued"] += 1

        self._queue.put(request)

        result_data = {
            "request_id": request_id,
            "url": url,
            "method": method,
            "priority": priority.name,
            "queue_size": self._queue.size()
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Request {request_id} enqueued ({priority.name}, "
                    f"{self._queue.size()} in queue)",
            data=result_data
        )

    def _batch_enqueue(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Add multiple requests to the queue."""
        requests_data = params.get("requests", [])
        default_priority = params.get("default_priority", "normal").upper()
        save_to_var = params.get("save_to_var", None)

        if not isinstance(requests_data, list):
            return ActionResult(
                success=False,
                message="Parameter 'requests' must be a list"
            )

        import uuid
        results = []

        try:
            default_prio = RequestPriority[default_priority]
        except KeyError:
            default_prio = RequestPriority.NORMAL

        for req_data in requests_data:
            if not isinstance(req_data, dict):
                continue
            request_id = str(uuid.uuid4())[:8]
            url = req_data.get("url", "")
            method = req_data.get("method", "GET").upper()

            try:
                priority = RequestPriority[req_data.get("priority", "").upper()]
            except (KeyError, AttributeError):
                priority = default_prio

            request = QueuedRequest(
                request_id=request_id,
                url=url,
                method=method,
                headers=req_data.get("headers", {}),
                data=req_data.get("data"),
                params=req_data.get("params", {}),
                priority=priority,
                max_attempts=req_data.get("max_attempts", 3),
                timeout=req_data.get("timeout", 30.0)
            )

            with self._lock:
                self._requests[request_id] = request
                self._stats["enqueued"] += 1

            self._queue.put(request)
            results.append({"request_id": request_id, "url": url, "priority": priority.name})

        result_data = {
            "enqueued": len(results),
            "queue_size": self._queue.size(),
            "requests": results
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Batch enqueued {len(results)} requests "
                    f"(queue size: {self._queue.size()})",
            data=result_data
        )

    def _process_queue(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Process requests from the queue."""
        batch_size = int(params.get("batch_size", 5))
        save_to_var = params.get("save_to_var", None)

        processed = []
        failed = []

        for _ in range(batch_size):
            request = self._queue.get(block=False)
            if not request:
                break

            request.status = RequestStatus.PROCESSING
            request.started_at = time.time()
            request.attempts += 1

            with self._lock:
                self._stats["processing"] += 1

            try:
                import urllib.request
                import urllib.parse
                import urllib.error

                full_url = request.url
                if request.params:
                    encoded = urllib.parse.urlencode(request.params)
                    sep = "&" if "?" in request.url else "?"
                    full_url = f"{request.url}{sep}{encoded}"

                body = None
                if request.data and request.method in ("POST", "PUT", "PATCH"):
                    if isinstance(request.data, dict):
                        body = json.dumps(request.data).encode("utf-8")
                        request.headers.setdefault("Content-Type", "application/json")
                    else:
                        body = str(request.data).encode("utf-8")

                req = urllib.request.Request(
                    full_url, data=body,
                    headers=request.headers,
                    method=request.method
                )

                with urllib.request.urlopen(req, timeout=request.timeout) as response:
                    raw = response.read()
                    content_type = response.headers.get("Content-Type", "")
                    if "application/json" in content_type:
                        request.response_data = json.loads(raw.decode("utf-8"))
                    else:
                        request.response_data = raw.decode("utf-8", errors="replace")

                    request.status = RequestStatus.COMPLETED
                    request.completed_at = time.time()
                    with self._lock:
                        self._stats["completed"] += 1
                        self._stats["processing"] = max(0, self._stats["processing"] - 1)
                    processed.append(request.request_id)

            except Exception as e:
                request.last_error = str(e)
                with self._lock:
                    self._stats["failed"] += 1
                    self._stats["processing"] = max(0, self._stats["processing"] - 1)

                if request.attempts < request.max_attempts:
                    wait_time = request.retry_delay * (2 ** (request.attempts - 1))
                    request.status = RequestStatus.PENDING
                    time.sleep(min(wait_time, 5.0))
                    self._queue.put(request)
                    processed.append(f"{request.request_id} (retry {request.attempts})")
                else:
                    request.status = RequestStatus.FAILED
                    failed.append(request.request_id)

        result_data = {
            "processed": len(processed),
            "failed": len(failed),
            "queue_size": self._queue.size(),
            "request_ids": processed
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=len(failed) == 0,
            message=f"Processed {len(processed)} requests, "
                    f"{len(failed)} failed, {self._queue.size()} remaining",
            data=result_data
        )

    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get queue status."""
        request_id = params.get("request_id", None)
        save_to_var = params.get("save_to_var", None)

        if request_id:
            with self._lock:
                req = self._requests.get(request_id)
                if not req:
                    return ActionResult(
                        success=False,
                        message=f"Request {request_id} not found"
                    )
                data = {
                    "request_id": req.request_id,
                    "status": req.status.value,
                    "url": req.url,
                    "priority": req.priority.name,
                    "attempts": req.attempts,
                    "last_error": req.last_error,
                    "created_at": req.created_at,
                    "started_at": req.started_at,
                    "completed_at": req.completed_at
                }
        else:
            data = {
                "queue_size": self._queue.size(),
                "stats": dict(self._stats),
                "requests": {
                    rid: {"status": r.status.value, "priority": r.priority.name}
                    for rid, r in list(self._requests.items())[:50]
                }
            }

        if save_to_var:
            context.variables[save_to_var] = data

        return ActionResult(
            success=True,
            message=f"Queue status retrieved",
            data=data
        )

    def _clear_queue(self, params: Dict[str, Any]) -> ActionResult:
        """Clear all pending requests."""
        with self._lock:
            pending = [r for r in self._requests.values()
                       if r.status == RequestStatus.PENDING]
            for r in pending:
                r.status = RequestStatus.CANCELLED
            cleared = len(pending)

        return ActionResult(
            success=True,
            message=f"Cleared {cleared} pending requests from queue"
        )

    def _cancel_request(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a specific queued request."""
        request_id = params.get("request_id", "")
        with self._lock:
            req = self._requests.get(request_id)
            if not req:
                return ActionResult(
                    success=False,
                    message=f"Request {request_id} not found"
                )
            if req.status == RequestStatus.PROCESSING:
                return ActionResult(
                    success=False,
                    message=f"Cannot cancel request {request_id} while processing"
                )
            req.status = RequestStatus.CANCELLED

        return ActionResult(
            success=True,
            message=f"Request {request_id} cancelled"
        )

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "url": "",
            "method": "GET",
            "priority": "normal",
            "headers": {},
            "data": None,
            "params": {},
            "max_attempts": 3,
            "timeout": 30.0,
            "retry_delay": 1.0,
            "batch_size": 5,
            "save_to_var": None,
            "requests": []
        }
