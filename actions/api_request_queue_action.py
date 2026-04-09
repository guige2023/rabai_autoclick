"""API Request Queue Action.

Manages a persistent queue for API requests with priority support,
deduplication, batching, and dead-letter handling.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class Priority(Enum):
    """Request priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BATCH = 4


@dataclass
class QueuedRequest:
    """A queued API request."""
    id: str
    endpoint: str
    method: str
    params: Dict[str, Any]
    priority: Priority
    created_at: float
    attempts: int = 0
    last_attempt: Optional[float] = None
    headers: Dict[str, str] = field(default_factory=dict)
    timeout_sec: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeadLetterEntry:
    """A failed request that exceeded retry limit."""
    request: QueuedRequest
    error: str
    failed_at: float
    retry_count: int


@dataclass
class QueueStats:
    """Queue statistics."""
    total_enqueued: int = 0
    total_processed: int = 0
    total_failed: int = 0
    total_deduplicated: int = 0
    current_size: int = 0
    avg_wait_time: float = 0.0
    avg_process_time: float = 0.0


class APIRequestQueueAction:
    """Persistent queue manager for API requests."""

    def __init__(
        self,
        max_retries: int = 3,
        deduplication_window_sec: float = 300.0,
        dead_letter_max: int = 1000,
    ) -> None:
        self.max_retries = max_retries
        self.dedup_window = deduplication_window_sec
        self.dead_letter_max = dead_letter_max

        self._queues: Dict[Priority, List[QueuedRequest]] = {
            p: [] for p in Priority
        }
        self._seen_hashes: Dict[str, float] = {}
        self._dead_letter: List[DeadLetterEntry] = []
        self._processing: Set[str] = set()
        self._stats = QueueStats()
        self._lock = __import__("threading").Lock()

    def enqueue(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        priority: Priority = Priority.NORMAL,
        headers: Optional[Dict[str, str]] = None,
        timeout_sec: float = 30.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Enqueue an API request."""
        request_id = self._generate_id(endpoint, method, params or {})

        with self._lock:
            if self._is_duplicate(endpoint, method, params or {}):
                self._stats.total_deduplicated += 1
                return request_id

            request = QueuedRequest(
                id=request_id,
                endpoint=endpoint,
                method=method,
                params=params or {},
                priority=priority,
                created_at=time.time(),
                headers=headers or {},
                timeout_sec=timeout_sec,
                metadata=metadata or {},
            )

            self._queues[priority].append(request)
            self._stats.total_enqueued += 1
            self._stats.current_size += 1

        return request_id

    def dequeue(self) -> Optional[QueuedRequest]:
        """Dequeue the highest priority request."""
        with self._lock:
            for priority in Priority:
                queue = self._queues[priority]
                if queue:
                    request = queue.pop(0)
                    self._stats.current_size -= 1
                    self._processing.add(request.id)
                    return request

        return None

    def mark_processed(self, request_id: str) -> None:
        """Mark a request as successfully processed."""
        with self._lock:
            if request_id in self._processing:
                self._processing.remove(request_id)
                self._stats.total_processed += 1

    def mark_failed(
        self,
        request_id: str,
        error: str,
        retry_count: int,
    ) -> bool:
        """Mark a request as failed. Returns True if moved to dead letter."""
        with self._lock:
            if request_id not in self._processing:
                return False

            request = None
            for queue in self._queues.values():
                for req in queue:
                    if req.id == request_id:
                        request = req
                        break

            if request is None:
                self._processing.remove(request_id)
                return False

            request.attempts += 1
            request.last_attempt = time.time()

            if request.attempts >= self.max_retries:
                self._processing.remove(request_id)
                self._move_to_dead_letter(request, error, retry_count)
                self._stats.total_failed += 1
                return True

            self._queues[request.priority].insert(0, request)
            self._stats.current_size += 1
            self._processing.remove(request_id)

        return False

    def _move_to_dead_letter(
        self,
        request: QueuedRequest,
        error: str,
        retry_count: int,
    ) -> None:
        """Move a failed request to dead letter queue."""
        entry = DeadLetterEntry(
            request=request,
            error=error,
            failed_at=time.time(),
            retry_count=retry_count,
        )
        self._dead_letter.append(entry)

        if len(self._dead_letter) > self.dead_letter_max:
            self._dead_letter = self._dead_letter[-self.dead_letter_max:]

    def _is_duplicate(
        self,
        endpoint: str,
        method: str,
        params: Dict[str, Any],
    ) -> bool:
        """Check if an identical request is in the deduplication window."""
        key = self._hash_request(endpoint, method, params)
        now = time.time()

        expired = [k for k, v in self._seen_hashes.items() if now - v > self.dedup_window]
        for k in expired:
            del self._seen_hashes[k]

        if key in self._seen_hashes:
            return True

        self._seen_hashes[key] = now
        return False

    def _generate_id(
        self,
        endpoint: str,
        method: str,
        params: Dict[str, Any],
    ) -> str:
        """Generate a unique ID for a request."""
        raw = f"{method}:{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _hash_request(
        self,
        endpoint: str,
        method: str,
        params: Dict[str, Any],
    ) -> str:
        """Create a hash for deduplication."""
        raw = f"{method}:{endpoint}:{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get_stats(self) -> QueueStats:
        """Get current queue statistics."""
        with self._lock:
            return QueueStats(
                total_enqueued=self._stats.total_enqueued,
                total_processed=self._stats.total_processed,
                total_failed=self._stats.total_failed,
                total_deduplicated=self._stats.total_deduplicated,
                current_size=self._stats.current_size,
                avg_wait_time=self._stats.avg_wait_time,
                avg_process_time=self._stats.avg_process_time,
            )

    def get_dead_letter(self, limit: int = 100) -> List[DeadLetterEntry]:
        """Get dead letter entries."""
        with self._lock:
            return self._dead_letter[-limit:]

    def size(self) -> int:
        """Get total queue size."""
        with self._lock:
            return self._stats.current_size

    def clear_dead_letter(self) -> int:
        """Clear dead letter queue. Returns count cleared."""
        with self._lock:
            count = len(self._dead_letter)
            self._dead_letter.clear()
            return count
