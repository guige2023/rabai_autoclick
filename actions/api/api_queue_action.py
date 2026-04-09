"""API Queue Action Module.

Provides queue-based API request handling with priority queuing,
request ordering, and queue depth management.

Example:
    >>> from actions.api.api_queue_action import APIQueueAction
    >>> action = APIQueueAction()
    >>> await action.enqueue(request, priority=10)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from heapq import heappush, heappop
from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid


class QueuePriority(Enum):
    """Queue priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class QueueStatus(Enum):
    """Queue status."""
    IDLE = "idle"
    PROCESSING = "processing"
    PAUSED = "paused"
    DRAINING = "draining"


@dataclass
class QueuedRequest:
    """Request in the queue.
    
    Attributes:
        request_id: Unique request identifier
        priority: Request priority
        data: Request payload
        created_at: Creation timestamp
        scheduled_at: Scheduled execution time
        retries: Number of retry attempts
    """
    request_id: str
    priority: int
    data: Dict[str, Any]
    created_at: float
    scheduled_at: float
    retries: int = 0
    timeout: float = 30.0


@dataclass
class QueueConfig:
    """Configuration for queue handling.
    
    Attributes:
        max_size: Maximum queue size
        max_retries: Maximum retry attempts
        retry_delay: Delay between retries
        processing_timeout: Request timeout
        drain_timeout: Drain operation timeout
    """
    max_size: int = 10000
    max_retries: int = 3
    retry_delay: float = 1.0
    processing_timeout: float = 30.0
    drain_timeout: float = 60.0


@dataclass
class QueueStats:
    """Queue statistics.
    
    Attributes:
        size: Current queue size
        processed: Total processed requests
        failed: Total failed requests
        avg_wait_time: Average wait time
        avg_process_time: Average process time
    """
    size: int
    processed: int
    failed: int
    avg_wait_time: float
    avg_process_time: float


class APIQueueAction:
    """Queue-based request handler for API operations.
    
    Manages request queuing with priority support, retries,
    and configurable processing behavior.
    
    Attributes:
        config: Queue configuration
        _queue: Priority queue heap
        _processed: Processed request IDs
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[QueueConfig] = None,
    ) -> None:
        """Initialize queue action.
        
        Args:
            config: Queue configuration
        """
        self.config = config or QueueConfig()
        self._queue: List[QueuedRequest] = []
        self._processed: Dict[str, bool] = {}
        self._lock = threading.RLock()
        self._status = QueueStatus.IDLE
        self._stats = QueueStats(0, 0, 0, 0.0, 0.0)
    
    async def enqueue(
        self,
        data: Dict[str, Any],
        priority: int = QueuePriority.NORMAL.value,
        scheduled_at: Optional[float] = None,
        timeout: float = 30.0,
    ) -> str:
        """Add request to queue.
        
        Args:
            data: Request payload
            priority: Priority level (lower = higher priority)
            scheduled_at: Optional scheduled execution time
            timeout: Request timeout
        
        Returns:
            Request ID
        """
        request_id = str(uuid.uuid4())
        current_time = time.time()
        
        request = QueuedRequest(
            request_id=request_id,
            priority=priority,
            data=data,
            created_at=current_time,
            scheduled_at=scheduled_at or current_time,
            timeout=timeout,
        )
        
        with self._lock:
            if len(self._queue) >= self.config.max_size:
                raise RuntimeError("Queue is full")
            
            heappush(self._queue, request)
            self._stats = QueueStats(
                size=len(self._queue),
                processed=self._stats.processed,
                failed=self._stats.failed,
                avg_wait_time=self._stats.avg_wait_time,
                avg_process_time=self._stats.avg_process_time,
            )
        
        return request_id
    
    async def dequeue(self) -> Optional[QueuedRequest]:
        """Remove and return highest priority request.
        
        Returns:
            QueuedRequest if available
        """
        with self._lock:
            while self._queue:
                request = heappop(self._queue)
                
                if request.request_id in self._processed:
                    continue
                
                if request.scheduled_at > time.time():
                    heappush(self._queue, request)
                    return None
                
                return request
            
            return None
    
    async def peek(self) -> Optional[QueuedRequest]:
        """View highest priority request without removing.
        
        Returns:
            QueuedRequest if available
        """
        with self._lock:
            current_time = time.time()
            
            for request in self._queue:
                if request.request_id not in self._processed:
                    if request.scheduled_at <= current_time:
                        return request
            
            return None
    
    async def remove(self, request_id: str) -> bool:
        """Remove specific request from queue.
        
        Args:
            request_id: Request identifier
        
        Returns:
            True if removed
        """
        with self._lock:
            original_len = len(self._queue)
            
            self._queue = [
                r for r in self._queue
                if r.request_id != request_id
            ]
            
            return len(self._queue) < original_len
    
    async def mark_processed(self, request_id: str) -> None:
        """Mark request as processed.
        
        Args:
            request_id: Request identifier
        """
        with self._lock:
            self._processed[request_id] = True
            
            if len(self._processed) > 10000:
                oldest_keys = list(self._processed.keys())[:5000]
                for key in oldest_keys:
                    del self._processed[key]
    
    async def retry_request(self, request: QueuedRequest) -> None:
        """Re-queue failed request for retry.
        
        Args:
            request: Request to retry
        """
        if request.retries < self.config.max_retries:
            request.retries += 1
            request.scheduled_at = time.time() + (
                self.config.retry_delay * request.retries
            )
            
            with self._lock:
                heappush(self._queue, request)
        else:
            with self._lock:
                self._stats = QueueStats(
                    size=self._stats.size,
                    processed=self._stats.processed,
                    failed=self._stats.failed + 1,
                    avg_wait_time=self._stats.avg_wait_time,
                    avg_process_time=self._stats.avg_process_time,
                )
    
    async def get_stats(self) -> QueueStats:
        """Get queue statistics.
        
        Returns:
            QueueStats
        """
        with self._lock:
            return QueueStats(
                size=len(self._queue),
                processed=self._stats.processed,
                failed=self._stats.failed,
                avg_wait_time=self._stats.avg_wait_time,
                avg_process_time=self._stats.avg_process_time,
            )
    
    async def clear(self) -> int:
        """Clear all queued requests.
        
        Returns:
            Number of cleared requests
        """
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            self._processed.clear()
            
            self._stats = QueueStats(
                size=0,
                processed=self._stats.processed,
                failed=self._stats.failed,
                avg_wait_time=self._stats.avg_wait_time,
                avg_process_time=self._stats.avg_process_time,
            )
            
            return count
