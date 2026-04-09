"""API Concurrency Action Module.

Provides concurrency control for API operations including
semaphore-based limiting, parallel execution, and
connection pool management.

Example:
    >>> from actions.api.api_concurrency_action import APIConcurrencyAction
    >>> action = APIConcurrencyAction(max_concurrent=10)
    >>> results = await action.run_parallel(tasks)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set
import threading
import time
import uuid


class ConcurrencyStrategy(Enum):
    """Concurrency execution strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    BATCHED = "batched"
    THROTTLED = "throttled"


@dataclass
class ConcurrencyConfig:
    """Configuration for concurrency control.
    
    Attributes:
        max_concurrent: Maximum concurrent operations
        max_per_host: Maximum concurrent per host
        acquire_timeout: Timeout for acquiring slot
        release_timeout: Timeout for releasing slot
    """
    max_concurrent: int = 10
    max_per_host: int = 5
    acquire_timeout: float = 30.0
    release_timeout: float = 5.0


@dataclass
class ConcurrencySlot:
    """Slot for concurrent operation.
    
    Attributes:
        slot_id: Unique slot identifier
        host: Target host
        acquired_at: Acquisition time
        task_id: Associated task ID
    """
    slot_id: str
    host: Optional[str]
    acquired_at: float
    task_id: Optional[str] = None


@dataclass
class ConcurrencyStats:
    """Concurrency statistics.
    
    Attributes:
        active_count: Currently active operations
        queued_count: Queued operations
        total_executed: Total executed operations
        total_failed: Total failed operations
    """
    active_count: int
    queued_count: int
    total_executed: int
    total_failed: int
    avg_duration: float


class APIConcurrencyAction:
    """Concurrency controller for API operations.
    
    Manages parallel execution with configurable limits
    and per-host concurrency controls.
    
    Attributes:
        config: Concurrency configuration
        _semaphore: asyncio semaphore
        _active_slots: Active operation slots
        _stats: Operation statistics
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[ConcurrencyConfig] = None,
    ) -> None:
        """Initialize concurrency action.
        
        Args:
            config: Concurrency configuration
        """
        self.config = config or ConcurrencyConfig()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        self._active_slots: Dict[str, ConcurrencySlot] = {}
        self._host_slots: Dict[str, int] = {}
        self._stats = ConcurrencyStats(0, 0, 0, 0, 0.0)
        self._lock = threading.RLock()
        self._total_executed = 0
        self._total_failed = 0
        self._durations: List[float] = []
    
    async def run_parallel(
        self,
        tasks: List[Awaitable[Any]],
        max_concurrent: Optional[int] = None,
    ) -> List[Any]:
        """Run tasks in parallel with concurrency limit.
        
        Args:
            tasks: List of awaitable tasks
            max_concurrent: Override max concurrent
        
        Returns:
            List of results
        """
        semaphore = asyncio.Semaphore(
            max_concurrent or self.config.max_concurrent
        )
        
        async def bounded_task(task: Awaitable[Any]) -> Any:
            async with semaphore:
                start = time.time()
                try:
                    result = await task
                    self._total_executed += 1
                    self._durations.append(time.time() - start)
                    return result
                except Exception as e:
                    self._total_failed += 1
                    self._durations.append(time.time() - start)
                    raise
        
        return await asyncio.gather(
            *[bounded_task(t) for t in tasks],
            return_exceptions=True,
        )
    
    async def run_batched(
        self,
        items: List[Any],
        processor: Callable[[Any], Awaitable[Any]],
        batch_size: int = 10,
    ) -> List[Any]:
        """Run items in batches.
        
        Args:
            items: Items to process
            processor: Async processor function
            batch_size: Size of each batch
        
        Returns:
            List of results
        """
        results = []
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_results = await self.run_parallel(
                [processor(item) for item in batch]
            )
            results.extend(batch_results)
        
        return results
    
    async def acquire_slot(
        self,
        host: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> str:
        """Acquire a concurrency slot.
        
        Args:
            host: Target host for rate limiting
            task_id: Associated task ID
        
        Returns:
            Slot ID
        """
        slot_id = str(uuid.uuid4())
        
        if host:
            async with asyncio.Lock():
                current = self._host_slots.get(host, 0)
                if current >= self.config.max_per_host:
                    raise RuntimeError(f"Host {host} at capacity")
                self._host_slots[host] = current + 1
        
        slot = ConcurrencySlot(
            slot_id=slot_id,
            host=host,
            acquired_at=time.time(),
            task_id=task_id,
        )
        
        with self._lock:
            self._active_slots[slot_id] = slot
        
        return slot_id
    
    async def release_slot(self, slot_id: str) -> None:
        """Release a concurrency slot.
        
        Args:
            slot_id: Slot identifier
        """
        with self._lock:
            if slot_id in self._active_slots:
                slot = self._active_slots[slot_id]
                
                if slot.host:
                    self._host_slots[slot.host] = max(
                        0,
                        self._host_slots.get(slot.host, 1) - 1
                    )
                
                del self._active_slots[slot_id]
    
    async def get_active_count(self) -> int:
        """Get number of active operations.
        
        Returns:
            Active operation count
        """
        with self._lock:
            return len(self._active_slots)
    
    async def get_stats(self) -> ConcurrencyStats:
        """Get concurrency statistics.
        
        Returns:
            ConcurrencyStats
        """
        avg_duration = (
            sum(self._durations) / len(self._durations)
            if self._durations else 0.0
        )
        
        return ConcurrencyStats(
            active_count=len(self._active_slots),
            queued_count=0,
            total_executed=self._total_executed,
            total_failed=self._total_failed,
            avg_duration=avg_duration,
        )
    
    async def wait_for_slot(
        self,
        host: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> str:
        """Wait for available slot.
        
        Args:
            host: Target host for rate limiting
            timeout: Maximum wait time
        
        Returns:
            Slot ID
        """
        timeout = timeout or self.config.acquire_timeout
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                slot_id = await self.acquire_slot(host)
                return slot_id
            except RuntimeError:
                await asyncio.sleep(0.1)
        
        raise TimeoutError("Failed to acquire slot within timeout")
