"""Automation Semaphore Action Module.

Provides semaphore-based concurrency control for automation tasks
including weighted semaphores and priority-aware acquisition.

Example:
    >>> from actions.automation.automation_semaphore_action import AutomationSemaphoreAction
    >>> action = AutomationSemaphoreAction(permits=5)
    >>> async with action.acquire():
    ...     await process()
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import threading
import time
import uuid


class SemaphoreStrategy(Enum):
    """Semaphore acquisition strategies."""
    FIFO = "fifo"
    PRIORITY = "priority"
    FAIR = "fair"


@dataclass
class SemaphorePermit:
    """Semaphore permit record.
    
    Attributes:
        permit_id: Unique permit identifier
        weight: Weight of this permit
        acquired_at: Acquisition time
        task_id: Associated task ID
        released: Whether permit has been released
    """
    permit_id: str
    weight: int
    acquired_at: float
    task_id: Optional[str] = None
    released: bool = False


@dataclass
class SemaphoreConfig:
    """Configuration for semaphore.
    
    Attributes:
        permits: Number of available permits
        weight: Default weight per acquisition
        max_weight: Maximum weight per acquisition
        acquire_timeout: Timeout for acquiring
        strategy: Acquisition strategy
    """
    permits: int = 10
    weight: int = 1
    max_weight: int = 5
    acquire_timeout: float = 30.0
    strategy: SemaphoreStrategy = SemaphoreStrategy.FIFO


@dataclass
class SemaphoreStats:
    """Semaphore statistics.
    
    Attributes:
        total_permits: Total available permits
        used_permits: Currently used permits
        total_acquisitions: Total successful acquisitions
        total_rejections: Total rejected acquisitions
    """
    total_permits: int
    used_permits: int
    total_acquisitions: int
    total_rejections: int
    wait_queue_size: int


class AutomationSemaphoreAction:
    """Semaphore controller for automation concurrency.
    
    Manages concurrent task execution with weighted permits
    and configurable acquisition strategies.
    
    Attributes:
        config: Semaphore configuration
        _permits: Available permits counter
        _acquired: Active permits
        _wait_queue: Waiting tasks
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[SemaphoreConfig] = None,
    ) -> None:
        """Initialize semaphore action.
        
        Args:
            config: Semaphore configuration
        """
        self.config = config or SemaphoreConfig()
        self._permits = self.config.permits
        self._acquired: Dict[str, SemaphorePermit] = {}
        self._wait_queue: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._total_acquisitions = 0
        self._total_rejections = 0
    
    async def acquire(
        self,
        weight: int = 1,
        task_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[str]:
        """Acquire permits from semaphore.
        
        Args:
            weight: Number of permits to acquire
            task_id: Associated task ID
            timeout: Acquisition timeout
        
        Returns:
            Permit ID if acquired, None otherwise
        """
        if weight > self.config.max_weight:
            weight = self.config.max_weight
        
        timeout = timeout or self.config.acquire_timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            async with asyncio.Lock():
                if self._can_acquire(weight):
                    permit_id = self._do_acquire(weight, task_id)
                    self._total_acquisitions += 1
                    return permit_id
            
            await asyncio.sleep(0.01)
        
        self._total_rejections += 1
        return None
    
    def _can_acquire(self, weight: int) -> bool:
        """Check if can acquire permits.
        
        Args:
            weight: Number of permits
        
        Returns:
            True if can acquire
        """
        active_weight = sum(
            p.weight for p in self._acquired.values()
            if not p.released
        )
        return (active_weight + weight) <= self._permits
    
    def _do_acquire(self, weight: int, task_id: Optional[str]) -> str:
        """Perform permit acquisition.
        
        Args:
            weight: Number of permits
            task_id: Task ID
        
        Returns:
            Permit ID
        """
        permit_id = str(uuid.uuid4())
        
        permit = SemaphorePermit(
            permit_id=permit_id,
            weight=weight,
            acquired_at=time.time(),
            task_id=task_id,
        )
        
        self._acquired[permit_id] = permit
        
        return permit_id
    
    async def release(self, permit_id: str) -> bool:
        """Release permits back to semaphore.
        
        Args:
            permit_id: Permit identifier
        
        Returns:
            True if released successfully
        """
        with self._lock:
            if permit_id not in self._acquired:
                return False
            
            permit = self._acquired[permit_id]
            
            if permit.released:
                return False
            
            permit.released = True
            del self._acquired[permit_id]
            
            return True
    
    async def release_all(self, task_id: str) -> int:
        """Release all permits for task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            Number of permits released
        """
        with self._lock:
            to_release = [
                pid for pid, p in self._acquired.items()
                if p.task_id == task_id and not p.released
            ]
            
            for pid in to_release:
                self._acquired[pid].released = True
                del self._acquired[pid]
            
            return len(to_release)
    
    def get_available(self) -> int:
        """Get number of available permits.
        
        Returns:
            Available permits count
        """
        with self._lock:
            active_weight = sum(
                p.weight for p in self._acquired.values()
                if not p.released
            )
            return self._permits - active_weight
    
    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics.
        
        Returns:
            SemaphoreStats
        """
        with self._lock:
            active_weight = sum(
                p.weight for p in self._acquired.values()
                if not p.released
            )
            
            return SemaphoreStats(
                total_permits=self._permits,
                used_permits=active_weight,
                total_acquisitions=self._total_acquisitions,
                total_rejections=self._total_rejections,
                wait_queue_size=len(self._wait_queue),
            )
    
    async def wait_for_permits(
        self,
        permits: int,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait until specified permits are available.
        
        Args:
            permits: Number of permits to wait for
            timeout: Maximum wait time
        
        Returns:
            True when permits available
        """
        timeout = timeout or self.config.acquire_timeout
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if self.get_available() >= permits:
                return True
            await asyncio.sleep(0.05)
        
        return False
    
    def set_permits(self, permits: int) -> None:
        """Update total permits count.
        
        Args:
            permits: New permits count
        """
        with self._lock:
            active_weight = sum(
                p.weight for p in self._acquired.values()
                if not p.released
            )
            
            if permits >= active_weight:
                self._permits = permits
    
    class Acquisition:
        """Context manager for semaphore acquisition."""
        
        def __init__(
            self,
            action: "AutomationSemaphoreAction",
            weight: int,
            task_id: Optional[str] = None,
            timeout: Optional[float] = None,
        ) -> None:
            self._action = action
            self._weight = weight
            self._task_id = task_id
            self._timeout = timeout
            self._permit_id: Optional[str] = None
        
        async def __aenter__(self) -> str:
            self._permit_id = await self._action.acquire(
                self._weight,
                self._task_id,
                self._timeout,
            )
            if not self._permit_id:
                raise RuntimeError("Failed to acquire semaphore")
            return self._permit_id
        
        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            if self._permit_id:
                await self._action.release(self._permit_id)
