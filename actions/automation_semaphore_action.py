"""
Automation Semaphore Action Module.

Provides semaphore-based concurrency control for automation workflows,
limiting parallel execution and managing resource usage.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import asyncio
import threading
import time
import uuid
import logging

logger = logging.getLogger(__name__)


class SemaphoreType(Enum):
    """Types of semaphores."""
    BINARY = auto()
    COUNTING = auto()
    WEIGHTED = auto()


@dataclass
class SemaphorePermit:
    """Represents a permit acquired from the semaphore."""
    permit_id: str
    resource: str
    acquired_at: datetime
    weight: int = 1
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if permit has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at

    def ttl_seconds(self) -> Optional[float]:
        """Get remaining TTL in seconds."""
        if self.expires_at is None:
            return None
        remaining = (self.expires_at - datetime.now(timezone.utc)).total_seconds()
        return max(0, remaining)


@dataclass
class SemaphoreStats:
    """Statistics for a semaphore."""
    total_permits: int
    available_permits: int
    acquired_permits: int
    total_acquisitions: int
    failed_acquisitions: int
    active_permits: List[str]

    def utilization(self) -> float:
        """Calculate semaphore utilization percentage."""
        if self.total_permits == 0:
            return 0.0
        return (self.acquired_permits / self.total_permits) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_permits": self.total_permits,
            "available_permits": self.available_permits,
            "acquired_permits": self.acquired_permits,
            "total_acquisitions": self.total_acquisitions,
            "failed_acquisitions": self.failed_acquisitions,
            "utilization_percent": self.utilization(),
            "active_permit_count": len(self.active_permits),
        }


class AutomationSemaphoreAction:
    """
    Provides semaphore-based concurrency control for automation.

    This action implements various semaphore types to control parallel
    execution in automation workflows, limiting resource usage and
    preventing overload.

    Example:
        >>> semaphore = AutomationSemaphoreAction(max_permits=5)
        >>> permit = await semaphore.acquire("task:123", timeout=10)
        >>> if permit:
        ...     try:
        ...         await run_task()
        ...     finally:
        ...         await semaphore.release("task:123", permit)
    """

    def __init__(
        self,
        max_permits: int = 1,
        semaphore_type: SemaphoreType = SemaphoreType.COUNTING,
        default_ttl: Optional[float] = None,
        fair: bool = True,
    ):
        """
        Initialize the Automation Semaphore.

        Args:
            max_permits: Maximum number of permits.
            semaphore_type: Type of semaphore.
            default_ttl: Default permit TTL in seconds.
            fair: Whether to use fair ordering.
        """
        self.max_permits = max_permits
        self.semaphore_type = semaphore_type
        self.default_ttl = default_ttl
        self.fair = fair

        self._semaphore = threading.Semaphore(max_permits)
        self._permits: Dict[str, SemaphorePermit] = {}
        self._weights: Dict[str, int] = {}
        self._acquired_set: Set[str] = set()
        self._lock = threading.RLock()
        self._total_acquisitions = 0
        self._failed_acquisitions = 0
        self._wait_queue: List[tuple] = []

    async def acquire(
        self,
        resource: str,
        weight: int = 1,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[SemaphorePermit]:
        """
        Acquire a permit from the semaphore.

        Args:
            resource: Resource identifier requesting the permit.
            weight: Number of permits to acquire (for weighted semaphores).
            timeout: Wait timeout in seconds.
            ttl: Permit TTL in seconds.
            metadata: Optional metadata.

        Returns:
            SemaphorePermit if acquired, None otherwise.
        """
        if self.semaphore_type == SemaphoreType.BINARY:
            weight = 1

        ttl = ttl or self.default_ttl

        deadline = time.time() + timeout if timeout else None

        while True:
            with self._lock:
                available = self._get_available_permits()

                if available >= weight:
                    permit_id = str(uuid.uuid4())
                    permit = SemaphorePermit(
                        permit_id=permit_id,
                        resource=resource,
                        acquired_at=datetime.now(timezone.utc),
                        weight=weight,
                        expires_at=(
                            datetime.now(timezone.utc) + timedelta(seconds=ttl)
                            if ttl else None
                        ),
                        metadata=metadata or {},
                    )

                    self._permits[permit_id] = permit
                    self._weights[permit_id] = weight
                    self._acquired_set.add(resource)
                    self._total_acquisitions += 1

                    return permit

                if deadline and time.time() >= deadline:
                    self._failed_acquisitions += 1
                    return None

            await self._sleep(0.01)

    async def try_acquire(
        self,
        resource: str,
        weight: int = 1,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[SemaphorePermit]:
        """
        Try to acquire a permit without waiting.

        Args:
            resource: Resource identifier.
            weight: Number of permits.
            ttl: Permit TTL.
            metadata: Optional metadata.

        Returns:
            Permit if acquired, None otherwise.
        """
        return await self.acquire(resource, weight, timeout=0, ttl=ttl, metadata=metadata)

    async def release(
        self,
        resource: str,
        permit: Optional[SemaphorePermit] = None,
    ) -> bool:
        """
        Release a permit back to the semaphore.

        Args:
            resource: Resource identifier.
            permit: Optional specific permit to release.

        Returns:
            True if released successfully.
        """
        with self._lock:
            if permit:
                if permit.permit_id not in self._permits:
                    return False

                del self._permits[permit.permit_id]
                del self._weights[permit.permit_id]
                self._acquired_set.discard(resource)
                return True

            for permit_id, p in list(self._permits.items()):
                if p.resource == resource:
                    del self._permits[permit_id]
                    del self._weights[permit_id]
                    self._acquired_set.discard(resource)
                    return True

            return False

    async def release_all(self, resource: str) -> int:
        """
        Release all permits held by a resource.

        Args:
            resource: Resource identifier.

        Returns:
            Number of permits released.
        """
        count = 0
        with self._lock:
            for permit_id, p in list(self._permits.items()):
                if p.resource == resource:
                    del self._permits[permit_id]
                    del self._weights[permit_id]
                    self._acquired_set.discard(resource)
                    count += 1
        return count

    def _get_available_permits(self) -> int:
        """Calculate available permits."""
        acquired = sum(self._weights.values())
        return self.max_permits - acquired

    def get_permit(self, permit_id: str) -> Optional[SemaphorePermit]:
        """Get a permit by ID."""
        return self._permits.get(permit_id)

    def get_permits_for_resource(self, resource: str) -> List[SemaphorePermit]:
        """Get all permits held by a resource."""
        return [p for p in self._permits.values() if p.resource == resource]

    def get_stats(self) -> SemaphoreStats:
        """Get semaphore statistics."""
        with self._lock:
            acquired = sum(self._weights.values())

            return SemaphoreStats(
                total_permits=self.max_permits,
                available_permits=self.max_permits - acquired,
                acquired_permits=acquired,
                total_acquisitions=self._total_acquisitions,
                failed_acquisitions=self._failed_acquisitions,
                active_permits=[p.resource for p in self._permits.values()],
            )

    def cleanup_expired(self) -> int:
        """Remove expired permits."""
        count = 0
        with self._lock:
            expired = [
                permit_id
                for permit_id, p in self._permits.items()
                if p.is_expired()
            ]
            for permit_id in expired:
                del self._permits[permit_id]
                weight = self._weights.pop(permit_id, 1)
                self._acquired_set.discard(
                    next((p.resource for p in self._permits.values() if p.permit_id == permit_id), None)
                )
                count += 1
        return count

    async def with_permit(
        self,
        resource: str,
        callback: Callable,
        weight: int = 1,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Execute a callback with a permit.

        Args:
            resource: Resource identifier.
            callback: Async function to execute.
            weight: Number of permits.
            timeout: Acquire timeout.
            ttl: Permit TTL.
            metadata: Optional metadata.

        Returns:
            Callback result.

        Raises:
            TimeoutError: If permit cannot be acquired.
        """
        permit = await self.acquire(resource, weight, timeout, ttl, metadata)

        if permit is None:
            raise TimeoutError(f"Failed to acquire permit for {resource}")

        try:
            return await callback()
        finally:
            await self.release(resource, permit)

    def reset(self) -> int:
        """
        Reset the semaphore, releasing all permits.

        Returns:
            Number of permits released.
        """
        with self._lock:
            count = len(self._permits)
            self._permits.clear()
            self._weights.clear()
            self._acquired_set.clear()
            return count

    @staticmethod
    async def _sleep(seconds: float) -> None:
        """Async sleep helper."""
        await asyncio.sleep(seconds)


class WeightedSemaphore(AutomationSemaphoreAction):
    """Weighted semaphore implementation."""

    def __init__(self, max_weight: int = 10, **kwargs):
        """
        Initialize weighted semaphore.

        Args:
            max_weight: Maximum total weight.
        """
        super().__init__(
            max_permits=max_weight,
            semaphore_type=SemaphoreType.WEIGHTED,
            **kwargs,
        )


def create_semaphore_action(
    max_permits: int = 1,
    semaphore_type: SemaphoreType = SemaphoreType.COUNTING,
    **kwargs,
) -> AutomationSemaphoreAction:
    """Factory function to create an AutomationSemaphoreAction."""
    return AutomationSemaphoreAction(
        max_permits=max_permits,
        semaphore_type=semaphore_type,
        **kwargs,
    )
