"""Automation Distributor Action Module.

Provides work distribution strategies for automation workflows
including round-robin, weighted allocation, and load balancing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class DistributionStrategy(Enum):
    """Work distribution strategies."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED = "weighted"
    LEAST_LOADED = "least_loaded"
    RANDOM = "random"
    HASH = "hash"
    PRIORITY = "priority"


@dataclass
class Worker:
    """Represents a worker in the pool."""
    worker_id: str
    name: str
    weight: float = 1.0
    current_load: int = 0
    max_load: int = 100
    available: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        """Check if worker is healthy and can accept work."""
        return self.available and self.current_load < self.max_load


@dataclass
class WorkItem:
    """Represents a unit of work."""
    item_id: str
    data: Any
    priority: int = 0
    key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributionResult:
    """Result of a distribution operation."""
    worker_id: str
    item: WorkItem
    success: bool
    error: Optional[str] = None


class RoundRobinDistributor:
    """Distributes work using round-robin."""

    def __init__(self):
        self._index = 0
        self._lock = asyncio.Lock()

    async def distribute(
        self,
        item: WorkItem,
        workers: List[Worker]
    ) -> Optional[Worker]:
        """Distribute work to next worker in round-robin order."""
        healthy = [w for w in workers if w.is_healthy()]
        if not healthy:
            return None

        async with self._lock:
            worker = healthy[self._index % len(healthy)]
            self._index += 1
            return worker


class WeightedDistributor:
    """Distributes work based on worker weights."""

    def __init__(self):
        self._cumulative_weights: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def distribute(
        self,
        item: WorkItem,
        workers: List[Worker]
    ) -> Optional[Worker]:
        """Distribute work based on weights."""
        healthy = [w for w in workers if w.is_healthy()]
        if not healthy:
            return None

        async with self._lock:
            total_weight = sum(w.weight for w in healthy)
            if total_weight <= 0:
                return healthy[0]

            # Calculate cumulative weights
            self._cumulative_weights = {}
            cumulative = 0
            for w in healthy:
                cumulative += w.weight
                self._cumulative_weights[w.worker_id] = cumulative

            # Random selection based on weight
            import random
            r = random.uniform(0, cumulative)
            for worker in healthy:
                if r <= self._cumulative_weights[worker.worker_id]:
                    return worker

            return healthy[-1]


class LeastLoadedDistributor:
    """Distributes work to the least loaded worker."""

    def __init__(self):
        pass

    async def distribute(
        self,
        item: WorkItem,
        workers: List[Worker]
    ) -> Optional[Worker]:
        """Distribute work to worker with lowest load."""
        healthy = [w for w in workers if w.is_healthy()]
        if not healthy:
            return None

        # Sort by load, then by weight as tiebreaker
        healthy.sort(key=lambda w: (w.current_load, -w.weight))
        return healthy[0]


class HashDistributor:
    """Distributes work based on key hashing."""

    def __init__(self):
        self._worker_count = 0

    async def distribute(
        self,
        item: WorkItem,
        workers: List[Worker]
    ) -> Optional[Worker]:
        """Distribute work based on hash of item key."""
        healthy = [w for w in workers if w.is_healthy()]
        if not healthy:
            return None

        self._worker_count = len(healthy)

        if item.key:
            import hashlib
            h = int(hashlib.md5(item.key.encode()).hexdigest(), 16)
            idx = h % len(healthy)
            return healthy[idx]

        # Fallback to round-robin if no key
        import random
        return healthy[random.randint(0, len(healthy) - 1)]


class PriorityDistributor:
    """Distributes work based on priority."""

    def __init__(self):
        self._queues: Dict[int, List[Tuple[WorkItem, Worker]]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def distribute(
        self,
        item: WorkItem,
        workers: List[Worker]
    ) -> Optional[Worker]:
        """Distribute high priority work first."""
        healthy = [w for w in workers if w.is_healthy()]
        if not healthy:
            return None

        async with self._lock:
            # Pick least loaded for this priority
            healthy.sort(key=lambda w: (w.current_load, -w.weight))
            return healthy[0]


class WorkDistributor:
    """Manages work distribution across workers."""

    def __init__(self):
        self._workers: Dict[str, Worker] = {}
        self._distributors: Dict[DistributionStrategy, Any] = {
            DistributionStrategy.ROUND_ROBIN: RoundRobinDistributor(),
            DistributionStrategy.WEIGHTED: WeightedDistributor(),
            DistributionStrategy.LEAST_LOADED: LeastLoadedDistributor(),
            DistributionStrategy.HASH: HashDistributor(),
            DistributionStrategy.PRIORITY: PriorityDistributor(),
        }
        self._strategy = DistributionStrategy.ROUND_ROBIN
        self._lock = asyncio.Lock()

    def set_strategy(self, strategy: DistributionStrategy) -> None:
        """Set the distribution strategy."""
        self._strategy = strategy

    def add_worker(
        self,
        worker_id: str,
        name: str,
        weight: float = 1.0,
        max_load: int = 100
    ) -> None:
        """Add a worker to the pool."""
        self._workers[worker_id] = Worker(
            worker_id=worker_id,
            name=name,
            weight=weight,
            max_load=max_load
        )

    def remove_worker(self, worker_id: str) -> bool:
        """Remove a worker from the pool."""
        return self._workers.pop(worker_id, None) is not None

    def update_worker_load(self, worker_id: str, load: int) -> bool:
        """Update a worker's current load."""
        worker = self._workers.get(worker_id)
        if worker:
            worker.current_load = load
            return True
        return False

    async def distribute(
        self,
        item: WorkItem
    ) -> Optional[DistributionResult]:
        """Distribute a work item to a worker."""
        workers = list(self._workers.values())
        distributor = self._distributors.get(self._strategy)

        if not distributor:
            return DistributionResult(
                worker_id="",
                item=item,
                success=False,
                error=f"Unknown strategy: {self._strategy}"
            )

        worker = await distributor.distribute(item, workers)

        if worker:
            worker.current_load += 1
            return DistributionResult(
                worker_id=worker.worker_id,
                item=item,
                success=True
            )

        return DistributionResult(
            worker_id="",
            item=item,
            success=False,
            error="No available workers"
        )

    async def distribute_batch(
        self,
        items: List[WorkItem]
    ) -> List[DistributionResult]:
        """Distribute multiple work items."""
        results = []
        for item in items:
            result = await self.distribute(item)
            results.append(result)
        return results

    def get_worker_stats(self) -> Dict[str, Any]:
        """Get statistics for all workers."""
        return {
            "workers": {
                w.worker_id: {
                    "name": w.name,
                    "weight": w.weight,
                    "current_load": w.current_load,
                    "max_load": w.max_load,
                    "available": w.available,
                    "healthy": w.is_healthy()
                }
                for w in self._workers.values()
            },
            "total_load": sum(w.current_load for w in self._workers.values()),
            "available_workers": len([w for w in self._workers.values() if w.is_healthy()])
        }


class AutomationDistributorAction:
    """Main action class for work distribution."""

    def __init__(self):
        self._distributor = WorkDistributor()

    def set_strategy(self, strategy: str) -> None:
        """Set the distribution strategy."""
        try:
            self._distributor.set_strategy(DistributionStrategy(strategy))
        except ValueError:
            pass

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation distributor action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with distribution results.
        """
        operation = context.get("operation", "distribute")

        if operation == "set_strategy":
            self.set_strategy(context.get("strategy", "round_robin"))
            return {"success": True}

        elif operation == "add_worker":
            self._distributor.add_worker(
                worker_id=context.get("worker_id", ""),
                name=context.get("name", ""),
                weight=context.get("weight", 1.0),
                max_load=context.get("max_load", 100)
            )
            return {"success": True}

        elif operation == "remove_worker":
            success = self._distributor.remove_worker(context.get("worker_id", ""))
            return {"success": success}

        elif operation == "update_load":
            success = self._distributor.update_worker_load(
                context.get("worker_id", ""),
                context.get("load", 0)
            )
            return {"success": success}

        elif operation == "distribute":
            item = WorkItem(
                item_id=context.get("item_id", ""),
                data=context.get("data"),
                priority=context.get("priority", 0),
                key=context.get("key")
            )
            result = await self._distributor.distribute(item)
            if result:
                return {
                    "success": result.success,
                    "worker_id": result.worker_id,
                    "error": result.error
                }
            return {"success": False, "error": "Distribution failed"}

        elif operation == "distribute_batch":
            items = [
                WorkItem(
                    item_id=ctx.get("item_id", f"item_{i}"),
                    data=ctx.get("data"),
                    priority=ctx.get("priority", 0),
                    key=ctx.get("key")
                )
                for i, ctx in enumerate(context.get("items", []))
            ]
            results = await self._distributor.distribute_batch(items)
            return {
                "success": True,
                "results": [
                    {
                        "worker_id": r.worker_id,
                        "item_id": r.item.item_id,
                        "success": r.success
                    }
                    for r in results
                ]
            }

        elif operation == "stats":
            return {
                "success": True,
                "stats": self._distributor.get_worker_stats()
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
