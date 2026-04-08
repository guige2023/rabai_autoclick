"""
Task Router Action Module.

Routes tasks to appropriate handlers based on content classification,
load balancing, priority, affinity, and dynamic routing rules.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class LoadBalancingStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_CONNECTIONS = "least_connections"
    WEIGHTED = "weighted"
    HASH = "hash"
    RANDOM = "random"
    PRIORITY = "priority"


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    ROUTED = "routed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Task:
    """A routable task."""
    id: str
    type: str
    payload: Any
    priority: int = 3
    affinity_tags: Set[str] = field(default_factory=set)
    routing_rules: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    deadline: Optional[datetime] = None
    max_retries: int = 3
    timeout_seconds: Optional[float] = None

    @classmethod
    def create(
        cls,
        task_type: str,
        payload: Any,
        priority: int = 3,
        **kwargs,
    ) -> "Task":
        return cls(
            id=str(uuid.uuid4()),
            type=task_type,
            payload=payload,
            priority=priority,
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "payload": self.payload,
            "priority": self.priority,
            "affinity_tags": list(self.affinity_tags),
            "routing_rules": self.routing_rules,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass
class Worker:
    """A task handler/worker."""
    id: str
    name: str
    capabilities: Set[str] = field(default_factory=set)
    affinity_tags: Set[str] = field(default_factory=set)
    active_tasks: int = 0
    max_concurrent: int = 10
    weight: float = 1.0
    status: str = "active"
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_available(self) -> bool:
        return self.status == "active" and self.active_tasks < self.max_concurrent

    @property
    def load(self) -> float:
        return self.active_tasks / self.max_concurrent if self.max_concurrent > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "capabilities": list(self.capabilities),
            "affinity_tags": list(self.affinity_tags),
            "active_tasks": self.active_tasks,
            "max_concurrent": self.max_concurrent,
            "weight": self.weight,
            "status": self.status,
            "is_available": self.is_available,
            "load": self.load,
        }


@dataclass
class RouteResult:
    """Result of routing a task."""
    task_id: str
    worker_id: Optional[str]
    routing_reason: str
    strategy_used: LoadBalancingStrategy
    alternatives: List[str] = field(default_factory=list)


class RoutingRule:
    """Custom routing rule."""

    def __init__(self, name: str, condition_fn: Callable[[Task, Worker], bool]):
        self.name = name
        self.condition_fn = condition_fn

    def matches(self, task: Task, worker: Worker) -> bool:
        return self.condition_fn(task, worker)


class TaskRouter:
    """
    Intelligent task routing engine.

    Routes tasks to appropriate workers using configurable strategies
    including load balancing, affinity-based routing, and custom rules.

    Example:
        >>> router = TaskRouter(strategy=LoadBalancingStrategy.LEAST_CONNECTIONS)
        >>> router.add_worker(Worker(id="w1", name="Worker 1", capabilities={"compute"}))
        >>> router.add_rule(RoutingRule("priority", lambda t, w: t.priority < 3))
        >>> result = router.route(Task.create("compute", {"data": 42}))
    """

    def __init__(
        self,
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.ROUND_ROBIN,
    ):
        self.strategy = strategy
        self._workers: Dict[str, Worker] = {}
        self._task_routes: Dict[str, str] = {}
        self._worker_task_counts: Dict[str, int] = defaultdict(int)
        self._round_robin_index: Dict[str, int] = defaultdict(int)
        self._custom_rules: List[RoutingRule] = []
        self._type_workers: Dict[str, List[str]] = defaultdict(list)
        self._stats = {
            "total_routed": 0,
            "total_completed": 0,
            "total_failed": 0,
            "routing_errors": 0,
        }

    def add_worker(
        self,
        worker_id: str,
        name: str,
        capabilities: Optional[Set[str]] = None,
        affinity_tags: Optional[Set[str]] = None,
        max_concurrent: int = 10,
        weight: float = 1.0,
    ) -> None:
        """Register a worker."""
        worker = Worker(
            id=worker_id,
            name=name,
            capabilities=capabilities or set(),
            affinity_tags=affinity_tags or set(),
            max_concurrent=max_concurrent,
            weight=weight,
        )
        self._workers[worker_id] = worker

    def remove_worker(self, worker_id: str) -> bool:
        """Remove a worker."""
        if worker_id in self._workers:
            del self._workers[worker_id]
            return True
        return False

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        """Get worker by ID."""
        return self._workers.get(worker_id)

    def add_rule(self, rule: RoutingRule) -> None:
        """Add a custom routing rule."""
        self._custom_rules.append(rule)

    def add_task_type_mapping(self, task_type: str, worker_ids: List[str]) -> None:
        """Map task types to specific workers."""
        self._type_workers[task_type] = worker_ids

    def route(self, task: Task) -> RouteResult:
        """
        Route a task to an appropriate worker.

        Returns:
            RouteResult with worker_id and routing information
        """
        candidates = self._get_candidates(task)

        if not candidates:
            self._stats["routing_errors"] += 1
            return RouteResult(
                task_id=task.id,
                worker_id=None,
                routing_reason="No available workers",
                strategy_used=self.strategy,
            )

        selected = self._select_worker(task, candidates)
        if selected:
            self._task_routes[task.id] = selected.id
            self._stats["total_routed"] += 1
            return RouteResult(
                task_id=task.id,
                worker_id=selected.id,
                routing_reason=f"Selected via {self.strategy.value}",
                strategy_used=self.strategy,
                alternatives=[w.id for w in candidates if w.id != selected.id],
            )

        self._stats["routing_errors"] += 1
        return RouteResult(
            task_id=task.id,
            worker_id=None,
            routing_reason="Routing failed",
            strategy_used=self.strategy,
        )

    def record_completion(self, task_id: str, worker_id: str, success: bool) -> None:
        """Record task completion for a worker."""
        if worker_id in self._workers:
            worker = self._workers[worker_id]
            worker.active_tasks = max(0, worker.active_tasks - 1)

        if success:
            self._stats["total_completed"] += 1
        else:
            self._stats["total_failed"] += 1

        if task_id in self._task_routes:
            del self._task_routes[task_id]

    def get_worker_load(self) -> Dict[str, Dict[str, Any]]:
        """Get current load information for all workers."""
        return {
            wid: {
                "active_tasks": w.active_tasks,
                "max_concurrent": w.max_concurrent,
                "load_percent": w.load * 100,
                "is_available": w.is_available,
            }
            for wid, w in self._workers.items()
        }

    def get_routing_stats(self) -> Dict[str, int]:
        """Get routing statistics."""
        return {
            **self._stats,
            "total_workers": len(self._workers),
            "active_workers": sum(1 for w in self._workers.values() if w.is_available),
        }

    def _get_candidates(self, task: Task) -> List[Worker]:
        """Get workers that can handle this task."""
        candidates = []

        # Type-based routing
        if task.type in self._type_workers:
            for wid in self._type_workers[task.type]:
                worker = self._workers.get(wid)
                if worker and worker.is_available:
                    candidates.append(worker)
            return candidates

        # Capability matching
        for worker in self._workers.values():
            if not worker.is_available:
                continue
            if task.affinity_tags:
                if not task.affinity_tags.intersection(worker.affinity_tags):
                    continue
            candidates.append(worker)

        return candidates

    def _select_worker(self, task: Task, candidates: List[Worker]) -> Optional[Worker]:
        """Select best worker based on strategy."""
        if not candidates:
            return None

        # Custom rules first
        for rule in self._custom_rules:
            matching = [w for w in candidates if rule.matches(task, w)]
            if matching:
                selected = matching[0]
                selected.active_tasks += 1
                return selected

        if self.strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_select(task, candidates)
        elif self.strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return min(candidates, key=lambda w: w.active_tasks)
        elif self.strategy == LoadBalancingStrategy.WEIGHTED:
            return self._weighted_select(candidates)
        elif self.strategy == LoadBalancingStrategy.HASH:
            return self._hash_select(task, candidates)
        elif self.strategy == LoadBalancingStrategy.RANDOM:
            import random
            selected = random.choice(candidates)
            selected.active_tasks += 1
            return selected
        elif self.strategy == LoadBalancingStrategy.PRIORITY:
            return min(candidates, key=lambda w: (w.load, -w.weight))

        return candidates[0]

    def _round_robin_select(self, task: Task, candidates: List[Worker]) -> Optional[Worker]:
        """Round-robin selection."""
        if not candidates:
            return None
        key = task.type or "default"
        idx = self._round_robin_index[key] % len(candidates)
        self._round_robin_index[key] += 1
        selected = candidates[idx]
        selected.active_tasks += 1
        return selected

    def _weighted_select(self, candidates: List[Worker]) -> Optional[Worker]:
        """Weighted random selection."""
        import random
        weights = [w.weight for w in candidates]
        total = sum(weights)
        if total == 0:
            return candidates[0]
        r = random.uniform(0, total)
        cumulative = 0
        for worker in candidates:
            cumulative += worker.weight
            if cumulative >= r:
                worker.active_tasks += 1
                return worker
        selected = candidates[-1]
        selected.active_tasks += 1
        return selected

    def _hash_select(self, task: Task, candidates: List[Worker]) -> Optional[Worker]:
        """Hash-based consistent selection."""
        if not candidates:
            return None
        hash_input = f"{task.id}:{task.type}"
        hash_val = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        idx = hash_val % len(candidates)
        selected = candidates[idx]
        selected.active_tasks += 1
        return selected


def create_task_router(
    strategy: str = "round_robin",
) -> TaskRouter:
    """Factory to create a configured task router."""
    return TaskRouter(strategy=LoadBalancingStrategy(strategy))
