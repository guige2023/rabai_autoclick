"""Workflow Routing Action Module.

Route workflow execution based on conditions and load balancing.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .automation_executor_action import StepStatus


class RoutingStrategy(Enum):
    """Routing strategies."""
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    WEIGHTED = "weighted"
    HASH = "hash"
    LEAST_LOADED = "least_loaded"


@dataclass
class RouteTarget:
    """Routing target endpoint."""
    id: str
    url: str
    weight: float = 1.0
    max_concurrent: int = 10
    current_load: int = 0
    enabled: bool = True


@dataclass
class RouteResult:
    """Result of routing decision."""
    target_id: str
    target: RouteTarget
    timestamp: float


class WorkflowRouter:
    """Route workflow tasks to appropriate targets."""

    def __init__(self, strategy: RoutingStrategy = RoutingStrategy.ROUND_ROBIN) -> None:
        self.strategy = strategy
        self._targets: dict[str, RouteTarget] = {}
        self._round_robin_index = 0
        self._lock = asyncio.Lock()
        self._request_counts: dict[str, int] = {}

    def add_target(self, target: RouteTarget) -> None:
        """Add a routing target."""
        self._targets[target.id] = target

    def remove_target(self, target_id: str) -> bool:
        """Remove a routing target."""
        if target_id in self._targets:
            del self._targets[target_id]
            return True
        return False

    async def route(self, context: dict | None = None) -> RouteResult | None:
        """Route to a target based on strategy."""
        async with self._lock:
            enabled_targets = [t for t in self._targets.values() if t.enabled]
            if not enabled_targets:
                return None
            if self.strategy == RoutingStrategy.ROUND_ROBIN:
                target = self._round_robin(enabled_targets)
            elif self.strategy == RoutingStrategy.RANDOM:
                target = self._random(enabled_targets)
            elif self.strategy == RoutingStrategy.WEIGHTED:
                target = self._weighted(enabled_targets)
            elif self.strategy == RoutingStrategy.HASH:
                target = await self._hash_route(enabled_targets, context)
            elif self.strategy == RoutingStrategy.LEAST_LOADED:
                target = self._least_loaded(enabled_targets)
            else:
                target = enabled_targets[0]
            target.current_load += 1
            self._request_counts[target.id] = self._request_counts.get(target.id, 0) + 1
            return RouteResult(
                target_id=target.id,
                target=target,
                timestamp=time.time()
            )

    def _round_robin(self, targets: list[RouteTarget]) -> RouteTarget:
        """Round-robin routing."""
        target = targets[self._round_robin_index % len(targets)]
        self._round_robin_index += 1
        return target

    def _random(self, targets: list[RouteTarget]) -> RouteTarget:
        """Random routing."""
        import random
        return random.choice(targets)

    def _weighted(self, targets: list[RouteTarget]) -> RouteTarget:
        """Weighted random routing."""
        import random
        total_weight = sum(t.weight for t in targets)
        r = random.uniform(0, total_weight)
        cumulative = 0
        for target in targets:
            cumulative += target.weight
            if r <= cumulative:
                return target
        return targets[-1]

    async def _hash_route(self, targets: list[RouteTarget], context: dict | None) -> RouteTarget:
        """Consistent hashing routing."""
        if not context:
            return targets[0]
        key = str(sorted(context.items()))
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        index = hash_value % len(targets)
        return targets[index]

    def _least_loaded(self, targets: list[RouteTarget]) -> RouteTarget:
        """Route to least loaded target."""
        return min(targets, key=lambda t: t.current_load / max(t.max_concurrent, 1))

    async def release(self, target_id: str) -> None:
        """Release a target (decrement load)."""
        async with self._lock:
            target = self._targets.get(target_id)
            if target and target.current_load > 0:
                target.current_load -= 1

    def get_stats(self) -> dict[str, Any]:
        """Get routing statistics."""
        return {
            "strategy": self.strategy.value,
            "total_targets": len(self._targets),
            "enabled_targets": sum(1 for t in self._targets.values() if t.enabled),
            "request_counts": dict(self._request_counts),
        }


class ConditionalRouter:
    """Router with conditional routing rules."""

    def __init__(self) -> None:
        self._rules: list[tuple[Callable[[dict], bool], str]] = []
        self._default_target_id: str | None = None

    def add_rule(self, condition: Callable[[dict], bool], target_id: str) -> None:
        """Add a routing rule."""
        self._rules.append((condition, target_id))

    def set_default(self, target_id: str) -> None:
        """Set default routing target."""
        self._default_target_id = target_id

    async def route(self, context: dict) -> str | None:
        """Route based on conditions."""
        for condition, target_id in self._rules:
            try:
                if condition(context):
                    return target_id
            except Exception:
                continue
        return self._default_target_id
