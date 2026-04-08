"""
Load Balancer Utilities

Provides utilities for load balancing across
multiple targets in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum, auto


class BalanceStrategy(Enum):
    """Load balancing strategies."""
    ROUND_ROBIN = auto()
    RANDOM = auto()
    LEAST_LOADED = auto()
    WEIGHTED = auto()
    HASH = auto()


@dataclass
class Target:
    """Represents a load-balanced target."""
    id: str
    weight: int = 1
    current_load: int = 0
    metadata: dict[str, Any] | None = None


class LoadBalancer:
    """
    Load balances requests across multiple targets.
    
    Supports various balancing strategies and
    target health tracking.
    """

    def __init__(
        self,
        strategy: BalanceStrategy = BalanceStrategy.ROUND_ROBIN,
    ) -> None:
        self._strategy = strategy
        self._targets: dict[str, Target] = {}
        self._round_robin_index = 0
        import random
        self._random = random

    def add_target(self, target: Target) -> None:
        """Add a target to the balancer."""
        self._targets[target.id] = target

    def remove_target(self, target_id: str) -> bool:
        """Remove a target from the balancer."""
        return self._targets.pop(target_id, None) is not None

    def select_target(self, key: str | None = None) -> Target | None:
        """
        Select a target using the configured strategy.
        
        Args:
            key: Optional key for hash-based routing.
            
        Returns:
            Selected Target or None.
        """
        if not self._targets:
            return None

        targets_list = list(self._targets.values())

        if self._strategy == BalanceStrategy.ROUND_ROBIN:
            target = targets_list[self._round_robin_index]
            self._round_robin_index = (self._round_robin_index + 1) % len(targets_list)
            return target

        elif self._strategy == BalanceStrategy.RANDOM:
            return self._random.choice(targets_list)

        elif self._strategy == BalanceStrategy.LEAST_LOADED:
            return min(targets_list, key=lambda t: t.current_load)

        elif self._strategy == BalanceStrategy.WEIGHTED:
            total_weight = sum(t.weight for t in targets_list)
            r = self._random.randint(1, total_weight)
            cumulative = 0
            for target in targets_list:
                cumulative += target.weight
                if r <= cumulative:
                    return target
            return targets_list[-1]

        elif self._strategy == BalanceStrategy.HASH and key:
            h = hash(key) % len(targets_list)
            return targets_list[h]

        return targets_list[0]

    def update_load(self, target_id: str, delta: int) -> None:
        """Update load on a target."""
        if target_id in self._targets:
            self._targets[target_id].current_load += delta

    def get_targets(self) -> list[Target]:
        """Get all targets."""
        return list(self._targets.values())
