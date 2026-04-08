"""
Weighted random selection utilities.

Provides weighted choice, reservoir sampling,
and lottery selection algorithms.
"""

from __future__ import annotations

import random
from typing import Callable, Generic, TypeVar
from collections import deque


T = TypeVar("T")


def weighted_choice(
    items: list[T],
    weights: list[float],
    random_func: Callable[[], float] | None = None,
) -> T:
    """
    Select weighted random item.

    Args:
        items: List of items
        weights: Corresponding weights (must be non-negative)
        random_func: Random function (defaults to random.random)

    Returns:
        Selected item

    Raises:
        ValueError: If items and weights have different lengths
    """
    if len(items) != len(weights):
        raise ValueError("items and weights must have same length")
    if not items:
        raise ValueError("items list cannot be empty")

    rng = random_func or random.random
    total = sum(weights)
    if total <= 0:
        raise ValueError("total weight must be positive")

    cumulative = 0.0
    threshold = rng() * total
    for item, weight in zip(items, weights):
        cumulative += weight
        if cumulative >= threshold:
            return item
    return items[-1]


def weighted_choices(
    items: list[T],
    weights: list[float],
    k: int,
    random_func: Callable[[], float] | None = None,
) -> list[T]:
    """
    Select multiple weighted random items (with replacement).

    Args:
        items: List of items
        weights: Corresponding weights
        k: Number of selections
        random_func: Random function

    Returns:
        List of selected items
    """
    return [weighted_choice(items, weights, random_func) for _ in range(k)]


def ranked_weighted_choice(
    items: list[T],
    decay: float = 0.9,
    random_func: Callable[[], float] | None = None,
) -> T:
    """
    Weighted choice where higher-ranked items have higher weight.

    Uses geometric decay: weight_i = decay^i

    Args:
        items: List of items (index = rank)
        decay: Decay factor (0 < decay <= 1)
        random_func: Random function

    Returns:
        Selected item
    """
    n = len(items)
    weights = [decay ** i for i in range(n)]
    return weighted_choice(items, weights, random_func)


class ReservoirSampler(Generic[T]):
    """
    Reservoir sampling for selecting k items from stream.

    Guarantees uniform random sampling without knowing stream size.
    """

    def __init__(self, k: int):
        self.k = k
        self._reservoir: list[T] = []
        self._count = 0

    def add(self, item: T) -> None:
        """Add item from stream."""
        self._count += 1
        if len(self._reservoir) < self.k:
            self._reservoir.append(item)
        else:
            j = random.randint(0, self._count - 1)
            if j < self.k:
                self._reservoir[j] = item

    def add_many(self, items: list[T]) -> None:
        """Add multiple items."""
        for item in items:
            self.add(item)

    def get_sample(self) -> list[T]:
        """Get current sample."""
        return list(self._reservoir)

    @property
    def sample_size(self) -> int:
        return len(self._reservoir)


class LotteryScheduler:
    """
    Lottery scheduling for weighted task selection.

    Tasks with more tickets get proportionally more CPU time.
    """

    def __init__(self):
        self._tickets: dict[str, int] = {}
        self._total = 0

    def register(self, task_id: str, tickets: int) -> None:
        """Register task with ticket count."""
        self._tickets[task_id] = tickets
        self._total = sum(self._tickets.values())

    def remove(self, task_id: str) -> None:
        """Remove task from scheduler."""
        if task_id in self._tickets:
            self._total -= self._tickets.pop(task_id)

    def update_tickets(self, task_id: str, tickets: int) -> None:
        """Update ticket count for task."""
        old = self._tickets.get(task_id, 0)
        self._tickets[task_id] = tickets
        self._total = self._total - old + tickets

    def select(self) -> str | None:
        """Select a task using lottery."""
        if not self._tickets:
            return None
        return weighted_choice(
            list(self._tickets.keys()),
            list(self._tickets.values()),
        )

    def get_weight(self, task_id: str) -> float:
        """Get probability of selecting task."""
        if task_id not in self._tickets or self._total == 0:
            return 0.0
        return self._tickets[task_id] / self._total


def bootstrap_sampling(
    population: list[T],
    sample_size: int | None = None,
    n_iterations: int = 1000,
) -> list[float]:
    """
    Bootstrap confidence interval estimation.

    Args:
        population: Data population
        sample_size: Size of each bootstrap sample
        n_iterations: Number of bootstrap iterations

    Returns:
        List of bootstrap estimates
    """
    if sample_size is None:
        sample_size = len(population)
    estimates = []
    for _ in range(n_iterations):
        sample = random.choices(population, k=sample_size)
        estimates.append(sum(sample) / len(sample))
    return estimates
