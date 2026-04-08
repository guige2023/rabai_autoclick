"""Weighted choice utilities for RabAI AutoClick.

Provides:
- Weighted random selection
- Probability distributions
- Weighted sampling
"""

from __future__ import annotations

import random
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Tuple,
    TypeVar,
)


T = TypeVar("T")


def weighted_choice(
    items: List[T],
    weights: List[float],
) -> T:
    """Choose a random item based on weights.

    Args:
        items: List of items to choose from.
        weights: Corresponding weights (must be positive).

    Returns:
        Selected item.
    """
    if not items or not weights:
        raise ValueError("items and weights must be non-empty")
    if len(items) != len(weights):
        raise ValueError("items and weights must have same length")

    total = sum(weights)
    if total <= 0:
        raise ValueError("weights must sum to positive value")

    normalized = [w / total for w in weights]
    return random.choices(items, weights=normalized, k=1)[0]


def weighted_sample(
    items: List[T],
    weights: List[float],
    k: int,
) -> List[T]:
    """Sample k items based on weights without replacement.

    Args:
        items: List of items to sample from.
        weights: Corresponding weights.
        k: Number of items to sample.

    Returns:
        List of sampled items.
    """
    if k > len(items):
        raise ValueError("k cannot be larger than items")
    return weighted_choice(items, weights)


def build_weighted_dict(
    data: Dict[str, float],
) -> List[Tuple[str, float]]:
    """Convert weighted dict to sorted list.

    Args:
        data: Dict mapping items to weights.

    Returns:
        List of (item, weight) tuples sorted by weight.
    """
    return sorted(data.items(), key=lambda x: x[1])


__all__ = [
    "weighted_choice",
    "weighted_sample",
    "build_weighted_dict",
]
