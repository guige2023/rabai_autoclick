"""
Random utilities v2 — advanced random sampling and generation.

Companion to random_utils.py. Adds weighted sampling,
random walks, and advanced distributions.
"""

from __future__ import annotations

import random
import statistics
from typing import Any, Callable


def weighted_choice(choices: list[tuple[Any, float]], seed: int | None = None) -> Any:
    """
    Choose random item with weighted probability.

    Args:
        choices: List of (item, weight) tuples
        seed: Optional random seed

    Returns:
        Selected item
    """
    if seed is not None:
        random.seed(seed)
    total = sum(w for _, w in choices)
    r = random.random() * total
    cumsum = 0.0
    for item, weight in choices:
        cumsum += weight
        if r <= cumsum:
            return item
    return choices[-1][0]


def weighted_sample(population: list[T], weights: list[float], k: int, seed: int | None = None) -> list[T]:
    """
    Sample k items from population with given weights (without replacement).

    Args:
        population: List of items to sample from
        weights: Weights corresponding to each item
        k: Number of items to sample
        seed: Optional random seed

    Returns:
        List of sampled items
    """
    if seed is not None:
        random.seed(seed)
    if k > len(population):
        raise ValueError("k cannot exceed population size")
    indices = list(range(len(population)))
    selected: list[tuple[float, int, T]] = []
    for i, w in zip(indices, weights):
        selected.append((random.random() ** (1 / w), i, population[i]))
    selected.sort(key=lambda x: x[0])
    return [item for _, _, item in selected[:k]]


def random_walk_1d(steps: int, step_size: float = 1.0, seed: int | None = None) -> list[float]:
    """
    Generate 1D random walk.

    Args:
        steps: Number of steps
        step_size: Size of each step
        seed: Optional random seed

    Returns:
        List of positions at each step
    """
    if seed is not None:
        random.seed(seed)
    positions = [0.0]
    for _ in range(steps):
        direction = random.choice([-1, 1])
        positions.append(positions[-1] + direction * step_size)
    return positions


def random_walk_2d(steps: int, step_size: float = 1.0, seed: int | None = None) -> list[tuple[float, float]]:
    """Generate 2D random walk."""
    if seed is not None:
        random.seed(seed)
    positions = [(0.0, 0.0)]
    for _ in range(steps):
        angle = random.uniform(0, 2 * 3.141592653589793)
        x = positions[-1][0] + step_size * random.cos(angle)
        y = positions[-1][1] + step_size * random.sin(angle)
        positions.append((x, y))
    return positions


def bootstrap_resample(data: list[float], n: int = 1000, sample_size: int | None = None, seed: int | None = None) -> list[list[float]]:
    """
    Generate bootstrap resamples of data.

    Args:
        data: Original sample
        n: Number of resamples
        sample_size: Size of each resample (default: len(data))
        seed: Optional random seed

    Returns:
        List of resampled arrays
    """
    if seed is not None:
        random.seed(seed)
    size = sample_size if sample_size is not None else len(data)
    return [random.choices(data, k=size) for _ in range(n)]


def shuffle_inplace(arr: list, seed: int | None = None) -> None:
    """Shuffle list in place (modifies original)."""
    if seed is not None:
        random.seed(seed)
    random.shuffle(arr)


def reservoir_sample(stream: list[T], k: int, seed: int | None = None) -> list[T]:
    """
    Reservoir sampling: sample k items uniformly from a large stream.

    Args:
        stream: Items to sample from
        k: Number of items to sample
        seed: Optional random seed

    Returns:
        List of k sampled items
    """
    if seed is not None:
        random.seed(seed)
    if k >= len(stream):
        return stream[:]
    result = stream[:k]
    for i in range(k, len(stream)):
        j = random.randint(0, i)
        if j < k:
            result[j] = stream[i]
    return result


def normalvariate_custom(mu: float = 0.0, sigma: float = 1.0, seed: int | None = None) -> float:
    """Generate normally distributed random number."""
    if seed is not None:
        random.seed(seed)
    return random.gauss(mu, sigma)


def exponentialvariate_custom(lam: float = 1.0, seed: int | None = None) -> float:
    """Generate exponentially distributed random number."""
    if seed is not None:
        random.seed(seed)
    return -random.lognormvariate(0, 1) / lam


def choice_without_replacement(population: list, k: int, seed: int | None = None) -> list:
    """Choose k unique items from population (no replacement)."""
    if seed is not None:
        random.seed(seed)
    return random.sample(population, k)
