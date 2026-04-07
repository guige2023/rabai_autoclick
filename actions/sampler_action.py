"""sampler_action module for rabai_autoclick.

Provides data sampling operations: random sampling, stratified sampling,
reservoir sampling, weighted sampling, and bootstrap sampling.
"""

from __future__ import annotations

import bisect
import random
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Sequence, TypeVar

__all__ = [
    "sample",
    "sample_without_replacement",
    "weighted_sample",
    "stratified_sample",
    "reservoir_sample",
    "bootstrap_sample",
    "shuffle_sample",
    "sample_n",
    "sample_frac",
    "cluster_sample",
    "SampleResult",
]


T = TypeVar("T")


@dataclass
class SampleResult(Generic[T]):
    """Result of a sampling operation."""
    sample: List[T]
    population_size: int
    sample_size: int
    with_replacement: bool = False


def sample(
    items: Sequence[T],
    n: int,
    replace: bool = False,
    seed: Optional[int] = None,
) -> SampleResult[T]:
    """Random sample from items.

    Args:
        items: Population to sample from.
        n: Number of items to sample.
        replace: With replacement if True.
        seed: Random seed for reproducibility.

    Returns:
        SampleResult with sampled items.
    """
    if seed is not None:
        random.seed(seed)

    pop = list(items)
    pop_size = len(pop)
    n = min(n, pop_size) if not replace else n

    if replace:
        sample_list = [random.choice(pop) for _ in range(n)]
    else:
        if n == pop_size:
            sample_list = list(pop)
        else:
            indices = random.sample(range(pop_size), n)
            sample_list = [pop[i] for i in indices]

    return SampleResult(
        sample=sample_list,
        population_size=pop_size,
        sample_size=len(sample_list),
        with_replacement=replace,
    )


def sample_without_replacement(
    items: Sequence[T],
    n: int,
    seed: Optional[int] = None,
) -> List[T]:
    """Sample without replacement (unique items only).

    Args:
        items: Population to sample from.
        n: Number of items to sample.
        seed: Random seed.

    Returns:
        List of unique sampled items.
    """
    return sample(items, n, replace=False, seed=seed).sample


def weighted_sample(
    items: Sequence[T],
    weights: Sequence[float],
    n: int,
    replace: bool = False,
    seed: Optional[int] = None,
) -> List[T]:
    """Weighted random sampling.

    Args:
        items: Items to sample from.
        weights: Probability weights for each item.
        n: Number of items to sample.
        replace: With replacement if True.
        seed: Random seed.

    Returns:
        Weighted sampled items.
    """
    if seed is not None:
        random.seed(seed)

    if len(items) != len(weights):
        raise ValueError("items and weights must have same length")

    if sum(weights) <= 0:
        raise ValueError("weights must sum to positive value")

    result: List[T] = []
    pop = list(items)
    w = list(weights)

    for _ in range(n):
        total = sum(w)
        if total <= 0:
            break
        r = random.random() * total
        cumsum = 0
        for i, weight in enumerate(w):
            cumsum += weight
            if cumsum >= r:
                result.append(pop[i])
                if not replace:
                    w[i] = 0
                break

    return result


def stratified_sample(
    items: Sequence[T],
    stratify_fn: Callable[[T], str],
    n: int,
    seed: Optional[int] = None,
) -> List[T]:
    """Stratified sampling: sample proportionally from each stratum.

    Args:
        items: Population to sample from.
        stratify_fn: Function returning stratum label.
        n: Total number of samples desired.
        seed: Random seed.

    Returns:
        List of stratified sampled items.
    """
    if seed is not None:
        random.seed(seed)

    groups: dict = {}
    for item in items:
        label = stratify_fn(item)
        if label not in groups:
            groups[label] = []
        groups[label].append(item)

    total = len(items)
    result: List[T] = []

    for label, group_items in groups.items():
        proportion = len(group_items) / total
        group_n = max(1, round(n * proportion))
        group_n = min(group_n, len(group_items))
        result.extend(random.sample(group_items, group_n))

    return result


def reservoir_sample(
    items: Iterable[T],
    k: int,
    seed: Optional[int] = None,
) -> List[T]:
    """Reservoir sampling: sample k items from stream of unknown size.

    Uses Algorithm R for unbiased sampling.

    Args:
        items: Stream of items (iterator).
        k: Number of items to sample.
        seed: Random seed.

    Returns:
        List of k sampled items.
    """
    if seed is not None:
        random.seed(seed)

    result: List[T] = []
    iterator = iter(items)
    count = 0

    for i, item in enumerate(iterator):
        count += 1
        if i < k:
            result.append(item)
        else:
            j = random.randint(0, i)
            if j < k:
                result[j] = item

    return result[:k]


def bootstrap_sample(
    items: Sequence[T],
    n: Optional[int] = None,
    seed: Optional[int] = None,
) -> List[T]:
    """Bootstrap sample: sample with replacement, same size as population.

    Args:
        items: Population to sample from.
        n: Sample size (default = population size).
        seed: Random seed.

    Returns:
        Bootstrap sample.
    """
    if seed is not None:
        random.seed(seed)

    pop_size = len(items)
    sample_size = n if n is not None else pop_size

    return [random.choice(items) for _ in range(sample_size)]


def shuffle_sample(
    items: Sequence[T],
    n: int,
    seed: Optional[int] = None,
) -> List[T]:
    """Shuffle then take first n items.

    Args:
        items: Items to shuffle and sample.
        n: Number of items to return.
        seed: Random seed.

    Returns:
        Shuffled sample of n items.
    """
    if seed is not None:
        random.seed(seed)

    shuffled = list(items)
    random.shuffle(shuffled)
    return shuffled[:min(n, len(shuffled))]


def sample_n(
    items: Sequence[T],
    n: int,
    replace: bool = False,
    seed: Optional[int] = None,
) -> List[T]:
    """Sample exactly n items (alias for sample without replacement).

    Args:
        items: Population.
        n: Sample size.
        replace: With replacement.
        seed: Random seed.

    Returns:
        List of n items.
    """
    return sample(items, n, replace=replace, seed=seed).sample


def sample_frac(
    items: Sequence[T],
    frac: float,
    replace: bool = False,
    seed: Optional[int] = None,
) -> List[T]:
    """Sample by fraction of population.

    Args:
        items: Population.
        frac: Fraction to sample (0.0-1.0).
        replace: With replacement.
        seed: Random seed.

    Returns:
        Sampled items.
    """
    if seed is not None:
        random.seed(seed)

    n = max(1, int(len(items) * frac))
    return sample(items, n, replace=replace, seed=seed).sample


def cluster_sample(
    items: Sequence[T],
    cluster_fn: Callable[[T], str],
    n_clusters: int,
    samples_per_cluster: int = 1,
    seed: Optional[int] = None,
) -> List[T]:
    """Cluster sampling: randomly select clusters, then sample within.

    Args:
        items: Population.
        cluster_fn: Function returning cluster ID.
        n_clusters: Number of clusters to select.
        samples_per_cluster: Samples to take per selected cluster.
        seed: Random seed.

    Returns:
        Cluster sampled items.
    """
    if seed is not None:
        random.seed(seed)

    clusters: dict = {}
    for item in items:
        label = cluster_fn(item)
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(item)

    cluster_ids = list(clusters.keys())
    if n_clusters < len(cluster_ids):
        selected_clusters = random.sample(cluster_ids, n_clusters)
    else:
        selected_clusters = cluster_ids

    result: List[T] = []
    for cid in selected_clusters:
        cluster_items = clusters[cid]
        n = min(samples_per_cluster, len(cluster_items))
        result.extend(random.sample(cluster_items, n))

    return result
