"""Data Sampling Engine.

This module provides statistical and random sampling:
- Simple random sampling
- Stratified sampling
- Reservoir sampling for streams
- Weighted sampling

Example:
    >>> from actions.data_sampling_action import DataSampler
    >>> sampler = DataSampler(seed=42)
    >>> sample = sampler.random_sample(large_dataset, n=1000)
"""

from __future__ import annotations

import random
import logging
import threading
import bisect
from typing import Any, Callable, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataSampler:
    """Data sampling with multiple strategies."""

    def __init__(self, seed: Optional[int] = None) -> None:
        """Initialize the sampler.

        Args:
            seed: Random seed for reproducibility.
        """
        self._seed = seed
        self._lock = threading.Lock()
        self._stats = {"samples_taken": 0}

    def random_sample(
        self,
        population: list[T],
        n: int,
        replace: bool = False,
    ) -> list[T]:
        """Simple random sampling.

        Args:
            population: List to sample from.
            n: Number of items to sample.
            replace: Whether to sample with replacement.

        Returns:
            List of sampled items.
        """
        if n < 0:
            raise ValueError("n must be non-negative")
        if n > len(population) and not replace:
            n = len(population)

        rng = random.Random(self._seed)
        with self._lock:
            self._stats["samples_taken"] += 1

        if replace:
            return [rng.choice(population) for _ in range(n)]
        else:
            return rng.sample(population, n)

    def stratified_sample(
        self,
        data: list[dict[str, Any]],
        stratify_key: str,
        n: int,
        min_per_stratum: int = 1,
    ) -> list[dict[str, Any]]:
        """Stratified sampling preserving group proportions.

        Args:
            data: List of dicts to sample.
            stratify_key: Key to stratify by.
            n: Total number of samples.
            min_per_stratum: Minimum samples per stratum.

        Returns:
            List of sampled items.
        """
        strata: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for item in data:
            stratum_value = item.get(stratify_key)
            strata[stratum_value].append(item)

        total = sum(len(s) for s in strata.values())
        if total == 0:
            return []

        samples = []
        remaining = n

        for stratum_value, stratum_data in strata.items():
            proportion = len(stratum_data) / total
            target = max(min_per_stratum, int(round(n * proportion)))
            target = min(target, len(stratum_data), remaining)

            stratum_sample = self.random_sample(stratum_data, target)
            samples.extend(stratum_sample)
            remaining -= target

            if remaining <= 0:
                break

        return samples

    def weighted_sample(
        self,
        items: list[T],
        weights: list[float],
        n: int,
        replace: bool = False,
    ) -> list[T]:
        """Weighted sampling.

        Args:
            items: List of items.
            weights: Weights corresponding to each item.
            n: Number of items to sample.
            replace: Whether to sample with replacement.

        Returns:
            List of sampled items.
        """
        if len(items) != len(weights):
            raise ValueError("items and weights must have same length")
        if any(w < 0 for w in weights):
            raise ValueError("weights must be non-negative")

        total_weight = sum(weights)
        if total_weight <= 0:
            raise ValueError("total weight must be positive")

        cumulative = []
        running_sum = 0
        for w in weights:
            running_sum += w
            cumulative.append(running_sum)

        rng = random.Random(self._seed)
        samples = []

        for _ in range(n):
            r = rng.uniform(0, total_weight)
            idx = bisect.bisect_left(cumulative, r)
            samples.append(items[idx])

            if not replace:
                break

        with self._lock:
            self._stats["samples_taken"] += 1

        return samples

    def reservoir_sample(
        self,
        stream: list[T],
        k: int,
    ) -> list[T]:
        """Reservoir sampling for large streams.

        Args:
            stream: Stream of items.
            k: Reservoir size.

        Returns:
            List of k sampled items.
        """
        if k < 0:
            raise ValueError("k must be non-negative")
        if k >= len(stream):
            return list(stream)

        rng = random.Random(self._seed)
        reservoir = list(stream[:k])

        for i, item in enumerate(stream[k:], start=k):
            j = rng.randint(0, i)
            if j < k:
                reservoir[j] = item

        with self._lock:
            self._stats["samples_taken"] += 1

        return reservoir

    def systematic_sample(
        self,
        population: list[T],
        n: int,
        skip: Optional[int] = None,
    ) -> list[T]:
        """Systematic sampling with fixed interval.

        Args:
            population: List to sample from.
            n: Number of items to sample.
            skip: Interval between samples. Computed if None.

        Returns:
            List of sampled items.
        """
        if n <= 0:
            return []
        if n >= len(population):
            return list(population)

        if skip is None:
            skip = len(population) // n

        start = random.Random(self._seed).randint(0, min(skip, len(population) - 1))
        samples = []
        idx = start

        while idx < len(population) and len(samples) < n:
            samples.append(population[idx])
            idx += skip

        return samples

    def cluster_sample(
        self,
        data: list[dict[str, Any]],
        cluster_key: str,
        n_clusters: int,
    ) -> list[dict[str, Any]]:
        """Cluster sampling - randomly select clusters.

        Args:
            data: List of dicts.
            cluster_key: Key defining clusters.
            n_clusters: Number of clusters to sample.

        Returns:
            All items in sampled clusters.
        """
        clusters = set(item.get(cluster_key) for item in data)
        sampled_clusters = self.random_sample(list(clusters), n_clusters, replace=False)
        return [item for item in data if item.get(cluster_key) in sampled_clusters]

    def get_stats(self) -> dict[str, int]:
        """Get sampling statistics."""
        with self._lock:
            return dict(self._stats)
