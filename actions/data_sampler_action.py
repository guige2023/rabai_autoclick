"""
Data Sampler Action Module.

Statistical and reservoir sampling for large datasets.
"""

from __future__ import annotations

import random
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar


T = TypeVar("T")


@dataclass
class SampleStats:
    """Statistics for a sample."""
    count: int
    mean: float
    variance: float
    min_val: float
    max_val: float


class DataSamplerAction:
    """
    Data sampling with multiple strategies.

    Supports: random, stratified, reservoir, and weighted sampling.
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self.rng = random.Random(seed)

    def random_sample(
        self,
        population: List[T],
        sample_size: int,
        replace: bool = False,
    ) -> List[T]:
        """
        Simple random sampling.

        Args:
            population: List to sample from
            sample_size: Number of items to sample
            replace: With replacement

        Returns:
            List of sampled items
        """
        if sample_size >= len(population) and not replace:
            return list(population)

        if replace:
            return [self.rng.choice(population) for _ in range(sample_size)]

        return self.rng.sample(population, min(sample_size, len(population)))

    def stratified_sample(
        self,
        data: Dict[str, List[T]],
        sample_per_stratum: int,
    ) -> Dict[str, List[T]]:
        """
        Stratified sampling - equal samples per stratum.

        Args:
            data: Dict mapping stratum name to items
            sample_per_stratum: Items per stratum

        Returns:
            Dict with sampled items per stratum
        """
        result = {}
        for stratum, items in data.items():
            result[stratum] = self.random_sample(
                items,
                sample_per_stratum,
                replace=False,
            )
        return result

    def reservoir_sample(
        self,
        stream: Iterator[T],
        reservoir_size: int,
    ) -> List[T]:
        """
        Reservoir sampling for streaming data.

        Uses Algorithm R for equal probability sampling.

        Args:
            stream: Data stream
            reservoir_size: Size of reservoir

        Returns:
            Reservoir containing sample
        """
        reservoir: List[T] = []

        for i, item in enumerate(stream):
            if i < reservoir_size:
                reservoir.append(item)
            else:
                j = self.rng.randint(0, i)
                if j < reservoir_size:
                    reservoir[j] = item

        return reservoir

    def weighted_sample(
        self,
        items: List[T],
        weights: List[float],
        sample_size: int,
    ) -> List[T]:
        """
        Weighted random sampling.

        Args:
            items: Items to sample from
            weights: Weights for each item
            sample_size: Number of items

        Returns:
            List of sampled items
        """
        if len(items) != len(weights):
            raise ValueError("Items and weights must have same length")

        if not weights:
            return []

        total_weight = sum(weights)
        if total_weight <= 0:
            return self.random_sample(items, sample_size)

        normalized = [w / total_weight for w in weights]

        return self.rng.choices(
            items,
            weights=normalized,
            k=min(sample_size, len(items)),
        )

    def systematic_sample(
        self,
        population: List[T],
        sample_size: int,
    ) -> List[T]:
        """
        Systematic sampling with random start.

        Args:
            population: List to sample from
            sample_size: Number of items

        Returns:
            Systematically sampled items
        """
        if sample_size >= len(population):
            return list(population)

        interval = len(population) // sample_size
        start = self.rng.randint(0, interval - 1)

        indices = [start + i * interval for i in range(sample_size)]
        return [population[i] for i in indices]

    def cluster_sample(
        self,
        clusters: List[List[T]],
        num_clusters: int,
    ) -> List[T]:
        """
        Sample entire clusters.

        Args:
            clusters: List of clusters
            num_clusters: Number of clusters to sample

        Returns:
            All items from sampled clusters
        """
        sampled_clusters = self.random_sample(clusters, num_clusters, replace=False)
        result = []
        for cluster in sampled_clusters:
            result.extend(cluster)
        return result

    def compute_stats(
        self,
        values: List[float],
        weights: Optional[List[float]] = None,
    ) -> SampleStats:
        """
        Compute statistics for a sample.

        Args:
            values: Numeric values
            weights: Optional weights

        Returns:
            Sample statistics
        """
        if not values:
            return SampleStats(0, 0.0, 0.0, 0.0, 0.0)

        if weights is None:
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
        else:
            weighted_sum = sum(v * w for v, w in zip(values, weights))
            mean = weighted_sum / sum(weights)

            weighted_var_sum = sum(w * (v - mean) ** 2 for v, w in zip(values, weights))
            variance = weighted_var_sum / sum(weights)

        return SampleStats(
            count=len(values),
            mean=mean,
            variance=variance,
            min_val=min(values),
            max_val=max(values),
        )

    def bootstrap(
        self,
        data: List[float],
        num_samples: int,
        sample_size: int,
    ) -> List[List[float]]:
        """
        Bootstrap sampling for confidence intervals.

        Args:
            data: Original data
            num_samples: Number of bootstrap samples
            sample_size: Size of each sample

        Returns:
            List of bootstrap samples
        """
        return [
            self.random_sample(data, sample_size, replace=True)
            for _ in range(num_samples)
        ]
