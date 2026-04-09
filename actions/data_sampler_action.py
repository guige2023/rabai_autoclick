"""Data Sampler Action Module.

Sample data using various statistical and algorithmic techniques.
"""

from __future__ import annotations

import random
import statistics
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class SamplingMethod(Enum):
    """Sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"
    WEIGHTED = "weighted"


@dataclass
class SampleResult(Generic[T]):
    """Result of sampling operation."""
    samples: list[T]
    method: SamplingMethod
    original_size: int
    sample_size: int
    metadata: dict[str, Any]


class DataSampler:
    """Data sampler with multiple sampling strategies."""

    def sample_random(
        self,
        data: list[T],
        sample_size: int,
        replacement: bool = False
    ) -> SampleResult[T]:
        """Random sampling with optional replacement."""
        if replacement:
            indices = [random.randint(0, len(data) - 1) for _ in range(sample_size)]
            samples = [data[i] for i in indices]
        else:
            sample_size = min(sample_size, len(data))
            samples = random.sample(data, sample_size)
        return SampleResult(
            samples=samples,
            method=SamplingMethod.RANDOM,
            original_size=len(data),
            sample_size=len(samples),
            metadata={"replacement": replacement}
        )

    def sample_stratified(
        self,
        data: list[dict],
        strata_field: str,
        sample_per_stratum: int
    ) -> SampleResult[dict]:
        """Stratified sampling - equal samples from each stratum."""
        strata: dict[str, list] = defaultdict(list)
        for item in data:
            strata_key = str(item.get(strata_field, "unknown"))
            strata[strata_key].append(item)
        samples = []
        for stratum_key, stratum_data in strata.items():
            stratum_sample = random.sample(
                stratum_data,
                min(sample_per_stratum, len(stratum_data))
            )
            samples.extend(stratum_sample)
        return SampleResult(
            samples=samples,
            method=SamplingMethod.STRATIFIED,
            original_size=len(data),
            sample_size=len(samples),
            metadata={"strata_field": strata_field, "num_strata": len(strata)}
        )

    def sample_systematic(
        self,
        data: list[T],
        sample_size: int
    ) -> SampleResult[T]:
        """Systematic sampling - every kth element."""
        if not data:
            return SampleResult([], SamplingMethod.SYSTEMATIC, 0, 0, {})
        k = max(1, len(data) // sample_size)
        samples = [data[i] for i in range(0, len(data), k)][:sample_size]
        return SampleResult(
            samples=samples,
            method=SamplingMethod.SYSTEMATIC,
            original_size=len(data),
            sample_size=len(samples),
            metadata={"k_interval": k}
        )

    def sample_cluster(
        self,
        data: list[dict],
        cluster_field: str,
        num_clusters: int
    ) -> SampleResult[dict]:
        """Cluster sampling - select entire clusters."""
        clusters: dict[str, list] = defaultdict(list)
        for item in data:
            cluster_key = str(item.get(cluster_field, "unknown"))
            clusters[cluster_key].append(item)
        all_clusters = list(clusters.keys())
        if len(all_clusters) <= num_clusters:
            selected_clusters = all_clusters
        else:
            selected_clusters = random.sample(all_clusters, num_clusters)
        samples = []
        for ck in selected_clusters:
            samples.extend(clusters[ck])
        return SampleResult(
            samples=samples,
            method=SamplingMethod.CLUSTER,
            original_size=len(data),
            sample_size=len(samples),
            metadata={"cluster_field": cluster_field, "num_clusters_selected": len(selected_clusters)}
        )


class ReservoirSampler(Generic[T]):
    """Reservoir sampling for streaming/unbounded data."""

    def __init__(self, k: int) -> None:
        self.k = k
        self._reservoir: list[T] = []
        self._count = 0

    def add(self, item: T) -> None:
        """Add an item using reservoir sampling."""
        if len(self._reservoir) < self.k:
            self._reservoir.append(item)
        else:
            j = random.randint(0, self._count)
            if j < self.k:
                self._reservoir[j] = item
        self._count += 1

    def get_samples(self) -> list[T]:
        """Get current samples."""
        return list(self._reservoir)

    def get_count(self) -> int:
        """Get total items seen."""
        return self._count


class WeightedSampler(Generic[T]):
    """Weighted random sampling."""

    def __init__(self) -> None:
        self._items: list[T] = []
        self._weights: list[float] = []
        self._total_weight = 0.0

    def add(self, item: T, weight: float) -> None:
        """Add item with weight."""
        self._items.append(item)
        self._weights.append(weight)
        self._total_weight += weight

    def sample(self) -> T:
        """Sample single item based on weights."""
        if not self._items:
            raise ValueError("No items to sample")
        r = random.uniform(0, self._total_weight)
        cumulative = 0
        for item, weight in zip(self._items, self._weights):
            cumulative += weight
            if r <= cumulative:
                return item
        return self._items[-1]

    def sample_n(self, n: int) -> list[T]:
        """Sample n items with replacement."""
        return [self.sample() for _ in range(n)]

    def clear(self) -> None:
        """Clear all items."""
        self._items.clear()
        self._weights.clear()
        self._total_weight = 0.0
