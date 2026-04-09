"""
Data Sampling Action Module.

Provides data sampling techniques for statistical analysis,
machine learning, and efficient data processing.

Author: RabAi Team
"""

from __future__ import annotations

import random
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


class SamplingMethod(Enum):
    """Sampling methods."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"
    WEIGHTED = "weighted"


from enum import Enum


@dataclass
class SampleResult:
    """Result of sampling operation."""
    data: List[Any]
    method: SamplingMethod
    sample_size: int
    original_size: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSampler:
    """Main data sampling engine."""

    def __init__(self, seed: Optional[int] = None) -> None:
        if seed is not None:
            random.seed(seed)
        self.seed = seed

    def random_sample(
        self,
        data: List[T],
        size: int,
        replace: bool = False,
    ) -> List[T]:
        """Simple random sampling."""
        if size > len(data) and not replace:
            size = len(data)

        if replace:
            return random.choices(data, k=size)
        else:
            return random.sample(data, size)

    def stratified_sample(
        self,
        data: List[Dict[str, Any]],
        stratify_key: str,
        sample_size: int,
        min_per_stratum: int = 1,
    ) -> List[Dict[str, Any]]:
        """Stratified sampling - samples proportionally from each stratum."""
        # Group by stratum
        strata: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            stratum_value = record.get(stratify_key)
            if stratum_value is not None:
                strata[stratum_value].append(record)

        # Calculate sample sizes proportionally
        total_records = len(data)
        sampled: List[Dict[str, Any]] = []

        for stratum_value, stratum_data in strata.items():
            proportion = len(stratum_data) / total_records
            stratum_size = max(min_per_stratum, int(sample_size * proportion))

            if stratum_size >= len(stratum_data):
                sampled.extend(stratum_data)
            else:
                sampled.extend(random.sample(stratum_data, stratum_size))

        return sampled

    def systematic_sample(
        self,
        data: List[T],
        sample_size: int,
    ) -> List[T]:
        """Systematic sampling - select every k-th element."""
        if sample_size >= len(data):
            return data.copy()

        k = len(data) // sample_size
        start_index = random.randint(0, k - 1)

        sampled = []
        for i in range(start_index, len(data), k):
            if len(sampled) >= sample_size:
                break
            sampled.append(data[i])

        return sampled

    def cluster_sample(
        self,
        data: List[Dict[str, Any]],
        cluster_key: str,
        num_clusters: int,
        sample_per_cluster: int,
    ) -> List[Dict[str, Any]]:
        """Cluster sampling - randomly select clusters, then sample within."""
        # Group by cluster
        clusters: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            cluster_id = record.get(cluster_key)
            if cluster_id is not None:
                clusters[cluster_id].append(record)

        cluster_ids = list(clusters.keys())

        if num_clusters >= len(cluster_ids):
            # Sample from all clusters
            selected_clusters = cluster_ids
        else:
            # Randomly select clusters
            selected_clusters = random.sample(cluster_ids, num_clusters)

        sampled = []
        for cluster_id in selected_clusters:
            cluster_data = clusters[cluster_id]
            if sample_per_cluster >= len(cluster_data):
                sampled.extend(cluster_data)
            else:
                sampled.extend(random.sample(cluster_data, sample_per_cluster))

        return sampled

    def reservoir_sample(
        self,
        data: List[T],
        sample_size: int,
    ) -> List[T]:
        """Reservoir sampling - streaming-friendly sampling algorithm."""
        if sample_size >= len(data):
            return data.copy()

        reservoir = data[:sample_size]

        for i in range(sample_size, len(data)):
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = data[i]

        return reservoir

    def weighted_sample(
        self,
        data: List[T],
        weights: List[float],
        sample_size: int,
        replace: bool = False,
    ) -> List[T]:
        """Weighted sampling - probability proportional to weight."""
        if len(weights) != len(data):
            raise ValueError("Data and weights must have same length")

        if replace:
            return random.choices(data, weights=weights, k=sample_size)
        else:
            indices = list(range(len(data)))
            sampled_indices = random.choices(indices, weights=weights, k=sample_size)
            if len(set(sampled_indices)) < len(sampled_indices):
                # Without replacement, need to handle duplicates
                seen = set()
                result = []
                for idx in sampled_indices:
                    while idx in seen:
                        idx = random.choices(indices, weights=weights, k=1)[0]
                    seen.add(idx)
                    result.append(data[idx])
                return result
            return [data[i] for i in sampled_indices]

    def sample(
        self,
        data: List[Any],
        method: SamplingMethod,
        sample_size: int,
        **kwargs,
    ) -> SampleResult:
        """Generic sample method with method selection."""
        method_map = {
            SamplingMethod.RANDOM: lambda: self.random_sample(data, sample_size, kwargs.get("replace", False)),
            SamplingMethod.STRATIFIED: lambda: self.stratified_sample(data, kwargs["stratify_key"], sample_size),
            SamplingMethod.SYSTEMATIC: lambda: self.systematic_sample(data, sample_size),
            SamplingMethod.CLUSTER: lambda: self.cluster_sample(data, kwargs["cluster_key"], kwargs["num_clusters"], kwargs["sample_per_cluster"]),
            SamplingMethod.RESERVOIR: lambda: self.reservoir_sample(data, sample_size),
            SamplingMethod.WEIGHTED: lambda: self.weighted_sample(data, kwargs["weights"], sample_size),
        }

        if method not in method_map:
            raise ValueError(f"Unknown sampling method: {method}")

        sampled = method_map[method]()

        return SampleResult(
            data=sampled,
            method=method,
            sample_size=len(sampled),
            original_size=len(data),
            metadata=kwargs,
        )


class StatisticalSampler(DataSampler):
    """Extended sampler with statistical methods."""

    def bootstrap_sample(
        self,
        data: List[float],
        sample_size: int,
        num_iterations: int = 1000,
    ) -> List[List[float]]:
        """Bootstrap sampling for confidence intervals."""
        results = []
        for _ in range(num_iterations):
            sample = self.random_sample(data, sample_size, replace=True)
            results.append(sample)
        return results

    def jackknife_sample(
        self,
        data: List[float],
    ) -> List[List[float]]:
        """Jackknife resampling - leave-one-out."""
        n = len(data)
        samples = []
        for i in range(n):
            sample = data[:i] + data[i+1:]
            samples.append(sample)
        return samples

    def calculate_statistics(
        self,
        samples: List[List[float]],
    ) -> Dict[str, Any]:
        """Calculate statistics from multiple samples."""
        all_means = [statistics.mean(s) for s in samples]

        return {
            "mean": statistics.mean(all_means),
            "std_error": statistics.stdev(all_means) if len(all_means) > 1 else 0,
            "confidence_interval_95": (
                statistics.mean(all_means) - 1.96 * statistics.stdev(all_means) / (len(all_means) ** 0.5),
                statistics.mean(all_means) + 1.96 * statistics.stdev(all_means) / (len(all_means) ** 0.5),
            ) if len(all_means) > 1 else (0, 0),
            "num_samples": len(samples),
        }


class StreamSampler:
    """Streaming-compatible sampler for large datasets."""

    def __init__(self, sample_size: int, seed: Optional[int] = None) -> None:
        self.sample_size = sample_size
        self.seeded_random = random.Random(seed)
        self.reservoir: List[Any] = []
        self.count = 0

    def add(self, item: T) -> None:
        """Add item to stream sampler."""
        self.count += 1

        if len(self.reservoir) < self.sample_size:
            self.reservoir.append(item)
        else:
            j = self.seeded_random.randint(0, self.count - 1)
            if j < self.sample_size:
                self.reservoir[j] = item

    def get_sample(self) -> List[T]:
        """Get current sample."""
        return self.reservoir.copy()
