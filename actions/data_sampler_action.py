"""Data Sampler Action Module.

Provides advanced sampling techniques for data selection including
random, stratified, systematic, and reservoir sampling.
"""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SamplingStrategy(Enum):
    """Sampling strategies."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"
    BOOTSTRAP = "bootstrap"


@dataclass
class SampleResult:
    """Result of a sampling operation."""
    success: bool
    sample: List[Any]
    original_size: int
    sample_size: int
    strategy: SamplingStrategy
    metadata: Dict[str, Any] = field(default_factory=dict)


class RandomSampler:
    """Random sampling utilities."""

    @staticmethod
    def sample(
        data: List[Any],
        n: int,
        replace: bool = False
    ) -> List[Any]:
        """Simple random sampling."""
        if replace:
            return random.choices(data, k=n)
        else:
            if n > len(data):
                n = len(data)
            return random.sample(data, n)

    @staticmethod
    def sample_with_weights(
        data: List[Any],
        weights: List[float],
        n: int,
        replace: bool = False
    ) -> List[Any]:
        """Weighted random sampling."""
        if replace:
            return random.choices(data, weights=weights, k=n)
        else:
            if n > len(data):
                n = len(data)
            indices = random.choices(range(len(data)), weights=weights, k=n)
            return [data[i] for i in sorted(set(indices))]


class StratifiedSampler:
    """Stratified sampling for grouped data."""

    @staticmethod
    def sample(
        data: List[Dict[str, Any]],
        stratify_key: str,
        n: int,
        weights: Optional[Dict[Any, float]] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """Stratified sampling maintaining proportions."""
        # Group by stratum
        strata: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            if isinstance(item, dict) and stratify_key in item:
                strata[item[stratify_key]].append(item)

        total = len(data)
        if weights is None:
            # Proportional allocation
            weights = {
                stratum: len(items) / total
                for stratum, items in strata.items()
            }

        # Calculate sample size per stratum
        samples_per_stratum: Dict[str, int] = {}
        for stratum, prop in weights.items():
            samples_per_stratum[stratum] = max(1, int(n * prop))

        # Adjust to match total n
        allocated = sum(samples_per_stratum.values())
        if allocated != n:
            diff = n - allocated
            largest = max(strata.keys(), key=lambda s: len(strata[s]))
            samples_per_stratum[largest] += diff

        # Sample from each stratum
        result = []
        for stratum, count in samples_per_stratum.items():
            stratum_data = strata.get(stratum, [])
            if stratum_data:
                sample_n = min(count, len(stratum_data))
                result.extend(random.sample(stratum_data, sample_n))

        return result, samples_per_stratum


class SystematicSampler:
    """Systematic sampling at regular intervals."""

    @staticmethod
    def sample(
        data: List[Any],
        n: int
    ) -> List[Any]:
        """Systematic sampling using fixed interval."""
        if n >= len(data):
            return list(data)

        interval = len(data) // n
        start = random.randint(0, interval - 1)

        result = []
        for i in range(start, len(data), interval):
            if len(result) >= n:
                break
            result.append(data[i])

        return result


class ReservoirSampler:
    """Reservoir sampling for streaming/unbounded data."""

    def __init__(self, k: int):
        self._k = k
        self._reservoir: List[Any] = []
        self._count = 0

    def add(self, item: Any) -> None:
        """Add an item to the reservoir."""
        if len(self._reservoir) < self._k:
            self._reservoir.append(item)
        else:
            j = random.randint(0, self._count)
            if j < self._k:
                self._reservoir[j] = item
        self._count += 1

    def add_many(self, items: List[Any]) -> None:
        """Add multiple items."""
        for item in items:
            self.add(item)

    def get_sample(self) -> List[Any]:
        """Get the current sample."""
        return list(self._reservoir)

    def get_count(self) -> int:
        """Get total items processed."""
        return self._count


class WeightedSampler:
    """Weighted sampling with various selection methods."""

    def __init__(self, items: List[Any], weights: List[float]):
        self._items = items
        self._weights = weights
        self._total_weight = sum(weights)
        self._cum_weights = self._cumulative_weights()

    def _cumulative_weights(self) -> List[float]:
        """Calculate cumulative weights."""
        cum = []
        total = 0
        for w in self._weights:
            total += w
            cum.append(total)
        return cum

    def sample(self, n: int = 1, replace: bool = False) -> List[Any]:
        """Sample items based on weights."""
        if replace:
            return random.choices(self._items, weights=self._weights, k=n)
        else:
            indices = random.choices(
                range(len(self._items)),
                weights=self._weights,
                k=n
            )
            seen = set()
            result = []
            for idx in indices:
                if idx not in seen or len(seen) >= len(self._items):
                    seen.add(idx)
                    result.append(self._items[idx])
            return result


class BootstrapSampler:
    """Bootstrap sampling with replacement."""

    @staticmethod
    def sample(
        data: List[Any],
        n: int
    ) -> List[Any]:
        """Bootstrap sampling."""
        return random.choices(data, k=n)

    @staticmethod
    def sample_with_stats(
        data: List[float],
        n: int,
        stat_func: Callable[[List[float]], float]
    ) -> Tuple[List[Any], float]:
        """Sample and calculate a statistic."""
        sample = BootstrapSampler.sample(data, n)
        stat = stat_func(sample)
        return sample, stat


class ClusterSampler:
    """Cluster sampling for grouped data."""

    @staticmethod
    def sample(
        data: List[Dict[str, Any]],
        cluster_key: str,
        n_clusters: int,
        sample_per_cluster: int = -1
    ) -> List[Dict[str, Any]]:
        """Cluster sampling - sample clusters then sample within."""
        # Group into clusters
        clusters: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            if isinstance(item, dict) and cluster_key in item:
                clusters[item[cluster_key]].append(item)

        cluster_ids = list(clusters.keys())
        if n_clusters >= len(cluster_ids):
            # Sample all clusters
            selected_clusters = cluster_ids
        else:
            selected_clusters = random.sample(cluster_ids, n_clusters)

        # Sample within clusters
        result = []
        for cluster_id in selected_clusters:
            cluster_data = clusters[cluster_id]
            if sample_per_cluster > 0 and sample_per_cluster < len(cluster_data):
                result.extend(random.sample(cluster_data, sample_per_cluster))
            else:
                result.extend(cluster_data)

        return result


class DataSamplerAction:
    """Main action class for data sampling."""

    def __init__(self):
        self._reservoir_samplers: Dict[str, ReservoirSampler] = {}

    def create_reservoir(self, key: str, k: int) -> None:
        """Create a named reservoir sampler."""
        self._reservoir_samplers[key] = ReservoirSampler(k)

    def add_to_reservoir(self, key: str, item: Any) -> bool:
        """Add an item to a reservoir sampler."""
        sampler = self._reservoir_samplers.get(key)
        if sampler:
            sampler.add(item)
            return True
        return False

    def get_reservoir_sample(self, key: str) -> Optional[List[Any]]:
        """Get sample from a reservoir sampler."""
        sampler = self._reservoir_samplers.get(key)
        return sampler.get_sample() if sampler else None

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data sampler action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - data: Data to sample
                - Other operation-specific fields

        Returns:
            Dictionary with sampling results.
        """
        operation = context.get("operation", "sample")

        if operation == "sample":
            data = context.get("data", [])
            n = context.get("n", 10)
            strategy_str = context.get("strategy", "random")
            replace = context.get("replace", False)

            try:
                strategy = SamplingStrategy(strategy_str)
            except ValueError:
                strategy = SamplingStrategy.RANDOM

            if strategy == SamplingStrategy.RANDOM:
                sample = RandomSampler.sample(data, n, replace)
            elif strategy == SamplingStrategy.SYSTEMATIC:
                sample = SystematicSampler.sample(data, n)
            elif strategy == SamplingStrategy.WEIGHTED:
                weights = context.get("weights", [1.0] * len(data))
                sample = RandomSampler.sample_with_weights(data, weights, n, replace)
            else:
                sample = RandomSampler.sample(data, n, replace)

            return {
                "success": True,
                "sample": sample,
                "original_size": len(data),
                "sample_size": len(sample),
                "strategy": strategy.value
            }

        elif operation == "stratified":
            data = context.get("data", [])
            stratify_key = context.get("stratify_key", "")
            n = context.get("n", 10)

            sample, counts = StratifiedSampler.sample(data, stratify_key, n)
            return {
                "success": True,
                "sample": sample,
                "original_size": len(data),
                "sample_size": len(sample),
                "strata_counts": counts
            }

        elif operation == "cluster":
            data = context.get("data", [])
            cluster_key = context.get("cluster_key", "")
            n_clusters = context.get("n_clusters", 2)
            sample_per_cluster = context.get("sample_per_cluster", -1)

            sample = ClusterSampler.sample(data, cluster_key, n_clusters, sample_per_cluster)
            return {
                "success": True,
                "sample": sample,
                "original_size": len(data),
                "sample_size": len(sample)
            }

        elif operation == "bootstrap":
            data = context.get("data", [])
            n = context.get("n", len(data))

            sample = BootstrapSampler.sample(data, n)
            return {
                "success": True,
                "sample": sample,
                "original_size": len(data),
                "sample_size": len(sample)
            }

        elif operation == "reservoir_create":
            key = context.get("key", "")
            k = context.get("k", 10)
            self.create_reservoir(key, k)
            return {"success": True, "key": key, "k": k}

        elif operation == "reservoir_add":
            key = context.get("key", "")
            item = context.get("item")
            success = self.add_to_reservoir(key, item)
            return {"success": success, "key": key}

        elif operation == "reservoir_get":
            key = context.get("key", "")
            sample = self.get_reservoir_sample(key)
            if sample is not None:
                sampler = self._reservoir_samplers.get(key)
                return {
                    "success": True,
                    "sample": sample,
                    "total_items": sampler.get_count() if sampler else 0
                }
            return {"success": False, "error": "Reservoir not found"}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
