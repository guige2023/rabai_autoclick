"""Data sampling action for extracting representative data samples.

Provides various sampling strategies to extract representative
subsets from large datasets for analysis and testing.
"""

import logging
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class SamplingStrategy(Enum):
    """Data sampling strategies."""
    RANDOM = "random"
    SYSTEMATIC = "systematic"
    STRATIFIED = "stratified"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"
    WEIGHTED = "weighted"


@dataclass
class SamplingConfig:
    """Configuration for sampling behavior."""
    sample_size: int
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    random_seed: Optional[int] = None
    stratified_field: Optional[str] = None
    weight_field: Optional[str] = None


@dataclass
class SamplingResult:
    """Result of a sampling operation."""
    sample: list[Any]
    sample_size: int
    original_size: int
    sampling_rate: float
    strategy: str


class DataSamplingAction:
    """Sample data using various strategies.

    Example:
        >>> sampler = DataSamplingAction()
        >>> result = sampler.sample(data, sample_size=1000)
    """

    def __init__(self) -> None:
        self._rng = random.Random()

    def sample(
        self,
        data: list[Any],
        sample_size: int,
        strategy: SamplingStrategy = SamplingStrategy.RANDOM,
        stratified_field: Optional[str] = None,
        weight_field: Optional[str] = None,
        random_seed: Optional[int] = None,
    ) -> SamplingResult:
        """Sample data from a dataset.

        Args:
            data: Dataset to sample from.
            sample_size: Number of items to sample.
            strategy: Sampling strategy to use.
            stratified_field: Field for stratified sampling.
            weight_field: Field for weighted sampling.
            random_seed: Optional random seed for reproducibility.

        Returns:
            Sampling result with sample and metadata.
        """
        if random_seed is not None:
            self._rng.seed(random_seed)

        original_size = len(data)

        if sample_size >= original_size:
            return SamplingResult(
                sample=data.copy(),
                sample_size=original_size,
                original_size=original_size,
                sampling_rate=1.0,
                strategy=strategy.value,
            )

        if strategy == SamplingStrategy.RANDOM:
            sample = self._random_sample(data, sample_size)
        elif strategy == SamplingStrategy.SYSTEMATIC:
            sample = self._systematic_sample(data, sample_size)
        elif strategy == SamplingStrategy.STRATIFIED:
            sample = self._stratified_sample(data, sample_size, stratified_field)
        elif strategy == SamplingStrategy.CLUSTER:
            sample = self._cluster_sample(data, sample_size)
        elif strategy == SamplingStrategy.RESERVOIR:
            sample = self._reservoir_sample(data, sample_size)
        elif strategy == SamplingStrategy.WEIGHTED:
            sample = self._weighted_sample(data, sample_size, weight_field)
        else:
            sample = self._random_sample(data, sample_size)

        sampling_rate = sample_size / original_size

        return SamplingResult(
            sample=sample,
            sample_size=len(sample),
            original_size=original_size,
            sampling_rate=sampling_rate,
            strategy=strategy.value,
        )

    def _random_sample(self, data: list[Any], size: int) -> list[Any]:
        """Random sampling without replacement.

        Args:
            data: Dataset to sample.
            size: Sample size.

        Returns:
            Random sample.
        """
        return self._rng.sample(data, size)

    def _systematic_sample(self, data: list[Any], size: int) -> list[Any]:
        """Systematic sampling (every kth element).

        Args:
            data: Dataset to sample.
            size: Sample size.

        Returns:
            Systematic sample.
        """
        k = len(data) // size
        if k < 1:
            k = 1
        start = self._rng.randint(0, k - 1)
        return [data[i] for i in range(start, len(data), k)][:size]

    def _stratified_sample(
        self,
        data: list[Any],
        size: int,
        field: Optional[str],
    ) -> list[Any]:
        """Stratified sampling by field value.

        Args:
            data: Dataset to sample.
            size: Sample size.
            field: Field to stratify by.

        Returns:
            Stratified sample.
        """
        if not field:
            return self._random_sample(data, size)

        groups: dict[Any, list[Any]] = {}
        for item in data:
            if isinstance(item, dict):
                key = item.get(field, "unknown")
            else:
                key = getattr(item, field, "unknown")
            groups.setdefault(key, []).append(item)

        total = sum(len(g) for g in groups.values())
        samples: list[Any] = []

        for group_key, group_items in groups.items():
            group_proportion = len(group_items) / total
            group_size = max(1, int(size * group_proportion))
            samples.extend(self._rng.sample(group_items, min(group_size, len(group_items))))

        return samples[:size]

    def _cluster_sample(self, data: list[Any], size: int) -> list[Any]:
        """Cluster sampling (random clusters).

        Args:
            data: Dataset to sample.
            size: Sample size.

        Returns:
            Cluster sample.
        """
        if len(data) < size:
            return data.copy()

        cluster_size = max(1, size // 10)
        num_clusters = (size + cluster_size - 1) // cluster_size

        cluster_indices = self._rng.sample(
            range(0, len(data), cluster_size),
            min(num_clusters, len(data) // cluster_size)
        )

        samples: list[Any] = []
        for start_idx in cluster_indices:
            end_idx = min(start_idx + cluster_size, len(data))
            samples.extend(data[start_idx:end_idx])

        return samples[:size]

    def _reservoir_sample(self, data: list[Any], size: int) -> list[Any]:
        """Reservoir sampling for streaming data.

        Args:
            data: Dataset to sample.
            size: Sample size.

        Returns:
            Reservoir sample.
        """
        reservoir = data[:size]

        for i in range(size, len(data)):
            j = self._rng.randint(0, i)
            if j < size:
                reservoir[j] = data[i]

        return reservoir

    def _weighted_sample(
        self,
        data: list[Any],
        size: int,
        weight_field: Optional[str],
    ) -> list[Any]:
        """Weighted sampling based on field values.

        Args:
            data: Dataset to sample.
            size: Sample size.
            weight_field: Field containing weights.

        Returns:
            Weighted sample.
        """
        if not weight_field:
            return self._random_sample(data, size)

        weights: list[float] = []
        for item in data:
            if isinstance(item, dict):
                w = float(item.get(weight_field, 1.0))
            else:
                w = float(getattr(item, weight_field, 1.0))
            weights.append(max(0.0, w))

        total_weight = sum(weights)
        if total_weight == 0:
            return self._random_sample(data, size)

        samples: list[Any] = []
        for _ in range(size):
            r = self._rng.uniform(0, total_weight)
            cumulative = 0.0
            for i, item in enumerate(data):
                cumulative += weights[i]
                if r <= cumulative:
                    samples.append(item)
                    break

        return samples

    def get_sample_sizes(
        self,
        population_size: int,
        confidence_level: float = 0.95,
        margin_of_error: float = 0.05,
    ) -> dict[str, int]:
        """Calculate recommended sample sizes.

        Args:
            population_size: Total population size.
            confidence_level: Desired confidence level.
            margin_of_error: Acceptable margin of error.

        Returns:
            Dictionary of sample size recommendations.
        """
        z_scores = {
            0.90: 1.645,
            0.95: 1.96,
            0.99: 2.576,
        }

        z = z_scores.get(confidence_level, 1.96)
        p = 0.5

        infinite_sample = (z ** 2 * p * (1 - p)) / (margin_of_error ** 2)

        n = infinite_sample / (1 + (infinite_sample - 1) / population_size)

        return {
            "infinite_population": int(infinite_sample) + 1,
            "finite_population": int(n) + 1,
        }
