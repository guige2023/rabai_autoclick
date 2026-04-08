"""Data Sampler Action Module.

Provides statistical sampling: random, stratified,
systematic, and reservoir sampling for large datasets.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar
from collections import defaultdict

T = TypeVar("T")


class SamplingStrategy(Enum):
    """Sampling strategy."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"


from enum import Enum


@dataclass
class SamplingConfig:
    """Sampling configuration."""
    strategy: SamplingStrategy
    sample_size: int
    random_seed: Optional[int] = None
    stratify_field: Optional[str] = None
    stratify_ratios: Optional[Dict[str, float]] = None
    replace: bool = False


class DataSamplerAction:
    """Data sampler with multiple strategies.

    Example:
        sampler = DataSamplerAction()

        sample = sampler.sample(
            data=list(range(1000)),
            config=SamplingConfig(
                strategy=SamplingStrategy.RANDOM,
                sample_size=100
            )
        )
    """

    def __init__(self) -> None:
        self._rng = random.Random()

    def sample(
        self,
        data: List[T],
        config: SamplingConfig,
    ) -> List[T]:
        """Sample data using specified strategy.

        Args:
            data: Data to sample from
            config: Sampling configuration

        Returns:
            Sampled data
        """
        if config.random_seed is not None:
            self._rng.seed(config.random_seed)

        if config.strategy == SamplingStrategy.RANDOM:
            return self._random_sample(data, config)
        elif config.strategy == SamplingStrategy.STRATIFIED:
            return self._stratified_sample(data, config)
        elif config.strategy == SamplingStrategy.SYSTEMATIC:
            return self._systematic_sample(data, config)
        elif config.strategy == SamplingStrategy.RESERVOIR:
            return self._reservoir_sample(data, config)
        elif config.strategy == SamplingStrategy.CLUSTER:
            return self._cluster_sample(data, config)

        return data[:config.sample_size]

    def _random_sample(
        self,
        data: List[T],
        config: SamplingConfig,
    ) -> List[T]:
        """Random sampling with optional replacement."""
        if config.replace:
            return [self._rng.choice(data) for _ in range(config.sample_size)]

        indices = self._rng.sample(range(len(data)), min(config.sample_size, len(data)))
        return [data[i] for i in indices]

    def _stratified_sample(
        self,
        data: List[Dict],
        config: SamplingConfig,
    ) -> List[Dict]:
        """Stratified sampling by field."""
        if not config.stratify_field:
            return self._random_sample(data, config)

        strata: Dict[str, List[Dict]] = defaultdict(list)
        for record in data:
            key = str(record.get(config.stratify_field, "unknown"))
            strata[key].append(record)

        samples: List[Dict] = []
        total_size = len(data)
        ratios = config.stratify_ratios or {}

        for key, stratum in strata.items():
            ratio = ratios.get(key, len(stratum) / total_size)
            stratum_size = max(1, int(config.sample_size * ratio))
            stratum_sample = self._rng.sample(stratum, min(stratum_size, len(stratum)))
            samples.extend(stratum_sample)

        return samples

    def _systematic_sample(
        self,
        data: List[T],
        config: SamplingConfig,
    ) -> List[T]:
        """Systematic sampling (every k-th element)."""
        if len(data) <= config.sample_size:
            return data

        step = len(data) // config.sample_size
        start = self._rng.randint(0, step - 1)

        return data[start::step][:config.sample_size]

    def _reservoir_sample(
        self,
        data: List[T],
        config: SamplingConfig,
    ) -> List[T]:
        """Reservoir sampling for large streams."""
        k = min(config.sample_size, len(data))
        reservoir = data[:k]

        for i in range(k, len(data)):
            j = self._rng.randint(0, i)
            if j < k:
                reservoir[j] = data[i]

        return reservoir

    def _cluster_sample(
        self,
        data: List[Dict],
        config: SamplingConfig,
    ) -> List[Dict]:
        """Cluster sampling (randomly select clusters)."""
        if not config.stratify_field:
            return self._random_sample(data, config)

        clusters: Dict[str, List[Dict]] = defaultdict(list)
        for record in data:
            key = str(record.get(config.stratify_field, "unknown"))
            clusters[key].append(record)

        cluster_keys = list(clusters.keys())
        num_clusters = max(1, min(config.sample_size, len(cluster_keys)))
        selected_keys = self._rng.sample(cluster_keys, num_clusters)

        return [record for key in selected_keys for record in clusters[key]]

    def split(
        self,
        data: List[T],
        ratios: List[float],
        random_seed: Optional[int] = None,
    ) -> List[List[T]]:
        """Split data into multiple samples by ratios.

        Args:
            data: Data to split
            ratios: List of ratios (must sum to 1.0)
            random_seed: Optional random seed

        Returns:
            List of split datasets
        """
        if abs(sum(ratios) - 1.0) > 0.001:
            raise ValueError("Ratios must sum to 1.0")

        if random_seed is not None:
            self._rng.seed(random_seed)

        shuffled = data.copy()
        self._rng.shuffle(shuffled)

        boundaries = []
        cumulative = 0
        for ratio in ratios[:-1]:
            cumulative += ratio
            boundaries.append(int(len(data) * cumulative))

        splits = []
        prev = 0
        for boundary in boundaries:
            splits.append(shuffled[prev:boundary])
            prev = boundary
        splits.append(shuffled[prev:])

        return splits
