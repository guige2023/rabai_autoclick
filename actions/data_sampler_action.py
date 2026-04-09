"""Data Sampler Action Module.

Provides various data sampling strategies including random, stratified,
systematic, and reservoir sampling for large datasets.
"""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class SamplingStrategy(Enum):
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"
    BOOTSTRAP = "bootstrap"


@dataclass
class SampleConfig:
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    sample_size: int = 100
    replace: bool = False
    random_seed: Optional[int] = None
    stratify_by: Optional[str] = None
    weights: Optional[List[float]] = None
    cluster_count: int = 10


@dataclass
class SampleResult:
    sample: List[Any]
    original_size: int
    sample_size: int
    strategy: SamplingStrategy
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSampler:
    def __init__(self, config: Optional[SampleConfig] = None):
        self.config = config or SampleConfig()
        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)

    def sample(self, data: List[Any]) -> SampleResult:
        if not data:
            return SampleResult(
                sample=[],
                original_size=0,
                sample_size=0,
                strategy=self.config.strategy,
            )

        if len(data) <= self.config.sample_size and not self.config.replace:
            return SampleResult(
                sample=data.copy(),
                original_size=len(data),
                sample_size=len(data),
                strategy=self.config.strategy,
            )

        if self.config.strategy == SamplingStrategy.RANDOM:
            return self._random_sample(data)
        elif self.config.strategy == SamplingStrategy.STRATIFIED:
            return self._stratified_sample(data)
        elif self.config.strategy == SamplingStrategy.SYSTEMATIC:
            return self._systematic_sample(data)
        elif self.config.strategy == SamplingStrategy.RESERVOIR:
            return self._reservoir_sample(data)
        elif self.config.strategy == SamplingStrategy.CLUSTER:
            return self._cluster_sample(data)
        elif self.config.strategy == SamplingStrategy.WEIGHTED:
            return self._weighted_sample(data)
        elif self.config.strategy == SamplingStrategy.BOOTSTRAP:
            return self._bootstrap_sample(data)

        return SampleResult(
            sample=data[:self.config.sample_size],
            original_size=len(data),
            sample_size=self.config.sample_size,
            strategy=self.config.strategy,
        )

    def _random_sample(self, data: List[Any]) -> SampleResult:
        if self.config.replace:
            indices = [random.randint(0, len(data) - 1) for _ in range(self.config.sample_size)]
            sample = [data[i] for i in indices]
        else:
            sample = random.sample(data, min(self.config.sample_size, len(data)))

        return SampleResult(
            sample=sample,
            original_size=len(data),
            sample_size=len(sample),
            strategy=SamplingStrategy.RANDOM,
        )

    def _stratified_sample(self, data: List[Any]) -> SampleResult:
        if not self.config.stratify_by:
            return self._random_sample(data)

        strata = defaultdict(list)
        for item in data:
            if isinstance(item, dict):
                key = item.get(self.config.stratify_by, "unknown")
            else:
                key = str(item)
            strata[key].append(item)

        sample = []
        total = len(data)
        for key, items in strata.items():
            proportion = len(items) / total
            n_samples = max(1, int(self.config.sample_size * proportion))
            n_samples = min(n_samples, len(items))
            sampled = random.sample(items, n_samples)
            sample.extend(sampled)

        return SampleResult(
            sample=sample[:self.config.sample_size],
            original_size=len(data),
            sample_size=len(sample),
            strategy=SamplingStrategy.STRATIFIED,
            metadata={"strata": {k: len(v) for k, v in strata.items()}},
        )

    def _systematic_sample(self, data: List[Any]) -> SampleResult:
        n = len(data)
        k = max(1, n // self.config.sample_size)
        start = random.randint(0, k - 1)
        indices = list(range(start, n, k))
        sample = [data[i] for i in indices[:self.config.sample_size]]

        return SampleResult(
            sample=sample,
            original_size=n,
            sample_size=len(sample),
            strategy=SamplingStrategy.SYSTEMATIC,
            metadata={"interval": k, "start": start},
        )

    def _reservoir_sample(self, data: List[Any]) -> SampleResult:
        reservoir = []
        n = len(data)
        k = min(self.config.sample_size, n)

        for i in range(k):
            reservoir.append(data[i])

        for i in range(k, n):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = data[i]

        return SampleResult(
            sample=reservoir,
            original_size=n,
            sample_size=k,
            strategy=SamplingStrategy.RESERVOIR,
        )

    def _cluster_sample(self, data: List[Any]) -> SampleResult:
        n = len(data)
        cluster_count = min(self.config.cluster_count, n)
        cluster_size = n // cluster_count

        clusters = []
        for i in range(cluster_count):
            start = i * cluster_size
            end = start + cluster_size if i < cluster_count - 1 else n
            clusters.append(data[start:end])

        n_clusters_to_sample = max(1, int(cluster_count * (self.config.sample_size / n)))
        sampled_clusters = random.sample(clusters, n_clusters_to_sample)
        sample = [item for cluster in sampled_clusters for item in cluster]

        return SampleResult(
            sample=sample,
            original_size=n,
            sample_size=len(sample),
            strategy=SamplingStrategy.CLUSTER,
            metadata={"cluster_count": cluster_count, "sampled_clusters": n_clusters_to_sample},
        )

    def _weighted_sample(self, data: List[Any]) -> SampleResult:
        if not self.config.weights:
            return self._random_sample(data)

        weights = self.config.weights[:len(data)]
        while len(weights) < len(data):
            weights.append(1.0)

        total_weight = sum(weights)
        normalized_weights = [w / total_weight for w in weights]

        indices = range(len(data))
        sample = random.choices(list(indices), weights=normalized_weights, k=self.config.sample_size)
        sample = [data[i] for i in sample]

        return SampleResult(
            sample=sample,
            original_size=len(data),
            sample_size=len(sample),
            strategy=SamplingStrategy.WEIGHTED,
        )

    def _bootstrap_sample(self, data: List[Any]) -> SampleResult:
        n = len(data)
        indices = [random.randint(0, n - 1) for _ in range(n)]
        sample = [data[i] for i in indices]

        return SampleResult(
            sample=sample,
            original_size=n,
            sample_size=n,
            strategy=SamplingStrategy.BOOTSTRAP,
        )


def random_sample(
    data: List[Any],
    size: int,
    replace: bool = False,
    seed: Optional[int] = None,
) -> List[Any]:
    if seed is not None:
        random.seed(seed)

    if not replace:
        return random.sample(data, min(size, len(data)))

    return [random.choice(data) for _ in range(size)]


def stratified_sample(
    data: List[Dict[str, Any]],
    size: int,
    stratify_by: str,
) -> List[Dict[str, Any]]:
    config = SampleConfig(
        strategy=SamplingStrategy.STRATIFIED,
        sample_size=size,
        stratify_by=stratify_by,
    )
    sampler = DataSampler(config)
    return sampler.sample(data).sample


def reservoir_sample(data: List[Any], k: int, seed: Optional[int] = None) -> List[Any]:
    if seed is not None:
        random.seed(seed)

    config = SampleConfig(strategy=SamplingStrategy.RESERVOIR, sample_size=k)
    sampler = DataSampler(config)
    return sampler.sample(data).sample
