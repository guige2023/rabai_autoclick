"""Data sampling and statistical sampling action."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class SamplingConfig:
    """Configuration for sampling."""

    sample_size: int
    method: str = "random"  # random, stratified, systematic, cluster, reservoir
    seed: Optional[int] = None
    replace: bool = False
    weights: Optional[dict[str, float]] = None


@dataclass
class SamplingResult:
    """Result of a sampling operation."""

    original_size: int
    sample_size: int
    sample: list[Any]
    method: str
    seed: Optional[int] = None


class DataSamplerAction:
    """Provides statistical sampling methods for datasets."""

    def __init__(self, default_seed: Optional[int] = None):
        """Initialize sampler.

        Args:
            default_seed: Default random seed for reproducibility.
        """
        self._default_seed = default_seed
        if default_seed is not None:
            random.seed(default_seed)

    def sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Sample from data using configured method.

        Args:
            data: Input data.
            config: Sampling configuration.

        Returns:
            SamplingResult with sampled data.
        """
        if config.seed is not None:
            random.seed(config.seed)
        elif self._default_seed is not None:
            random.seed(self._default_seed)

        if config.method == "random":
            return self._random_sample(data, config)
        elif config.method == "stratified":
            return self._stratified_sample(data, config)
        elif config.method == "systematic":
            return self._systematic_sample(data, config)
        elif config.method == "cluster":
            return self._cluster_sample(data, config)
        elif config.method == "reservoir":
            return self._reservoir_sample(data, config)
        else:
            return self._random_sample(data, config)

    def _random_sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Random sampling with optional weights."""
        sample_size = min(config.sample_size, len(data))

        if config.weights and not config.replace:
            weights = [config.weights.get(str(i), 1.0) for i in range(len(data))]
            total_weight = sum(weights)
            normalized = [w / total_weight for w in weights]
            indices = random.choices(
                range(len(data)),
                weights=normalized,
                k=sample_size,
            )
            sample = [data[i] for i in indices]
        elif config.replace:
            sample = random.choices(list(data), k=sample_size)
        else:
            sample = random.sample(list(data), sample_size)

        return SamplingResult(
            original_size=len(data),
            sample_size=len(sample),
            sample=sample,
            method="random",
            seed=config.seed,
        )

    def _stratified_sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Stratified sampling by group."""
        if not data:
            return SamplingResult(0, 0, [], "stratified", config.seed)

        groups: dict[str, list] = {}
        for item in data:
            if isinstance(item, dict):
                key = str(item.get("stratum", item.get("category", "default")))
            else:
                key = "default"
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        total = len(data)
        sample = []
        for group_key, group_data in groups.items():
            group_proportion = len(group_data) / total
            group_size = max(1, int(config.sample_size * group_proportion))
            group_size = min(group_size, len(group_data))
            group_sample = random.sample(group_data, group_size)
            sample.extend(group_sample)

        if len(sample) > config.sample_size:
            sample = sample[: config.sample_size]

        return SamplingResult(
            original_size=len(data),
            sample_size=len(sample),
            sample=sample,
            method="stratified",
            seed=config.seed,
        )

    def _systematic_sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Systematic sampling (every kth element)."""
        if len(data) <= config.sample_size:
            return SamplingResult(
                original_size=len(data),
                sample_size=len(data),
                sample=list(data),
                method="systematic",
                seed=config.seed,
            )

        step = len(data) // config.sample_size
        start = random.randint(0, step - 1)
        indices = range(start, len(data), step)
        sample = [data[i] for i in indices][: config.sample_size]

        return SamplingResult(
            original_size=len(data),
            sample_size=len(sample),
            sample=sample,
            method="systematic",
            seed=config.seed,
        )

    def _cluster_sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Cluster sampling (random clusters)."""
        n_clusters = min(config.sample_size, len(data))
        cluster_size = max(1, len(data) // n_clusters)

        data_list = list(data)
        clusters = [
            data_list[i : i + cluster_size]
            for i in range(0, len(data_list), cluster_size)
        ]

        if config.seed is not None:
            random.seed(config.seed)

        selected_clusters = random.sample(clusters, min(n_clusters, len(clusters)))
        sample = [item for cluster in selected_clusters for item in cluster]

        return SamplingResult(
            original_size=len(data),
            sample_size=len(sample),
            sample=sample,
            method="cluster",
            seed=config.seed,
        )

    def _reservoir_sample(
        self,
        data: Sequence[Any],
        config: SamplingConfig,
    ) -> SamplingResult:
        """Reservoir sampling (stream-friendly)."""
        if len(data) <= config.sample_size:
            return SamplingResult(
                original_size=len(data),
                sample_size=len(data),
                sample=list(data),
                method="reservoir",
                seed=config.seed,
            )

        reservoir = list(data[: config.sample_size])

        for i in range(config.sample_size, len(data)):
            j = random.randint(0, i)
            if j < config.sample_size:
                reservoir[j] = data[i]

        return SamplingResult(
            original_size=len(data),
            sample_size=config.sample_size,
            sample=reservoir,
            method="reservoir",
            seed=config.seed,
        )

    def boot_mean(
        self,
        data: list[float],
        n_iterations: int = 1000,
        confidence: float = 0.95,
    ) -> dict[str, float]:
        """Compute bootstrap confidence interval for mean.

        Args:
            data: Numeric data.
            n_iterations: Number of bootstrap iterations.
            confidence: Confidence level.

        Returns:
            Dict with mean, lower, upper bounds.
        """
        if config.seed is not None:
            random.seed(config.seed)

        means = []
        for _ in range(n_iterations):
            sample = random.choices(data, k=len(data))
            means.append(sum(sample) / len(sample))

        means.sort()
        alpha = 1 - confidence
        lower_idx = int(n_iterations * alpha / 2)
        upper_idx = int(n_iterations * (1 - alpha / 2))

        return {
            "mean": sum(data) / len(data),
            "lower": means[lower_idx],
            "upper": means[upper_idx],
            "confidence": confidence,
        }
