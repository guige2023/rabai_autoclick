"""
Data Sampling Action Module.

Provides statistical and algorithmic data sampling strategies including
random, stratified, systematic, cluster, and reservoir sampling.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar

import numpy as np
import pandas as pd

T = TypeVar("T")


class SamplingStrategy(Enum):
    """Supported sampling strategies."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    RESERVOIR = "reservoir"
    WEIGHTED = "weighted"
    BOOTSTRAP = "bootstrap"
    SEQUENCE = "sequence"


@dataclass
class SampleConfig:
    """Configuration for sampling operations."""
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    sample_size: Optional[int] = None
    sample_fraction: Optional[float] = None
    seed: Optional[int] = None
    replace: bool = False
    stratify_column: Optional[str] = None
    cluster_column: Optional[str] = None
    weight_column: Optional[str] = None
    error_margin: float = 0.05
    confidence_level: float = 0.95


@dataclass
class SampleResult:
    """Result of a sampling operation."""
    sample: pd.DataFrame
    sample_size: int
    original_size: int
    strategy: SamplingStrategy
    selection_probability: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def sampling_ratio(self) -> float:
        return self.sample_size / self.original_size if self.original_size > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sample_size": self.sample_size,
            "original_size": self.original_size,
            "sampling_ratio": self.sampling_ratio,
            "strategy": self.strategy.value,
            "selection_probability": self.selection_probability,
            "metadata": self.metadata,
        }


class RandomSampler:
    """Simple random sampling."""

    def __init__(self, config: SampleConfig):
        self.config = config
        if config.seed is not None:
            random.seed(config.seed)
            np.random.seed(config.seed)

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        n = len(df)
        if self.config.sample_size:
            k = min(self.config.sample_size, n)
        elif self.config.sample_fraction:
            k = max(1, int(n * self.config.sample_fraction))
        else:
            k = n

        indices = np.random.choice(n, size=k, replace=self.config.replace)
        return df.iloc[indices].copy()


class StratifiedSampler:
    """Stratified sampling maintaining proportions of categorical variable."""

    def __init__(self, config: SampleConfig):
        self.config = config
        if config.seed is not None:
            random.seed(config.seed)
            np.random.seed(config.seed)

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.stratify_column:
            raise ValueError("stratify_column required for stratified sampling")

        column = self.config.stratify_column
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")

        target_size = self._calculate_target_size(len(df))
        samples = []

        for stratum_value, group in df.groupby(column):
            stratum_size = len(group)
            stratum_fraction = target_size / len(df) if len(df) > 0 else 1
            stratum_sample_size = max(1, int(stratum_size * stratum_fraction))

            if self.config.sample_size:
                stratum_sample_size = max(1, int(self.config.sample_size * stratum_size / len(df)))

            k = min(stratum_sample_size, stratum_size)
            sampled = group.sample(n=k, random_state=self.config.seed, replace=self.config.replace)
            samples.append(sampled)

        result = pd.concat(samples, ignore_index=True)
        if self.config.seed:
            return result.sample(frac=1, random_state=self.config.seed).reset_index(drop=True)
        return result

    def _calculate_target_size(self, n: int) -> int:
        if self.config.sample_size:
            return min(self.config.sample_size, n)
        if self.config.sample_fraction:
            return max(1, int(n * self.config.sample_fraction))
        return n


class SystematicSampler:
    """Systematic sampling with fixed interval selection."""

    def __init__(self, config: SampleConfig):
        self.config = config

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        n = len(df)
        if self.config.sample_size:
            k = min(self.config.sample_size, n)
        elif self.config.sample_fraction:
            k = max(1, int(n * self.config.sample_fraction))
        else:
            k = n

        interval = max(1, n // k)
        start = random.randint(0, interval - 1) if self.config.seed else 0
        indices = list(range(start, n, interval))[:k]
        return df.iloc[indices].copy()


class ClusterSampler:
    """Cluster sampling - randomly selects clusters, includes all their members."""

    def __init__(self, config: SampleConfig):
        self.config = config

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.cluster_column:
            raise ValueError("cluster_column required for cluster sampling")

        column = self.config.cluster_column
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")

        unique_clusters = df[column].unique()
        n_clusters = len(unique_clusters)

        if self.config.sample_size:
            n_select = min(self.config.sample_size, n_clusters)
        elif self.config.sample_fraction:
            n_select = max(1, int(n_clusters * self.config.sample_fraction))
        else:
            n_select = n_clusters

        if self.config.seed:
            selected_clusters = random.sample(list(unique_clusters), k=n_select)
        else:
            selected_clusters = np.random.choice(unique_clusters, size=n_select, replace=False)

        return df[df[column].isin(selected_clusters)].copy()


class ReservoirSampler:
    """
    Reservoir sampling for streaming/unbounded data.

    Maintains a statistically representative sample of size k from a
    potentially infinite stream using Algorithm R.
    """

    def __init__(self, config: SampleConfig):
        if not config.sample_size or config.sample_size < 1:
            raise ValueError("sample_size must be positive for reservoir sampling")
        self.k = config.sample_size
        self.config = config
        self._reservoir: List[Any] = []
        self._count = 0

    def add(self, item: Any) -> None:
        """Add an item to the reservoir."""
        self._count += 1
        if len(self._reservoir) < self.k:
            self._reservoir.append(item)
        else:
            j = random.randint(0, self._count - 1)
            if j < self.k:
                self._reservoir[j] = item

    def get_sample(self) -> List[Any]:
        """Get current reservoir contents."""
        return list(self._reservoir)

    def size(self) -> int:
        return len(self._reservoir)

    def total_seen(self) -> int:
        return self._count


class WeightedSampler:
    """Weighted random sampling based on item weights."""

    def __init__(self, config: SampleConfig):
        self.config = config
        if config.seed is not None:
            random.seed(config.seed)

    def sample(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.config.weight_column:
            raise ValueError("weight_column required for weighted sampling")

        column = self.config.weight_column
        if column not in df.columns:
            raise ValueError(f"Column {column} not found in dataframe")

        n = len(df)
        if self.config.sample_size:
            k = min(self.config.sample_size, n)
        elif self.config.sample_fraction:
            k = max(1, int(n * self.config.sample_fraction))
        else:
            k = n

        weights = df[column].values.astype(float)
        weights /= weights.sum()

        indices = np.random.choice(n, size=k, replace=self.config.replace, p=weights)
        return df.iloc[indices].copy()


class DataSampler:
    """
    Unified data sampling interface supporting multiple strategies.

    Provides simple API for sampling from datasets using various
    statistical and algorithmic approaches.

    Example:
        >>> sampler = DataSampler(strategy=SamplingStrategy.STRATIFIED)
        >>> result = sampler.sample(df, sample_size=1000, stratify_column="category")
        >>> print(result.sample.head())
    """

    def __init__(self, config: Optional[SampleConfig] = None):
        self.config = config or SampleConfig()
        self._samplers = {
            SamplingStrategy.RANDOM: RandomSampler(self.config),
            SamplingStrategy.STRATIFIED: StratifiedSampler(self.config),
            SamplingStrategy.SYSTEMATIC: SystematicSampler(self.config),
            SamplingStrategy.CLUSTER: ClusterSampler(self.config),
            SamplingStrategy.WEIGHTED: WeightedSampler(self.config),
        }

    def sample(self, df: pd.DataFrame) -> SampleResult:
        """Sample data using configured strategy."""
        if self.config.strategy == SamplingStrategy.RESERVOIR:
            raise ValueError("Use sample_stream() for reservoir sampling")

        sampler = self._samplers.get(self.config.strategy)
        if not sampler:
            raise ValueError(f"Unsupported strategy: {self.config.strategy}")

        original_size = len(df)
        sampled_df = sampler.sample(df)

        return SampleResult(
            sample=sampled_df,
            sample_size=len(sampled_df),
            original_size=original_size,
            strategy=self.config.strategy,
            selection_probability=len(sampled_df) / original_size if original_size > 0 else 0.0,
            metadata={"seed": self.config.seed},
        )

    def sample_stream(self, items: Iterator[T]) -> List[T]:
        """Sample from a stream using reservoir sampling."""
        if self.config.strategy != SamplingStrategy.RESERVOIR:
            raise ValueError("sample_stream requires RESERVOIR strategy")

        sampler = ReservoirSampler(self.config)
        for item in items:
            sampler.add(item)
        return sampler.get_sample()

    def calculate_sample_size(
        self,
        population_size: int,
        error_margin: float = 0.05,
        confidence_level: float = 0.95,
    ) -> int:
        """Calculate required sample size for desired statistical precision."""
        z_scores = {
            0.90: 1.645,
            0.95: 1.96,
            0.99: 2.576,
        }
        z = z_scores.get(confidence_level, 1.96)
        p = 0.5  # Maximum variance case

        numerator = (z ** 2 * p * (1 - p)) / (error_margin ** 2)
        denominator = 1 + ((z ** 2 * p * (1 - p)) / (error_margin ** 2 * population_size))

        n = numerator / denominator
        return max(1, int(math.ceil(n)))


def create_sampler(
    strategy: str = "random",
    sample_size: Optional[int] = None,
    seed: Optional[int] = None,
) -> DataSampler:
    """Factory to create a configured data sampler."""
    config = SampleConfig(
        strategy=SamplingStrategy(strategy),
        sample_size=sample_size,
        seed=seed,
    )
    return DataSampler(config=config)
