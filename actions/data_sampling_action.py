"""
Data Sampling Action Module.

Provides data sampling strategies including
random, stratified, cluster, and systematic sampling.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
import random

logger = logging.getLogger(__name__)


class SamplingStrategy(Enum):
    """Sampling strategies."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    CLUSTER = "cluster"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"


class SamplingConfig:
    """Sampling configuration."""
    def __init__(
        self,
        strategy: SamplingStrategy,
        sample_size: int,
        replace: bool = False,
        random_state: Optional[int] = None
    ):
        self.strategy = strategy
        self.sample_size = sample_size
        self.replace = replace
        self.random_state = random_state


class RandomSampler:
    """Random sampling."""

    def __init__(self, config: SamplingConfig):
        self.config = config
        if config.random_state is not None:
            random.seed(config.random_state)

    def sample(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform random sampling."""
        if self.config.replace:
            return random.choices(data, k=self.config.sample_size)
        else:
            return random.sample(
                data,
                min(self.config.sample_size, len(data))
            )


class StratifiedSampler:
    """Stratified sampling."""

    def __init__(self, config: SamplingConfig, stratify_by: str):
        self.config = config
        self.stratify_by = stratify_by
        if config.random_state is not None:
            random.seed(config.random_state)

    def sample(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform stratified sampling."""
        strata: Dict[Any, List[Dict[str, Any]]] = {}

        for record in data:
            key = record.get(self.stratify_by)
            if key not in strata:
                strata[key] = []
            strata[key].append(record)

        total_size = self.config.sample_size
        total_records = len(data)

        sampled = []
        for key, stratum in strata.items():
            proportion = len(stratum) / total_records
            stratum_size = max(1, int(total_size * proportion))

            if self.config.replace:
                sampled.extend(random.choices(stratum, k=stratum_size))
            else:
                sampled.extend(random.sample(stratum, min(stratum_size, len(stratum))))

        return sampled


class ClusterSampler:
    """Cluster sampling."""

    def __init__(self, config: SamplingConfig, cluster_by: str):
        self.config = config
        self.cluster_by = cluster_by
        if config.random_state is not None:
            random.seed(config.random_state)

    def sample(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform cluster sampling."""
        clusters: Dict[Any, List[Dict[str, Any]]] = {}

        for record in data:
            key = record.get(self.cluster_by)
            if key not in clusters:
                clusters[key] = []
            clusters[key].append(record)

        cluster_keys = list(clusters.keys())
        num_clusters_to_sample = min(
            self.config.sample_size,
            len(cluster_keys)
        )

        if not self.config.replace:
            selected_clusters = random.sample(
                cluster_keys,
                num_clusters_to_sample
            )
        else:
            selected_clusters = random.choices(
                cluster_keys,
                k=num_clusters_to_sample
            )

        sampled = []
        for cluster_key in selected_clusters:
            sampled.extend(clusters[cluster_key])

        return sampled


class SystematicSampler:
    """Systematic sampling."""

    def __init__(self, config: SamplingConfig, interval: Optional[int] = None):
        self.config = config
        self.interval = interval

    def sample(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform systematic sampling."""
        n = len(data)
        k = self.interval or max(1, n // self.config.sample_size)

        sampled = []
        for i in range(0, n, k):
            if len(sampled) >= self.config.sample_size:
                break
            sampled.append(data[i])

        return sampled


class ReservoirSampler:
    """Reservoir sampling for streaming data."""

    def __init__(self, config: SamplingConfig):
        self.config = config
        self.reservoir: List[Dict[str, Any]] = []
        self.count = 0
        if config.random_state is not None:
            random.seed(config.random_state)

    def add(self, item: Dict[str, Any]):
        """Add item to reservoir."""
        self.count += 1

        if len(self.reservoir) < self.config.sample_size:
            self.reservoir.append(item)
        else:
            j = random.randint(0, self.count)
            if j < self.config.sample_size:
                self.reservoir[j] = item

    def get_sample(self) -> List[Dict[str, Any]]:
        """Get current sample."""
        return self.reservoir.copy()


class DataSampler:
    """Main sampling orchestrator."""

    def __init__(self):
        self.samplers: Dict[SamplingStrategy, Any] = {}

    def sample(
        self,
        data: List[Dict[str, Any]],
        strategy: SamplingStrategy,
        sample_size: int,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """Sample data using specified strategy."""
        config = SamplingConfig(
            strategy=strategy,
            sample_size=sample_size,
            random_state=kwargs.get("random_state")
        )

        if strategy == SamplingStrategy.RANDOM:
            sampler = RandomSampler(config)
        elif strategy == SamplingStrategy.STRATIFIED:
            sampler = StratifiedSampler(config, kwargs.get("stratify_by", ""))
        elif strategy == SamplingStrategy.CLUSTER:
            sampler = ClusterSampler(config, kwargs.get("cluster_by", ""))
        elif strategy == SamplingStrategy.SYSTEMATIC:
            sampler = SystematicSampler(config, kwargs.get("interval"))
        elif strategy == SamplingStrategy.RESERVOIR:
            reservoir = ReservoirSampler(config)
            for item in data:
                reservoir.add(item)
            return reservoir.get_sample()
        else:
            sampler = RandomSampler(config)

        return sampler.sample(data)


from enum import Enum


def main():
    """Demonstrate data sampling."""
    data = [
        {"id": i, "category": random.choice(["A", "B", "C"]), "value": i * 10}
        for i in range(100)
    ]

    sampler = DataSampler()

    random_sample = sampler.sample(data, SamplingStrategy.RANDOM, 10)
    print(f"Random sample: {len(random_sample)} records")

    stratified_sample = sampler.sample(
        data,
        SamplingStrategy.STRATIFIED,
        10,
        stratify_by="category"
    )
    print(f"Stratified sample: {len(stratified_sample)} records")


if __name__ == "__main__":
    main()
