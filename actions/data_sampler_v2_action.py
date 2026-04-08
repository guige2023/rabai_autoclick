"""Data Sampler v2 Action.

Advanced sampling strategies including stratified, cluster, and reservoir.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass
import random
import hashlib


T = TypeVar("T")


class DataSamplerV2Action:
    """Advanced data sampling strategies."""

    def __init__(self, seed: Optional[int] = None) -> None:
        if seed is not None:
            random.seed(seed)
        self.stats = {"total_sampled": 0, "total_population": 0}

    def random_sample(
        self,
        population: List[T],
        sample_size: int,
        replace: bool = False,
    ) -> List[T]:
        if replace:
            return [random.choice(population) for _ in range(sample_size)]
        return random.sample(population, min(sample_size, len(population)))

    def stratified_sample(
        self,
        population: List[T],
        stratify_fn: Callable[[T], str],
        sample_size: int,
    ) -> List[T]:
        buckets: Dict[str, List[T]] = {}
        for item in population:
            key = stratify_fn(item)
            buckets.setdefault(key, []).append(item)
        total = len(population)
        result = []
        for key, items in buckets.items():
            proportion = len(items) / total
            n = max(1, int(proportion * sample_size))
            result.extend(self.random_sample(items, n))
        return result[:sample_size]

    def reservoir_sample(
        self,
        stream: List[T],
        k: int,
    ) -> List[T]:
        if len(stream) <= k:
            return list(stream)
        reservoir = list(stream[:k])
        for i in range(k, len(stream)):
            j = random.randint(0, i)
            if j < k:
                reservoir[j] = stream[i]
        return reservoir

    def cluster_sample(
        self,
        population: List[T],
        cluster_fn: Callable[[T], str],
        num_clusters: int,
    ) -> List[T]:
        clusters: Dict[str, List[T]] = {}
        for item in population:
            key = cluster_fn(item)
            clusters.setdefault(key, []).append(item)
        cluster_keys = list(clusters.keys())
        selected_keys = random.sample(cluster_keys, min(num_clusters, len(cluster_keys)))
        result = []
        for key in selected_keys:
            result.extend(clusters[key])
        return result

    def get_stats(self) -> Dict[str, int]:
        return dict(self.stats)
