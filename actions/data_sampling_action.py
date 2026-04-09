"""
Data Sampling Module.

Provides various sampling strategies for datasets including
random, stratified, systematic, cluster, and reservoir sampling
for efficient data processing and analysis.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    TypeVar, Generic, Iterator, Sequence
)
from dataclasses import dataclass, field
from enum import Enum, auto
import random
import math
from collections import defaultdict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class SamplingStrategy(Enum):
    """Available sampling strategies."""
    RANDOM = auto()
    STRATIFIED = auto()
    SYSTEMATIC = auto()
    CLUSTER = auto()
    RESERVOIR = auto()
    WEIGHTED = auto()
    BOOSTRAP = auto()
    SMOTE = auto()


@dataclass
class SampleResult:
    """Result of sampling operation."""
    data: List[Any]
    strategy: SamplingStrategy
    sample_size: int
    original_size: int
    sampling_rate: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSampler:
    """
    Comprehensive data sampling utilities.
    
    Supports multiple sampling strategies for different
    use cases including ML training, analysis, and testing.
    """
    
    def __init__(
        self,
        seed: Optional[int] = None,
        random_state: Optional[random.Random] = None
    ) -> None:
        if seed is not None:
            self._random = random.Random(seed)
        elif random_state is not None:
            self._random = random_state
        else:
            self._random = random.Random()
    
    def sample(
        self,
        data: List[T],
        size: int,
        strategy: SamplingStrategy = SamplingStrategy.RANDOM,
        **kwargs
    ) -> List[T]:
        """
        Sample from data using specified strategy.
        
        Args:
            data: Input data list
            size: Number of samples to draw
            strategy: Sampling strategy to use
            **kwargs: Strategy-specific parameters
            
        Returns:
            Sampled data list
        """
        if not data:
            return []
        
        original_size = len(data)
        size = min(size, original_size)
        
        if strategy == SamplingStrategy.RANDOM:
            return self._random_sample(data, size)
        elif strategy == SamplingStrategy.STRATIFIED:
            return self._stratified_sample(
                data, size, kwargs.get("stratify_by")
            )
        elif strategy == SamplingStrategy.SYSTEMATIC:
            return self._systematic_sample(
                data, size, kwargs.get("interval")
            )
        elif strategy == SamplingStrategy.CLUSTER:
            return self._cluster_sample(
                data, size, kwargs.get("cluster_key")
            )
        elif strategy == SamplingStrategy.RESERVOIR:
            return self._reservoir_sample(
                data, size, kwargs.get("weight_func")
            )
        elif strategy == SamplingStrategy.WEIGHTED:
            return self._weighted_sample(
                data, size, kwargs.get("weights")
            )
        elif strategy == SamplingStrategy.BOOTSTRAP:
            return self._bootstrap_sample(data, size)
        
        return self._random_sample(data, size)
    
    def _random_sample(self, data: List[T], size: int) -> List[T]:
        """Simple random sampling without replacement."""
        return self._random.sample(data, size)
    
    def _stratified_sample(
        self,
        data: List[Dict[str, Any]],
        size: int,
        stratify_key: str
    ) -> List[Dict[str, Any]]:
        """Stratified sampling maintaining proportion of groups."""
        if not data or not stratify_key:
            return self._random_sample(data, size)
        
        # Group by stratify key
        groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            if isinstance(item, dict) and stratify_key in item:
                groups[item[stratify_key]].append(item)
        
        # Sample proportionally from each group
        total = len(data)
        sampled = []
        
        for group_key, group_data in groups.items():
            group_proportion = len(group_data) / total
            group_size = max(1, round(size * group_proportion))
            group_size = min(group_size, len(group_data))
            
            group_sample = self._random.sample(group_data, group_size)
            sampled.extend(group_sample)
        
        # Adjust if oversampled
        if len(sampled) > size:
            self._random.shuffle(sampled)
            sampled = sampled[:size]
        
        return sampled
    
    def _systematic_sample(
        self,
        data: List[T],
        size: int,
        interval: Optional[int] = None
    ) -> List[T]:
        """Systematic sampling with fixed interval."""
        if not data:
            return []
        
        interval = interval or max(1, len(data) // size)
        
        start = self._random.randint(0, min(interval - 1, len(data) - 1))
        
        sampled = []
        idx = start
        while idx < len(data) and len(sampled) < size:
            sampled.append(data[idx])
            idx += interval
        
        return sampled
    
    def _cluster_sample(
        self,
        data: List[Dict[str, Any]],
        size: int,
        cluster_key: str
    ) -> List[Dict[str, Any]]:
        """Cluster sampling - select entire clusters."""
        if not data or not cluster_key:
            return self._random_sample(data, size)
        
        # Identify clusters
        clusters: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            if isinstance(item, dict) and cluster_key in item:
                clusters[item[cluster_key]].append(item)
        
        # Sample clusters
        num_clusters = max(1, min(size, len(clusters)))
        selected_clusters = self._random.sample(
            list(clusters.keys()), num_clusters
        )
        
        # Return all items from selected clusters
        sampled = []
        for cluster_key_val in selected_clusters:
            sampled.extend(clusters[cluster_key_val])
        
        return sampled
    
    def _reservoir_sample(
        self,
        data: List[T],
        size: int,
        weight_func: Optional[Callable[[T], float]] = None
    ) -> List[T]:
        """Reservoir sampling - useful for streaming data."""
        if not data or size >= len(data):
            return data[:size] if size < len(data) else data
        
        if weight_func:
            # Weighted reservoir sampling
            return self._weighted_reservoir_sample(data, size, weight_func)
        
        # Standard reservoir sampling (Algorithm R)
        reservoir = data[:size]
        
        for i in range(size, len(data)):
            j = self._random.randint(0, i)
            if j < size:
                reservoir[j] = data[i]
        
        return reservoir
    
    def _weighted_sample(
        self,
        data: List[T],
        size: int,
        weights: Optional[List[float]] = None
    ) -> List[T]:
        """Weighted random sampling."""
        if not data:
            return []
        
        if weights is None:
            weights = [1.0] * len(data)
        
        if len(weights) != len(data):
            raise ValueError("Weights must match data length")
        
        total_weight = sum(weights)
        if total_weight <= 0:
            return self._random_sample(data, size)
        
        # Normalize weights
        normalized = [w / total_weight for w in weights]
        
        # Cumulative distribution
        cumsum = []
        running = 0
        for w in normalized:
            running += w
            cumsum.append(running)
        
        sampled = []
        for _ in range(size):
            r = self._random.random()
            for i, threshold in enumerate(cumsum):
                if r <= threshold:
                    sampled.append(data[i])
                    break
        
        return sampled
    
    def _weighted_reservoir_sample(
        self,
        data: List[T],
        size: int,
        weight_func: Callable[[T], float]
    ) -> List[T]:
        """Weighted reservoir sampling (Algorithm A-Chao)."""
        if not data or size == 0:
            return []
        
        weights = [weight_func(item) for item in data]
        
        # Initialize reservoir
        reservoir = data[:size]
        res_weights = weights[:size]
        sum_weights = sum(res_weights)
        
        for i in range(size, len(data)):
            sum_weights += weights[i]
            j = self._random.randint(0, i)
            
            if j < size:
                # Replace with new item based on weight ratio
                if weights[i] > res_weights[j]:
                    reservoir[j] = data[i]
                    res_weights[j] = weights[i]
        
        return reservoir
    
    def _bootstrap_sample(
        self,
        data: List[T],
        size: int
    ) -> List[T]:
        """Bootstrap sampling with replacement."""
        return [self._random.choice(data) for _ in range(size)]


class SampleSizeCalculator:
    """Calculate required sample sizes for statistical validity."""
    
    @staticmethod
    def for_population(
        confidence_level: float = 0.95,
        margin_of_error: float = 0.05,
        proportion: float = 0.5
    ) -> int:
        """
        Calculate sample size for population proportion.
        
        Args:
            confidence_level: Z-score for confidence (0.95 = 1.96)
            margin_of_error: Desired margin of error
            proportion: Expected proportion
            
        Returns:
            Required sample size
        """
        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence_level, 1.96)
        
        p = proportion
        q = 1 - p
        
        numerator = z ** 2 * p * q
        denominator = margin_of_error ** 2
        
        return math.ceil(numerator / denominator)
    
    @staticmethod
    def for_mean(
        std_dev: float,
        confidence_level: float = 0.95,
        margin_of_error: float = 0.05
    ) -> int:
        """Calculate sample size for population mean."""
        z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
        z = z_scores.get(confidence_level, 1.96)
        
        return math.ceil((z * std_dev / margin_of_error) ** 2)


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    data = [
        {"id": i, "category": ["A", "B", "C"][i % 3], "value": i * 10}
        for i in range(100)
    ]
    
    sampler = DataSampler(seed=42)
    
    print("=== Sampling Demo ===")
    
    # Random sample
    random_sample = sampler.sample(data, 10, SamplingStrategy.RANDOM)
    print(f"Random sample: {len(random_sample)} items")
    
    # Stratified sample
    stratified_sample = sampler.sample(
        data, 10, SamplingStrategy.STRATIFIED, stratify_by="category"
    )
    print(f"Stratified sample: {len(stratified_sample)} items")
    
    # Systematic sample
    systematic_sample = sampler.sample(data, 10, SamplingStrategy.SYSTEMATIC)
    print(f"Systematic sample: {len(systematic_sample)} items")
    
    # Weighted sample
    weighted_data = [1, 2, 3, 4, 5]
    weights = [0.1, 0.2, 0.4, 0.2, 0.1]
    weighted_sample = sampler.sample(
        weighted_data, 3, SamplingStrategy.WEIGHTED, weights=weights
    )
    print(f"Weighted sample: {weighted_sample}")
    
    # Bootstrap sample
    bootstrap_sample = sampler.sample(
        data, 20, SamplingStrategy.BOOTSTRAP
    )
    print(f"Bootstrap sample: {len(bootstrap_sample)} items (with replacement)")
    
    print("\n=== Sample Size Calculator ===")
    required = SampleSizeCalculator.for_population(
        confidence_level=0.95,
        margin_of_error=0.05
    )
    print(f"For 95% confidence, ±5% margin: {required} samples needed")
