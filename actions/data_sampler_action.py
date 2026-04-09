"""
Data Sampler Module.

Provides statistical and reservoir sampling for large datasets,
with support for stratification and weighted sampling.
"""

from __future__ import annotations

import random
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class SamplingStrategy(Enum):
    """Sampling strategy types."""
    RANDOM = "random"
    RESERVOIR = "reservoir"
    STRATIFIED = "stratified"
    WEIGHTED = "weighted"
    SYSTEMATIC = "systematic"
    CLUSTER = "cluster"
    BOOSTRAP = "bootstrap"


@dataclass
class SampleConfig:
    """Configuration for sampling."""
    strategy: SamplingStrategy = SamplingStrategy.RESERVOIR
    sample_size: int = 100
    seed: Optional[int] = None
    replace: bool = False  # With replacement
    stratified_field: Optional[str] = None
    weight_field: Optional[str] = None
    strata_sizes: Optional[Dict[str, int]] = None


@dataclass
class Sample:
    """Container for a sample."""
    data: List[Any]
    strategy: SamplingStrategy
    total_size: int
    sample_size: int
    sampling_rate: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_empty(self) -> bool:
        return len(self.data) == 0
        
    def __iter__(self) -> Iterator[Any]:
        return iter(self.data)
        
    def __len__(self) -> int:
        return len(self.data)


class DataSampler:
    """
    Data sampling with multiple strategies.
    
    Example:
        sampler = DataSampler(SampleConfig(
            strategy=SamplingStrategy.RESERVOIR,
            sample_size=1000
        ))
        
        # Reservoir sampling from large dataset
        sample = sampler.sample(large_data_iterator)
        
        # Stratified sampling
        sampler.strategy = SamplingStrategy.STRATIFIED
        sampler.stratified_field = "category"
        sample = sampler.sample(data, strata_sizes={"A": 100, "B": 200})
    """
    
    def __init__(self, config: Optional[SampleConfig] = None) -> None:
        """
        Initialize the sampler.
        
        Args:
            config: Sampling configuration.
        """
        self.config = config or SampleConfig()
        self._rng = random.Random(self.config.seed)
        
    def sample(self, data: List[T]) -> Sample:
        """
        Sample from a list of data.
        
        Args:
            data: List of items to sample from.
            
        Returns:
            Sample object containing sampled items.
        """
        if not data:
            return Sample(
                data=[],
                strategy=self.config.strategy,
                total_size=0,
                sample_size=self.config.sample_size,
                sampling_rate=0.0,
            )
            
        if self.config.strategy == SamplingStrategy.RANDOM:
            return self._sample_random(data)
        elif self.config.strategy == SamplingStrategy.RESERVOIR:
            return self._sample_reservoir(data)
        elif self.config.strategy == SamplingStrategy.STRATIFIED:
            return self._sample_stratified(data)
        elif self.config.strategy == SamplingStrategy.WEIGHTED:
            return self._sample_weighted(data)
        elif self.config.strategy == SamplingStrategy.SYSTEMATIC:
            return self._sample_systematic(data)
        elif self.config.strategy == SamplingStrategy.BOOTSTRAP:
            return self._sample_bootstrap(data)
        else:
            return self._sample_random(data)
            
    def sample_streaming(
        self,
        data_iterator: Iterator[T],
    ) -> Sample:
        """
        Sample from a streaming iterator (reservoir sampling).
        
        Args:
            data_iterator: Iterator of data items.
            
        Returns:
            Sample object.
        """
        if self.config.strategy == SamplingStrategy.RESERVOIR:
            return self._sample_reservoir_streaming(data_iterator)
        else:
            # For other strategies, collect first
            data = list(data_iterator)
            return self.sample(data)
            
    def _sample_random(self, data: List[T]) -> Sample:
        """Simple random sampling."""
        n = len(data)
        sample_size = min(self.config.sample_size, n)
        
        if self.config.replace:
            indices = [self._rng.randint(0, n - 1) for _ in range(sample_size)]
            sampled = [data[i] for i in indices]
        else:
            sampled = self._rng.sample(data, sample_size)
            
        return Sample(
            data=sampled,
            strategy=SamplingStrategy.RANDOM,
            total_size=n,
            sample_size=sample_size,
            sampling_rate=sample_size / n if n > 0 else 0,
        )
        
    def _sample_reservoir(self, data: List[T]) -> Sample:
        """
        Reservoir sampling (Algorithm R) for uniform sampling.
        
        Maintains a reservoir of size k, processing each element
        with probability k/n to be in the reservoir.
        """
        n = len(data)
        k = min(self.config.sample_size, n)
        
        if k == 0:
            return Sample(
                data=[],
                strategy=SamplingStrategy.RESERVOIR,
                total_size=n,
                sample_size=0,
                sampling_rate=0,
            )
            
        # First k elements go into reservoir
        reservoir = list(data[:k])
        
        # Process remaining elements
        for i in range(k, n):
            j = self._rng.randint(0, i)
            if j < k:
                reservoir[j] = data[i]
                
        return Sample(
            data=reservoir,
            strategy=SamplingStrategy.RESERVOIR,
            total_size=n,
            sample_size=k,
            sampling_rate=k / n if n > 0 else 0,
        )
        
    def _sample_reservoir_streaming(self, data_iterator: Iterator[T]) -> Sample:
        """
        Streaming reservoir sampling (Algorithm R).
        
        Args:
            data_iterator: Iterator of data items.
            
        Returns:
            Sample object.
        """
        k = self.config.sample_size
        reservoir: List[T] = []
        n = 0
        
        for item in data_iterator:
            n += 1
            if len(reservoir) < k:
                reservoir.append(item)
            else:
                j = self._rng.randint(0, n - 1)
                if j < k:
                    reservoir[j] = item
                    
        return Sample(
            data=reservoir,
            strategy=SamplingStrategy.RESERVOIR,
            total_size=n,
            sample_size=len(reservoir),
            sampling_rate=len(reservoir) / n if n > 0 else 0,
        )
        
    def _sample_stratified(self, data: List[T]) -> Sample:
        """
        Stratified sampling - sample proportionally from subgroups.
        
        Args:
            data: List of items (must have stratified_field).
            
        Returns:
            Sample object with stratified samples.
        """
        field = self.config.stratified_field
        if not field:
            return self._sample_random(data)
            
        # Group by stratum
        strata: Dict[str, List[T]] = defaultdict(list)
        for item in data:
            if isinstance(item, dict):
                stratum = item.get(field, "unknown")
            elif hasattr(item, field):
                stratum = getattr(item, field)
            else:
                stratum = "unknown"
            strata[stratum].append(item)
            
        total = len(data)
        samples: List[T] = []
        
        # Calculate sample sizes
        if self.config.strata_sizes:
            sizes = self.config.strata_sizes
        else:
            # Proportional allocation
            sizes = {
                stratum: max(1, int(self.config.sample_size * len(items) / total))
                for stratum, items in strata.items()
            }
            
        # Sample from each stratum
        for stratum, items in strata.items():
            stratum_size = sizes.get(stratum, 1)
            actual_size = min(stratum_size, len(items))
            
            if self.config.replace:
                for _ in range(actual_size):
                    samples.append(self._rng.choice(items))
            else:
                samples.extend(self._rng.sample(items, actual_size))
                
        # Adjust if over sample size
        if len(samples) > self.config.sample_size:
            self._rng.shuffle(samples)
            samples = samples[:self.config.sample_size]
            
        return Sample(
            data=samples,
            strategy=SamplingStrategy.STRATIFIED,
            total_size=total,
            sample_size=len(samples),
            sampling_rate=len(samples) / total if total > 0 else 0,
            metadata={"strata": list(strata.keys())},
        )
        
    def _sample_weighted(self, data: List[T]) -> Sample:
        """
        Weighted sampling based on item weights.
        
        Args:
            data: List of items (must have weight_field).
            
        Returns:
            Sample object with weighted samples.
        """
        field = self.config.weight_field
        if not field:
            return self._sample_random(data)
            
        weights: List[float] = []
        for item in data:
            if isinstance(item, dict):
                weight = float(item.get(field, 1.0))
            elif hasattr(item, field):
                weight = float(getattr(item, field))
            else:
                weight = 1.0
            weights.append(weight)
            
        total_weight = sum(weights)
        n = len(data)
        sample_size = min(self.config.sample_size, n)
        
        if total_weight == 0:
            return self._sample_random(data)
            
        samples: List[T] = []
        
        if self.config.replace:
            # Weighted random sampling with replacement
            for _ in range(sample_size):
                r = self._rng.uniform(0, total_weight)
                cumulative = 0
                for i, w in enumerate(weights):
                    cumulative += w
                    if cumulative >= r:
                        samples.append(data[i])
                        break
        else:
            # Weighted random sampling without replacement (Algorithm A-Chen)
            items = list(range(n))
            cum_weights = [0]
            
            for w in weights:
                cum_weights.append(cum_weights[-1] + w)
                
            for _ in range(sample_size):
                r = self._rng.uniform(0, cum_weights[-1])
                for i, cw in enumerate(cum_weights[1:], 1):
                    if r < cw:
                        samples.append(data[i - 1])
                        # Update cumulative weights
                        delta = weights[i - 1]
                        for j in range(i, len(cum_weights)):
                            cum_weights[j] -= delta
                        break
                        
        return Sample(
            data=samples,
            strategy=SamplingStrategy.WEIGHTED,
            total_size=n,
            sample_size=len(samples),
            sampling_rate=len(samples) / n if n > 0 else 0,
        )
        
    def _sample_systematic(self, data: List[T]) -> Sample:
        """
        Systematic sampling - select every k-th element.
        
        Args:
            data: List of items.
            
        Returns:
            Sample object.
        """
        n = len(data)
        k = max(1, n // self.config.sample_size)
        
        start = self._rng.randint(0, k - 1)
        indices = range(start, n, k)
        
        sampled = [data[i] for i in indices if i < n]
        
        return Sample(
            data=sampled,
            strategy=SamplingStrategy.SYSTEMATIC,
            total_size=n,
            sample_size=len(sampled),
            sampling_rate=len(sampled) / n if n > 0 else 0,
            metadata={"interval": k, "start": start},
        )
        
    def _sample_bootstrap(self, data: List[T]) -> Sample:
        """
        Bootstrap sampling - sampling with replacement.
        
        Args:
            data: List of items.
            
        Returns:
            Sample object.
        """
        n = len(data)
        sample_size = self.config.sample_size
        
        if n == 0:
            return Sample(
                data=[],
                strategy=SamplingStrategy.BOOTSTRAP,
                total_size=0,
                sample_size=0,
                sampling_rate=0,
            )
            
        # Sample with replacement
        indices = [self._rng.randint(0, n - 1) for _ in range(sample_size)]
        sampled = [data[i] for i in indices]
        
        return Sample(
            data=sampled,
            strategy=SamplingStrategy.BOOTSTRAP,
            total_size=n,
            sample_size=sample_size,
            sampling_rate=1.0,  # With replacement, always 100%
            metadata={"with_replacement": True},
        )


class StreamingReservoirSampler(Iterator):
    """
    Memory-efficient streaming reservoir sampler.
    
    Example:
        sampler = StreamingReservoirSampler(k=1000)
        
        for item in large_dataset:
            sampler.add(item)
            
        sample = sampler.get_sample()
    """
    
    def __init__(self, k: int, seed: Optional[int] = None) -> None:
        """
        Initialize streaming sampler.
        
        Args:
            k: Reservoir size.
            seed: Random seed.
        """
        self.k = k
        self.rng = random.Random(seed)
        self.reservoir: List[Any] = []
        self.n = 0  # Total items seen
        
    def add(self, item: T) -> None:
        """Add an item to the sample."""
        self.n += 1
        
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            j = self.rng.randint(0, self.n - 1)
            if j < self.k:
                self.reservoir[j] = item
                
    def get_sample(self) -> Sample:
        """Get the current sample."""
        return Sample(
            data=self.reservoir.copy(),
            strategy=SamplingStrategy.RESERVOIR,
            total_size=self.n,
            sample_size=len(self.reservoir),
            sampling_rate=len(self.reservoir) / self.n if self.n > 0 else 0,
        )
        
    def __iter__(self) -> Iterator:
        return iter(self.reservoir)
        
    def __len__(self) -> int:
        return len(self.reservoir)
