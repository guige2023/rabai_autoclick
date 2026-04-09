"""Data Sampler Action Module.

Provides data sampling capabilities for datasets including
random, stratified, systematic, and reservoir sampling algorithms.

Example:
    >>> from actions.data.data_sampler_action import DataSamplerAction
    >>> action = DataSamplerAction()
    >>> sample = action.random_sample(data, size=100)
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class SamplingStrategy(Enum):
    """Sampling strategy types."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"


@dataclass
class SampleConfig:
    """Configuration for sampling.
    
    Attributes:
        strategy: Sampling strategy
        sample_size: Desired sample size
        seed: Random seed for reproducibility
        replace: Sampling with replacement
        weights: Optional weight function
    """
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    sample_size: int = 100
    seed: Optional[int] = None
    replace: bool = False
    weights: Optional[Callable[[Any], float]] = None


@dataclass
class SampleResult:
    """Result of sampling operation.
    
    Attributes:
        data: Sampled data
        strategy: Strategy used
        original_size: Original dataset size
        sample_size: Actual sample size
        metadata: Additional metadata
    """
    data: List[Any]
    strategy: SamplingStrategy
    original_size: int
    sample_size: int
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSamplerAction:
    """Data sampler for datasets.
    
    Provides various sampling algorithms to extract
    representative subsets from larger datasets.
    
    Attributes:
        config: Sampler configuration
        _rng: Random number generator
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[SampleConfig] = None,
    ) -> None:
        """Initialize sampler action.
        
        Args:
            config: Sampler configuration
        """
        self.config = config or SampleConfig()
        self._rng = random.Random(self.config.seed)
        self._lock = threading.Lock()
    
    def random_sample(
        self,
        data: List[Any],
        size: int,
        replace: bool = False,
    ) -> SampleResult:
        """Take random sample from data.
        
        Args:
            data: Input data
            size: Sample size
            replace: With replacement
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        if replace:
            sample = [self._rng.choice(data) for _ in range(size)]
        else:
            if size > len(data):
                size = len(data)
            sample = self._rng.sample(data, size)
        
        return SampleResult(
            data=sample,
            strategy=SamplingStrategy.RANDOM,
            original_size=len(data),
            sample_size=len(sample),
            duration=time.time() - start,
        )
    
    def stratified_sample(
        self,
        data: List[Any],
        size: int,
        stratify_key: Callable[[Any], Any],
    ) -> SampleResult:
        """Take stratified sample preserving proportions.
        
        Args:
            data: Input data
            size: Total sample size
            stratify_key: Function to extract stratification key
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        strata: Dict[Any, List[Any]] = {}
        for item in data:
            key = stratify_key(item)
            if key not in strata:
                strata[key] = []
            strata[key].append(item)
        
        total = len(data)
        sample: List[Any] = []
        
        for key, stratum in strata.items():
            proportion = len(stratum) / total
            stratum_size = max(1, int(size * proportion))
            stratum_size = min(stratum_size, len(stratum))
            
            stratum_sample = self._rng.sample(stratum, stratum_size)
            sample.extend(stratum_sample)
        
        if len(sample) > size:
            sample = self._rng.sample(sample, size)
        
        return SampleResult(
            data=sample,
            strategy=SamplingStrategy.STRATIFIED,
            original_size=len(data),
            sample_size=len(sample),
            duration=time.time() - start,
            metadata={"num_strata": len(strata)},
        )
    
    def systematic_sample(
        self,
        data: List[Any],
        size: int,
    ) -> SampleResult:
        """Take systematic sample at regular intervals.
        
        Args:
            data: Input data
            size: Sample size
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        if size > len(data):
            size = len(data)
        
        interval = len(data) / size
        
        sample = []
        for i in range(size):
            idx = int(i * interval)
            if idx < len(data):
                sample.append(data[idx])
        
        return SampleResult(
            data=sample,
            strategy=SamplingStrategy.SYSTEMATIC,
            original_size=len(data),
            sample_size=len(sample),
            duration=time.time() - start,
        )
    
    def reservoir_sample(
        self,
        data: List[Any],
        size: int,
    ) -> SampleResult:
        """Take reservoir sample (streaming-friendly).
        
        Args:
            data: Input data
            size: Sample size
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        if size >= len(data):
            return SampleResult(
                data=data.copy(),
                strategy=SamplingStrategy.RESERVOIR,
                original_size=len(data),
                sample_size=len(data),
                duration=time.time() - start,
            )
        
        reservoir = data[:size]
        
        for i in range(size, len(data)):
            j = self._rng.randint(0, i)
            if j < size:
                reservoir[j] = data[i]
        
        return SampleResult(
            data=reservoir,
            strategy=SamplingStrategy.RESERVOIR,
            original_size=len(data),
            sample_size=size,
            duration=time.time() - start,
        )
    
    def weighted_sample(
        self,
        data: List[Any],
        size: int,
        weight_func: Callable[[Any], float],
        replace: bool = False,
    ) -> SampleResult:
        """Take weighted sample.
        
        Args:
            data: Input data
            size: Sample size
            weight_func: Function to compute weight
            replace: With replacement
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        weights = [weight_func(item) for item in data]
        total_weight = sum(weights)
        
        if total_weight == 0:
            return self.random_sample(data, min(size, len(data)), replace)
        
        if replace:
            sample = self._rng.choices(data, weights=weights, k=size)
        else:
            if size > len(data):
                size = len(data)
            
            cum_weights = []
            cumsum = 0
            for w in weights:
                cumsum += w
                cum_weights.append(cumsum)
            
            selected_idx: set = set()
            sample = []
            
            while len(sample) < size and len(selected_idx) < len(data):
                r = self._rng.uniform(0, total_weight)
                
                for idx, cw in enumerate(cum_weights):
                    if r <= cw:
                        if idx not in selected_idx:
                            selected_idx.add(idx)
                            sample.append(data[idx])
                        break
        
        return SampleResult(
            data=sample,
            strategy=SamplingStrategy.WEIGHTED,
            original_size=len(data),
            sample_size=len(sample),
            duration=time.time() - start,
        )
    
    def cluster_sample(
        self,
        data: List[Any],
        size: int,
        cluster_key: Callable[[Any], Any],
    ) -> SampleResult:
        """Take cluster sample.
        
        Args:
            data: Input data
            size: Number of clusters to sample
            cluster_key: Function to extract cluster key
        
        Returns:
            SampleResult
        """
        import time
        start = time.time()
        
        clusters: Dict[Any, List[Any]] = {}
        for item in data:
            key = cluster_key(item)
            if key not in clusters:
                clusters[key] = []
            clusters[key].append(item)
        
        cluster_keys = list(clusters.keys())
        
        if size >= len(cluster_keys):
            return SampleResult(
                data=data.copy(),
                strategy=SamplingStrategy.CLUSTER,
                original_size=len(data),
                sample_size=len(data),
                duration=time.time() - start,
                metadata={"num_clusters": len(clusters)},
            )
        
        selected_keys = self._rng.sample(cluster_keys, size)
        
        sample = []
        for key in selected_keys:
            sample.extend(clusters[key])
        
        return SampleResult(
            data=sample,
            strategy=SamplingStrategy.CLUSTER,
            original_size=len(data),
            sample_size=len(sample),
            duration=time.time() - start,
            metadata={"num_clusters": len(clusters), "sampled_clusters": size},
        )
