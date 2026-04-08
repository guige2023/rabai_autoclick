"""
Data Sampling Action.

Provides data sampling techniques for large datasets.
Supports:
- Simple random sampling
- Stratified sampling
- Systematic sampling
- Cluster sampling
- Reservoir sampling
"""

from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import random
import logging
import math

logger = logging.getLogger(__name__)


@dataclass
class SampleResult:
    """Result of a sampling operation."""
    sample: List[Any]
    sample_size: int
    original_size: int
    sampling_method: str
    sampling_fraction: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "sample_size": self.sample_size,
            "original_size": self.original_size,
            "sampling_method": self.sampling_method,
            "sampling_fraction": self.sampling_fraction,
            "metadata": self.metadata
        }


class DataSamplingAction:
    """
    Data Sampling Action.
    
    Provides various sampling techniques:
    - Simple random sampling
    - Stratified sampling
    - Systematic sampling
    - Cluster sampling
    - Reservoir sampling (for streaming data)
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the Data Sampling Action.
        
        Args:
            seed: Random seed for reproducibility
        """
        if seed is not None:
            random.seed(seed)
        self._seed = seed
    
    def simple_random_sample(
        self,
        data: List[Any],
        sample_size: int,
        replace: bool = False
    ) -> SampleResult:
        """
        Perform simple random sampling.
        
        Args:
            data: Input data list
            sample_size: Number of samples to draw
            replace: Whether to sample with replacement
        
        Returns:
            SampleResult with sampled data
        """
        if sample_size > len(data) and not replace:
            raise ValueError(
                f"Sample size {sample_size} exceeds population size {len(data)}"
            )
        
        if replace:
            sample = random.choices(data, k=sample_size)
        else:
            sample = random.sample(data, sample_size)
        
        return SampleResult(
            sample=sample,
            sample_size=len(sample),
            original_size=len(data),
            sampling_method="simple_random",
            sampling_fraction=len(sample) / len(data) if data else 0,
            metadata={"replace": replace}
        )
    
    def stratified_sample(
        self,
        data: List[Dict[str, Any]],
        stratify_by: str,
        sample_size: int,
        proportions: Optional[Dict[str, float]] = None
    ) -> SampleResult:
        """
        Perform stratified sampling.
        
        Args:
            data: Input data list (dictionaries)
            stratify_by: Key to stratify by
            sample_size: Total number of samples
            proportions: Optional proportions per stratum
        
        Returns:
            SampleResult with sampled data
        """
        # Group by stratum
        strata: Dict[str, List[Dict[str, Any]]] = {}
        for record in data:
            value = record.get(stratify_by)
            if value is not None:
                if value not in strata:
                    strata[value] = []
                strata[value].append(record)
        
        if not strata:
            raise ValueError(f"No valid strata found for key '{stratify_by}'")
        
        # Determine sample counts per stratum
        if proportions is None:
            # Proportional allocation
            total = len(data)
            proportions = {
                k: len(v) / total for k, v in strata.items()
            }
        
        # Sample from each stratum
        sampled = []
        stratum_counts = {}
        
        for stratum, records in strata.items():
            stratum_sample_size = max(1, int(sample_size * proportions.get(stratum, 0)))
            stratum_sample_size = min(stratum_sample_size, len(records))
            
            stratum_sample = random.sample(records, stratum_sample_size)
            sampled.extend(stratum_sample)
            stratum_counts[stratum] = stratum_sample_size
        
        return SampleResult(
            sample=sampled,
            sample_size=len(sampled),
            original_size=len(data),
            sampling_method="stratified",
            sampling_fraction=len(sampled) / len(data) if data else 0,
            metadata={
                "stratify_by": stratify_by,
                "stratum_counts": stratum_counts,
                "proportions": proportions
            }
        )
    
    def systematic_sample(
        self,
        data: List[Any],
        sample_size: int,
        start_index: Optional[int] = None
    ) -> SampleResult:
        """
        Perform systematic sampling.
        
        Args:
            data: Input data list
            sample_size: Number of samples to draw
            start_index: Starting index (random if not provided)
        
        Returns:
            SampleResult with sampled data
        """
        if sample_size > len(data):
            raise ValueError(
                f"Sample size {sample_size} exceeds population size {len(data)}"
            )
        
        # Calculate sampling interval
        interval = len(data) // sample_size
        
        if start_index is None:
            start_index = random.randint(0, interval - 1)
        
        # Select items at regular intervals
        sampled = []
        for i in range(sample_size):
            index = (start_index + i * interval) % len(data)
            sampled.append(data[index])
        
        return SampleResult(
            sample=sampled,
            sample_size=len(sampled),
            original_size=len(data),
            sampling_method="systematic",
            sampling_fraction=len(sampled) / len(data) if data else 0,
            metadata={
                "interval": interval,
                "start_index": start_index
            }
        )
    
    def cluster_sample(
        self,
        data: List[Any],
        cluster_size: int,
        num_clusters: int,
        replace: bool = False
    ) -> SampleResult:
        """
        Perform cluster sampling.
        
        Args:
            data: Input data list
            cluster_size: Size of each cluster
            num_clusters: Number of clusters to sample
            replace: Whether to sample clusters with replacement
        
        Returns:
            SampleResult with sampled data
        """
        if cluster_size <= 0:
            raise ValueError("Cluster size must be positive")
        
        num_full_clusters = len(data) // cluster_size
        if num_clusters > num_full_clusters and not replace:
            raise ValueError(
                f"Cannot sample {num_clusters} clusters from {num_full_clusters} available"
            )
        
        # Create clusters
        clusters = [
            data[i * cluster_size:(i + 1) * cluster_size]
            for i in range(num_full_clusters)
        ]
        
        # Sample clusters
        if replace:
            cluster_indices = random.choices(range(len(clusters)), k=num_clusters)
        else:
            cluster_indices = random.sample(range(len(clusters)), num_clusters)
        
        # Flatten selected clusters
        sampled = []
        for idx in cluster_indices:
            sampled.extend(clusters[idx])
        
        return SampleResult(
            sample=sampled,
            sample_size=len(sampled),
            original_size=len(data),
            sampling_method="cluster",
            sampling_fraction=len(sampled) / len(data) if data else 0,
            metadata={
                "cluster_size": cluster_size,
                "num_clusters_sampled": len(cluster_indices),
                "cluster_indices": cluster_indices
            }
        )
    
    def reservoir_sample(
        self,
        data: List[Any],
        sample_size: int
    ) -> SampleResult:
        """
        Perform reservoir sampling (Algorithm R).
        
        Useful for sampling from streaming data or large datasets
        where you want to maintain a representative sample.
        
        Args:
            data: Input data list
            sample_size: Size of reservoir
        
        Returns:
            SampleResult with sampled data
        """
        if sample_size > len(data):
            return self.simple_random_sample(data, len(data))
        
        reservoir = data[:sample_size]
        
        for i in range(sample_size, len(data)):
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = data[i]
        
        return SampleResult(
            sample=reservoir,
            sample_size=len(reservoir),
            original_size=len(data),
            sampling_method="reservoir",
            sampling_fraction=len(reservoir) / len(data) if data else 0,
            metadata={"algorithm": "Algorithm R"}
        )
    
    def weighted_sample(
        self,
        data: List[Any],
        weights: List[float],
        sample_size: int,
        replace: bool = False
    ) -> SampleResult:
        """
        Perform weighted sampling.
        
        Args:
            data: Input data list
            weights: Weights for each item
            sample_size: Number of samples to draw
            replace: Whether to sample with replacement
        
        Returns:
            SampleResult with sampled data
        """
        if len(data) != len(weights):
            raise ValueError("Data and weights must have same length")
        
        if any(w < 0 for w in weights):
            raise ValueError("Weights must be non-negative")
        
        total_weight = sum(weights)
        if total_weight == 0:
            raise ValueError("Total weight must be positive")
        
        # Normalize weights
        normalized = [w / total_weight for w in weights]
        
        if replace:
            indices = random.choices(range(len(data)), weights=normalized, k=sample_size)
            sample = [data[i] for i in indices]
        else:
            # Without replacement: sequential sampling
            remaining_indices = list(range(len(data)))
            remaining_weights = normalized.copy()
            sample = []
            
            for _ in range(min(sample_size, len(data))):
                total = sum(remaining_weights)
                if total == 0:
                    break
                
                normalized_remaining = [w / total for w in remaining_weights]
                idx = random.choices(range(len(remaining_indices)), weights=normalized_remaining, k=1)[0]
                sample.append(data[remaining_indices[idx]])
                
                # Remove selected
                removed_weight = remaining_weights.pop(idx)
                removed_idx = remaining_indices.pop(idx)
                
                # Normalize remaining weights
                if remaining_weights:
                    remaining_weights = [w / (1 - removed_weight) for w in remaining_weights]
        
        return SampleResult(
            sample=sample,
            sample_size=len(sample),
            original_size=len(data),
            sampling_method="weighted",
            sampling_fraction=len(sample) / len(data) if data else 0,
            metadata={"replace": replace}
        )
    
    def bootstrap_sample(
        self,
        data: List[Any],
        sample_size: Optional[int] = None,
        num_samples: int = 1
    ) -> List[SampleResult]:
        """
        Perform bootstrap sampling.
        
        Args:
            data: Input data list
            sample_size: Size of each bootstrap sample (default: same as data)
            num_samples: Number of bootstrap samples to generate
        
        Returns:
            List of SampleResults
        """
        if sample_size is None:
            sample_size = len(data)
        
        results = []
        for _ in range(num_samples):
            result = self.simple_random_sample(data, sample_size, replace=True)
            result.sampling_method = "bootstrap"
            result.metadata["num_samples"] = num_samples
            results.append(result)
        
        return results


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create sampling action with seed for reproducibility
    sampler = DataSamplingAction(seed=42)
    
    # Generate sample data
    data = list(range(1000))
    stratified_data = [
        {"id": i, "category": cat, "value": i * 10}
        for i, cat in enumerate(["A"] * 400 + ["B"] * 300 + ["C"] * 300)
    ]
    
    # Simple random sample
    result = sampler.simple_random_sample(data, 100)
    print(f"Simple random: {result.sample_size}/{result.original_size}")
    
    # Stratified sample
    result = sampler.stratified_sample(stratified_data, "category", 50)
    print(f"Stratified: {result.sample_size}/{result.original_size}")
    print(f"  Stratum counts: {result.metadata['stratum_counts']}")
    
    # Systematic sample
    result = sampler.systematic_sample(data, 50)
    print(f"Systematic: {result.sample_size}/{result.original_size}")
    print(f"  Interval: {result.metadata['interval']}")
    
    # Reservoir sample
    result = sampler.reservoir_sample(data, 100)
    print(f"Reservoir: {result.sample_size}/{result.original_size}")
    
    # Weighted sample
    weighted_data = list(range(10))
    weights = [10, 5, 3, 8, 2, 7, 4, 6, 9, 1]
    result = sampler.weighted_sample(weighted_data, weights, 5)
    print(f"Weighted: {result.sample_size}/{result.original_size}")
    print(f"  Sample: {result.sample}")
    
    # Bootstrap
    bootstrap_results = sampler.bootstrap_sample(data[:100], num_samples=5)
    print(f"Bootstrap: {len(bootstrap_results)} samples generated")
    
    print("\nSampling completed successfully!")
