"""
Data Sampler Action Module.

Provides various data sampling techniques including random sampling,
stratified sampling, reservoir sampling, and systematic sampling for
machine learning and statistical analysis workflows.
"""

import random
import math
from typing import Optional, List, Dict, Any, Callable, TypeVar, Iterator
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class SamplingStrategy(Enum):
    """Sampling strategy types."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    SYSTEMATIC = "systematic"
    RESERVOIR = "reservoir"
    CLUSTER = "cluster"
    WEIGHTED = "weighted"
    BOOSTRAP = "bootstrap"
    SMOTE = "smote"  # Synthetic Minority Over-sampling


@dataclass
class SamplerConfig:
    """Configuration for sampling."""
    strategy: SamplingStrategy = SamplingStrategy.RANDOM
    sample_size: Optional[int] = None  # absolute or None for fraction
    sample_fraction: Optional[float] = None  # 0.0-1.0
    random_seed: Optional[int] = None
    replace: bool = False  # with/without replacement
    stratify_by: Optional[str] = None  # field name for stratified sampling
    weights_field: Optional[str] = None  # field name for weighted sampling
    cluster_field: Optional[str] = None  # field name for cluster sampling


@dataclass
class SampleResult:
    """Result of sampling operation."""
    samples: List[Any]
    sample_size: int
    original_size: int
    sampling_rate: float
    method: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataSamplerAction:
    """
    Data sampling action with multiple sampling strategies.

    Supports random, stratified, systematic, reservoir, cluster,
    weighted, bootstrap, and SMOTE oversampling techniques.
    """

    def __init__(self, config: Optional[SamplerConfig] = None):
        self.config = config or SamplerConfig()
        if self.config.random_seed is not None:
            random.seed(self.config.random_seed)

    def _resolve_sample_size(
        self,
        data: List[Any],
        sample_fraction: Optional[float] = None,
    ) -> int:
        """Resolve actual sample size from fraction or config."""
        if self.config.sample_size is not None:
            return min(self.config.sample_size, len(data))
        frac = sample_fraction or self.config.sample_fraction or 0.1
        return max(1, int(len(data) * frac))

    def random_sample(
        self,
        data: List[Any],
        sample_size: Optional[int] = None,
    ) -> SampleResult:
        """
        Simple random sampling.

        Args:
            data: Input data list
            sample_size: Number of samples to draw

        Returns:
            SampleResult with sampled data
        """
        size = sample_size or self._resolve_sample_size(data)
        size = min(size, len(data))

        if self.config.replace:
            samples = random.choices(data, k=size)
        else:
            samples = random.sample(data, size)

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=len(data),
            sampling_rate=len(samples) / len(data),
            method="random",
        )

    def stratified_sample(
        self,
        data: List[Dict[str, Any]],
        stratify_by: str,
        sample_fraction: float = 0.1,
    ) -> SampleResult:
        """
        Stratified sampling - maintains proportions of strata.

        Args:
            data: Input data as list of dicts
            stratify_by: Field name to stratify by
            sample_fraction: Fraction to sample from each stratum

        Returns:
            SampleResult with stratified samples
        """
        # Group by stratum
        strata: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            strata[record.get(stratify_by)].append(record)

        # Sample from each stratum proportionally
        samples = []
        strata_info = {}

        for stratum_value, stratum_data in strata.items():
            stratum_size = max(1, int(len(stratum_data) * sample_fraction))
            stratum_samples = random.sample(
                stratum_data,
                min(stratum_size, len(stratum_data))
            )
            samples.extend(stratum_samples)
            strata_info[stratum_value] = {
                "original": len(stratum_data),
                "sampled": len(stratum_samples),
            }

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=len(data),
            sampling_rate=len(samples) / len(data),
            method="stratified",
            metadata={"strata": strata_info},
        )

    def systematic_sample(
        self,
        data: List[Any],
        sample_fraction: float = 0.1,
    ) -> SampleResult:
        """
        Systematic sampling - every k-th element.

        Args:
            data: Input data list
            sample_fraction: Fraction of data to sample

        Returns:
            SampleResult with systematically sampled data
        """
        n = len(data)
        k = max(1, int(1 / sample_fraction))

        # Random starting point
        start = random.randint(0, k - 1)
        indices = range(start, n, k)

        samples = [data[i] for i in indices]

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=n,
            sampling_rate=len(samples) / n,
            method="systematic",
            metadata={"interval": k, "start_index": start},
        )

    def reservoir_sample(
        self,
        data: Iterator[Any],
        sample_size: int,
    ) -> SampleResult:
        """
        Reservoir sampling for streaming/unbounded data.

        Uses Algorithm R for fair sampling.

        Args:
            data: Iterator of data items
            sample_size: Size of reservoir

        Returns:
            SampleResult with reservoir samples
        """
        reservoir: List[Any] = []
        count = 0

        for item in data:
            count += 1
            if len(reservoir) < sample_size:
                reservoir.append(item)
            else:
                # Randomly replace
                j = random.randint(0, count - 1)
                if j < sample_size:
                    reservoir[j] = item

        return SampleResult(
            samples=reservoir,
            sample_size=len(reservoir),
            original_size=count,
            sampling_rate=len(reservoir) / count if count > 0 else 0,
            method="reservoir",
        )

    def cluster_sample(
        self,
        data: List[Dict[str, Any]],
        cluster_field: str,
        sample_fraction: float = 0.1,
    ) -> SampleResult:
        """
        Cluster sampling - randomly select entire clusters.

        Args:
            data: Input data as list of dicts
            cluster_field: Field name for cluster ID
            sample_fraction: Fraction of clusters to sample

        Returns:
            SampleResult with cluster samples
        """
        # Group by cluster
        clusters: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            clusters[record.get(cluster_field)].append(record)

        cluster_ids = list(clusters.keys())
        num_clusters_to_sample = max(1, int(len(cluster_ids) * sample_fraction))

        sampled_cluster_ids = random.sample(
            cluster_ids,
            min(num_clusters_to_sample, len(cluster_ids))
        )

        samples = []
        for cid in sampled_cluster_ids:
            samples.extend(clusters[cid])

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=len(data),
            sampling_rate=len(samples) / len(data),
            method="cluster",
            metadata={
                "clusters_original": len(cluster_ids),
                "clusters_sampled": len(sampled_cluster_ids),
                "sampled_clusters": sampled_cluster_ids,
            },
        )

    def weighted_sample(
        self,
        data: List[Dict[str, Any]],
        weight_field: str,
        sample_size: Optional[int] = None,
    ) -> SampleResult:
        """
        Weighted sampling - higher weight = higher probability.

        Args:
            data: Input data as list of dicts
            weight_field: Field name containing weights
            sample_size: Number of samples to draw

        Returns:
            SampleResult with weighted samples
        """
        size = sample_size or self._resolve_sample_size(data)
        weights = [record.get(weight_field, 1.0) for record in data]

        # Normalize weights
        total_weight = sum(weights)
        if total_weight > 0:
            normalized = [w / total_weight for w in weights]
        else:
            normalized = [1.0 / len(data)] * len(data)

        indices = random.choices(
            range(len(data)),
            weights=normalized,
            k=min(size, len(data))
        )

        samples = [data[i] for i in indices]

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=len(data),
            sampling_rate=len(samples) / len(data),
            method="weighted",
            metadata={"weight_field": weight_field},
        )

    def bootstrap_sample(
        self,
        data: List[Any],
        sample_size: Optional[int] = None,
    ) -> SampleResult:
        """
        Bootstrap sampling - sampling with replacement.

        Args:
            data: Input data list
            sample_size: Size of bootstrap sample (default: same as input)

        Returns:
            SampleResult with bootstrap samples
        """
        size = sample_size or len(data)
        samples = random.choices(data, k=size)

        return SampleResult(
            samples=samples,
            sample_size=len(samples),
            original_size=len(data),
            sampling_rate=1.0,  # with replacement
            method="bootstrap",
        )

    def sample(
        self,
        data: List[Any],
        sample_size: Optional[int] = None,
    ) -> SampleResult:
        """
        Sample using configured strategy.

        Args:
            data: Input data
            sample_size: Optional override for sample size

        Returns:
            SampleResult with sampled data
        """
        strategy = self.config.strategy

        if strategy == SamplingStrategy.RANDOM:
            return self.random_sample(data, sample_size)

        elif strategy == SamplingStrategy.STRATIFIED:
            return self.stratified_sample(
                data,
                self.config.stratify_by or "class",
                self.config.sample_fraction or 0.1,
            )

        elif strategy == SamplingStrategy.SYSTEMATIC:
            return self.systematic_sample(
                data,
                self.config.sample_fraction or 0.1,
            )

        elif strategy == SamplingStrategy.RESERVOIR:
            if isinstance(data, list):
                data = iter(data)
            return self.reservoir_sample(data, sample_size or self._resolve_sample_size([]))

        elif strategy == SamplingStrategy.CLUSTER:
            return self.cluster_sample(
                data,
                self.config.cluster_field or "cluster",
                self.config.sample_fraction or 0.1,
            )

        elif strategy == SamplingStrategy.WEIGHTED:
            return self.weighted_sample(
                data,
                self.config.weights_field or "weight",
                sample_size,
            )

        elif strategy == SamplingStrategy.BOOTSTRAP:
            return self.bootstrap_sample(data, sample_size)

        else:
            return self.random_sample(data, sample_size)

    def train_test_split(
        self,
        data: List[Any],
        test_fraction: float = 0.2,
        shuffle: bool = True,
    ) -> tuple[List[Any], List[Any]]:
        """
        Split data into train and test sets.

        Args:
            data: Input data
            test_fraction: Fraction for test set
            shuffle: Whether to shuffle before splitting

        Returns:
            Tuple of (train_data, test_data)
        """
        if shuffle:
            data = list(data)
            random.shuffle(data)

        split_idx = max(1, int(len(data) * (1 - test_fraction)))
        return data[:split_idx], data[split_idx:]


class ImbalancedSampler:
    """Sampler for handling imbalanced datasets."""

    def __init__(self, random_seed: Optional[int] = None):
        if random_seed is not None:
            random.seed(random_seed)

    def undersample_majority(
        self,
        data: List[Dict[str, Any]],
        label_field: str,
        target_ratio: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Undersample majority class to balance dataset.

        Args:
            data: Input data
            label_field: Field name for class label
            target_ratio: Desired minority/majority ratio

        Returns:
            Balanced dataset
        """
        # Group by class
        by_class: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            by_class[record.get(label_field)].append(record)

        if len(by_class) < 2:
            return data

        # Find majority and minority
        class_sizes = sorted(by_class.items(), key=lambda x: len(x[1]))
        minority_class, minority_data = class_sizes[0]
        majority_data = class_sizes[-1][1]

        # Calculate target size
        target_size = min(
            len(minority_data),
            int(len(majority_data) * target_ratio)
        )

        # Sample from majority
        sampled_majority = random.sample(
            majority_data,
            min(target_size, len(majority_data))
        )

        return minority_data + sampled_majority

    def oversample_minority_smote(
        self,
        data: List[Dict[str, Any]],
        label_field: str,
        numeric_fields: List[str],
        target_ratio: float = 1.0,
    ) -> List[Dict[str, Any]]:
        """
        Oversample minority class using SMOTE-like interpolation.

        Args:
            data: Input data
            label_field: Field name for class label
            numeric_fields: Numeric fields for interpolation
            target_ratio: Desired minority/majority ratio

        Returns:
            Oversampled dataset
        """
        # Group by class
        by_class: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for record in data:
            by_class[record.get(label_field)].append(record)

        if len(by_class) < 2:
            return data

        class_sizes = sorted(by_class.items(), key=lambda x: len(x[1]))
        minority_class, minority_data = class_sizes[0]
        majority_size = len(class_sizes[-1][1])

        # Calculate number of synthetic samples needed
        target_size = int(majority_size * target_ratio)
        n_synthetic = max(0, target_size - len(minority_data))

        # Generate synthetic samples
        synthetic = []
        for _ in range(n_synthetic):
            # Select two random minority samples
            a, b = random.sample(minority_data, 2)

            # Interpolate numeric fields
            synthetic_record = dict(a)
            for field in numeric_fields:
                if field in a and field in b:
                    alpha = random.random()
                    synthetic_record[field] = (
                        a[field] + alpha * (b[field] - a[field])
                    )

            synthetic.append(synthetic_record)

        return data + synthetic
