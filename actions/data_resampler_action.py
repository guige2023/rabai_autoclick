"""Data resampling and transformation action."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class ResampleConfig:
    """Configuration for resampling."""

    target_size: int
    strategy: str = "upsample"  # upsample, downsample, mix
    replacement: bool = False
    seed: Optional[int] = None
    class_field: Optional[str] = None


@dataclass
class ResampleResult:
    """Result of resampling."""

    original_size: int
    resampled_size: int
    strategy: str
    class_distribution: dict[str, int] = field(default_factory=dict)


class DataResamplerAction:
    """Resamples datasets for balanced or targeted distributions."""

    def __init__(self, default_seed: Optional[int] = None):
        """Initialize resampler.

        Args:
            default_seed: Default random seed.
        """
        self._default_seed = default_seed

    def _get_random_state(self, seed: Optional[int]) -> random.Random:
        """Get random state with seed."""
        if seed is not None:
            return random.Random(seed)
        if self._default_seed is not None:
            return random.Random(self._default_seed)
        return random.Random()

    def upsample(
        self,
        data: Sequence[dict[str, Any]],
        target_size: int,
        seed: Optional[int] = None,
        replacement: bool = True,
    ) -> list[dict[str, Any]]:
        """Upsample dataset to target size.

        Args:
            data: Input data.
            target_size: Desired size.
            seed: Random seed.
            replacement: Whether to sample with replacement.

        Returns:
            Resampled data.
        """
        rng = self._get_random_state(seed)

        if target_size <= len(data):
            return list(data)

        result = list(data)
        while len(result) < target_size:
            if replacement:
                result.append(rng.choice(data))
            else:
                result.append(data[len(result) % len(data)])

        return result

    def downsample(
        self,
        data: Sequence[dict[str, Any]],
        target_size: int,
        seed: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Downsample dataset to target size.

        Args:
            data: Input data.
            target_size: Desired size.
            seed: Random seed.

        Returns:
            Resampled data.
        """
        rng = self._get_random_state(seed)

        if target_size >= len(data):
            return list(data)

        return rng.sample(list(data), target_size)

    def resample_stratified(
        self,
        data: Sequence[dict[str, Any]],
        class_field: str,
        target_ratio: Optional[dict[str, float]] = None,
        seed: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Resample to achieve target class distribution.

        Args:
            data: Input data.
            class_field: Field containing class labels.
            target_ratio: Target ratios for each class.
            seed: Random seed.

        Returns:
            Resampled data.
        """
        rng = self._get_random_state(seed)

        classes: dict[str, list[dict[str, Any]]] = {}
        for record in data:
            label = str(record.get(class_field, "unknown"))
            if label not in classes:
                classes[label] = []
            classes[label].append(record)

        if target_ratio is None:
            total = len(data)
            target_ratio = {cls: len(samples) / total for cls, samples in classes.items()}

        result = []
        for cls, samples in classes.items():
            target_size = int(len(data) * target_ratio.get(cls, 0))
            target_size = max(target_size, 1)

            if len(samples) >= target_size:
                result.extend(rng.sample(samples, target_size))
            else:
                result.extend(samples)
                while len([r for r in result if str(r.get(class_field)) == cls]) < target_size:
                    result.append(rng.choice(samples))

        rng.shuffle(result)
        return result

    def resample_bootstrap(
        self,
        data: Sequence[dict[str, Any]],
        n_iterations: int = 100,
        seed: Optional[int] = None,
    ) -> list[list[dict[str, Any]]]:
        """Create bootstrap samples.

        Args:
            data: Input data.
            n_iterations: Number of bootstrap samples.
            seed: Random seed.

        Returns:
            List of bootstrap samples.
        """
        rng = self._get_random_state(seed)
        samples = []

        for _ in range(n_iterations):
            sample = rng.choices(list(data), k=len(data))
            samples.append(sample)

        return samples

    def resample_k_fold(
        self,
        data: Sequence[dict[str, Any]],
        k: int = 5,
        seed: Optional[int] = None,
    ) -> list[tuple[list[dict[str, Any]], list[dict[str, Any]]]]:
        """Create K-fold cross-validation splits.

        Args:
            data: Input data.
            k: Number of folds.
            seed: Random seed.

        Returns:
            List of (train, test) tuples.
        """
        rng = self._get_random_state(seed)
        data_list = list(data)
        rng.shuffle(data_list)

        fold_size = len(data_list) // k
        folds = []

        for i in range(k):
            start = i * fold_size
            end = start + fold_size if i < k - 1 else len(data_list)
            test = data_list[start:end]
            train = data_list[:start] + data_list[end:]
            folds.append((train, test))

        return folds

    def resample_by_group(
        self,
        data: Sequence[dict[str, Any]],
        group_field: str,
        samples_per_group: int,
        seed: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Resample with fixed samples per group.

        Args:
            data: Input data.
            group_field: Field to group by.
            samples_per_group: Samples to take per group.
            seed: Random seed.

        Returns:
            Resampled data.
        """
        rng = self._get_random_state(seed)

        groups: dict[str, list[dict[str, Any]]] = {}
        for record in data:
            group = str(record.get(group_field, "unknown"))
            if group not in groups:
                groups[group] = []
            groups[group].append(record)

        result = []
        for group, records in groups.items():
            n = min(samples_per_group, len(records))
            result.extend(rng.sample(records, n))

        return result

    def get_distribution(
        self,
        data: Sequence[dict[str, Any]],
        field_name: str,
    ) -> dict[str, int]:
        """Get distribution of values for a field."""
        dist: dict[str, int] = {}
        for record in data:
            value = str(record.get(field_name, "unknown"))
            dist[value] = dist.get(value, 0) + 1
        return dist
