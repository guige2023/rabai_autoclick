"""
Data Resample Action Module.

Resamples data for upsampling, downsampling,
and creating balanced datasets.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass
import logging
import random

logger = logging.getLogger(__name__)


class ResampleStrategy(Enum):
    """Resampling strategies."""
    UPSAMPLE = "upsample"
    DOWNSAMPLE = "downsample"
    SMOTE = "smote"
    RANDOM = "random"


@dataclass
class ResampleResult:
    """Result of resampling operation."""
    original_size: int
    resampled_size: int
    strategy: str
    label_counts: dict[str, int]


class DataResampleAction:
    """
    Data resampling for balanced datasets.

    Supports upsampling minority class,
    downsampling majority class, and SMOTE-like approaches.

    Example:
        resampler = DataResampleAction()
        balanced = resampler.resample(data, target_column="label", strategy="upsample")
    """

    def __init__(
        self,
        random_seed: Optional[int] = None,
    ) -> None:
        self.random_seed = random_seed
        if random_seed:
            random.seed(random_seed)

    def resample(
        self,
        data: list[dict],
        target_column: str,
        strategy: str = "upsample",
        target_size: Optional[int] = None,
    ) -> list[dict]:
        """Resample data to balance classes."""
        if not data:
            return []

        label_counts = self._count_labels(data, target_column)

        if strategy == "upsample":
            return self._upsample(data, target_column, label_counts, target_size)
        elif strategy == "downsample":
            return self._downsample(data, target_column, label_counts, target_size)
        elif strategy == "random":
            return self._random_sample(data, target_size or len(data))
        else:
            return data

    def _count_labels(
        self,
        data: list[dict],
        target_column: str,
    ) -> dict[str, int]:
        """Count samples per label."""
        counts: dict[str, int] = {}
        for row in data:
            label = str(row.get(target_column, "unknown"))
            counts[label] = counts.get(label, 0) + 1
        return counts

    def _upsample(
        self,
        data: list[dict],
        target_column: str,
        label_counts: dict[str, int],
        target_size: Optional[int],
    ) -> list[dict]:
        """Upsample minority classes."""
        max_count = max(label_counts.values()) if label_counts else 1

        if target_size:
            max_count = max(max_count, target_size // len(label_counts))

        by_label: dict[str, list[dict]] = {}
        for row in data:
            label = str(row.get(target_column, "unknown"))
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(row)

        resampled = []
        for label, samples in by_label.items():
            count = len(samples)

            if count < max_count:
                oversample = []
                while len(oversample) < max_count - count:
                    oversample.append(random.choice(samples))
                resampled.extend(samples + oversample)
            else:
                resampled.extend(samples)

        random.shuffle(resampled)
        return resampled

    def _downsample(
        self,
        data: list[dict],
        target_column: str,
        label_counts: dict[str, int],
        target_size: Optional[int],
    ) -> list[dict]:
        """Downsample majority class."""
        min_count = min(label_counts.values()) if label_counts else 1

        if target_size:
            min_count = min(min_count, target_size // len(label_counts))

        by_label: dict[str, list[dict]] = {}
        for row in data:
            label = str(row.get(target_column, "unknown"))
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(row)

        resampled = []
        for label, samples in by_label.items():
            if len(samples) > min_count:
                resampled.extend(random.sample(samples, min_count))
            else:
                resampled.extend(samples)

        random.shuffle(resampled)
        return resampled

    def _random_sample(
        self,
        data: list[dict],
        target_size: int,
    ) -> list[dict]:
        """Random sample without replacement."""
        if target_size >= len(data):
            return list(data)
        return random.sample(data, target_size)

    def stratified_split(
        self,
        data: list[dict],
        target_column: str,
        test_ratio: float = 0.2,
    ) -> tuple[list[dict], list[dict]]:
        """Split data maintaining class distribution."""
        if not data:
            return [], []

        by_label: dict[str, list[dict]] = {}
        for row in data:
            label = str(row.get(target_column, "unknown"))
            if label not in by_label:
                by_label[label] = []
            by_label[label].append(row)

        train = []
        test = []

        for label, samples in by_label.items():
            test_count = max(1, int(len(samples) * test_ratio))
            test.extend(samples[:test_count])
            train.extend(samples[test_count:])

        random.shuffle(train)
        random.shuffle(test)

        return train, test
