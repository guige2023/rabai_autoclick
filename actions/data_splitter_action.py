"""
Data Splitter Action Module.

Splits datasets into train/test/validation splits with
 stratification, shuffling, and configurable ratios.
"""

from __future__ import annotations

import random
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class SplitResult:
    """Result of a data split operation."""
    train: list[Any] = field(default_factory=list)
    test: list[Any] = field(default_factory=list)
    validation: list[Any] = field(default_factory=list)
    split_ratios: dict[str, float] = field(default_factory=dict)
    original_count: int = 0


class DataSplitterAction:
    """
    Dataset splitting for machine learning and evaluation.

    Splits data into train/test/validation sets with support for
    stratification, shuffling, and custom split logic.

    Example:
        splitter = DataSplitterAction()
        result = splitter.split(data, ratios={"train": 0.8, "test": 0.2})
        train_data = result.train
        test_data = result.test
    """

    def __init__(
        self,
        seed: Optional[int] = None,
    ) -> None:
        self.seed = seed
        if seed is not None:
            random.seed(seed)

    def split(
        self,
        data: list[Any],
        ratios: Optional[dict[str, float]] = None,
        shuffle: bool = True,
        stratify_by: Optional[Callable[[Any], Any]] = None,
    ) -> SplitResult:
        """Split data according to ratios."""
        ratios = ratios or {"train": 0.7, "test": 0.2, "validation": 0.1}

        total_ratio = sum(ratios.values())
        if abs(total_ratio - 1.0) > 0.001:
            raise ValueError(f"Ratios must sum to 1.0, got {total_ratio}")

        if shuffle:
            data = list(data)
            random.shuffle(data)

        if stratify_by:
            data = self._stratify_split(data, ratios, stratify_by)
        else:
            data = self._simple_split(data, ratios)

        return SplitResult(
            train=data[0],
            test=data[1],
            validation=data[2] if len(data) > 2 else [],
            split_ratios=ratios,
            original_count=len(data[0]) + len(data[1]) + (len(data[2]) if len(data) > 2 else 0),
        )

    def _simple_split(
        self,
        data: list[Any],
        ratios: dict[str, float],
    ) -> list[list[Any]]:
        """Perform simple random split."""
        n = len(data)
        result: list[list[Any]] = []

        sorted_ratios = sorted(ratios.items(), key=lambda x: x[0])
        cumulative = 0

        for name, ratio in sorted_ratios:
            cumulative += ratio
            end_idx = int(n * cumulative)
            start_idx = int(n * (cumulative - ratio))
            result.append(data[start_idx:end_idx])

        return result

    def _stratify_split(
        self,
        data: list[Any],
        ratios: dict[str, float],
        stratify_by: Callable[[Any], Any],
    ) -> list[list[Any]]:
        """Perform stratified split maintaining class distribution."""
        groups: dict[Any, list[Any]] = {}

        for item in data:
            key = stratify_by(item)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        splits: list[list[Any]] = [[], [], []]
        ratio_list = list(ratios.values())

        for group_items in groups.values():
            group_splits = self._simple_split(group_items, ratios)

            for i, split in enumerate(group_splits):
                splits[i].extend(split)

        for split in splits:
            random.shuffle(split)

        return splits

    def k_fold_split(
        self,
        data: list[Any],
        k: int = 5,
        shuffle: bool = True,
    ) -> list[tuple[list[Any], list[Any]]]:
        """Create K-fold cross-validation splits."""
        if shuffle:
            data = list(data)
            random.shuffle(data)

        n = len(data)
        fold_size = n // k
        folds: list[tuple[list[Any], list[Any]]] = []

        for i in range(k):
            start = i * fold_size
            end = start + fold_size if i < k - 1 else n

            test = data[start:end]
            train = data[:start] + data[end:]
            folds.append((train, test))

        return folds

    def train_test_split(
        self,
        data: list[Any],
        test_size: float = 0.2,
        shuffle: bool = True,
    ) -> tuple[list[Any], list[Any]]:
        """Simple train/test split."""
        result = self.split(data, ratios={"train": 1 - test_size, "test": test_size}, shuffle=shuffle)
        return result.train, result.test
