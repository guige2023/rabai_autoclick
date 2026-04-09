"""Data resampling for balanced dataset creation.

Provides oversampling and undersampling strategies for handling
imbalanced datasets in automation workflows.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import copy


class ResamplingStrategy(Enum):
    """Resampling strategies for imbalanced data."""
    OVERSAMPLE_MINORITY = "oversample_minority"
    UNDERSAMPLE_MAJORITY = "undersample_majority"
    SMOTE = "smote"
    RANDOM = "random"
    STRATIFIED = "stratified"
    BOOTSTRAP = "bootstrap"


@dataclass
class ResamplingResult:
    """Result of a resampling operation."""
    operation_id: str
    original_size: int
    resampled_size: int
    strategy_used: ResamplingStrategy
    class_distribution_before: Dict[str, int]
    class_distribution_after: Dict[str, int]
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class RandomSampler:
    """Random sampling utilities."""

    @staticmethod
    def oversample(data: List[Any], target_count: int, random_seed: Optional[int] = None) -> List[Any]:
        """Oversample by randomly duplicating items."""
        if random_seed is not None:
            random.seed(random_seed)

        if len(data) >= target_count:
            return data[:target_count]

        result = list(data)
        while len(result) < target_count:
            result.append(random.choice(data))
        return result

    @staticmethod
    def undersample(data: List[Any], target_count: int, random_seed: Optional[int] = None) -> List[Any]:
        """Undersample by randomly selecting items."""
        if random_seed is not None:
            random.seed(random_seed)

        if len(data) <= target_count:
            return data

        return random.sample(data, target_count)

    @staticmethod
    def stratified_sample(
        data: List[Dict[str, Any]],
        target_count: int,
        stratify_by: str,
        random_seed: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Perform stratified sampling."""
        if random_seed is not None:
            random.seed(random_seed)

        class_counts = Counter(item.get(stratify_by) for item in data)

        if not class_counts:
            return RandomSampler.undersample(data, target_count, random_seed)

        total = len(data)
        target_per_class = {
            cls: max(1, int(target_count * count / total))
            for cls, count in class_counts.items()
        }

        result = []
        for cls, target in target_per_class.items():
            class_items = [item for item in data if item.get(stratify_by) == cls]
            sampled = RandomSampler.undersample(class_items, min(target, len(class_items)), random_seed)
            result.extend(sampled)

        return result


class SMOTEGenerator:
    """SMOTE-like synthetic sample generation."""

    @staticmethod
    def generate_synthetic_samples(
        minority_samples: List[List[float]],
        n_samples: int,
        k_neighbors: int = 5,
        random_seed: Optional[int] = None,
    ) -> List[List[float]]:
        """Generate synthetic samples using SMOTE-like interpolation."""
        if random_seed is not None:
            random.seed(random_seed)

        if len(minority_samples) < 2:
            return []

        if len(minority_samples) <= k_neighbors:
            k_neighbors = len(minority_samples) - 1

        synthetic_samples = []

        for _ in range(n_samples):
            sample = random.choice(minority_samples)

            neighbors = random.sample(minority_samples, min(k_neighbors, len(minority_samples)))
            neighbor = random.choice(neighbors)

            alpha = random.random()
            synthetic = [
                sample[i] + alpha * (neighbor[i] - sample[i])
                for i in range(len(sample))
            ]
            synthetic_samples.append(synthetic)

        return synthetic_samples


class DataResampler:
    """Core data resampling engine."""

    def __init__(self):
        self._results: List[ResamplingResult] = []
        self._lock = threading.Lock()

    def resample(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        strategy: ResamplingStrategy,
        target_ratio: Optional[float] = None,
        target_count: Optional[int] = None,
        random_seed: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], ResamplingResult]:
        """Resample data to balance class distribution."""
        if random_seed is not None:
            random.seed(random_seed)

        original_size = len(data)

        class_counts = Counter(item.get(label_column) for item in data)

        if not class_counts:
            return data, ResamplingResult(
                operation_id=str(uuid.uuid4())[:12],
                original_size=original_size,
                resampled_size=len(data),
                strategy_used=strategy,
                class_distribution_before={},
                class_distribution_after=dict(class_counts),
            )

        max_count = max(class_counts.values())
        min_count = min(class_counts.values())
        majority_class = max(class_counts, key=class_counts.get)
        minority_class = min(class_counts, key=class_counts.get)

        if strategy == ResamplingStrategy.OVERSAMPLE_MINORITY:
            target_minority = target_count or int(max_count * (target_ratio or 1.0))
            result = self._oversample_minority(data, label_column, minority_class, target_minority, random_seed)

        elif strategy == ResamplingStrategy.UNDERSAMPLE_MAJORITY:
            target_majority = target_count or int(min_count * (target_ratio or 1.0))
            result = self._undersample_majority(data, label_column, majority_class, target_majority, random_seed)

        elif strategy == ResamplingStrategy.RANDOM:
            target = target_count or int(sum(class_counts.values()) / len(class_counts))
            result = self._random_resample(data, label_column, target, random_seed)

        elif strategy == ResamplingStrategy.STRATIFIED:
            target = target_count or max_count
            result = self._stratified_resample(data, label_column, target, random_seed)

        elif strategy == ResamplingStrategy.BOOTSTRAP:
            target = target_count or original_size
            result = self._bootstrap_sample(data, target, random_seed)

        else:
            return data, ResamplingResult(
                operation_id=str(uuid.uuid4())[:12],
                original_size=original_size,
                resampled_size=len(data),
                strategy_used=strategy,
                class_distribution_before=dict(class_counts),
                class_distribution_after=dict(class_counts),
            )

        new_class_counts = Counter(item.get(label_column) for item in result)

        operation_result = ResamplingResult(
            operation_id=str(uuid.uuid4())[:12],
            original_size=original_size,
            resampled_size=len(result),
            strategy_used=strategy,
            class_distribution_before=dict(class_counts),
            class_distribution_after=dict(new_class_counts),
        )

        with self._lock:
            self._results.append(operation_result)

        return result, operation_result

    def _oversample_minority(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        minority_class: Any,
        target_count: int,
        random_seed: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Oversample minority class."""
        minority_samples = [item for item in data if item.get(label_column) == minority_class]
        majority_samples = [item for item in data if item.get(label_column) != minority_class]

        if not minority_samples:
            return data

        oversampled = RandomSampler.oversample(minority_samples, target_count, random_seed)
        return majority_samples + oversampled

    def _undersample_majority(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        majority_class: Any,
        target_count: int,
        random_seed: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Undersample majority class."""
        minority_samples = [item for item in data if item.get(label_column) != majority_class]
        majority_samples = [item for item in data if item.get(label_column) == majority_class]

        if not majority_samples:
            return data

        undersampled = RandomSampler.undersample(majority_samples, target_count, random_seed)
        return minority_samples + undersampled

    def _random_resample(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        target_per_class: int,
        random_seed: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Random resampling to balance all classes."""
        class_samples: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            class_samples[item.get(label_column)].append(item)

        result = []
        for cls, samples in class_samples.items():
            if len(samples) < target_per_class:
                resampled = RandomSampler.oversample(samples, target_per_class, random_seed)
            else:
                resampled = RandomSampler.undersample(samples, target_per_class, random_seed)
            result.extend(resampled)

        return result

    def _stratified_resample(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        target_count: int,
        random_seed: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Stratified resampling maintaining proportions."""
        return RandomSampler.stratified_sample(
            data, target_count, label_column, random_seed
        )

    def _bootstrap_sample(
        self,
        data: List[Dict[str, Any]],
        target_count: int,
        random_seed: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Bootstrap sampling with replacement."""
        if random_seed is not None:
            random.seed(random_seed)

        return [random.choice(data) for _ in range(target_count)]


class AutomationResamplerAction:
    """Action providing data resampling for automation workflows."""

    def __init__(self, resampler: Optional[DataResampler] = None):
        self._resampler = resampler or DataResampler()

    def resample(
        self,
        data: List[Dict[str, Any]],
        label_column: str,
        strategy: str = "oversample_minority",
        target_ratio: Optional[float] = None,
        target_count: Optional[int] = None,
        random_seed: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Resample data to balance class distribution."""
        try:
            strategy_enum = ResamplingStrategy(strategy.lower())
        except ValueError:
            strategy_enum = ResamplingStrategy.OVERSAMPLE_MINORITY

        result, operation_result = self._resampler.resample(
            data=data,
            label_column=label_column,
            strategy=strategy_enum,
            target_ratio=target_ratio,
            target_count=target_count,
            random_seed=random_seed,
        )

        return {
            "operation_id": operation_result.operation_id,
            "original_size": operation_result.original_size,
            "resampled_size": operation_result.resampled_size,
            "strategy": operation_result.strategy_used.value,
            "class_distribution_before": operation_result.class_distribution_before,
            "class_distribution_after": operation_result.class_distribution_after,
            "data": result,
        }

    def analyze_imbalance(self, data: List[Dict[str, Any]], label_column: str) -> Dict[str, Any]:
        """Analyze class imbalance in dataset."""
        class_counts = Counter(item.get(label_column) for item in data)

        if not class_counts:
            return {"error": "No data or label column not found"}

        total = sum(class_counts.values())
        max_count = max(class_counts.values())
        min_count = min(class_counts.values())
        imbalance_ratio = max_count / min_count if min_count > 0 else float('inf')

        return {
            "total_samples": total,
            "class_count": len(class_counts),
            "class_distribution": dict(class_counts),
            "imbalance_ratio": round(imbalance_ratio, 2),
            "max_class": max(class_counts, key=class_counts.get),
            "min_class": min(class_counts, key=class_counts.get),
            "is_balanced": imbalance_ratio < 2.0,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a resampling operation.

        Required params:
            operation: str - 'resample', 'analyze', or 'get_history'
            data: list - Data to resample
            label_column: str - Column containing class labels

        Optional params:
            strategy: str - Resampling strategy
            target_ratio: float - Target ratio between majority and minority
            target_count: int - Target count for resampled class
            random_seed: int - Random seed for reproducibility
        """
        operation = params.get("operation")

        if operation == "resample":
            data = params.get("data")
            label_column = params.get("label_column")

            if not data or not label_column:
                raise ValueError("data and label_column are required")

            return self.resample(
                data=data,
                label_column=label_column,
                strategy=params.get("strategy", "oversample_minority"),
                target_ratio=params.get("target_ratio"),
                target_count=params.get("target_count"),
                random_seed=params.get("random_seed"),
            )

        elif operation == "analyze":
            data = params.get("data")
            label_column = params.get("label_column")

            if not data or not label_column:
                raise ValueError("data and label_column are required")

            return self.analyze_imbalance(data, label_column)

        elif operation == "get_history":
            return {
                "history": [
                    {
                        "operation_id": r.operation_id,
                        "original_size": r.original_size,
                        "resampled_size": r.resampled_size,
                        "strategy": r.strategy_used.value,
                        "timestamp": datetime.fromtimestamp(r.timestamp).isoformat(),
                    }
                    for r in self._resampler._results
                ]
            }

        else:
            raise ValueError(f"Unknown operation: {operation}")
