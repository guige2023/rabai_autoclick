"""Data split action module.

Provides train/test splitting, stratified sampling, k-fold cross-validation
splitting, and chunk-based data partitioning.
"""

from __future__ import annotations

import random
import logging
from typing import Optional, Dict, Any, List, Callable, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataSplitAction:
    """Data splitting engine for ML and evaluation workflows.

    Provides train/test splits, stratified splits, k-fold splitting, and more.

    Example:
        splitter = DataSplitAction(seed=42)
        train, test = splitter.train_test(data, test_size=0.2)
        folds = splitter.k_fold(data, n_folds=5)
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        """Initialize data splitter.

        Args:
            seed: Random seed for reproducibility.
        """
        self.seed = seed
        if seed is not None:
            random.seed(seed)

    def train_test(
        self,
        data: List[Any],
        test_size: float = 0.2,
        shuffle: bool = True,
        stratify_by: Optional[Callable[[Any], Any]] = None,
    ) -> Tuple[List[Any], List[Any]]:
        """Split data into train and test sets.

        Args:
            data: List of items to split.
            test_size: Fraction of data for test set (0.0-1.0).
            shuffle: Whether to shuffle before splitting.
            stratify_by: Optional function to extract stratification key.

        Returns:
            Tuple of (train_data, test_data).
        """
        if shuffle:
            data = list(data)
            random.shuffle(data)

        if stratify_by:
            return self._stratified_split(data, stratify_by, test_size)

        n = len(data)
        split_idx = int(n * (1 - test_size))
        return data[:split_idx], data[split_idx:]

    def k_fold(
        self,
        data: List[Any],
        n_folds: int = 5,
        shuffle: bool = True,
    ) -> List[Tuple[List[Any], List[Any]]]:
        """Create k-fold cross-validation splits.

        Args:
            data: List of items to split.
            n_folds: Number of folds.
            shuffle: Whether to shuffle before splitting.

        Returns:
            List of (train, test) tuples, one per fold.
        """
        if shuffle:
            data = list(data)
            random.shuffle(data)

        n = len(data)
        fold_size = n // n_folds
        folds = []

        for i in range(n_folds):
            test_start = i * fold_size
            test_end = test_start + fold_size if i < n_folds - 1 else n
            test_data = data[test_start:test_end]
            train_data = data[:test_start] + data[test_end:]
            folds.append((train_data, test_data))

        return folds

    def stratified_k_fold(
        self,
        data: List[Dict[str, Any]],
        stratify_key: str,
        n_folds: int = 5,
    ) -> List[Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
        """Create stratified k-fold splits maintaining class distribution.

        Args:
            data: List of dicts to split.
            stratify_key: Field to use for stratification.
            n_folds: Number of folds.

        Returns:
            List of (train, test) tuples.
        """
        buckets: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            buckets[item.get(stratify_key)].append(item)

        fold_data: List[List[Dict[str, Any]]] = [[] for _ in range(n_folds)]

        for bucket_items in buckets.values():
            random.shuffle(bucket_items)
            for i, item in enumerate(bucket_items):
                fold_data[i % n_folds].append(item)

        folds = []
        for i in range(n_folds):
            test_data = fold_data[i]
            train_data = [item for j, fd in enumerate(fold_data) if j != i for item in fd]
            folds.append((train_data, test_data))

        return folds

    def chunk(
        self,
        data: List[Any],
        n_chunks: int,
        drop_last: bool = False,
    ) -> List[List[Any]]:
        """Split data into n equal chunks.

        Args:
            data: List of items to split.
            n_chunks: Number of chunks.
            drop_last: Drop the last partial chunk.

        Returns:
            List of chunks.
        """
        n = len(data)
        chunk_size = (n + n_chunks - 1) // n_chunks
        if drop_last:
            n = (n // chunk_size) * chunk_size

        return [data[i:i + chunk_size] for i in range(0, n, chunk_size)]

    def time_series_split(
        self,
        data: List[Any],
        train_size: Optional[int] = None,
        test_size: Optional[int] = None,
        gap: int = 0,
    ) -> List[Tuple[List[Any], List[Any]]]:
        """Create expanding window time series splits.

        Args:
            data: Time-ordered list of items.
            train_size: Initial training set size.
            test_size: Test set size for each split.
            gap: Gap between train and test (for time series).

        Returns:
            List of (train, test) tuples for each time step.
        """
        n = len(data)
        if test_size is None:
            test_size = max(1, n // 10)

        if train_size is None:
            train_size = test_size

        splits = []
        for i in range(train_size, n - test_size + 1, test_size):
            train_end = i - gap
            test_start = i
            test_end = min(i + test_size, n)
            if train_end > 0:
                splits.append((data[:train_end], data[test_start:test_end]))

        return splits

    def leave_one_out(self, data: List[Any]) -> List[Tuple[List[Any], Any]]:
        """Create leave-one-out cross-validation splits.

        Args:
            data: List of items.

        Returns:
            List of (train_without_item, item) tuples.
        """
        result = []
        for i, item in enumerate(data):
            train = data[:i] + data[i + 1:]
            result.append((train, item))
        return result

    def bootstrap(
        self,
        data: List[Any],
        n_samples: int = 1,
        sample_size: float = 1.0,
        test_size: Optional[float] = None,
    ) -> List[Tuple[List[Any], Optional[List[Any]]]]:
        """Create bootstrap resampling splits.

        Args:
            data: Original dataset.
            n_samples: Number of bootstrap samples.
            sample_size: Fraction of data per sample.
            test_size: If set, create out-of-bag test sets.

        Returns:
            List of (bootstrap_sample, oob_sample) tuples.
        """
        n = len(data)
        sample_n = int(n * sample_size)
        results = []

        for _ in range(n_samples):
            sample = [random.choice(data) for _ in range(sample_n)]

            if test_size is not None:
                oob = [item for item in data if item not in sample]
                results.append((sample, oob))
            else:
                results.append((sample, None))

        return results

    def split_by_predicate(
        self,
        data: List[Dict[str, Any]],
        predicate: Callable[[Dict[str, Any]], bool],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Split data using a predicate function.

        Args:
            data: List of dicts to split.
            predicate: Function returning True for first group.

        Returns:
            Tuple of (matching, non_matching).
        """
        matching = []
        non_matching = []
        for item in data:
            if predicate(item):
                matching.append(item)
            else:
                non_matching.append(item)
        return matching, non_matching

    def split_by_field(
        self,
        data: List[Dict[str, Any]],
        field: str,
        values: Optional[List[Any]] = None,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Split data by field values into groups.

        Args:
            data: List of dicts.
            field: Field to split by.
            values: Specific values to split on (None = all unique values).

        Returns:
            Dict mapping field value to list of matching items.
        """
        result: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            key = item.get(field)
            result[key].append(item)
        return dict(result)

    def _stratified_split(
        self,
        data: List[Dict[str, Any]],
        stratify_by: Callable[[Any], Any],
        test_size: float,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Internal stratified split implementation."""
        buckets: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            buckets[stratify_by(item)].append(item)

        train = []
        test = []

        for bucket_items in buckets.values():
            random.shuffle(bucket_items)
            split_idx = int(len(bucket_items) * (1 - test_size))
            train.extend(bucket_items[:split_idx])
            test.extend(bucket_items[split_idx:])

        return train, test
