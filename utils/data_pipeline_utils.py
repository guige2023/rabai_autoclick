"""
Data pipeline utilities for building processing pipelines.

Provides pipeline construction, data loading, batching, and
transformation utilities for ML workflows.
"""
from __future__ import annotations

import queue
import threading
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple, TypeVar

import numpy as np

T = TypeVar("T")
U = TypeVar("U")


class Pipeline:
    """Simple data processing pipeline."""

    def __init__(self, steps: List[Callable] = None):
        self.steps = steps or []

    def add(self, step: Callable) -> "Pipeline":
        """Add a processing step."""
        self.steps.append(step)
        return self

    def process(self, data: Any) -> Any:
        """Process data through all steps."""
        result = data
        for step in self.steps:
            result = step(result)
        return result

    def __call__(self, data: Any) -> Any:
        return self.process(data)


class Transformer(Pipeline):
    """Data transformer with fit/transform pattern."""

    def fit(self, data: Any) -> "Transformer":
        """Fit transformer to data."""
        return self

    def transform(self, data: Any) -> Any:
        """Transform data."""
        return self.process(data)

    def fit_transform(self, data: Any) -> Any:
        """Fit and transform in one step."""
        self.fit(data)
        return self.transform(data)


def min_max_scale(data: np.ndarray, feature_range: Tuple[float, float] = (0, 1)) -> np.ndarray:
    """
    Min-max scaling.

    Args:
        data: Input array
        feature_range: Target range (min, max)

    Returns:
        Scaled array

    Example:
        >>> min_max_scale(np.array([0, 50, 100]))
        array([0. , 0.5, 1. ])
    """
    data_min, data_max = data.min(), data.max()
    if data_max == data_min:
        return np.full_like(data, feature_range[0])
    a, b = feature_range
    return a + (data - data_min) * (b - a) / (data_max - data_min)


def standard_scale(data: np.ndarray) -> np.ndarray:
    """
    Standard scaling (z-score normalization).

    Args:
        data: Input array

    Returns:
        Standardized array

    Example:
        >>> standard_scale(np.array([0, 50, 100]))
        array([-1.22474487,  0.        ,  1.22474487])
    """
    mean, std = data.mean(), data.std()
    if std == 0:
        return data - mean
    return (data - mean) / std


def robust_scale(data: np.ndarray) -> np.ndarray:
    """
    Robust scaling using median and IQR.

    Args:
        data: Input array

    Returns:
        Robustly scaled array

    Example:
        >>> robust_scale(np.array([0, 25, 50, 75, 100]))
        array([-2., -1.,  0.,  1.,  2.])
    """
    median = np.median(data)
    q75, q25 = np.percentile(data, [75, 25])
    iqr = q75 - q25
    if iqr == 0:
        return data - median
    return (data - median) / iqr


def normalize_l1(vec: np.ndarray) -> np.ndarray:
    """
    L1 normalization.

    Args:
        vec: Input vector

    Returns:
        L1 normalized vector

    Example:
        >>> normalize_l1(np.array([1, 2, 3]))
        array([0.16666667, 0.33333333, 0.5       ])
    """
    norm = np.abs(vec).sum()
    if norm == 0:
        return vec
    return vec / norm


def normalize_l2(vec: np.ndarray) -> np.ndarray:
    """
    L2 normalization.

    Args:
        vec: Input vector

    Returns:
        L2 normalized vector

    Example:
        >>> normalize_l2(np.array([3, 4]))
        array([0.6, 0.8])
    """
    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return vec / norm


class DataBatcher:
    """Batch data iterator."""

    def __init__(
        self,
        data: Sequence,
        batch_size: int,
        shuffle: bool = False,
        drop_last: bool = False,
    ):
        self.data = data
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.drop_last = drop_last
        self._indices = np.arange(len(data))

    def __iter__(self) -> Iterator[List]:
        if self.shuffle:
            np.random.shuffle(self._indices)
        for i in range(0, len(self._indices), self.batch_size):
            batch_indices = self._indices[i : i + self.batch_size]
            if len(batch_indices) < self.batch_size and self.drop_last:
                break
            yield [self.data[j] for j in batch_indices]

    def __len__(self) -> int:
        n = len(self.data) // self.batch_size
        if not self.drop_last and len(self.data) % self.batch_size != 0:
            n += 1
        return n


class PrefetchDataLoader:
    """Data loader with background prefetching."""

    def __init__(
        self,
        data: Sequence,
        batch_size: int,
        transform: Callable = None,
        num_workers: int = 2,
        buffer_size: int = 100,
    ):
        self.data = data
        self.batch_size = batch_size
        self.transform = transform or (lambda x: x)
        self.num_workers = num_workers
        self.buffer_size = buffer_size
        self._queue = queue.Queue(maxsize=buffer_size)
        self._threads = []

    def _prefetch_worker(self):
        batcher = DataBatcher(self.data, self.batch_size, shuffle=False)
        for batch in batcher:
            transformed = [self.transform(item) for item in batch]
            self._queue.put(transformed)

    def __iter__(self) -> Iterator[List]:
        self._queue = queue.Queue(maxsize=self.buffer_size)
        self._threads = []
        for _ in range(self.num_workers):
            t = threading.Thread(target=self._prefetch_worker)
            t.daemon = True
            t.start()
            self._threads.append(t)
        for _ in range(len(self)):
            yield self._queue.get()

    def __len__(self) -> int:
        return len(DataBatcher(self.data, self.batch_size, drop_last=True))


class ChainTransformer(Transformer):
    """Chain multiple transformers sequentially."""

    def fit(self, data: Any) -> "ChainTransformer":
        for step in self.steps:
            if hasattr(step, "fit"):
                step.fit(data)
        return self

    def transform(self, data: Any) -> Any:
        return self.process(data)


class ParallelTransformer(Transformer):
    """Apply multiple transformers in parallel."""

    def __init__(self, steps: List[Callable] = None):
        super().__init__(steps)

    def transform(self, data: Any) -> List[Any]:
        return [step(data) for step in self.steps]


def create_train_test_split(
    X: Sequence, y: Sequence = None, test_size: float = 0.2, random_state: int = None
) -> Tuple:
    """
    Split data into train and test sets.

    Args:
        X: Features
        y: Labels (optional)
        test_size: Proportion for test set
        random_state: Random seed

    Returns:
        Train/test split tuple(s)

    Example:
        >>> X_train, X_test = create_train_test_split([1,2,3,4,5], test_size=0.2)
    """
    if random_state is not None:
        np.random.seed(random_state)
    n = len(X)
    indices = np.random.permutation(n)
    split_idx = int(n * (1 - test_size))
    train_idx, test_idx = indices[:split_idx], indices[split_idx:]
    if y is None:
        return [X[i] for i in train_idx], [X[i] for i in test_idx]
    return (
        [X[i] for i in train_idx],
        [X[i] for i in test_idx],
        [y[i] for i in train_idx],
        [y[i] for i in test_idx],
    )


def stratified_split(
    X: Sequence, y: Sequence, test_size: float = 0.2, random_state: int = None
) -> Tuple:
    """
    Stratified train/test split preserving class distribution.

    Args:
        X: Features
        y: Labels
        test_size: Proportion for test set
        random_state: Random seed

    Returns:
        (X_train, X_test, y_train, y_test)
    """
    if random_state is not None:
        np.random.seed(random_state)
    labels = np.array(y)
    unique_labels = np.unique(labels)
    train_idx, test_idx = [], []
    for label in unique_labels:
        label_indices = np.where(labels == label)[0]
        np.random.shuffle(label_indices)
        split = int(len(label_indices) * (1 - test_size))
        train_idx.extend(label_indices[:split])
        test_idx.extend(label_indices[split:])
    train_idx, test_idx = np.array(train_idx), np.array(test_idx)
    np.random.shuffle(train_idx)
    np.random.shuffle(test_idx)
    X_train = [X[i] for i in train_idx]
    X_test = [X[i] for i in test_idx]
    y_train = [y[i] for i in train_idx]
    y_test = [y[i] for i in test_idx]
    return X_train, X_test, y_train, y_test


def k_fold_cross_validation(
    data: Sequence, n_folds: int = 5, shuffle: bool = True, random_state: int = None
) -> List[Tuple[List, List]]:
    """
    Create K-fold cross validation splits.

    Args:
        data: Data to split
        n_folds: Number of folds
        shuffle: Whether to shuffle data
        random_state: Random seed

    Returns:
        List of (train_indices, val_indices) tuples

    Example:
        >>> folds = k_fold_cross_validation(range(10), n_folds=3)
        >>> len(folds)
        3
    """
    if random_state is not None:
        np.random.seed(random_state)
    indices = np.arange(len(data))
    if shuffle:
        np.random.shuffle(indices)
    fold_sizes = np.full(n_folds, len(data) // n_folds, dtype=int)
    fold_sizes[: len(data) % n_folds] += 1
    folds = []
    current = 0
    for size in fold_sizes:
        start, stop = current, current + size
        val_idx = indices[start:stop]
        train_idx = np.concatenate([indices[:start], indices[stop:]])
        folds.append((train_idx.tolist(), val_idx.tolist()))
        current = stop
    return folds
