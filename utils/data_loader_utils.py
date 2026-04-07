"""
Dataset and data loader utilities.

Provides dataset classes, data batching, and data loading
utilities for machine learning workflows.
"""
from __future__ import annotations

import queue
import threading
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple, TypeVar

import numpy as np

T = TypeVar("T")


class Dataset:
    """Base dataset class."""

    def __len__(self) -> int:
        raise NotImplementedError

    def __getitem__(self, idx: int) -> Any:
        raise NotImplementedError


class TensorDataset(Dataset):
    """Dataset from tensors."""

    def __init__(self, *tensors: np.ndarray):
        assert all(len(t) == len(tensors[0]) for t in tensors)
        self.tensors = tensors

    def __len__(self) -> int:
        return len(self.tensors[0])

    def __getitem__(self, idx: int) -> Tuple[np.ndarray, ...]:
        return tuple(t[idx] for t in self.tensors)


class IterableDataset(Dataset):
    """Iterable dataset for streaming data."""

    def __init__(self, data_source: Iterator):
        self.data_source = data_source

    def __iter__(self) -> Iterator:
        return iter(self.data_source)


class Subset(Dataset):
    """Subset of a dataset."""

    def __init__(self, dataset: Dataset, indices: Sequence[int]):
        self.dataset = dataset
        self.indices = indices

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, idx: int) -> Any:
        return self.dataset[self.indices[idx]]


class ConcatDataset(Dataset):
    """Concatenation of multiple datasets."""

    def __init__(self, datasets: List[Dataset]):
        self.datasets = datasets
        self.cumulative_sizes = [0]
        for d in datasets:
            self.cumulative_sizes.append(self.cumulative_sizes[-1] + len(d))

    def __len__(self) -> int:
        return self.cumulative_sizes[-1]

    def __getitem__(self, idx: int) -> Any:
        dataset_idx = np.searchsorted(self.cumulative_sizes, idx, side="right") - 1
        sample_idx = idx - self.cumulative_sizes[dataset_idx]
        return self.datasets[dataset_idx][sample_idx]


class DataLoader:
    """Data loader with batching and shuffling."""

    def __init__(
        self,
        dataset: Dataset,
        batch_size: int = 1,
        shuffle: bool = False,
        num_workers: int = 0,
        drop_last: bool = False,
        collate_fn: Callable = None,
        pin_memory: bool = False,
    ):
        self.dataset = dataset
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.num_workers = num_workers
        self.drop_last = drop_last
        self.collate_fn = collate_fn or self._default_collate
        self.pin_memory = pin_memory
        self._epoch = 0

    def _default_collate(self, batch: List) -> np.ndarray:
        """Default collate function."""
        if isinstance(batch[0], np.ndarray):
            return np.stack(batch)
        elif isinstance(batch[0], (tuple, list)):
            return tuple(self._default_collate(items) for items in zip(*batch))
        else:
            return np.array(batch)

    def __iter__(self) -> Iterator:
        if self.shuffle:
            generator = np.random.default_rng(self._epoch)
            indices = list(generator.permutation(len(self.dataset)))
        else:
            indices = list(range(len(self.dataset)))
        self._epoch += 1
        return self._batch_iterator(indices)

    def _batch_iterator(self, indices: List[int]) -> Iterator:
        batch = []
        for idx in indices:
            batch.append(self.dataset[idx])
            if len(batch) == self.batch_size:
                yield self.collate_fn(batch)
                batch = []
        if batch and not self.drop_last:
            yield self.collate_fn(batch)

    def __len__(self) -> int:
        if self.drop_last:
            return len(self.dataset) // self.batch_size
        return (len(self.dataset) + self.batch_size - 1) // self.batch_size


class MultiEpochDataLoader:
    """DataLoader that persists workers across epochs."""

    def __init__(
        self,
        dataset: Dataset,
        batch_size: int = 1,
        shuffle: bool = True,
        num_workers: int = 0,
        drop_last: bool = False,
        collate_fn: Callable = None,
    ):
        self.dataset = dataset
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.num_workers = num_workers
        self.drop_last = drop_last
        self.collate_fn = collate_fn or self._default_collate
        self._epoch = 0
        self._worker_queue = queue.Queue()
        self._num_workers = num_workers

    def _default_collate(self, batch: List) -> np.ndarray:
        if isinstance(batch[0], np.ndarray):
            return np.stack(batch)
        elif isinstance(batch[0], (tuple, list)):
            return tuple(self._default_collate(items) for items in zip(*batch))
        return np.array(batch)

    def __iter__(self) -> Iterator:
        return DataLoader(
            self.dataset,
            batch_size=self.batch_size,
            shuffle=self.shuffle,
            drop_last=self.drop_last,
            collate_fn=self.collate_fn,
        ).__iter__()

    def __len__(self) -> int:
        return DataLoader(
            self.dataset,
            batch_size=self.batch_size,
            shuffle=self.shuffle,
            drop_last=self.drop_last,
        ).__len__()


class Sampler:
    """Base sampler class."""

    def __init__(self, data_source: Dataset):
        self.data_source = data_source

    def __iter__(self) -> Iterator[int]:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError


class SequentialSampler(Sampler):
    """Sequential sampling."""

    def __iter__(self) -> Iterator[int]:
        return iter(range(len(self.data_source)))

    def __len__(self) -> int:
        return len(self.data_source)


class RandomSampler(Sampler):
    """Random sampling without replacement."""

    def __init__(self, data_source: Dataset, replacement: bool = False, num_samples: int = None):
        super().__init__(data_source)
        self.replacement = replacement
        self.num_samples = num_samples or len(data_source)

    def __iter__(self) -> Iterator[int]:
        if self.replacement:
            for _ in range(self.num_samples):
                yield np.random.randint(len(self.data_source))
        else:
            for idx in np.random.permutation(len(self.data_source)):
                yield idx

    def __len__(self) -> int:
        return self.num_samples


class WeightedRandomSampler(Sampler):
    """Weighted random sampling."""

    def __init__(self, weights: Sequence[float], num_samples: int, replacement: bool = True):
        self.weights = np.array(weights)
        self.num_samples = num_samples
        self.replacement = replacement

    def __iter__(self) -> Iterator[int]:
        indices = np.random.choice(len(self.weights), self.num_samples, replace=self.replacement, p=self.weights / self.weights.sum())
        for idx in indices:
            yield idx

    def __len__(self) -> int:
        return self.num_samples


class BatchSampler(Sampler):
    """Yields batches of indices."""

    def __init__(self, sampler: Sampler, batch_size: int, drop_last: bool):
        self.sampler = sampler
        self.batch_size = batch_size
        self.drop_last = drop_last

    def __iter__(self) -> Iterator[List[int]]:
        batch = []
        for idx in self.sampler:
            batch.append(idx)
            if len(batch) == self.batch_size:
                yield batch
                batch = []
        if batch and not self.drop_last:
            yield batch

    def __len__(self) -> int:
        if self.drop_last:
            return len(self.sampler) // self.batch_size
        return (len(self.sampler) + self.batch_size - 1) // self.batch_size


def random_split(
    dataset: Dataset, lengths: Sequence[int], generator: np.random.Generator = None
) -> List[Subset]:
    """
    Randomly split a dataset.

    Args:
        dataset: Dataset to split
        lengths: List of split sizes
        generator: Random generator

    Returns:
        List of dataset subsets
    """
    if generator is None:
        generator = np.random.default_rng()
    n = len(dataset)
    indices = generator.permutation(n).tolist()
    splits = []
    start = 0
    for length in lengths:
        splits.append(Subset(dataset, indices[start : start + length]))
        start += length
    return splits


class CacheDataset(Dataset):
    """Dataset that caches all items in memory."""

    def __init__(self, dataset: Dataset):
        self.dataset = dataset
        self._cache = [None] * len(dataset)

    def __len__(self) -> int:
        return len(self.dataset)

    def __getitem__(self, idx: int) -> Any:
        if self._cache[idx] is None:
            self._cache[idx] = self.dataset[idx]
        return self._cache[idx]


class TransformDataset(Dataset):
    """Dataset with per-sample transforms."""

    def __init__(self, dataset: Dataset, transform: Callable):
        self.dataset = dataset
        self.transform = transform

    def __len__(self) -> int:
        return len(self.dataset)

    def __getitem__(self, idx: int) -> Any:
        item = self.dataset[idx]
        if isinstance(item, tuple):
            return (self.transform(item[0]),) + item[1:]
        return self.transform(item)


class PrefetchLoader:
    """DataLoader with background prefetching."""

    def __init__(
        self,
        loader: DataLoader,
        num_prefetch: int = 2,
    ):
        self.loader = loader
        self.num_prefetch = num_prefetch
        self._queue = queue.Queue(maxsize=num_prefetch)
        self._thread = None

    def _prefetch_worker(self):
        for batch in self.loader:
            self._queue.put(batch)
        self._queue.put(None)

    def __iter__(self) -> Iterator:
        self._thread = threading.Thread(target=self._prefetch_worker)
        self._thread.daemon = True
        self._thread.start()
        while True:
            batch = self._queue.get()
            if batch is None:
                break
            yield batch

    def __len__(self) -> int:
        return len(self.loader)
