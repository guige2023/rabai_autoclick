"""Data Shuffling Action.

Shuffles and randomizes data for ML training, privacy, and sampling.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass
import random


T = TypeVar("T")


@dataclass
class ShuffleConfig:
    seed: Optional[int] = None
    preserve_order: Optional[List[str]] = None
    group_by: Optional[str] = None


class DataShufflingAction:
    """Shuffles and randomizes data."""

    def __init__(self, config: Optional[ShuffleConfig] = None) -> None:
        self.config = config or ShuffleConfig()
        if self.config.seed is not None:
            random.seed(self.config.seed)

    def shuffle(self, data: List[T], seed: Optional[int] = None) -> List[T]:
        if seed is not None:
            rng = random.Random(seed)
            result = list(data)
            rng.shuffle(result)
            return result
        result = list(data)
        random.shuffle(result)
        return result

    def split(
        self,
        data: List[T],
        ratios: List[float],
        seed: Optional[int] = None,
    ) -> Tuple[List[T], ...]:
        if not ratios or sum(ratios) == 0:
            raise ValueError("Ratios must be non-empty and sum to > 0")
        normalized = [r / sum(ratios) for r in ratios]
        shuffled = self.shuffle(data, seed=seed)
        boundaries = [0]
        for r in normalized:
            boundaries.append(boundaries[-1] + int(len(shuffled) * r))
        boundaries[-1] = len(shuffled)
        return tuple(shuffled[boundaries[i]:boundaries[i+1]] for i in range(len(boundaries)-1))

    def k_fold_split(
        self,
        data: List[T],
        k: int = 5,
        seed: Optional[int] = None,
    ) -> List[Tuple[List[T], List[T]]]:
        if k <= 1:
            raise ValueError("k must be > 1")
        shuffled = self.shuffle(data, seed=seed)
        fold_size = len(shuffled) // k
        folds = []
        for i in range(k):
            start = i * fold_size
            end = start + fold_size if i < k - 1 else len(shuffled)
            val_set = shuffled[start:end]
            train_set = shuffled[:start] + shuffled[end:]
            folds.append((train_set, val_set))
        return folds

    def bootstrap_sample(
        self,
        data: List[T],
        n_samples: Optional[int] = None,
        seed: Optional[int] = None,
    ) -> List[T]:
        size = n_samples or len(data)
        rng = random.Random(seed)
        return [rng.choice(data) for _ in range(size)]
