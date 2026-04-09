"""Data Splitter Action Module.

Provides data splitting capabilities for dividing datasets
into train/test/validation splits with stratification support.

Example:
    >>> from actions.data.data_splitter_action import DataSplitterAction
    >>> action = DataSplitterAction()
    >>> train, test = action.split(data, test_size=0.2)
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import threading


class SplitType(Enum):
    """Split type strategies."""
    RANDOM = "random"
    STRATIFIED = "stratified"
    ORDERED = "ordered"
    TIME_BASED = "time_based"


@dataclass
class Split:
    """Data split result.
    
    Attributes:
        name: Split name (train/test/val)
        data: Split data
        indices: Original indices
        size: Number of items
    """
    name: str
    data: List[Any]
    indices: List[int]
    size: int


@dataclass
class SplitterConfig:
    """Configuration for data splitting.
    
    Attributes:
        split_type: Type of split
        test_size: Test set proportion
        val_size: Validation set proportion
        shuffle: Shuffle before splitting
        seed: Random seed
        stratify_key: Key function for stratification
    """
    split_type: SplitType = SplitType.RANDOM
    test_size: float = 0.2
    val_size: float = 0.0
    shuffle: bool = True
    seed: Optional[int] = None
    stratify_key: Optional[Callable[[Any], Any]] = None


@dataclass
class SplittingResult:
    """Result of splitting operation.
    
    Attributes:
        splits: List of splits
        original_size: Original dataset size
        total_split_size: Total size of all splits
    """
    splits: List[Split]
    original_size: int
    total_split_size: int
    duration: float = 0.0


class DataSplitterAction:
    """Data splitter for machine learning workflows.
    
    Provides train/test/validation splitting with
    random, stratified, and ordered strategies.
    
    Attributes:
        config: Splitter configuration
        _rng: Random number generator
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[SplitterConfig] = None,
    ) -> None:
        """Initialize splitter action.
        
        Args:
            config: Splitter configuration
        """
        self.config = config or SplitterConfig()
        self._rng = random.Random(self.config.seed)
        self._lock = threading.Lock()
    
    def split(
        self,
        data: List[Any],
        test_size: Optional[float] = None,
        val_size: Optional[float] = None,
        stratify_key: Optional[Callable[[Any], Any]] = None,
    ) -> SplittingResult:
        """Split data into train/test/val.
        
        Args:
            data: Data to split
            test_size: Test set proportion
            val_size: Validation set proportion
            stratify_key: Stratification key function
        
        Returns:
            SplittingResult
        """
        import time
        start = time.time()
        
        test_size = test_size if test_size is not None else self.config.test_size
        val_size = val_size if val_size is not None else self.config.val_size
        
        indices = list(range(len(data)))
        
        if self.config.shuffle:
            self._rng.shuffle(indices)
        
        if stratify_key is None and self.config.stratify_key:
            stratify_key = self.config.stratify_key
        
        if stratify_key and self.config.split_type != SplitType.STRATIFIED:
            return self._split_stratified(
                data, indices, test_size, val_size, stratify_key
            )
        
        return self._split_simple(
            data, indices, test_size, val_size, start
        )
    
    def _split_simple(
        self,
        data: List[Any],
        indices: List[int],
        test_size: float,
        val_size: float,
        start: float,
    ) -> SplittingResult:
        """Perform simple random split.
        
        Args:
            data: Data to split
            indices: Shuffled indices
            test_size: Test proportion
            val_size: Val proportion
            start: Start time
        
        Returns:
            SplittingResult
        """
        n = len(data)
        test_count = max(1, int(n * test_size))
        val_count = max(0, int(n * val_size))
        
        test_indices = indices[:test_count]
        val_indices = indices[test_count:test_count + val_count]
        train_indices = indices[test_count + val_count:]
        
        train_data = [data[i] for i in train_indices]
        test_data = [data[i] for i in test_indices]
        val_data = [data[i] for i in val_indices]
        
        splits = [
            Split("train", train_data, train_indices, len(train_data)),
        ]
        
        if val_data:
            splits.append(Split("val", val_data, val_indices, len(val_data)))
        
        splits.append(Split("test", test_data, test_indices, len(test_data)))
        
        total_size = sum(s.size for s in splits)
        
        return SplittingResult(
            splits=splits,
            original_size=n,
            total_split_size=total_size,
            duration=time.time() - start,
        )
    
    def _split_stratified(
        self,
        data: List[Any],
        indices: List[int],
        test_size: float,
        val_size: float,
        stratify_key: Callable[[Any], Any],
    ) -> SplittingResult:
        """Perform stratified split.
        
        Args:
            data: Data to split
            indices: Shuffled indices
            test_size: Test proportion
            val_size: Val proportion
            stratify_key: Stratification key function
        
        Returns:
            SplittingResult
        """
        import time
        start = time.time()
        
        strata: Dict[Any, List[int]] = {}
        for idx in indices:
            key = stratify_key(data[idx])
            if key not in strata:
                strata[key] = []
            strata[key].append(idx)
        
        train_indices: List[int] = []
        val_indices: List[int] = []
        test_indices: List[int] = []
        
        for key, stratum_indices in strata.items():
            n = len(stratum_indices)
            test_count = max(1, int(n * test_size))
            val_count = max(0, int(n * val_size))
            
            test_indices.extend(stratum_indices[:test_count])
            val_indices.extend(stratum_indices[test_count:test_count + val_count])
            train_indices.extend(stratum_indices[test_count + val_count:])
        
        train_data = [data[i] for i in train_indices]
        test_data = [data[i] for i in test_indices]
        val_data = [data[i] for i in val_indices]
        
        splits = [
            Split("train", train_data, train_indices, len(train_data)),
        ]
        
        if val_data:
            splits.append(Split("val", val_data, val_indices, len(val_data)))
        
        splits.append(Split("test", test_data, test_indices, len(test_data)))
        
        total_size = sum(s.size for s in splits)
        
        return SplittingResult(
            splits=splits,
            original_size=len(data),
            total_split_size=total_size,
            duration=time.time() - start,
        )
    
    def k_fold_split(
        self,
        data: List[Any],
        k: int = 5,
        stratify_key: Optional[Callable[[Any], Any]] = None,
    ) -> List[Tuple[Split, Split]]:
        """Create K-fold cross-validation splits.
        
        Args:
            data: Data to split
            k: Number of folds
            stratify_key: Stratification key function
        
        Returns:
            List of (train, test) split tuples
        """
        indices = list(range(len(data)))
        
        if self.config.shuffle:
            self._rng.shuffle(indices)
        
        fold_size = len(data) // k
        folds: List[List[int]] = []
        
        for i in range(k):
            start = i * fold_size
            end = start + fold_size if i < k - 1 else len(data)
            folds.append(indices[start:end])
        
        results: List[Tuple[Split, Split]] = []
        
        for i in range(k):
            test_indices = folds[i]
            train_indices = [idx for j, fold in enumerate(folds) if j != i for idx in fold]
            
            train_data = [data[i] for i in train_indices]
            test_data = [data[i] for i in test_indices]
            
            results.append((
                Split("train", train_data, train_indices, len(train_data)),
                Split("test", test_data, test_indices, len(test_data)),
            ))
        
        return results
    
    def time_based_split(
        self,
        data: List[Any],
        time_key: Callable[[Any], float],
        train_ratio: float = 0.8,
    ) -> Tuple[Split, Split]:
        """Split data based on time ordering.
        
        Args:
            data: Data to split
            time_key: Function to extract timestamp
            train_ratio: Training set proportion
        
        Returns:
            (train, test) splits
        """
        sorted_data = sorted(enumerate(data), key=lambda x: time_key(x[1]))
        
        split_idx = int(len(sorted_data) * train_ratio)
        
        train_items = [(idx, item) for idx, item in sorted_data[:split_idx]]
        test_items = [(idx, item) for idx, item in sorted_data[split_idx:]]
        
        train_indices = [idx for idx, _ in train_items]
        test_indices = [idx for idx, _ in test_items]
        
        return (
            Split("train", [item for _, item in train_items], train_indices, len(train_items)),
            Split("test", [item for _, item in test_items], test_indices, len(test_items)),
        )
