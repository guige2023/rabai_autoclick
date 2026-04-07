"""
Cross-validation utilities.

Provides various cross-validation strategies and helpers
for model evaluation and hyperparameter tuning.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, Tuple, TypeVar

import numpy as np

T = TypeVar("T")


def k_fold_split(
    n_samples: int, n_splits: int = 5, shuffle: bool = True, random_state: int = None
) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Generate K-fold splits.

    Args:
        n_samples: Number of samples
        n_splits: Number of folds
        shuffle: Whether to shuffle data
        random_state: Random seed

    Returns:
        List of (train_indices, val_indices) tuples

    Example:
        >>> splits = k_fold_split(100, n_splits=5)
        >>> len(splits)
        5
    """
    if random_state is not None:
        np.random.seed(random_state)
    indices = np.arange(n_samples)
    if shuffle:
        indices = np.random.permutation(indices)
    fold_sizes = np.full(n_splits, n_samples // n_splits, dtype=int)
    fold_sizes[: n_samples % n_splits] += 1
    splits = []
    current = 0
    for size in fold_sizes:
        start, stop = current, current + size
        val_idx = indices[start:stop]
        train_idx = np.concatenate([indices[:start], indices[stop:]])
        splits.append((train_idx, val_idx))
        current = stop
    return splits


def stratified_k_fold_split(
    y: np.ndarray, n_splits: int = 5, shuffle: bool = True, random_state: int = None
) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Generate stratified K-fold splits preserving class distribution.

    Args:
        y: Class labels
        n_splits: Number of folds
        shuffle: Whether to shuffle
        random_state: Random seed

    Returns:
        List of (train_indices, val_indices) tuples
    """
    if random_state is not None:
        np.random.seed(random_state)
    classes, y_indices = np.unique(y, return_inverse=True)
    n_classes = len(classes)
    class_indices = [np.where(y == c)[0] for c in classes]
    for idx in class_indices:
        np.random.shuffle(idx)
    fold_class_indices = [[] for _ in range(n_splits)]
    for c_indices in class_indices:
        for i, idx in enumerate(c_indices):
            fold_class_indices[i % n_splits].append(idx)
    splits = []
    for fold_idx in range(n_splits):
        val_idx = np.array(fold_class_indices[fold_idx])
        train_idx = np.concatenate([fold_class_indices[i] for i in range(n_splits) if i != fold_idx])
        splits.append((train_idx, val_idx))
    return splits


def leave_one_out_split(n_samples: int) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Generate leave-one-out splits.

    Returns:
        List of (train_indices, val_indices) where val has 1 sample
    """
    indices = np.arange(n_samples)
    splits = []
    for i in range(n_samples):
        val_idx = np.array([i])
        train_idx = np.concatenate([indices[:i], indices[i + 1 :]])
        splits.append((train_idx, val_idx))
    return splits


def group_k_fold_split(
    groups: np.ndarray, n_splits: int = 5, shuffle: bool = True, random_state: int = None
) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Generate group K-fold splits ensuring same group not in train and val.

    Args:
        groups: Group labels for each sample
        n_splits: Number of folds
        shuffle: Whether to shuffle
        random_state: Random seed

    Returns:
        List of (train_indices, val_indices) tuples
    """
    if random_state is not None:
        np.random.seed(random_state)
    unique_groups = np.unique(groups)
    if shuffle:
        np.random.shuffle(unique_groups)
    n_groups = len(unique_groups)
    fold_sizes = np.full(n_splits, n_groups // n_splits, dtype=int)
    fold_sizes[: n_groups % n_splits] += 1
    group_folds = []
    current = 0
    for size in fold_sizes:
        group_folds.append(unique_groups[current : current + size])
        current += size
    splits = []
    for fold_idx in range(n_splits):
        val_groups = set(group_folds[fold_idx])
        val_idx = np.where(np.isin(groups, list(val_groups)))[0]
        train_idx = np.where(~np.isin(groups, list(val_groups)))[0]
        splits.append((train_idx, val_idx))
    return splits


def time_series_split(
    n_samples: int, n_splits: int = 5, gap: int = 0
) -> List[Tuple[np.ndarray, np.ndarray]]:
    """
    Generate time series cross-validation splits.

    Args:
        n_samples: Number of samples
        n_splits: Number of folds
        gap: Gap between train and validation

    Returns:
        List of (train_indices, val_indices) tuples
    """
    train_size = int(n_samples * 0.6)
    test_size = int(n_samples * 0.2)
    splits = []
    for i in range(n_splits):
        train_end = train_size + i * test_size // n_splits
        val_start = train_end + gap
        val_end = val_start + test_size
        if val_end > n_samples:
            break
        train_idx = np.arange(0, train_end)
        val_idx = np.arange(val_start, val_end)
        splits.append((train_idx, val_idx))
    return splits


class CrossValidator:
    """Cross-validation executor."""

    def __init__(
        self,
        estimator: Any,
        cv: int = 5,
        scoring: Callable[[np.ndarray, np.ndarray], float] = None,
        verbose: int = 0,
    ):
        self.estimator = estimator
        self.cv = cv
        self.scoring = scoring or (lambda y_true, y_pred: np.mean(y_true == y_pred))
        self.verbose = verbose
        self.results_ = []

    def fit(
        self, X: np.ndarray, y: np.ndarray, groups: np.ndarray = None
    ) -> "CrossValidator":
        """Run cross-validation."""
        if groups is not None:
            splits = group_k_fold_split(groups, self.cv)
        else:
            splits = k_fold_split(len(X), self.cv)
        self.results_ = []
        for fold_idx, (train_idx, val_idx) in enumerate(splits):
            X_train, X_val = X[train_idx], X[val_idx]
            y_train, y_val = y[train_idx], y[val_idx]
            model = self._clone_estimator()
            model.fit(X_train, y_train)
            y_pred = model.predict(X_val)
            score = self.scoring(y_val, y_pred)
            self.results_.append({"fold": fold_idx, "score": score, "train_idx": train_idx, "val_idx": val_idx})
            if self.verbose > 0:
                print(f"Fold {fold_idx}: {score:.4f}")
        return self

    def _clone_estimator(self) -> Any:
        """Clone the estimator."""
        import copy
        return copy.deepcopy(self.estimator)

    def get_results(self) -> Dict[str, Any]:
        """Get cross-validation results."""
        scores = [r["score"] for r in self.results_]
        return {
            "scores": scores,
            "mean": np.mean(scores),
            "std": np.std(scores),
            "min": np.min(scores),
            "max": np.max(scores),
        }


class RepeatedCrossValidator:
    """Repeated cross-validation for more robust estimates."""

    def __init__(
        self,
        estimator: Any,
        cv: int = 5,
        n_repeats: int = 3,
        scoring: Callable = None,
        random_state: int = None,
    ):
        self.estimator = estimator
        self.cv = cv
        self.n_repeats = n_repeats
        self.scoring = scoring
        self.random_state = random_state

    def fit(self, X: np.ndarray, y: np.ndarray) -> "RepeatedCrossValidator":
        """Run repeated cross-validation."""
        self.results_ = []
        for repeat in range(self.n_repeats):
            seed = None if self.random_state is None else self.random_state + repeat
            cv = CrossValidator(self.estimator, self.cv, self.scoring)
            cv.fit(X, y)
            repeat_results = cv.get_results()
            repeat_results["repeat"] = repeat
            self.results_.append(repeat_results)
        return self

    def get_results(self) -> Dict[str, float]:
        """Get aggregated results across repeats."""
        all_scores = [r["mean"] for r in self.results_]
        return {
            "overall_mean": np.mean(all_scores),
            "overall_std": np.std(all_scores),
            "repeat_means": all_scores,
        }


def bootstrap_sample(
    X: np.ndarray, y: np.ndarray = None, n_samples: int = None, random_state: int = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Generate bootstrap sample.

    Args:
        X: Feature matrix
        y: Labels
        n_samples: Number of samples (default: len(X))
        random_state: Random seed

    Returns:
        Tuple of (X_boot, y_boot) or (X_boot,) if y is None
    """
    if random_state is not None:
        np.random.seed(random_state)
    n = n_samples or len(X)
    indices = np.random.choice(len(X), n, replace=True)
    if y is None:
        return X[indices]
    return X[indices], y[indices]


def out_of_bag_score(
    model_factory: Callable, X: np.ndarray, y: np.ndarray, n_estimators: int = 100
) -> float:
    """
    Compute out-of-bag score for bagging models.

    Args:
        model_factory: Function that creates a new model instance
        X: Feature matrix
        y: Labels
        n_estimators: Number of bootstrap iterations

    Returns:
        OOB accuracy score
    """
    n_samples = len(X)
    oob_predictions = np.zeros(n_samples, dtype=int)
    oob_counts = np.zeros(n_samples)
    for _ in range(n_estimators):
        indices = np.random.choice(n_samples, n_samples, replace=True)
        oob_idx = np.setdiff1d(np.arange(n_samples), np.unique(indices))
        if len(oob_idx) == 0:
            continue
        X_boot, y_boot = X[indices], y[indices]
        model = model_factory()
        model.fit(X_boot, y_boot)
        predictions = model.predict(X[oob_idx])
        oob_predictions[oob_idx] = predictions
        oob_counts[oob_idx] += 1
    mask = oob_counts > 0
    if mask.sum() == 0:
        return 0.0
    accuracy = (oob_predictions[mask] == y[mask]).mean()
    return float(accuracy)


def nested_cv_estimate(
    X: np.ndarray,
    y: np.ndarray,
    inner_cv: int = 5,
    outer_cv: int = 5,
    model_factory: Callable = None,
) -> Tuple[float, float]:
    """
    Nested cross-validation for unbiased performance estimate.

    Returns:
        Tuple of (mean_score, std_score)
    """
    outer_splits = k_fold_split(len(X), outer_cv)
    outer_scores = []
    for train_idx, val_idx in outer_splits:
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]
        inner_cv_obj = CrossValidator(model_factory(), inner_cv)
        inner_cv_obj.fit(X_train, y_train)
        best_score = max(inner_cv_obj.results_, key=lambda x: x["score"])["score"]
        outer_scores.append(best_score)
    return float(np.mean(outer_scores)), float(np.std(outer_scores))
