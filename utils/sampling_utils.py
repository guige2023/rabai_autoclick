"""
Sampling and resampling utilities.

Provides random sampling, stratified sampling, reservoir sampling,
bootstrap, and Latin hypercube sampling.
"""

from __future__ import annotations

import math
import random
from typing import Any, Callable


def random_sample(items: list, k: int, seed: int | None = None) -> list:
    """
    Random sample of k items without replacement.

    Args:
        items: Population
        k: Sample size
        seed: Random seed

    Returns:
        List of k randomly selected items.
    """
    if k >= len(items):
        return list(items)
    rng = random.Random(seed)
    return rng.sample(items, k)


def bootstrap_sample(
    data: list,
    n_samples: int | None = None,
    seed: int | None = None,
) -> list:
    """
    Bootstrap sampling with replacement.

    Args:
        data: Original dataset
        n_samples: Number of samples (default: len(data))
        seed: Random seed

    Returns:
        Bootstrap sample.
    """
    n = n_samples if n_samples is not None else len(data)
    rng = random.Random(seed)
    return [rng.choice(data) for _ in range(n)]


def stratified_sample(
    data: list,
    strata: list[str],
    ratios: dict[str, float] | None = None,
    seed: int | None = None,
) -> list:
    """
    Stratified sampling: sample proportionally from strata.

    Args:
        data: Population
        strata: Stratum labels for each item
        ratios: Target ratios per stratum (default: proportional to population)
        seed: Random seed

    Returns:
        Stratified sample.
    """
    if len(data) != len(strata):
        raise ValueError("data and strata must have same length")
    strata_groups: dict[str, list] = {}
    for item, label in zip(data, strata):
        if label not in strata_groups:
            strata_groups[label] = []
        strata_groups[label].append(item)

    rng = random.Random(seed)
    result = []
    total = len(data)

    if ratios is None:
        # Proportional allocation
        for label, group in strata_groups.items():
            n = round(len(group) / total * len(data))
            result.extend(rng.sample(group, min(n, len(group))))
    else:
        for label, group in strata_groups.items():
            ratio = ratios.get(label, 1.0 / len(strata_groups))
            n = round(ratio * len(data))
            result.extend(rng.sample(group, min(n, len(group))))

    return result


def reservoir_sample(data: list, k: int, seed: int | None = None) -> list:
    """
    Algorithm R reservoir sampling.

    Selects k items uniformly at random from a stream of unknown size.

    Args:
        data: Stream (list) of items
        k: Sample size
        seed: Random seed

    Returns:
        Sample of k items.
    """
    if k >= len(data):
        return list(data)
    rng = random.Random(seed)
    reservoir: list = data[:k]
    for i in range(k, len(data)):
        j = rng.randint(0, i)
        if j < k:
            reservoir[j] = data[i]
    return reservoir


def weighted_sample(
    items: list,
    weights: list[float],
    k: int = 1,
    seed: int | None = None,
) -> list:
    """
    Weighted random sampling (with replacement).

    Args:
        items: Population
        weights: Weights (must be positive, will be normalized)
        k: Number of samples
        seed: Random seed

    Returns:
        List of k sampled items.
    """
    if len(items) != len(weights):
        raise ValueError("items and weights must have same length")
    rng = random.Random(seed)
    total = sum(weights)
    if total <= 0:
        raise ValueError("weights must sum to positive value")
    cumsum = 0.0
    cumulative = [0.0]
    for w in weights:
        cumsum += w
        cumulative.append(cumsum)
    result = []
    for _ in range(k):
        r = rng.random() * total
        for i in range(len(cumulative) - 1):
            if cumulative[i] <= r < cumulative[i + 1]:
                result.append(items[i])
                break
    return result


def latin_hypercube_sample(
    dimensions: int,
    n_samples: int,
    seed: int | None = None,
) -> list[list[float]]:
    """
    Latin Hypercube Sampling (LHS).

    Each dimension is divided into n equal intervals, and samples
    are placed so that each interval is used exactly once.

    Args:
        dimensions: Number of dimensions
        n_samples: Number of samples
        seed: Random seed

    Returns:
        List of sample points (each point is a list of floats).
    """
    rng = random.Random(seed)
    samples: list[list[float]] = []
    intervals = 1.0 / n_samples
    for dim in range(dimensions):
        breakpoints = [rng.random() * intervals + i * intervals for i in range(n_samples)]
        samples.append(breakpoints)
    # Shuffle each dimension independently
    result: list[list[float]] = [[] for _ in range(n_samples)]
    for dim_samples in samples:
        rng.shuffle(dim_samples)
        for i, val in enumerate(dim_samples):
            result[i].append(val)
    return result


def sobol_sequence(index: int, dimension: int) -> list[float]:
    """
    Sobol low-discrepancy sequence (van der Corput based).

    Args:
        index: Sample index (1-based)
        dimension: Dimension

    Returns:
        Vector of quasi-random values.
    """
    # Direction numbers for Sobol sequence (simplified for small dimensions)
    sobol_dirs = [
        [],  # dim 0 unused
        [1],
        [1, 3],
        [1, 3, 5],
        [1, 3, 5, 7],
        [1, 3, 5, 7, 11],
        [1, 3, 5, 7, 11, 13],
        [1, 3, 5, 7, 11, 13, 15],
        [1, 3, 5, 7, 11, 13, 15, 17],
    ]
    result = []
    for d in range(1, dimension + 1):
        if d > len(sobol_dirs) - 1:
            d = len(sobol_dirs) - 1
        direction = sobol_dirs[d]
        if index == 0:
            result.append(0.0)
            continue
        binary = bin(index)[2:]
        val = 0.0
        for i, bit in enumerate(reversed(binary)):
            if bit == "1" and i < len(direction):
                val += direction[i] / (2 ** (i + 1))
        result.append(val)
    return result


def stratified_bootstrap_ci(
    statistic_fn: Callable[[list], float],
    data: list,
    n_bootstrap: int = 1000,
    confidence: float = 0.95,
    seed: int | None = None,
) -> tuple[float, float]:
    """
    Stratified bootstrap confidence interval.

    Args:
        statistic_fn: Function that computes the statistic
        data: Dataset
        n_bootstrap: Number of bootstrap iterations
        confidence: Confidence level

    Returns:
        Tuple of (lower, upper) bounds.
    """
    rng = random.Random(seed)
    estimates = []
    for _ in range(n_bootstrap):
        sample = [rng.choice(data) for _ in range(len(data))]
        estimates.append(statistic_fn(sample))
    estimates.sort()
    alpha = (1 - confidence) / 2
    lower_idx = int(alpha * n_bootstrap)
    upper_idx = int((1 - alpha) * n_bootstrap)
    return estimates[lower_idx], estimates[upper_idx]


def leave_one_out_cross_validate(
    model_fn: Callable[[list, list], float],
    X: list,
    y: list,
) -> list[float]:
    """
    Leave-One-Out cross-validation.

    Args:
        model_fn: Function that takes (X_train, y_train) and returns a score
        X: Feature matrix
        y: Labels

    Returns:
        List of per-fold scores.
    """
    n = len(X)
    scores = []
    for i in range(n):
        X_train = X[:i] + X[i + 1:]
        y_train = y[:i] + y[i + 1:]
        score = model_fn(X_train, y_train)
        scores.append(score)
    return scores


def k_fold_cross_validate(
    model_fn: Callable[[list, list], float],
    X: list,
    y: list,
    k: int = 5,
    seed: int | None = None,
) -> tuple[list[float], list[float]]:
    """
    K-fold cross-validation.

    Args:
        model_fn: Function that takes (X_train, y_train) and returns score
        X: Feature matrix
        y: Labels
        k: Number of folds
        seed: Random seed

    Returns:
        Tuple of (train_scores, test_scores).
    """
    rng = random.Random(seed)
    indices = list(range(len(X)))
    rng.shuffle(indices)
    fold_size = len(X) // k
    train_scores = []
    test_scores = []
    for fold in range(k):
        start = fold * fold_size
        end = start + fold_size if fold < k - 1 else len(X)
        test_idx = indices[start:end]
        train_idx = indices[:start] + indices[end:]
        X_train = [X[i] for i in train_idx]
        y_train = [y[i] for i in train_idx]
        X_test = [X[i] for i in test_idx]
        y_test = [y[i] for i in test_idx]
        test_scores.append(model_fn(X_test, y_test))
    return train_scores, test_scores
