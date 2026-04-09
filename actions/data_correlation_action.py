"""Data Correlation Action module.

Computes correlations between data streams and fields.
Supports Pearson, Spearman, Kendall correlation coefficients,
and cross-correlation for time series.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np


@dataclass
class CorrelationResult:
    """Result of correlation computation."""

    coefficient: float
    p_value: float
    method: str
    n_observations: int
    significant: bool = False
    confidence_level: float = 0.95


def pearson_correlation(x: list[float], y: list[float]) -> CorrelationResult:
    """Compute Pearson correlation coefficient.

    Args:
        x: First variable
        y: Second variable

    Returns:
        CorrelationResult with coefficient and p-value
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")

    n = len(x)
    if n < 3:
        return CorrelationResult(
            coefficient=0.0,
            p_value=1.0,
            method="pearson",
            n_observations=n,
        )

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))

    sum_sq_x = sum((xi - mean_x) ** 2 for xi in x)
    sum_sq_y = sum((yi - mean_y) ** 2 for yi in y)

    denominator = math.sqrt(sum_sq_x * sum_sq_y)

    if denominator == 0:
        return CorrelationResult(
            coefficient=0.0,
            p_value=1.0,
            method="pearson",
            n_observations=n,
        )

    r = numerator / denominator

    t = r * math.sqrt((n - 2) / (1 - r * r)) if abs(r) < 1 else 0.0

    from scipy import stats

    p_value = 2 * (1 - stats.t.cdf(abs(t), n - 2)) if n > 2 else 1.0

    return CorrelationResult(
        coefficient=r,
        p_value=p_value,
        method="pearson",
        n_observations=n,
        significant=p_value < 0.05,
    )


def spearman_correlation(x: list[float], y: list[float]) -> CorrelationResult:
    """Compute Spearman rank correlation.

    Args:
        x: First variable
        y: Second variable

    Returns:
        CorrelationResult with coefficient and p-value
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")

    n = len(x)
    if n < 3:
        return CorrelationResult(
            coefficient=0.0,
            p_value=1.0,
            method="spearman",
            n_observations=n,
        )

    def rank(data: list[float]) -> list[float]:
        sorted_indices = sorted(range(len(data)), key=lambda i: data[i])
        ranks = [0] * len(data)
        for rank_val, idx in enumerate(sorted_indices, 1):
            ranks[idx] = rank_val
        return ranks

    rank_x = rank(list(x))
    rank_y = rank(list(y))

    return pearson_correlation(rank_x, rank_y)


def cross_correlation(
    x: list[float],
    y: list[float],
    max_lag: Optional[int] = None,
) -> tuple[list[float], int]:
    """Compute cross-correlation between two time series.

    Args:
        x: First time series
        y: Second time series
        max_lag: Maximum lag to consider

    Returns:
        Tuple of (correlation values by lag, best lag)
    """
    if len(x) != len(y):
        raise ValueError("x and y must have same length")

    n = len(x)
    if max_lag is None:
        max_lag = n - 1

    max_lag = min(max_lag, n - 1)

    correlations = []
    best_lag = 0
    best_corr = -1.0

    for lag in range(-max_lag, max_lag + 1):
        if lag < 0:
            xi = x[:lag]
            yi = y[-lag:]
        elif lag > 0:
            xi = x[lag:]
            yi = y[:-lag]
        else:
            xi = x
            yi = y

        if len(xi) < 2:
            correlations.append(0.0)
            continue

        result = pearson_correlation(xi, yi)
        correlations.append(result.coefficient)

        if abs(result.coefficient) > abs(best_corr):
            best_corr = result.coefficient
            best_lag = lag

    return correlations, best_lag


@dataclass
class CorrelationMatrix:
    """Correlation matrix for multiple variables."""

    matrix: list[list[float]]
    variables: list[str]
    method: str = "pearson"

    def get(self, var1: str, var2: str) -> Optional[float]:
        """Get correlation between two variables."""
        try:
            i = self.variables.index(var1)
            j = self.variables.index(var2)
            return self.matrix[i][j]
        except (ValueError, IndexError):
            return None

    def to_dict(self) -> dict[str, dict[str, float]]:
        """Convert to nested dictionary."""
        return {
            self.variables[i]: {
                self.variables[j]: self.matrix[i][j]
                for j in range(len(self.variables))
            }
            for i in range(len(self.variables))
        }


def compute_correlation_matrix(
    data: dict[str, list[float]],
    method: str = "pearson",
) -> CorrelationMatrix:
    """Compute correlation matrix for multiple variables.

    Args:
        data: Dictionary mapping variable names to their values
        method: Correlation method ('pearson' or 'spearman')

    Returns:
        CorrelationMatrix
    """
    variables = list(data.keys())
    n_vars = len(variables)
    matrix = [[0.0] * n_vars for _ in range(n_vars)]

    for i in range(n_vars):
        for j in range(n_vars):
            if i == j:
                matrix[i][j] = 1.0
            elif j > i:
                xi = data[variables[i]]
                xj = data[variables[j]]

                min_len = min(len(xi), len(xj))
                xi = xi[:min_len]
                xj = xj[:min_len]

                if method == "pearson":
                    result = pearson_correlation(xi, xj)
                elif method == "spearman":
                    result = spearman_correlation(xi, xj)
                else:
                    raise ValueError(f"Unknown method: {method}")

                matrix[i][j] = result.coefficient
                matrix[j][i] = result.coefficient

    return CorrelationMatrix(matrix=matrix, variables=variables, method=method)


@dataclass
class LagAnalysis:
    """Time lag analysis between two series."""

    best_lag: int
    best_correlation: float
    correlations_by_lag: list[float]
    lag_range: tuple[int, int]


def find_optimal_lag(
    x: list[float],
    y: list[float],
    max_lag: int = 10,
) -> LagAnalysis:
    """Find optimal time lag between two series.

    Args:
        x: First series
        y: Second series
        max_lag: Maximum lag to search

    Returns:
        LagAnalysis with results
    """
    correlations, best_lag = cross_correlation(x, y, max_lag)

    return LagAnalysis(
        best_lag=best_lag,
        best_correlation=correlations[max_lag] if len(correlations) > max_lag else 0.0,
        correlations_by_lag=correlations,
        lag_range=(-max_lag, max_lag),
    )
