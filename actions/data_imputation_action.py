"""Data Imputation Action module.

Provides missing value imputation strategies for datasets
including mean, median, mode, forward fill, backward fill,
KNN, and regression-based imputation.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import numpy as np


@dataclass
class ImputationResult:
    """Result of imputation operation."""

    data: list[dict[str, Any]]
    imputed_fields: dict[str, int]
    method: str
    details: dict[str, Any] = field(default_factory=dict)


def mean_impute(data: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    """Impute missing values with mean.

    Args:
        data: Input data
        field_name: Field to impute

    Returns:
        Data with imputed values
    """
    values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
    if not values:
        return data

    mean_val = sum(values) / len(values)
    result = [dict(d) for d in data]

    for record in result:
        if field_name not in record or record[field_name] is None:
            record[field_name] = mean_val

    return result


def median_impute(data: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    """Impute missing values with median.

    Args:
        data: Input data
        field_name: Field to impute

    Returns:
        Data with imputed values
    """
    values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
    if not values:
        return data

    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 0:
        median_val = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
    else:
        median_val = sorted_values[n // 2]

    result = [dict(d) for d in data]

    for record in result:
        if field_name not in record or record[field_name] is None:
            record[field_name] = median_val

    return result


def mode_impute(data: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    """Impute missing values with mode (most frequent).

    Args:
        data: Input data
        field_name: Field to impute

    Returns:
        Data with imputed values
    """
    values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
    if not values:
        return data

    counter = Counter(values)
    mode_val = counter.most_common(1)[0][0]

    result = [dict(d) for d in data]

    for record in result:
        if field_name not in record or record[field_name] is None:
            record[field_name] = mode_val

    return result


def forward_fill(data: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    """Forward fill missing values.

    Args:
        data: Input data
        field_name: Field to fill

    Returns:
        Data with forward-filled values
    """
    result = [dict(d) for d in data]
    last_valid: Any = None

    for record in result:
        if field_name in record and record[field_name] is not None:
            last_valid = record[field_name]
        elif last_valid is not None:
            record[field_name] = last_valid

    return result


def backward_fill(data: list[dict[str, Any]], field_name: str) -> list[dict[str, Any]]:
    """Backward fill missing values.

    Args:
        data: Input data
        field_name: Field to fill

    Returns:
        Data with backward-filled values
    """
    result = [dict(d) for d in data]
    next_valid: Any = None

    for record in reversed(result):
        if field_name in record and record[field_name] is not None:
            next_valid = record[field_name]
        elif next_valid is not None:
            record[field_name] = next_valid

    return result


def interpolate_linear(
    data: list[dict[str, Any]],
    field_name: str,
    index_field: str = "__index__",
) -> list[dict[str, Any]]:
    """Linear interpolation for missing values.

    Args:
        data: Input data
        field_name: Field to interpolate
        index_field: Field to use for ordering

    Returns:
        Data with interpolated values
    """
    result = [dict(d) for d in data]

    for i, record in enumerate(result):
        if index_field not in record:
            record[index_field] = i

    values = []
    indices = []

    for i, record in enumerate(result):
        if field_name in record and record[field_name] is not None:
            values.append(record[field_name])
            indices.append(record.get(index_field, i))

    if len(values) < 2:
        return result

    for i, record in enumerate(result):
        idx = record.get(index_field, i)

        if field_name not in record or record[field_name] is None:
            if values:
                for j in range(len(indices) - 1):
                    if indices[j] <= idx <= indices[j + 1]:
                        t = (idx - indices[j]) / (indices[j + 1] - indices[j]) if indices[j + 1] != indices[j] else 0
                        record[field_name] = values[j] + t * (values[j + 1] - values[j])
                        break

    return result


def knn_impute(
    data: list[dict[str, Any]],
    field_name: str,
    k: int = 5,
    numeric_fields: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """K-Nearest Neighbors imputation.

    Args:
        data: Input data
        field_name: Field to impute
        k: Number of neighbors
        numeric_fields: Fields to use for distance calculation

    Returns:
        Data with KNN-imputed values
    """
    result = [dict(d) for d in data]

    if numeric_fields is None:
        numeric_fields = [k for k in result[0].keys() if isinstance(result[0].get(k), (int, float))] if result else []
        numeric_fields = [f for f in numeric_fields if f != field_name]

    if not numeric_fields or not data:
        return result

    indices_with_values = [
        (i, d[field_name]) for i, d in enumerate(result)
        if field_name in d and d[field_name] is not None
    ]

    if len(indices_with_values) < k:
        return mean_impute(result, field_name)

    indices_no_values = [
        i for i, d in enumerate(result)
        if field_name not in d or d[field_name] is None
    ]

    if not indices_no_values:
        return result

    valid_data = [(d, idx) for idx, d in enumerate(result) if idx in [i for i, _ in indices_with_values]]

    def distance(d1: dict, d2: dict) -> float:
        """Calculate Euclidean distance."""
        dist = 0.0
        for f in numeric_fields:
            v1 = d1.get(f, 0)
            v2 = d2.get(f, 0)
            if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                dist += (v1 - v2) ** 2
        return np.sqrt(dist)

    for target_idx in indices_no_values:
        target = result[target_idx]

        distances = []
        for record, idx in valid_data:
            d = distance(target, record)
            distances.append((d, record[field_name]))

        distances.sort(key=lambda x: x[0])
        neighbors = distances[:k]

        if neighbors:
            imputed_value = sum(v for _, v in neighbors) / len(neighbors)
            target[field_name] = imputed_value

    return result


@dataclass
class Imputer:
    """Configurable data imputer."""

    strategy: str = "mean"
    numeric_strategy: str = "mean"
    categorical_strategy: str = "mode"
    k_neighbors: int = 5

    def fit_transform(self, data: list[dict[str, Any]]) -> ImputationResult:
        """Fit imputer and transform data.

        Args:
            data: Input data

        Returns:
            ImputationResult
        """
        if not data:
            return ImputationResult(
                data=[],
                imputed_fields={},
                method=self.strategy,
            )

        result = [dict(d) for d in data]
        imputed_fields: dict[str, int] = {}

        for field_name in result[0].keys():
            values = [d.get(field_name) for d in result]
            null_count = sum(1 for v in values if v is None)

            if null_count == 0:
                continue

            if all(isinstance(v, (int, float)) for v in values if v is not None):
                if self.strategy == "mean":
                    result = mean_impute(result, field_name)
                elif self.strategy == "median":
                    result = median_impute(result, field_name)
                elif self.strategy == "forward":
                    result = forward_fill(result, field_name)
                elif self.strategy == "backward":
                    result = backward_fill(result, field_name)
                elif self.strategy == "knn":
                    result = knn_impute(result, field_name, k=self.k_neighbors)
            else:
                if self.categorical_strategy == "mode":
                    result = mode_impute(result, field_name)
                elif self.categorical_strategy == "forward":
                    result = forward_fill(result, field_name)
                elif self.categorical_strategy == "backward":
                    result = backward_fill(result, field_name)

            imputed_fields[field_name] = null_count

        return ImputationResult(
            data=result,
            imputed_fields=imputed_fields,
            method=self.strategy,
            details={"strategy": self.strategy},
        )
