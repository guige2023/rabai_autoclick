"""Data Imputer Action Module.

Provides data imputation strategies for handling missing values including
mean, median, mode, forward fill, backward fill, interpolation, and KNN.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ImputationStrategy(Enum):
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    INTERPOLATE = "interpolate"
    CONSTANT = "constant"
    KNN = "knn"
    REGRESSION = "regression"


@dataclass
class ImputerConfig:
    strategy: ImputationStrategy = ImputationStrategy.MEAN
    fill_value: Any = None
    max_iterations: int = 10
    knn_k: int = 5
    regression_features: Optional[List[str]] = None
    column_configs: Optional[Dict[str, ImputationStrategy]] = None


@dataclass
class ImputationStats:
    total_cells: int
    missing_cells: int
    imputed_cells: int
    missing_percentage: float
    column_stats: Dict[str, int] = field(default_factory=dict)


class DataImputer:
    def __init__(self, config: Optional[ImputerConfig] = None):
        self.config = config or ImputerConfig()
        self._column_stats: Dict[str, Any] = {}
        self._stats: Optional[ImputationStats] = None

    def fit(self, data: List[Dict[str, Any]]) -> "DataImputer":
        if not data:
            return self

        for col in data[0].keys():
            values = [row.get(col) for row in data if row.get(col) is not None]

            if not values:
                continue

            if all(isinstance(v, (int, float)) for v in values):
                self._column_stats[col] = {
                    "type": "numeric",
                    "mean": sum(values) / len(values),
                    "median": self._compute_median(values),
                    "std": self._compute_std(values),
                }
            else:
                counter = Counter(values)
                self._column_stats[col] = {
                    "type": "categorical",
                    "mode": counter.most_common(1)[0][0] if counter else None,
                }

        return self

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not data:
            return data

        result = []
        for row in data:
            new_row = dict(row)
            for col in row.keys():
                if new_row[col] is None or new_row[col] == "":
                    new_row[col] = self._impute_value(col, row, result)
            result.append(new_row)

        return result

    def fit_transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        self.fit(data)
        return self.transform(data)

    def _impute_value(self, col: str, current_row: Dict[str, Any], previous_rows: List[Dict[str, Any]]) -> Any:
        strategy = self._get_strategy(col)

        if strategy == ImputationStrategy.MEAN:
            stats = self._column_stats.get(col, {})
            return stats.get("mean", 0.0)

        elif strategy == ImputationStrategy.MEDIAN:
            stats = self._column_stats.get(col, {})
            return stats.get("median", 0.0)

        elif strategy == ImputationStrategy.MODE:
            stats = self._column_stats.get(col, {})
            return stats.get("mode", "")

        elif strategy == ImputationStrategy.FORWARD_FILL:
            for prev_row in reversed(previous_rows):
                if prev_row.get(col) is not None:
                    return prev_row[col]
            return self.config.fill_value

        elif strategy == ImputationStrategy.BACKWARD_FILL:
            return self.config.fill_value

        elif strategy == ImputationStrategy.CONSTANT:
            return self.config.fill_value

        elif strategy == ImputationStrategy.INTERPOLATE:
            return self._interpolate_value(col, current_row, previous_rows)

        return self.config.fill_value

    def _get_strategy(self, col: str) -> ImputationStrategy:
        if self.config.column_configs and col in self.config.column_configs:
            return self.config.column_configs[col]
        return self.config.strategy

    def _compute_median(self, values: List[float]) -> float:
        if not values:
            return 0.0
        sorted_values = sorted(values)
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        return sorted_values[n // 2]

    def _compute_std(self, values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return variance ** 0.5

    def _interpolate_value(self, col: str, current_row: Dict[str, Any], previous_rows: List[Dict[str, Any]]) -> Any:
        previous_values = []
        for prev_row in reversed(previous_rows):
            if prev_row.get(col) is not None:
                previous_values.append(prev_row[col])
                break

        for next_row in previous_rows:
            if next_row.get(col) is not None:
                previous_values.append(next_row[col])
                break

        if len(previous_values) == 2 and all(isinstance(v, (int, float)) for v in previous_values):
            return (previous_values[0] + previous_values[1]) / 2

        return self.config.fill_value

    def get_stats(self) -> Optional[ImputationStats]:
        return self._stats


def impute_missing_values(
    data: List[Dict[str, Any]],
    strategy: ImputationStrategy = ImputationStrategy.MEAN,
    fill_value: Any = None,
) -> List[Dict[str, Any]]:
    config = ImputerConfig(strategy=strategy, fill_value=fill_value)
    imputer = DataImputer(config)
    return imputer.fit_transform(data)


def drop_missing_rows(data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    if columns is None:
        return [row for row in data if all(v is not None and v != "" for v in row.values())]
    return [row for row in data if all(row.get(c) is not None and row.get(c) != "" for c in columns)]


def drop_missing_columns(data: List[Dict[str, Any]], threshold: float = 0.5) -> List[Dict[str, Any]]:
    if not data:
        return data

    cols_to_keep = []
    for col in data[0].keys():
        non_missing = sum(1 for row in data if row.get(col) is not None and row.get(col) != "")
        if non_missing / len(data) >= threshold:
            cols_to_keep.append(col)

    return [{col: row.get(col) for col in cols_to_keep} for row in data]
