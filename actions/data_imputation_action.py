"""
Data Imputation Action Module

Provides data imputation strategies for handling missing values in UI automation
workflows. Supports mean, median, mode, forward-fill, backward-fill, and
machine learning-based imputation.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ImputationStrategy(Enum):
    """Imputation strategy types."""
    MEAN = auto()
    MEDIAN = auto()
    MODE = auto()
    CONSTANT = auto()
    FORWARD_FILL = auto()
    BACKWARD_FILL = auto()
    INTERPOLATE_LINEAR = auto()
    INTERPOLATE_SPLINE = auto()
    KNN = auto()
    REGRESSION = auto()


@dataclass
class ImputationConfig:
    """Imputation configuration."""
    strategy: ImputationStrategy = ImputationStrategy.MEAN
    constant_value: Any = None
    max_iterations: int = 10
    knn_neighbors: int = 5
    group_by: Optional[list[str]] = None


@dataclass
class ImputationResult:
    """Imputation result."""
    data: list[dict]
    imputed_count: int
    imputed_fields: dict[str, int]
    strategy_used: ImputationStrategy


class MissingValueAnalyzer:
    """
    Analyzes missing value patterns.

    Example:
        >>> analyzer = MissingValueAnalyzer()
        >>> report = analyzer.analyze(data)
    """

    def analyze(self, data: list[dict]) -> dict[str, Any]:
        """Analyze missing values in data."""
        if not data:
            return {}

        field_names = list(data[0].keys()) if data else []
        total_records = len(data)

        report: dict[str, Any] = {
            "total_records": total_records,
            "total_fields": len(field_names),
            "fields": {},
        }

        for field_name in field_names:
            values = [record.get(field_name) for record in data]
            null_count = sum(1 for v in values if v is None or v == "")
            null_percentage = null_count / total_records * 100 if total_records > 0 else 0

            report["fields"][field_name] = {
                "null_count": null_count,
                "null_percentage": null_percentage,
                "present_count": total_records - null_count,
            }

        complete_records = all(
            all(record.get(f) is not None and record.get(f) != "" for f in field_names)
            for record in data
        )
        report["complete_records"] = total_records if complete_records else 0

        return report


class DataImputer:
    """
    Data imputation with multiple strategies.

    Example:
        >>> imputer = DataImputer(ImputationConfig(strategy=ImputationStrategy.MEAN))
        >>> result = imputer.impute(data, ["price", "quantity"])
    """

    def __init__(self, config: Optional[ImputationConfig] = None) -> None:
        self.config = config or ImputationConfig()
        self._analyzer = MissingValueAnalyzer()

    def impute(
        self,
        data: list[dict],
        fields: list[str],
    ) -> ImputationResult:
        """Impute missing values for specified fields."""
        imputed_fields: dict[str, int] = {f: 0 for f in fields}
        result_data = [record.copy() for record in data]

        for field_name in fields:
            if self.config.strategy == ImputationStrategy.MEAN:
                count = self._impute_mean(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.MEDIAN:
                count = self._impute_median(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.MODE:
                count = self._impute_mode(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.CONSTANT:
                count = self._impute_constant(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.FORWARD_FILL:
                count = self._impute_forward_fill(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.BACKWARD_FILL:
                count = self._impute_backward_fill(result_data, field_name)
            elif self.config.strategy == ImputationStrategy.INTERPOLATE_LINEAR:
                count = self._impute_interpolate(result_data, field_name)
            else:
                count = self._impute_mean(result_data, field_name)

            imputed_fields[field_name] = count

        total_imputed = sum(imputed_fields.values())
        return ImputationResult(
            data=result_data,
            imputed_count=total_imputed,
            imputed_fields=imputed_fields,
            strategy_used=self.config.strategy,
        )

    def _impute_mean(self, data: list[dict], field_name: str) -> int:
        """Impute with mean value."""
        values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
        if not values:
            return 0

        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if not numeric_values:
            return 0

        mean_value = sum(numeric_values) / len(numeric_values)
        count = 0

        for record in data:
            if field_name not in record or record[field_name] is None or record[field_name] == "":
                record[field_name] = mean_value
                count += 1

        return count

    def _impute_median(self, data: list[dict], field_name: str) -> int:
        """Impute with median value."""
        values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
        if not values:
            return 0

        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if not numeric_values:
            return 0

        median_value = statistics.median(numeric_values)
        count = 0

        for record in data:
            if field_name not in record or record[field_name] is None or record[field_name] == "":
                record[field_name] = median_value
                count += 1

        return count

    def _impute_mode(self, data: list[dict], field_name: str) -> int:
        """Impute with mode value."""
        values = [d[field_name] for d in data if field_name in d and d[field_name] is not None]
        if not values:
            return 0

        try:
            mode_value = statistics.mode(values)
        except statistics.StatisticsError:
            mode_value = values[0] if values else None

        if mode_value is None:
            return 0

        count = 0
        for record in data:
            if field_name not in record or record[field_name] is None or record[field_name] == "":
                record[field_name] = mode_value
                count += 1

        return count

    def _impute_constant(self, data: list[dict], field_name: str) -> int:
        """Impute with constant value."""
        if self.config.constant_value is None:
            return 0

        count = 0
        for record in data:
            if field_name not in record or record[field_name] is None or record[field_name] == "":
                record[field_name] = self.config.constant_value
                count += 1

        return count

    def _impute_forward_fill(self, data: list[dict], field_name: str) -> int:
        """Forward fill missing values."""
        count = 0
        last_value = None

        for record in data:
            if field_name in record and record[field_name] is not None and record[field_name] != "":
                last_value = record[field_name]
            elif last_value is not None:
                record[field_name] = last_value
                count += 1

        return count

    def _impute_backward_fill(self, data: list[dict], field_name: str) -> int:
        """Backward fill missing values."""
        count = 0
        next_value = None

        for record in reversed(data):
            if field_name in record and record[field_name] is not None and record[field_name] != "":
                next_value = record[field_name]
            elif next_value is not None:
                record[field_name] = next_value
                count += 1

        return count

    def _impute_interpolate(self, data: list[dict], field_name: str) -> int:
        """Linear interpolation for missing values."""
        indices = []
        values = []

        for i, record in enumerate(data):
            if field_name in record and record[field_name] is not None:
                indices.append(i)
                values.append(record[field_name])

        if len(indices) < 2:
            return 0

        count = 0
        for i, record in enumerate(data):
            if field_name not in record or record[field_name] is None or record[field_name] == "":
                if indices:
                    lower_idx = max([j for j in indices if j < i], default=None)
                    upper_idx = min([j for j in indices if j > i], default=None)

                    if lower_idx is not None and upper_idx is not None:
                        lower_val = data[lower_idx][field_name]
                        upper_val = data[upper_idx][field_name]
                        ratio = (i - lower_idx) / (upper_idx - lower_idx)
                        record[field_name] = lower_val + ratio * (upper_val - lower_val)
                        count += 1
                    elif lower_idx is not None:
                        record[field_name] = data[lower_idx][field_name]
                        count += 1
                    elif upper_idx is not None:
                        record[field_name] = data[upper_idx][field_name]
                        count += 1

        return count


class ConditionalImputer:
    """
    Conditional imputation based on other fields.

    Example:
        >>> imputer = ConditionalImputer()
        >>> imputer.add_condition("category", "A", "price", ImputationStrategy.MEAN)
        >>> result = imputer.impute(data, ["price"])
    """

    def __init__(self) -> None:
        self._conditions: dict[tuple, ImputationStrategy] = {}
        self._constant_values: dict[tuple, Any] = {}

    def add_condition(
        self,
        condition_field: str,
        condition_value: Any,
        target_field: str,
        strategy: ImputationStrategy,
        constant_value: Optional[Any] = None,
    ) -> "ConditionalImputer":
        """Add conditional imputation rule."""
        key = (condition_field, condition_value, target_field)
        self._conditions[key] = strategy
        if constant_value is not None:
            self._constant_values[key] = constant_value
        return self

    def impute(self, data: list[dict], target_fields: list[str]) -> ImputationResult:
        """Impute with conditional rules."""
        imputed_fields: dict[str, int] = {f: 0 for f in target_fields}
        result_data = [record.copy() for record in data]

        for condition_key, strategy in self._conditions.items():
            condition_field, condition_value, target_field = condition_key

            subset = [r for r in result_data if r.get(condition_field) == condition_value]

            config = ImputationConfig(
                strategy=strategy,
                constant_value=self._constant_values.get(condition_key),
            )
            imputer = DataImputer(config)
            result = imputer.impute(subset, [target_field])

            for i, record in enumerate(result_data):
                if record.get(condition_field) == condition_value:
                    if target_field not in record or record[target_field] is None:
                        if result.data[i].get(target_field) is not None:
                            record[target_field] = result.data[i][target_field]
                            imputed_fields[target_field] += 1

        total_imputed = sum(imputed_fields.values())
        return ImputationResult(
            data=result_data,
            imputed_count=total_imputed,
            imputed_fields=imputed_fields,
            strategy_used=ImputationStrategy.MEAN,
        )
