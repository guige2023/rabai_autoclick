"""Data imputation for handling missing values."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class ImputationStrategy(str, Enum):
    """Strategy for imputing missing values."""

    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    CONSTANT = "constant"
    INTERPOLATE = "interpolate"
    KNN = "knn"
    CUSTOM = "custom"


@dataclass
class ImputationConfig:
    """Configuration for imputation."""

    field_name: str
    strategy: ImputationStrategy
    constant_value: Any = None
    custom_func: Optional[Callable[[list], Any]] = None
    limit: Optional[int] = None  # For forward/backward fill


@dataclass
class ImputationResult:
    """Result of imputation operation."""

    field_name: str
    total_records: int
    missing_count: int
    imputed_count: int
    strategy: ImputationStrategy
    imputed_value: Any = None


class DataImputerAction:
    """Imputes missing values in datasets."""

    def __init__(self):
        """Initialize imputer."""
        self._configs: list[ImputationConfig] = []

    def add_config(self, config: ImputationConfig) -> None:
        """Add an imputation configuration."""
        self._configs.append(config)

    def _calculate_mean(self, values: list[float]) -> float:
        """Calculate mean of values."""
        valid = [v for v in values if v is not None]
        return sum(valid) / len(valid) if valid else 0.0

    def _calculate_median(self, values: list[float]) -> float:
        """Calculate median of values."""
        valid = sorted([v for v in values if v is not None])
        if not valid:
            return 0.0
        n = len(valid)
        if n % 2 == 0:
            return (valid[n // 2 - 1] + valid[n // 2]) / 2
        return valid[n // 2]

    def _calculate_mode(self, values: list[Any]) -> Any:
        """Calculate mode of values."""
        valid = [v for v in values if v is not None]
        if not valid:
            return None
        counter = Counter(valid)
        return counter.most_common(1)[0][0]

    def _forward_fill(
        self,
        values: list[Any],
        limit: Optional[int] = None,
    ) -> list[Any]:
        """Forward fill missing values."""
        result = []
        last_valid = None
        fill_count = 0

        for value in values:
            if value is not None:
                last_valid = value
                fill_count = 0
                result.append(value)
            elif last_valid is not None:
                if limit is None or fill_count < limit:
                    result.append(last_valid)
                    fill_count += 1
                else:
                    result.append(None)
            else:
                result.append(None)

        return result

    def _backward_fill(
        self,
        values: list[Any],
        limit: Optional[int] = None,
    ) -> list[Any]:
        """Backward fill missing values."""
        result = [None] * len(values)
        next_valid = None
        fill_count = 0

        for i in range(len(values) - 1, -1, -1):
            value = values[i]
            if value is not None:
                next_valid = value
                fill_count = 0
                result[i] = value
            elif next_valid is not None:
                if limit is None or fill_count < limit:
                    result[i] = next_valid
                    fill_count += 1

        return result

    def _interpolate_linear(self, values: list[float]) -> list[float]:
        """Linear interpolation."""
        result = list(values)
        n = len(values)

        start_idx = 0
        while start_idx < n and values[start_idx] is None:
            start_idx += 1

        if start_idx == n:
            return result

        for i in range(start_idx + 1, n):
            if values[i] is None:
                end_idx = i + 1
                while end_idx < n and values[end_idx] is None:
                    end_idx += 1

                if end_idx < n:
                    start_val = values[start_idx]
                    end_val = values[end_idx]
                    step = (end_val - start_val) / (end_idx - start_idx)
                    for j in range(start_idx + 1, end_idx):
                        result[j] = start_val + step * (j - start_idx)
                else:
                    for j in range(start_idx + 1, end_idx):
                        result[j] = values[start_idx]

                start_idx = end_idx
            else:
                start_idx = i

        return result

    def impute_field(
        self,
        records: Sequence[dict[str, Any]],
        config: ImputationConfig,
    ) -> ImputationResult:
        """Impute missing values for a single field.

        Args:
            records: Input records.
            config: Imputation configuration.

        Returns:
            ImputationResult with statistics.
        """
        values = [record.get(config.field_name) for record in records]
        missing_count = sum(1 for v in values if v is None)

        if missing_count == 0:
            return ImputationResult(
                field_name=config.field_name,
                total_records=len(records),
                missing_count=0,
                imputed_count=0,
                strategy=config.strategy,
            )

        imputed_value = None

        if config.strategy == ImputationStrategy.MEAN:
            imputed_value = self._calculate_mean([v for v in values if v is not None])
            values = [imputed_value if v is None else v for v in values]

        elif config.strategy == ImputationStrategy.MEDIAN:
            imputed_value = self._calculate_median([v for v in values if v is not None])
            values = [imputed_value if v is None else v for v in values]

        elif config.strategy == ImputationStrategy.MODE:
            imputed_value = self._calculate_mode(values)
            values = [imputed_value if v is None else v for v in values]

        elif config.strategy == ImputationStrategy.CONSTANT:
            imputed_value = config.constant_value
            values = [imputed_value if v is None else v for v in values]

        elif config.strategy == ImputationStrategy.FORWARD_FILL:
            values = self._forward_fill(values, config.limit)

        elif config.strategy == ImputationStrategy.BACKWARD_FILL:
            values = self._backward_fill(values, config.limit)

        elif config.strategy == ImputationStrategy.INTERPOLATE:
            float_values = [float(v) if v is not None else None for v in values]
            float_values = self._interpolate_linear(float_values)
            values = float_values

        elif config.strategy == ImputationStrategy.CUSTOM and config.custom_func:
            valid_values = [v for v in values if v is not None]
            imputed_value = config.custom_func(valid_values)
            values = [imputed_value if v is None else v for v in values]

        for record, value in zip(records, values):
            record[config.field_name] = value

        return ImputationResult(
            field_name=config.field_name,
            total_records=len(records),
            missing_count=missing_count,
            imputed_count=missing_count,
            strategy=config.strategy,
            imputed_value=imputed_value,
        )

    def impute_batch(
        self,
        records: Sequence[dict[str, Any]],
        configs: Optional[list[ImputationConfig]] = None,
    ) -> list[ImputationResult]:
        """Impute missing values for multiple fields.

        Args:
            records: Input records.
            configs: List of imputation configurations.

        Returns:
            List of ImputationResult for each field.
        """
        configs = configs or self._configs
        results = []

        for config in configs:
            records_copy = [r.copy() for r in records]
            result = self.impute_field(records_copy, config)
            results.append(result)

            for r, r_copy in zip(records, records_copy):
                r[config.field_name] = r_copy[config.field_name]

        return results
