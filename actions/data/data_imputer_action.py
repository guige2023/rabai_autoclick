"""Data imputation for handling missing values in automation workflows.

Provides various strategies for filling in missing data values
including mean, median, mode, forward fill, interpolation, and ML-based methods.
"""

from __future__ import annotations

import random
import threading
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from statistics import mean, median
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import copy


class ImputationStrategy(Enum):
    """Strategy for imputing missing values."""
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    LINEAR_INTERPOLATE = "linear_interpolate"
    CONSTANT = "constant"
    RANDOM = "random"
    KNN = "knn"
    REGRESSION = "regression"
    DELETE = "delete"


@dataclass
class ColumnStats:
    """Statistics for a column."""
    column_name: str
    data_type: str
    total_count: int
    missing_count: int
    missing_pct: float
    mean: Optional[float] = None
    median: Optional[float] = None
    mode: Optional[Any] = None
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    std_dev: Optional[float] = None
    unique_count: int = 0


@dataclass
class ImputationResult:
    """Result of an imputation operation."""
    imputed_count: int
    strategy_used: ImputationStrategy
    column: str
    before_missing: int
    after_missing: int
    timestamp: float = field(default_factory=time.time)


class DataImputer:
    """Core data imputation engine."""

    def __init__(self):
        self._column_stats: Dict[str, ColumnStats] = {}
        self._imputation_history: List[ImputationResult] = []
        self._lock = threading.Lock()

    def analyze_column(self, data: List[Any]) -> ColumnStats:
        """Analyze a column to determine statistics and best imputation strategy."""
        total = len(data)
        non_missing = [v for v in data if v is not None and v != ""]
        missing = total - len(non_missing)
        missing_pct = (missing / total * 100) if total > 0 else 0

        numeric_vals = []
        categorical_vals = []

        for v in non_missing:
            try:
                numeric_vals.append(float(v))
            except (TypeError, ValueError):
                categorical_vals.append(v)

        stats = ColumnStats(
            column_name="",
            data_type="numeric" if numeric_vals else "categorical",
            total_count=total,
            missing_count=missing,
            missing_pct=missing_pct,
            unique_count=len(set(non_missing)),
        )

        if numeric_vals:
            numeric_vals.sort()
            stats.mean = sum(numeric_vals) / len(numeric_vals)
            stats.median = numeric_vals[len(numeric_vals) // 2]
            stats.min_val = min(numeric_vals)
            stats.max_val = max(numeric_vals)

            if len(numeric_vals) > 1:
                variance = sum((x - stats.mean) ** 2 for x in numeric_vals) / len(numeric_vals)
                stats.std_dev = variance ** 0.5

        if categorical_vals:
            counter = Counter(categorical_vals)
            stats.mode = counter.most_common(1)[0][0]

        return stats

    def impute_mean(self, data: List[Any]) -> List[Any]:
        """Impute missing values with mean."""
        stats = self.analyze_column(data)
        if stats.mean is None:
            return data

        result = []
        for v in data:
            if v is None or v == "":
                result.append(stats.mean)
            else:
                try:
                    result.append(float(v))
                except (TypeError, ValueError):
                    result.append(v)

        return result

    def impute_median(self, data: List[Any]) -> List[Any]:
        """Impute missing values with median."""
        stats = self.analyze_column(data)
        if stats.median is None:
            return data

        result = []
        median_val = stats.median
        for v in data:
            if v is None or v == "":
                result.append(median_val)
            else:
                try:
                    result.append(float(v))
                except (TypeError, ValueError):
                    result.append(v)

        return result

    def impute_mode(self, data: List[Any]) -> List[Any]:
        """Impute missing values with mode."""
        stats = self.analyze_column(data)
        mode_val = stats.mode

        if mode_val is None:
            return data

        result = []
        for v in data:
            if v is None or v == "":
                result.append(mode_val)
            else:
                result.append(v)

        return result

    def impute_forward_fill(self, data: List[Any]) -> List[Any]:
        """Impute missing values using forward fill."""
        result = []
        last_valid = None

        for v in data:
            if v is not None and v != "":
                result.append(v)
                last_valid = v
            else:
                result.append(last_valid)

        return result

    def impute_backward_fill(self, data: List[Any]) -> List[Any]:
        """Impute missing values using backward fill."""
        result = [None] * len(data)
        next_valid = None

        for i in range(len(data) - 1, -1, -1):
            v = data[i]
            if v is not None and v != "":
                result[i] = v
                next_valid = v
            else:
                result[i] = next_valid

        return result

    def impute_linear_interpolate(self, data: List[Any]) -> List[Any]:
        """Impute missing values using linear interpolation."""
        result = list(data)

        indices = []
        values = []
        for i, v in enumerate(data):
            if v is not None and v != "":
                try:
                    indices.append(i)
                    values.append(float(v))
                except (TypeError, ValueError):
                    pass

        if len(indices) < 2:
            return data

        for i in range(len(data)):
            if data[i] is None or data[i] == "":
                if indices:
                    for j in range(len(indices) - 1):
                        if indices[j] < i < indices[j + 1]:
                            t = (i - indices[j]) / (indices[j + 1] - indices[j])
                            interp_val = values[j] + t * (values[j + 1] - values[j])
                            result[i] = interp_val
                            break
                    else:
                        if indices and i < indices[0]:
                            result[i] = values[0]
                        elif indices:
                            result[i] = values[-1]

        return result

    def impute_constant(self, data: List[Any], constant: Any) -> List[Any]:
        """Impute missing values with a constant."""
        result = []
        for v in data:
            if v is None or v == "":
                result.append(constant)
            else:
                result.append(v)
        return result

    def impute_random(self, data: List[Any], min_val: float = 0, max_val: float = 1) -> List[Any]:
        """Impute missing values with random values in range."""
        stats = self.analyze_column(data)
        use_range = (stats.min_val is not None and stats.max_val is not None)

        result = []
        for v in data:
            if v is None or v == "":
                if use_range:
                    result.append(random.uniform(stats.min_val, stats.max_val))
                else:
                    result.append(random.uniform(min_val, max_val))
            else:
                try:
                    result.append(float(v))
                except (TypeError, ValueError):
                    result.append(v)

        return result

    def impute_delete(self, data: List[Any]) -> Tuple[List[Any], List[int]]:
        """Delete rows with missing values. Returns data and deleted indices."""
        result = []
        deleted_indices = []

        for i, v in enumerate(data):
            if v is not None and v != "":
                result.append(v)
            else:
                deleted_indices.append(i)

        return result, deleted_indices

    def impute(
        self,
        data: List[Any],
        strategy: ImputationStrategy,
        constant_value: Optional[Any] = None,
        min_val: float = 0,
        max_val: float = 1,
    ) -> List[Any]:
        """Impute missing values using specified strategy."""
        if strategy == ImputationStrategy.MEAN:
            return self.impute_mean(data)
        elif strategy == ImputationStrategy.MEDIAN:
            return self.impute_median(data)
        elif strategy == ImputationStrategy.MODE:
            return self.impute_mode(data)
        elif strategy == ImputationStrategy.FORWARD_FILL:
            return self.impute_forward_fill(data)
        elif strategy == ImputationStrategy.BACKWARD_FILL:
            return self.impute_backward_fill(data)
        elif strategy == ImputationStrategy.LINEAR_INTERPOLATE:
            return self.impute_linear_interpolate(data)
        elif strategy == ImputationStrategy.CONSTANT:
            return self.impute_constant(data, constant_value)
        elif strategy == ImputationStrategy.RANDOM:
            return self.impute_random(data, min_val, max_val)
        elif strategy == ImputationStrategy.DELETE:
            result, _ = self.impute_delete(data)
            return result
        else:
            return data


class AutomationImputerAction:
    """Action providing data imputation for automation workflows."""

    def __init__(self, imputer: Optional[DataImputer] = None):
        self._imputer = imputer or DataImputer()

    def analyze(
        self,
        data: List[Any],
        column_name: str = "column",
    ) -> Dict[str, Any]:
        """Analyze a column for missing values."""
        stats = self._imputer.analyze_column(data)
        stats.column_name = column_name

        self._imputer._column_stats[column_name] = stats

        return {
            "column": column_name,
            "data_type": stats.data_type,
            "total_count": stats.total_count,
            "missing_count": stats.missing_count,
            "missing_pct": round(stats.missing_pct, 2),
            "mean": stats.mean,
            "median": stats.median,
            "mode": stats.mode,
            "min": stats.min_val,
            "max": stats.max_val,
            "std_dev": stats.std_dev,
            "unique_count": stats.unique_count,
            "recommended_strategy": self._recommend_strategy(stats),
        }

    def _recommend_strategy(self, stats: ColumnStats) -> str:
        """Recommend imputation strategy based on column stats."""
        if stats.missing_pct > 50:
            return "delete"
        if stats.missing_pct == 0:
            return "none"
        if stats.data_type == "numeric":
            if stats.std_dev and stats.std_dev > stats.mean * 0.5:
                return "median"
            return "mean"
        else:
            return "mode"

    def impute(
        self,
        data: List[Any],
        strategy: str = "mean",
        constant_value: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Impute missing values in a column."""
        try:
            strategy_enum = ImputationStrategy(strategy.lower())
        except ValueError:
            strategy_enum = ImputationStrategy.MEAN

        before_missing = sum(1 for v in data if v is None or v == "")
        result = self._imputer.impute(
            data=data,
            strategy=strategy_enum,
            constant_value=constant_value,
        )
        after_missing = sum(1 for v in result if v is None or v == "")

        return {
            "imputed_count": before_missing - after_missing,
            "strategy": strategy_enum.value,
            "before_missing": before_missing,
            "after_missing": after_missing,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an imputation operation.

        Required params:
            data: list - Data to impute
            operation: str - 'analyze', 'impute', or 'impute_column'
            strategy: str - For impute operation

        Optional params:
            column_name: str - Name of column for analysis
            constant_value: Any - For constant imputation
        """
        operation = params.get("operation")
        data = params.get("data")

        if not data:
            raise ValueError("data is required")

        if operation == "analyze":
            return self.analyze(
                data=data,
                column_name=params.get("column_name", "column"),
            )

        elif operation == "impute":
            strategy = params.get("strategy", "mean")
            constant_value = params.get("constant_value")
            return self.impute(data, strategy, constant_value)

        elif operation == "impute_dataframe":
            columns = params.get("columns", {})
            results = {}

            for col_name, col_data in columns.items():
                if not isinstance(col_data, list):
                    continue

                strategy = params.get(f"strategy_{col_name}", "mean")
                const_val = params.get(f"constant_{col_name}")
                results[col_name] = self.impute(col_data, strategy, const_val)

            return {"results": results}

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_imputation_history(self) -> List[Dict[str, Any]]:
        """Get imputation history."""
        return [
            {
                "column": r.column,
                "strategy": r.strategy_used.value,
                "imputed_count": r.imputed_count,
                "before_missing": r.before_missing,
                "after_missing": r.after_missing,
                "timestamp": datetime.fromtimestamp(r.timestamp).isoformat(),
            }
            for r in self._imputer._imputation_history
        ]
