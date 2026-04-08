"""
Data Imputer Action Module.

Imputes missing values in datasets using various strategies
 including mean, median, mode, forward fill, and custom imputation.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import Counter
import logging

logger = logging.getLogger(__name__)


class ImputationStrategy(Enum):
    """Strategy for imputing missing values."""
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FORWARD_FILL = "forward_fill"
    BACKWARD_FILL = "backward_fill"
    CONSTANT = "constant"
    INTERPOLATE = "interpolate"
    CUSTOM = "custom"


@dataclass
class ImputationRule:
    """Rule for imputing a specific field."""
    field: str
    strategy: ImputationStrategy
    constant_value: Any = None
    custom_func: Optional[Callable[[list[Any]], Any]] = None


@dataclass
class ImputationResult:
    """Result of imputation operation."""
    success: bool
    data: list[dict[str, Any]] = field(default_factory=list)
    fields_imputed: int = 0
    values_imputed: int = 0


class DataImputerAction:
    """
    Missing value imputation for datasets.

    Handles missing values using various statistical and
    deterministic strategies with support for custom imputation.

    Example:
        imputer = DataImputerAction()
        imputer.add_rule("age", ImputationStrategy.MEDIAN)
        imputer.add_rule("name", ImputationStrategy.CONSTANT, constant_value="Unknown")
        result = imputer.impute(data)
    """

    def __init__(self) -> None:
        self._rules: list[ImputationRule] = []

    def add_rule(
        self,
        field: str,
        strategy: ImputationStrategy,
        constant_value: Any = None,
        custom_func: Optional[Callable[[list[Any]], Any]] = None,
    ) -> "DataImputerAction":
        """Add an imputation rule for a field."""
        rule = ImputationRule(
            field=field,
            strategy=strategy,
            constant_value=constant_value,
            custom_func=custom_func,
        )
        self._rules.append(rule)
        return self

    def add_mean_rule(self, field: str) -> "DataImputerAction":
        """Add mean imputation rule."""
        return self.add_rule(field, ImputationStrategy.MEAN)

    def add_median_rule(self, field: str) -> "DataImputerAction":
        """Add median imputation rule."""
        return self.add_rule(field, ImputationStrategy.MEDIAN)

    def add_mode_rule(self, field: str) -> "DataImputerAction":
        """Add mode (most frequent) imputation rule."""
        return self.add_rule(field, ImputationStrategy.MODE)

    def add_constant_rule(
        self,
        field: str,
        value: Any,
    ) -> "DataImputerAction":
        """Add constant value imputation rule."""
        return self.add_rule(field, ImputationStrategy.CONSTANT, constant_value=value)

    def impute(
        self,
        data: list[dict[str, Any]],
    ) -> ImputationResult:
        """Impute missing values in the dataset."""
        if not data:
            return ImputationResult(success=True, data=data)

        result_data = [dict(row) for row in data]
        fields_imputed = 0
        values_imputed = 0

        for rule in self._rules:
            imputation_value = self._compute_imputation_value(rule, data)

            for row in result_data:
                if rule.field in row and row[rule.field] is None:
                    row[rule.field] = imputation_value
                    values_imputed += 1

            fields_imputed += 1

        return ImputationResult(
            success=True,
            data=result_data,
            fields_imputed=fields_imputed,
            values_imputed=values_imputed,
        )

    def _compute_imputation_value(
        self,
        rule: ImputationRule,
        data: list[dict[str, Any]],
    ) -> Any:
        """Compute the imputation value for a rule."""
        values = [
            row.get(rule.field)
            for row in data
            if rule.field in row and row[rule.field] is not None
        ]

        if rule.strategy == ImputationStrategy.MEAN:
            numeric = [v for v in values if isinstance(v, (int, float))]
            return sum(numeric) / len(numeric) if numeric else 0

        elif rule.strategy == ImputationStrategy.MEDIAN:
            numeric = sorted([v for v in values if isinstance(v, (int, float))])
            n = len(numeric)
            if n == 0:
                return 0
            if n % 2 == 0:
                return (numeric[n // 2 - 1] + numeric[n // 2]) / 2
            return numeric[n // 2]

        elif rule.strategy == ImputationStrategy.MODE:
            if not values:
                return None
            counter = Counter(values)
            return counter.most_common(1)[0][0]

        elif rule.strategy == ImputationStrategy.CONSTANT:
            return rule.constant_value

        elif rule.strategy == ImputationStrategy.CUSTOM:
            if rule.custom_func:
                return rule.custom_func(values)
            return None

        elif rule.strategy == ImputationStrategy.FORWARD_FILL:
            last_value = None
            for row in data:
                if rule.field in row and row[rule.field] is not None:
                    last_value = row[rule.field]
            return last_value

        elif rule.strategy == ImputationStrategy.BACKWARD_FILL:
            next_value = None
            for row in reversed(data):
                if rule.field in row and row[rule.field] is not None:
                    next_value = row[rule.field]
                    break
            return next_value

        return None

    def auto_impute(
        self,
        data: list[dict[str, Any]],
        default_strategy: ImputationStrategy = ImputationStrategy.MEDIAN,
    ) -> ImputationResult:
        """Automatically impute all fields with missing values."""
        if not data:
            return ImputationResult(success=True, data=data)

        all_fields: set[str] = set()
        for row in data:
            all_fields.update(row.keys())

        for field_name in all_fields:
            values = [row.get(field_name) for row in data if field_name in row]
            non_null = [v for v in values if v is not None]

            if len(non_null) < len(values):
                numeric = [v for v in non_null if isinstance(v, (int, float))]
                if numeric and len(numeric) == len(non_null):
                    self.add_median_rule(field_name)
                else:
                    self.add_mode_rule(field_name)

        return self.impute(data)


from enum import Enum
