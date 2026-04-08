"""
Data Aggregator Action Module.

Provides multi-dimensional data aggregation with rolling windows,
cumulative sums, and advanced grouping operations.

Author: RabAi Team
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd


class AggregationType(Enum):
    """Types of aggregation."""
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    STD = "std"
    VAR = "var"
    FIRST = "first"
    LAST = "last"
    RANK = "rank"
    PERCENTILE = "percentile"
    CUMULATIVE_SUM = "cumsum"
    CUMULATIVE_MAX = "cummax"
    ROLLING_SUM = "rolling_sum"
    ROLLING_MEAN = "rolling_mean"


@dataclass
class AggregationConfig:
    """Configuration for an aggregation operation."""
    column: str
    agg_type: AggregationType
    window: Optional[int] = None
    min_periods: int = 1
    quantile: Optional[float] = None


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    aggregations: Dict[str, Any]
    group_keys: Optional[Tuple] = None
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aggregations": self.aggregations,
            "group_keys": str(self.group_keys) if self.group_keys else None,
            "timestamp": self.timestamp.isoformat(),
        }


class DataAggregator:
    """
    Multi-dimensional data aggregation engine.

    Supports group-by, rolling windows, cumulative operations,
    and complex multi-column aggregations.

    Example:
        >>> aggregator = DataAggregator()
        >>> result = aggregator.groupby(df, ["category"], {"amount": AggregationType.SUM})
    """

    def groupby(
        self,
        df: pd.DataFrame,
        group_columns: List[str],
        agg_specs: Dict[str, AggregationType],
    ) -> pd.DataFrame:
        """Group by columns and apply aggregations."""
        agg_dict = {}
        for col, agg_type in agg_specs.items():
            if agg_type == AggregationType.SUM:
                agg_dict[col] = "sum"
            elif agg_type == AggregationType.MEAN:
                agg_dict[col] = "mean"
            elif agg_type == AggregationType.MEDIAN:
                agg_dict[col] = "median"
            elif agg_type == AggregationType.MIN:
                agg_dict[col] = "min"
            elif agg_type == AggregationType.MAX:
                agg_dict[col] = "max"
            elif agg_type == AggregationType.COUNT:
                agg_dict[col] = "count"
            elif agg_type == AggregationType.STD:
                agg_dict[col] = "std"
            elif agg_type == AggregationType.FIRST:
                agg_dict[col] = "first"
            elif agg_type == AggregationType.LAST:
                agg_dict[col] = "last"

        return df.groupby(group_columns).agg(agg_dict).reset_index()

    def rolling_aggregate(
        self,
        df: pd.DataFrame,
        column: str,
        window: int,
        agg_type: AggregationType,
        min_periods: int = 1,
    ) -> pd.Series:
        """Apply rolling window aggregation."""
        if agg_type == AggregationType.ROLLING_SUM:
            return df[column].rolling(window=window, min_periods=min_periods).sum()
        elif agg_type == AggregationType.ROLLING_MEAN:
            return df[column].rolling(window=window, min_periods=min_periods).mean()
        elif agg_type == AggregationType.CUMULATIVE_SUM:
            return df[column].cumsum()
        elif agg_type == AggregationType.CUMULATIVE_MAX:
            return df[column].cummax()
        elif agg_type == AggregationType.RANK:
            return df[column].rolling(window=window, min_periods=min_periods).rank()
        else:
            return df[column].rolling(window=window, min_periods=min_periods).mean()

    def multi_level_aggregate(
        self,
        df: pd.DataFrame,
        group_columns: List[str],
        agg_specs: Dict[str, List[AggregationType]],
    ) -> pd.DataFrame:
        """Apply multiple aggregations per column."""
        agg_dict = {}
        for col, agg_types in agg_specs.items():
            col_aggs = []
            for agg_type in agg_types:
                if agg_type == AggregationType.SUM:
                    col_aggs.append("sum")
                elif agg_type == AggregationType.MEAN:
                    col_aggs.append("mean")
                elif agg_type == AggregationType.COUNT:
                    col_aggs.append("count")
                elif agg_type == AggregationType.MIN:
                    col_aggs.append("min")
                elif agg_type == AggregationType.MAX:
                    col_aggs.append("max")
            if col_aggs:
                agg_dict[col] = col_aggs

        return df.groupby(group_columns).agg(agg_dict)

    def pivot_aggregate(
        self,
        df: pd.DataFrame,
        index: str,
        columns: str,
        values: str,
        agg_func: str = "sum",
    ) -> pd.DataFrame:
        """Aggregate and pivot data."""
        return df.pivot_table(
            index=index,
            columns=columns,
            values=values,
            aggfunc=agg_func,
            fill_value=0,
        )


def create_aggregator() -> DataAggregator:
    """Factory to create a data aggregator."""
    return DataAggregator()
