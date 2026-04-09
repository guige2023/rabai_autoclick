"""Data Pivot Table Action Module.

Provides pivot table functionality for data analysis with support
for multiple aggregations, grouping, and hierarchical indexes.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class AggregationFunction(Enum):
    SUM = "sum"
    COUNT = "count"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    STD = "std"
    VAR = "var"
    CONCAT = "concat"


@dataclass
class PivotConfig:
    index: List[str] = field(default_factory=list)
    columns: Optional[List[str]] = None
    values: Optional[List[str]] = None
    aggfunc: Union[AggregationFunction, Dict[str, List[AggregationFunction]]] = AggregationFunction.SUM
    fill_value: Any = None
    margins: bool = False
    dropna: bool = True


@dataclass
class PivotResult:
    data: List[Dict[str, Any]]
    index_names: List[str]
    column_names: List[str]
    value_names: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)


class PivotTable:
    def __init__(self, config: Optional[PivotConfig] = None):
        self.config = config or PivotConfig()
        self._result: Optional[PivotResult] = None

    def transform(self, data: List[Dict[str, Any]]) -> PivotResult:
        if not data:
            return PivotResult(data=[], index_names=[], column_names=[], value_names=[])

        index = self.config.index or []
        columns = self.config.columns or []
        values = self.config.values or []

        grouped = self._group_data(data, index)
        result = []

        for key, group in grouped.items():
            row = {}
            for i, idx_name in enumerate(index):
                row[idx_name] = key[i] if isinstance(key, tuple) else key

            pivot_data = self._pivot_group(group, columns, values)

            for col_key, col_values in pivot_data.items():
                for val_name, val in col_values.items():
                    col_name = col_key if not isinstance(col_key, tuple) else "_".join(str(k) for k in col_key)
                    result.append({**row, **{"column": col_name, val_name: val}})

        self._result = PivotResult(
            data=result,
            index_names=index,
            column_names=columns,
            value_names=values,
        )

        if self.config.margins:
            result = self._add_margins(result, index, columns, values)

        if self.config.fill_value is not None:
            result = self._fill_na(result)

        return PivotResult(
            data=result,
            index_names=index,
            column_names=columns,
            value_names=values,
        )

    def fit_transform(self, data: List[Dict[str, Any]]) -> PivotResult:
        return self.transform(data)

    def _group_data(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
    ) -> Dict[Tuple, List[Dict[str, Any]]]:
        grouped = defaultdict(list)
        for row in data:
            if not index:
                key = tuple()
            elif len(index) == 1:
                key = (row.get(index[0]),)
            else:
                key = tuple(row.get(i) for i in index)

            grouped[key].append(row)

        return dict(grouped)

    def _pivot_group(
        self,
        group: List[Dict[str, Any]],
        columns: List[str],
        values: List[str],
    ) -> Dict[Tuple, Dict[str, Any]]:
        if not columns:
            return {(): self._aggregate_values(group, values)}

        pivoted = defaultdict(dict)
        for row in group:
            if len(columns) == 1:
                col_key = (row.get(columns[0]),)
            else:
                col_key = tuple(row.get(c) for c in columns)

            pivoted[col_key].update(row)

        result = {}
        for col_key, col_data in pivoted.items():
            result[col_key] = self._aggregate_values([col_data], values)

        return result

    def _aggregate_values(
        self,
        data: List[Dict[str, Any]],
        values: List[str],
    ) -> Dict[str, Any]:
        if not values:
            return {}

        if isinstance(self.config.aggfunc, dict):
            agg_map = self.config.aggfunc
        else:
            agg_map = {val: [self.config.aggfunc] for val in values}

        result = {}
        for val_name, agg_funcs in agg_map.items():
            val_data = [row.get(val_name) for row in data if row.get(val_name) is not None]

            for agg_func in agg_funcs:
                key = f"{val_name}_{agg_func.value}"
                result[key] = self._apply_aggregation(val_data, agg_func)

        return result

    def _apply_aggregation(
        self,
        values: List[Any],
        func: AggregationFunction,
    ) -> Any:
        if not values:
            return None

        numeric_values = [v for v in values if isinstance(v, (int, float))]

        if func == AggregationFunction.SUM:
            return sum(numeric_values) if numeric_values else None
        elif func == AggregationFunction.COUNT:
            return len(values)
        elif func == AggregationFunction.MEAN:
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif func == AggregationFunction.MEDIAN:
            return self._compute_median(values)
        elif func == AggregationFunction.MIN:
            return min(values) if values else None
        elif func == AggregationFunction.MAX:
            return max(values) if values else None
        elif func == AggregationFunction.FIRST:
            return values[0]
        elif func == AggregationFunction.LAST:
            return values[-1]
        elif func == AggregationFunction.STD:
            return self._compute_std(values)
        elif func == AggregationFunction.CONCAT:
            return ", ".join(str(v) for v in values)

        return None

    def _compute_median(self, values: List[Any]) -> Any:
        sorted_values = sorted([v for v in values if isinstance(v, (int, float))])
        if not sorted_values:
            return None
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        return sorted_values[n // 2]

    def _compute_std(self, values: List[Any]) -> float:
        numeric = [v for v in values if isinstance(v, (int, float))]
        if len(numeric) < 2:
            return 0.0
        mean = sum(numeric) / len(numeric)
        variance = sum((v - mean) ** 2 for v in numeric) / (len(numeric) - 1)
        return variance ** 0.5

    def _add_margins(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: List[str],
        values: List[str],
    ) -> List[Dict[str, Any]]:
        return data

    def _fill_na(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        result = []
        for row in data:
            new_row = {}
            for k, v in row.items():
                if v is None:
                    new_row[k] = self.config.fill_value
                else:
                    new_row[k] = v
            result.append(new_row)
        return result


def create_pivot_table(
    data: List[Dict[str, Any]],
    index: List[str],
    columns: Optional[List[str]] = None,
    values: Optional[List[str]] = None,
    aggfunc: AggregationFunction = AggregationFunction.SUM,
) -> List[Dict[str, Any]]:
    config = PivotConfig(index=index, columns=columns, values=values, aggfunc=aggfunc)
    pivot = PivotTable(config)
    return pivot.fit_transform(data).data
