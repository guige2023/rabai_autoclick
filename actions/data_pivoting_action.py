"""
Data Pivoting Action Module.

Provides data pivoting and unpivoting with
aggregation and multi-level grouping.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from collections import defaultdict


class AggregationFunc(Enum):
    """Aggregation functions."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    LIST = "list"


@dataclass
class PivotConfig:
    """Pivot configuration."""
    index: list[str]
    columns: list[str]
    values: list[str]
    aggregation: AggregationFunc = AggregationFunc.SUM
    fill_value: Any = None
    margins: bool = False


class DataPivotingAction:
    """
    Data pivoting and unpivoting.

    Example:
        pivot = DataPivotingAction()
        pivoted = pivot.pivot(data, index=["date"], columns=["region"], values=["sales"])
    """

    def __init__(self):
        pass

    def pivot(
        self,
        data: list[dict],
        index: list[str],
        columns: list[str],
        values: list[str],
        aggregation: AggregationFunc = AggregationFunc.SUM
    ) -> list[dict]:
        """Pivot data."""
        if not data:
            return []

        result = []
        grouped = defaultdict(lambda: defaultdict(list))

        for record in data:
            index_key = tuple(record.get(k) for k in index)
            column_key = tuple(record.get(k) for k in columns)

            for value_key in values:
                grouped[index_key][column_key].append(record.get(value_key))

        agg_funcs = {
            AggregationFunc.SUM: lambda x: sum(v for v in x if v is not None),
            AggregationFunc.COUNT: lambda x: len(x),
            AggregationFunc.AVG: lambda x: sum(v for v in x if v is not None) / len([v for v in x if v is not None]) if any(v is not None for v in x) else None,
            AggregationFunc.MIN: lambda x: min((v for v in x if v is not None), default=None),
            AggregationFunc.MAX: lambda x: max((v for v in x if v is not None), default=None),
            AggregationFunc.FIRST: lambda x: next((v for v in x if v is not None), None),
            AggregationFunc.LAST: lambda x: next((v for v in reversed(x) if v is not None), None),
            AggregationFunc.LIST: lambda x: [v for v in x if v is not None],
        }

        agg_func = agg_funcs.get(aggregation, agg_funcs[AggregationFunc.SUM])

        for index_key, column_values in grouped.items():
            result_row = dict(zip(index, index_key))

            for col_key, values_list in column_values.items():
                col_name = "_".join(str(k) for k in col_key)
                for value_key, value in zip(values, values_list):
                    result_row[f"{col_name}_{value_key}"] = value

            result.append(result_row)

        return result

    def unpivot(
        self,
        data: list[dict],
        index: list[str],
        value_columns: list[str],
        var_name: str = "variable",
        val_name: str = "value"
    ) -> list[dict]:
        """Unpivot data."""
        if not data:
            return []

        result = []

        for record in data:
            index_data = {k: record[k] for k in index}

            for col in value_columns:
                if col in record:
                    row = {**index_data, var_name: col, val_name: record[col]}
                    result.append(row)

        return result

    async def pivot_async(
        self,
        data: list[dict],
        **kwargs: Any
    ) -> list[dict]:
        """Pivot data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.pivot(data, **kwargs)
        )
