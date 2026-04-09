"""
Data BI Action Module

Provides BI-style analytics operations including
pivot tables, cross-tabs, ranking, and window functions.

Author: RabAi Team
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import logging

logger = logging.getLogger(__name__)


class WindowFunction(Enum):
    """Window function types."""

    ROW_NUMBER = auto()
    RANK = auto()
    DENSE_RANK = auto()
    PERCENT_RANK = auto()
    CUME_DIST = auto()
    LAG = auto()
    LEAD = auto()
    FIRST_VALUE = auto()
    LAST_VALUE = auto()
    SUM = auto()
    AVG = auto()
    COUNT = auto()
    MAX = auto()
    MIN = auto()


@dataclass
class PivotConfig:
    """Configuration for pivot table generation."""

    index: List[str]
    columns: str
    values: str
    aggfunc: str = "sum"
    fill_value: Any = None
    margins: bool = False
    dropna: bool = True


@dataclass
class WindowSpec:
    """Window specification for window functions."""

    partition_by: List[str] = field(default_factory=list)
    order_by: List[Tuple[str, str]] = field(default_factory=list)
    preceding: Optional[int] = None
    following: Optional[int] = None


@dataclass
class RankResult:
    """Result of a ranking operation."""

    partition_key: Tuple
    rank: int
    dense_rank: int
    percent_rank: float
    row_number: int
    value: Any


@dataclass
class CrossTabResult:
    """Cross-tabulation result."""

    row_keys: List[Any]
    col_keys: List[Any]
    data: Dict[Tuple[Any, Any], Any]
    row_totals: Dict[Any, Any] = field(default_factory=dict)
    col_totals: Dict[Any, Any] = field(default_factory=dict)
    grand_total: Any = 0


class PivotTableBuilder:
    """Builds pivot tables from records."""

    def __init__(self, config: PivotConfig) -> None:
        self.config = config

    def build(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Build a pivot table from records."""
        idx = self.config.index
        col_field = self.config.columns
        val_field = self.config.values

        cells: Dict[Tuple, Any] = {}
        row_keys_set: set = set()
        col_keys_set: set = set()

        for record in records:
            row_key = tuple(record.get(k) for k in idx)
            col_key = record.get(col_field)
            val = record.get(val_field, 0)

            row_keys_set.add(row_key)
            col_keys_set.add(col_key)

            if row_key not in cells:
                cells[row_key] = {}
            if col_key not in cells[row_key]:
                cells[row_key][col_key] = []

            if isinstance(val, (int, float)):
                cells[row_key][col_key].append(val)
            else:
                cells[row_key][col_key].append(1)

        row_keys = sorted(row_keys_set, key=lambda x: str(x))
        col_keys = sorted(col_keys_set, key=lambda x: str(x))

        result: Dict[str, Any] = {"_index": idx, "_columns": col_keys}

        for row_key in row_keys:
            row_dict = dict(zip(idx, row_key))
            row_dict["_total"] = 0

            for col_key in col_keys:
                vals = cells.get(row_key, {}).get(col_key, [])
                if not vals:
                    if self.config.fill_value is not None:
                        row_dict[str(col_key)] = self.config.fill_value
                    continue

                if self.config.aggfunc == "sum":
                    agg_val = sum(vals)
                elif self.config.aggfunc == "mean":
                    agg_val = sum(vals) / len(vals)
                elif self.config.aggfunc == "count":
                    agg_val = len(vals)
                elif self.config.aggfunc == "min":
                    agg_val = min(vals)
                elif self.config.aggfunc == "max":
                    agg_val = max(vals)
                else:
                    agg_val = vals[0]

                row_dict[str(col_key)] = agg_val
                row_dict["_total"] += agg_val

            result[str(row_key)] = row_dict

        if self.config.margins:
            result["_margins"] = {}
            for col_key in col_keys:
                col_vals = []
                for row_key in row_keys:
                    vals = cells.get(row_key, {}).get(col_key, [])
                    if vals and isinstance(vals[0], (int, float)):
                        col_vals.extend(vals)
                if col_vals:
                    result["_margins"][str(col_key)] = sum(col_vals) / len(col_vals) if self.config.aggfunc == "mean" else sum(col_vals)

        return result


class CrossTabBuilder:
    """Builds cross-tabulation (contingency) tables."""

    def build(self, records: List[Dict[str, Any]], row_field: str, col_field: str, val_field: str) -> CrossTabResult:
        """Build a cross-tabulation from records."""
        row_keys_set: set = set()
        col_keys_set: set = set()
        data: Dict[Tuple[Any, Any], List[Any]] = defaultdict(list)

        for record in records:
            rk = record.get(row_field)
            ck = record.get(col_field)
            val = record.get(val_field, 1)

            row_keys_set.add(rk)
            col_keys_set.add(ck)
            data[(rk, ck)].append(val)

        row_keys = sorted(row_keys_set, key=lambda x: str(x))
        col_keys = sorted(col_keys_set, key=lambda x: str(x))

        row_totals: Dict[Any, Any] = defaultdict(float)
        col_totals: Dict[Any, Any] = defaultdict(float)
        grand_total = 0.0

        computed: Dict[Tuple[Any, Any], Any] = {}
        for (rk, ck), vals in data.items():
            if vals and isinstance(vals[0], (int, float)):
                total = sum(vals)
                computed[(rk, ck)] = total
                row_totals[rk] += total
                col_totals[ck] += total
                grand_total += total
            else:
                computed[(rk, ck)] = len(vals)

        return CrossTabResult(
            row_keys=row_keys,
            col_keys=col_keys,
            data=computed,
            row_totals=dict(row_totals),
            col_totals=dict(col_totals),
            grand_total=grand_total,
        )


class WindowFunctionExecutor:
    """Executes window functions over data partitions."""

    def __init__(self) -> None:
        pass

    def execute(
        self,
        records: List[Dict[str, Any]],
        window_spec: WindowSpec,
        functions: List[Tuple[str, WindowFunction]],
        result_field: str = "_window_result",
    ) -> List[Dict[str, Any]]:
        """Execute window functions over partitioned data."""
        if not window_spec.partition_by:
            partitions = [records]
        else:
            partition_groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
            for record in records:
                key = tuple(record.get(p) for p in window_spec.partition_by)
                partition_groups[key].append(record)
            partitions = list(partition_groups.values())

        results = []
        for partition in partitions:
            ordered = sorted(
                partition,
                key=lambda r: tuple(r.get(o[0], "") for o in window_spec.order_by),
            )

            partition_result = []
            for i, record in enumerate(ordered):
                row_result = record.copy()

                for field_name, func in functions:
                    value = self._apply_window_func(ordered, i, func, window_spec)
                    row_result[field_name] = value

                    if func == WindowFunction.ROW_NUMBER:
                        row_result["_row_num"] = i + 1
                    elif func == WindowFunction.RANK:
                        row_result["_rank"] = self._rank(ordered, i, window_spec.order_by)
                    elif func == WindowFunction.DENSE_RANK:
                        row_result["_dense_rank"] = self._dense_rank(ordered, i, window_spec.order_by)

                partition_result.append(row_result)

            results.extend(partition_result)

        return results

    def _apply_window_func(
        self,
        partition: List[Dict[str, Any]],
        current_idx: int,
        func: WindowFunction,
        spec: WindowSpec,
    ) -> Any:
        """Apply a specific window function."""
        window = self._get_window_frame(partition, current_idx, spec)

        if func == WindowFunction.LAG:
            offset = spec.preceding or 1
            idx = current_idx - offset
            return partition[idx]["_value"] if 0 <= idx < len(partition) else None

        if func == WindowFunction.LEAD:
            offset = spec.following or 1
            idx = current_idx + offset
            return partition[idx]["_value"] if 0 <= idx < len(partition) else None

        numeric_vals = [r.get("_value", 0) for r in window if isinstance(r.get("_value"), (int, float))]

        if not numeric_vals:
            return 0

        if func == WindowFunction.SUM:
            return sum(numeric_vals)
        elif func == WindowFunction.AVG:
            return sum(numeric_vals) / len(numeric_vals)
        elif func == WindowFunction.COUNT:
            return len(numeric_vals)
        elif func == WindowFunction.MAX:
            return max(numeric_vals)
        elif func == WindowFunction.MIN:
            return min(numeric_vals)
        elif func == WindowFunction.FIRST_VALUE:
            return window[0].get("_value") if window else None
        elif func == WindowFunction.LAST_VALUE:
            return window[-1].get("_value") if window else None

        return 0

    def _get_window_frame(
        self,
        partition: List[Dict[str, Any]],
        current_idx: int,
        spec: WindowSpec,
    ) -> List[Dict[str, Any]]:
        """Get window frame for current row."""
        start = max(0, current_idx - (spec.preceding or 0))
        end = min(len(partition), current_idx + 1 + (spec.following or 0))
        return partition[start:end]

    def _rank(self, partition: List[Dict[str, Any]], current_idx: int, order_by: List[Tuple[str, str]]) -> int:
        current_val = tuple(partition[current_idx].get(o[0]) for o in order_by)
        rank = 1
        for i in range(current_idx):
            val = tuple(partition[i].get(o[0]) for o in order_by)
            if val < current_val:
                rank += 1
        return rank

    def _dense_rank(self, partition: List[Dict[str, Any]], current_idx: int, order_by: List[Tuple[str, str]]) -> int:
        current_val = tuple(partition[current_idx].get(o[0]) for o in order_by)
        rank = 1
        seen_lower = False
        for i in range(current_idx):
            val = tuple(partition[i].get(o[0]) for o in order_by)
            if val < current_val:
                seen_lower = True
            elif val == current_val:
                pass
        return rank


class BIAction:
    """Action class for BI analytics operations."""

    def __init__(self) -> None:
        self.window_executor = WindowFunctionExecutor()

    def pivot(
        self,
        records: List[Dict[str, Any]],
        index: List[str],
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> Dict[str, Any]:
        """Generate a pivot table from records."""
        config = PivotConfig(index=index, columns=columns, values=values, aggfunc=aggfunc)
        builder = PivotTableBuilder(config)
        return builder.build(records)

    def crosstab(
        self,
        records: List[Dict[str, Any]],
        row_field: str,
        col_field: str,
        val_field: str,
    ) -> CrossTabResult:
        """Generate a cross-tabulation from records."""
        builder = CrossTabBuilder()
        return builder.build(records, row_field, col_field, val_field)

    def rank(
        self,
        records: List[Dict[str, Any]],
        partition_by: List[str],
        order_by: List[Tuple[str, str]],
        rank_field: str = "rank",
    ) -> List[Dict[str, Any]]:
        """Add ranking columns to records."""
        spec = WindowSpec(partition_by=partition_by, order_by=order_by)
        return self.window_executor.execute(
            records,
            spec,
            [(rank_field, WindowFunction.ROW_NUMBER)],
        )

    def running_total(
        self,
        records: List[Dict[str, Any]],
        value_field: str,
        partition_by: Optional[List[str]] = None,
        result_field: str = "running_total",
    ) -> List[Dict[str, Any]]:
        """Calculate running totals over records."""
        spec = WindowSpec(
            partition_by=partition_by or [],
            order_by=[],
            following=0,
        )
        for r in records:
            r["_value"] = r.get(value_field, 0)
        return self.window_executor.execute(
            records,
            spec,
            [(result_field, WindowFunction.SUM)],
        )
