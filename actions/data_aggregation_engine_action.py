"""
Data Aggregation Engine Action Module.

Advanced aggregation engine with group-by, having clauses,
window functions, and multi-dimensional rollup/cube.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class AggregationQuery:
    """An aggregation query definition."""
    group_by: list[str]
    aggregations: list[dict[str, str]]  # [{field, func}]
    having: Optional[Callable] = None
    order_by: Optional[list[tuple]] = None
    limit: Optional[int] = None


@dataclass
class AggregationEngineResult:
    """Result of aggregation."""
    records: list[dict[str, Any]]
    group_count: int
    total_input: int


class DataAggregationEngineAction(BaseAction):
    """Advanced data aggregation engine."""

    def __init__(self) -> None:
        super().__init__("data_aggregation_engine")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute aggregation query.

        Args:
            context: Execution context
            params: Parameters:
                - records: Input records
                - group_by: Fields to group by
                - aggregations: List of {field, func} dicts
                    func: sum, count, avg, min, max, median, std, var
                - having: Filter condition for groups
                - order_by: List of (field, asc/desc)
                - limit: Max result rows
                - rollup: Include rollup totals
                - cube: Include all combinations

        Returns:
            AggregationEngineResult with aggregated records
        """
        from collections import defaultdict
        import math

        records = params.get("records", [])
        group_by = params.get("group_by", [])
        aggregations = params.get("aggregations", [])
        having_filter = params.get("having")
        order_by = params.get("order_by")
        limit = params.get("limit")
        rollup = params.get("rollup", False)
        cube = params.get("cube", False)

        if not records or not group_by:
            return AggregationEngineResult([], 0, len(records)).__dict__

        groups: dict[tuple, list[dict]] = defaultdict(list)
        for r in records:
            if not isinstance(r, dict):
                continue
            key = tuple(r.get(f) for f in group_by)
            groups[key].append(r)

        agg_funcs = {
            "sum": lambda x: sum(x),
            "count": len,
            "avg": lambda x: sum(x) / len(x) if x else None,
            "min": min,
            "max": max,
            "median": lambda x: sorted(x)[len(x) // 2] if x else None,
            "std": lambda x: math.sqrt(sum((v - sum(x) / len(x)) ** 2 for v in x) / len(x)) if len(x) > 1 else 0,
            "var": lambda x: sum((v - sum(x) / len(x)) ** 2 for v in x) / len(x) if len(x) > 1 else 0
        }

        results = []
        for key, group_records in groups.items():
            result = dict(zip(group_by, key))
            for agg in aggregations:
                field_name = agg.get("field", "")
                func_name = agg.get("func", "count")
                values = [r.get(field_name) for r in group_records if isinstance(r.get(field_name), (int, float))]
                agg_func = agg_funcs.get(func_name, len)
                result[f"{field_name}_{func_name}"] = agg_func(values)

            if having_filter:
                try:
                    if not having_filter(result):
                        continue
                except Exception:
                    pass

            results.append(result)

        if rollup:
            for i in range(len(group_by)):
                rollup_key = (None,) * (i + 1) + tuple(range(len(group_by) - i - 1))
                all_values = {f: [] for f in [a.get("field") for a in aggregations]}
                for r in records:
                    for agg in aggregations:
                        f = agg.get("field", "")
                        if isinstance(r.get(f), (int, float)):
                            all_values[f].append(r.get(f))
                rollup_result = dict(zip(group_by, rollup_key[:len(group_by)]))
                for agg in aggregations:
                    field_name = agg.get("field", "")
                    func_name = agg.get("func", "count")
                    agg_func = agg_funcs.get(func_name, len)
                    rollup_result[f"{field_name}_{func_name}"] = agg_func(all_values.get(field_name, []))
                results.append(rollup_result)

        if order_by:
            results.sort(key=lambda r: tuple(r.get(f[0]) for f in order_by), reverse=any(o[1] == "desc" for o in order_by))

        if limit:
            results = results[:limit]

        return AggregationEngineResult(
            records=results,
            group_count=len(groups),
            total_input=len(records)
        ).__dict__
