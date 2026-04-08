"""
Data Funnel Action Module.

Filters and transforms data through multiple stages like a funnel:
applies sequential filters, transformations, and aggregations.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class FunnelStage:
    """A single stage in the funnel."""
    name: str
    filter_func: Optional[Callable] = None
    transform_func: Optional[Callable] = None
    stage_type: str = "filter"  # filter, transform, aggregate


@dataclass
class FunnelResult:
    """Result of funnel execution."""
    input_count: int
    output_count: int
    stage_results: dict[str, int]
    final_output: list[dict[str, Any]]


class DataFunnelAction(BaseAction):
    """Filter and transform data through funnel stages."""

    def __init__(self) -> None:
        super().__init__("data_funnel")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Process data through funnel stages.

        Args:
            context: Execution context
            params: Parameters:
                - records: Input records
                - stages: List of stage configs
                    - name: Stage name
                    - type: filter, transform, aggregate
                    - filter_func: Function(record) -> bool
                    - transform_func: Function(record) -> record
                    - aggregate_func: Function(records) -> aggregated
                    - condition: For filter type, simple condition dict

        Returns:
            FunnelResult with stage counts and final output
        """
        records = params.get("records", [])
        stage_configs = params.get("stages", [])

        current_data = list(records)
        stage_results: dict[str, int] = {}
        input_count = len(current_data)

        for stage_cfg in stage_configs:
            name = stage_cfg.get("name", "unnamed")
            stage_type = stage_cfg.get("type", "filter")

            if stage_type == "filter":
                condition = stage_cfg.get("condition")
                filter_func = stage_cfg.get("filter_func")

                if filter_func:
                    before_count = len(current_data)
                    current_data = [r for r in current_data if filter_func(r)]
                    after_count = len(current_data)
                    stage_results[name] = before_count - after_count
                elif condition:
                    before_count = len(current_data)
                    current_data = self._apply_condition(current_data, condition)
                    after_count = len(current_data)
                    stage_results[name] = before_count - after_count
                else:
                    stage_results[name] = 0

            elif stage_type == "transform":
                transform_func = stage_cfg.get("transform_func")
                expression = stage_cfg.get("expression")

                if transform_func:
                    current_data = [transform_func(r) for r in current_data]
                elif expression:
                    current_data = self._apply_expression(current_data, expression)
                stage_results[name] = len(current_data)

            elif stage_type == "aggregate":
                agg_func = stage_cfg.get("agg_func")
                group_by = stage_cfg.get("group_by")
                if agg_func and group_by:
                    current_data = self._aggregate(current_data, group_by, agg_func)
                stage_results[name] = len(current_data)

        return FunnelResult(
            input_count=input_count,
            output_count=len(current_data),
            stage_results=stage_results,
            final_output=current_data
        )

    def _apply_condition(self, records: list[dict], condition: dict) -> list[dict]:
        """Apply a simple condition filter."""
        field = condition.get("field", "")
        operator = condition.get("operator", "eq")
        value = condition.get("value")

        result = []
        for r in records:
            if not isinstance(r, dict):
                continue
            field_val = r.get(field)
            if operator == "eq" and field_val == value:
                result.append(r)
            elif operator == "ne" and field_val != value:
                result.append(r)
            elif operator == "gt" and field_val is not None and field_val > value:
                result.append(r)
            elif operator == "gte" and field_val is not None and field_val >= value:
                result.append(r)
            elif operator == "lt" and field_val is not None and field_val < value:
                result.append(r)
            elif operator == "lte" and field_val is not None and field_val <= value:
                result.append(r)
            elif operator == "in" and field_val in (value if isinstance(value, list) else [value]):
                result.append(r)
            elif operator == "contains" and isinstance(field_val, str) and value in field_val:
                result.append(r)
        return result

    def _apply_expression(self, records: list[dict], expression: str) -> list[dict]:
        """Apply a simple transform expression."""
        import re
        new_records = []
        for r in records:
            if not isinstance(r, dict):
                new_records.append(r)
                continue
            new_r = dict(r)
            for match in re.finditer(r'\{(\w+)\}\s*=\s*(.+)', expression):
                field_name = match.group(1)
                expr = match.group(2)
                try:
                    new_r[field_name] = eval(expr, {"r": r}, {})
                except Exception:
                    pass
            new_records.append(new_r)
        return new_records

    def _aggregate(self, records: list[dict], group_by: str, agg_func: str) -> list[dict]:
        """Aggregate records by group."""
        from collections import defaultdict
        groups: dict[Any, list] = defaultdict(list)
        for r in records:
            if isinstance(r, dict) and group_by in r:
                groups[r[group_by]].append(r)

        agg_funcs = {
            "sum": lambda x: sum(x),
            "count": len,
            "mean": lambda x: sum(x) / len(x) if x else None,
            "min": min,
            "max": max
        }
        agg = agg_funcs.get(agg_func, len)

        result = []
        for group_key, group_records in groups.items():
            numeric_values = [r.get("value", 0) for r in group_records if isinstance(r.get("value"), (int, float))]
            result.append({
                group_by: group_key,
                f"{agg_func}_value": agg(numeric_values) if numeric_values else 0,
                "count": len(group_records)
            })
        return result
