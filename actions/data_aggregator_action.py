"""Data Aggregator Action Module. Aggregates data using various operations."""
import sys, os, statistics
from typing import Any
from dataclasses import dataclass, field
from collections import defaultdict
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class AggregationResult:
    operation: str; field: str; value: Any; group_by: str = ""
    group_values: dict = field(default_factory=dict)

class DataAggregatorAction(BaseAction):
    action_type = "data_aggregator"; display_name = "数据聚合"
    description = "聚合数据"
    def __init__(self) -> None: super().__init__()
    def _agg_simple(self, records: list, field_name: str, operation: str) -> Any:
        values = [r.get(field_name) for r in records if r.get(field_name) is not None]
        if not values: return None
        if operation == "sum": return sum(values)
        elif operation in ("avg","mean"): return statistics.mean(values)
        elif operation == "count": return len(values)
        elif operation == "min": return min(values)
        elif operation == "max": return max(values)
        elif operation == "median": return statistics.median(values)
        elif operation == "stddev": return statistics.stdev(values) if len(values)>1 else 0
        elif operation == "p50":
            sv = sorted(values); idx = int(len(sv)*0.50)
            return sv[min(idx, len(sv)-1)]
        elif operation == "p95":
            sv = sorted(values); idx = int(len(sv)*0.95)
            return sv[min(idx, len(sv)-1)]
        return None
    def execute(self, context: Any, params: dict) -> ActionResult:
        data = params.get("data",[]); operation = params.get("operation","count")
        field_name = params.get("field"); group_by = params.get("group_by")
        operations = params.get("operations",[])
        if not data: return ActionResult(success=False, message="No data")
        if operations:
            results = {}
            for spec in operations:
                op = spec.get("operation","count"); fld = spec.get("field")
                results[f"{op}_{fld}"] = self._agg_simple(data, fld, op) if fld else len(data)
            return ActionResult(success=True, message=f"Multi-agg: {len(results)} ops", data={"results": results})
        if group_by:
            groups = defaultdict(list)
            for record in data: groups[str(record.get(group_by,"unknown"))].append(record)
            group_results = {}
            for key, group_records in groups.items():
                group_results[key] = self._agg_simple(group_records, field_name, operation) if field_name else len(group_records)
            result = AggregationResult(operation=operation, field=field_name or "count", value=group_results, group_by=group_by, group_values=group_results)
        else:
            value = self._agg_simple(data, field_name, operation)
            result = AggregationResult(operation=operation, field=field_name or "count", value=value)
        return ActionResult(success=True, message=f"{operation.upper()} of {field_name or 'count'}: {result.value}", data=vars(result))
