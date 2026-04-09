"""Data aggregation action module for RabAI AutoClick.

Provides data aggregation operations:
- SumAggregatorAction: Sum aggregation
- AverageAggregatorAction: Average aggregation
- CountAggregatorAction: Count aggregation
- MinMaxAggregatorAction: Min/Max aggregation
- GroupAggregatorAction: Group-based aggregation
"""

from typing import Any, Dict, List, Optional
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SumAggregatorAction(BaseAction):
    """Sum aggregation."""
    action_type = "sum_aggregator"
    display_name = "求和聚合"
    description = "对数据进行求和聚合"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                values = [item.get(key, 0) for item in data if isinstance(item, dict)]
            else:
                values = [item for item in data if isinstance(item, (int, float))]
            
            total = sum(values)
            
            return ActionResult(
                success=True,
                message="Sum aggregation complete",
                data={
                    "aggregation": "sum",
                    "total": total,
                    "count": len(values),
                    "values": values[:100]
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class AverageAggregatorAction(BaseAction):
    """Average aggregation."""
    action_type = "average_aggregator"
    display_name = "平均值聚合"
    description = "对数据进行平均值聚合"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                values = [item.get(key, 0) for item in data if isinstance(item, dict)]
            else:
                values = [item for item in data if isinstance(item, (int, float))]
            
            if not values:
                return ActionResult(success=False, message="No numeric values found")
            
            total = sum(values)
            average = total / len(values)
            
            return ActionResult(
                success=True,
                message="Average aggregation complete",
                data={
                    "aggregation": "average",
                    "average": average,
                    "total": total,
                    "count": len(values),
                    "min": min(values),
                    "max": max(values)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class CountAggregatorAction(BaseAction):
    """Count aggregation."""
    action_type = "count_aggregator"
    display_name = "计数聚合"
    description = "对数据进行计数聚合"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            unique = params.get("unique", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                values = [item.get(key) for item in data if isinstance(item, dict)]
            else:
                values = data
            
            if unique:
                count = len(set(values))
                unique_values = list(set(values))
            else:
                count = len(values)
                unique_values = None
            
            return ActionResult(
                success=True,
                message="Count aggregation complete",
                data={
                    "aggregation": "count",
                    "count": count,
                    "unique_count": len(set(values)) if unique else None,
                    "unique_values": unique_values[:100] if unique_values else None
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class MinMaxAggregatorAction(BaseAction):
    """Min/Max aggregation."""
    action_type = "minmax_aggregator"
    display_name = "最值聚合"
    description = "对数据进行最小值和最大值聚合"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            return_index = params.get("return_index", False)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                items_with_values = [(i, item, item.get(key, 0)) 
                                    for i, item in enumerate(data) 
                                    if isinstance(item, dict)]
            else:
                items_with_values = [(i, item, item) 
                                    for i, item in enumerate(data) 
                                    if isinstance(item, (int, float))]
            
            if not items_with_values:
                return ActionResult(success=False, message="No numeric values found")
            
            sorted_items = sorted(items_with_values, key=lambda x: x[2])
            
            min_item = sorted_items[0]
            max_item = sorted_items[-1]
            
            result = {
                "aggregation": "minmax",
                "min": min_item[2],
                "max": max_item[2],
                "range": max_item[2] - min_item[2]
            }
            
            if return_index:
                result["min_index"] = min_item[0]
                result["max_index"] = max_item[0]
                result["min_item"] = min_item[1]
                result["max_item"] = max_item[1]
            
            return ActionResult(
                success=True,
                message="Min/Max aggregation complete",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class GroupAggregatorAction(BaseAction):
    """Group-based aggregation."""
    action_type = "group_aggregator"
    display_name = "分组聚合"
    description = "对数据进行分组聚合"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by")
            aggregate_on = params.get("aggregate_on")
            aggregation = params.get("aggregation", "count")
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if not group_by:
                return ActionResult(success=False, message="group_by is required")
            
            groups: Dict[str, List[Any]] = defaultdict(list)
            
            for item in data:
                if isinstance(item, dict):
                    key = item.get(group_by, "unknown")
                    groups[key].append(item)
            
            results = {}
            
            for group_key, items in groups.items():
                if aggregate_on:
                    values = [item.get(aggregate_on, 0) for item in items 
                             if isinstance(item, dict)]
                else:
                    values = items
                
                if aggregation == "count":
                    group_result = len(values)
                elif aggregation == "sum":
                    group_result = sum(values)
                elif aggregation == "avg":
                    group_result = sum(values) / len(values) if values else 0
                elif aggregation == "min":
                    group_result = min(values) if values else None
                elif aggregation == "max":
                    group_result = max(values) if values else None
                elif aggregation == "list":
                    group_result = values
                else:
                    group_result = len(values)
                
                results[group_key] = group_result
            
            return ActionResult(
                success=True,
                message="Group aggregation complete",
                data={
                    "aggregation": f"group_{aggregation}",
                    "group_by": group_by,
                    "aggregate_on": aggregate_on,
                    "group_count": len(groups),
                    "total_items": len(data),
                    "results": results
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class PercentileAggregatorAction(BaseAction):
    """Percentile aggregation."""
    action_type = "percentile_aggregator"
    display_name = "百分位聚合"
    description = "计算数据的百分位数"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            percentiles = params.get("percentiles", [25, 50, 75, 90, 95, 99])
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                values = sorted([item.get(key, 0) for item in data if isinstance(item, dict)])
            else:
                values = sorted([item for item in data if isinstance(item, (int, float))])
            
            if not values:
                return ActionResult(success=False, message="No numeric values found")
            
            def get_percentile(sorted_values: List, p: float) -> float:
                idx = (len(sorted_values) - 1) * p / 100
                lower = int(idx)
                upper = lower + 1
                weight = idx - lower
                
                if upper >= len(sorted_values):
                    return sorted_values[-1]
                
                return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
            
            result = {
                "aggregation": "percentile",
                "count": len(values),
                "min": values[0],
                "max": values[-1],
                "percentiles": {}
            }
            
            for p in percentiles:
                result["percentiles"][f"p{p}"] = get_percentile(values, p)
            
            return ActionResult(
                success=True,
                message="Percentile aggregation complete",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class HistogramAggregatorAction(BaseAction):
    """Histogram aggregation."""
    action_type = "histogram_aggregator"
    display_name = "直方图聚合"
    description = "计算数据的直方图分布"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            bins = params.get("bins", 10)
            
            if not data:
                return ActionResult(success=False, message="No data provided")
            
            if key:
                values = [item.get(key, 0) for item in data if isinstance(item, dict)]
            else:
                values = [item for item in data if isinstance(item, (int, float))]
            
            if not values:
                return ActionResult(success=False, message="No numeric values found")
            
            min_val = min(values)
            max_val = max(values)
            
            if min_val == max_val:
                return ActionResult(
                    success=True,
                    message="Histogram aggregation complete",
                    data={
                        "aggregation": "histogram",
                        "bins": bins,
                        "min": min_val,
                        "max": max_val,
                        "histogram": [{"range": [min_val, max_val], "count": len(values)}],
                        "total": len(values)
                    }
                )
            
            bin_width = (max_val - min_val) / bins
            histogram = []
            
            for i in range(bins):
                bin_start = min_val + i * bin_width
                bin_end = bin_start + bin_width
                count = sum(1 for v in values if bin_start <= v < bin_end)
                histogram.append({
                    "bin": i,
                    "range": [bin_start, bin_end],
                    "count": count
                })
            
            return ActionResult(
                success=True,
                message="Histogram aggregation complete",
                data={
                    "aggregation": "histogram",
                    "bins": bins,
                    "min": min_val,
                    "max": max_val,
                    "histogram": histogram,
                    "total": len(values)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
