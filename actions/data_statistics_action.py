"""Data statistics action module for RabAI AutoClick.

Provides statistical operations:
- StatsMeanAction: Calculate mean
- StatsMedianAction: Calculate median
- StatsModeAction: Calculate mode
- StatsStdDevAction: Standard deviation
- StatsPercentileAction: Calculate percentiles
"""

import math
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StatsMeanAction(BaseAction):
    """Calculate mean."""
    action_type = "stats_mean"
    display_name = "计算均值"
    description = "计算平均值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            mean = sum(values) / len(values) if values else 0

            return ActionResult(success=True, data={"mean": mean, "count": len(values), "field": field}, message=f"Mean of {field}: {mean:.4f}")
        except Exception as e:
            return ActionResult(success=False, message=f"Stats mean failed: {e}")


class StatsMedianAction(BaseAction):
    """Calculate median."""
    action_type = "stats_median"
    display_name = "计算中位数"
    description = "计算中位数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = sorted([d.get(field, 0) for d in data])
            n = len(values)
            median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2

            return ActionResult(success=True, data={"median": median, "count": len(values), "field": field}, message=f"Median of {field}: {median:.4f}")
        except Exception as e:
            return ActionResult(success=False, message=f"Stats median failed: {e}")


class StatsModeAction(BaseAction):
    """Calculate mode."""
    action_type = "stats_mode"
    display_name = "计算众数"
    description = "计算众数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            freq: Dict = {}
            for v in values:
                freq[v] = freq.get(v, 0) + 1
            mode_val = max(freq, key=freq.get)
            mode_count = freq[mode_val]

            return ActionResult(
                success=True,
                data={"mode": mode_val, "mode_count": mode_count, "unique_values": len(freq), "field": field},
                message=f"Mode of {field}: {mode_val} (appears {mode_count} times)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stats mode failed: {e}")


class StatsStdDevAction(BaseAction):
    """Calculate standard deviation."""
    action_type = "stats_stddev"
    display_name = "计算标准差"
    description = "计算标准差"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            n = len(values)
            mean = sum(values) / n
            variance = sum((v - mean) ** 2 for v in values) / n
            stddev = math.sqrt(variance)

            return ActionResult(
                success=True,
                data={"stddev": stddev, "variance": variance, "mean": mean, "count": n, "field": field},
                message=f"StdDev of {field}: {stddev:.4f} (variance={variance:.4f})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stats stddev failed: {e}")


class StatsPercentileAction(BaseAction):
    """Calculate percentiles."""
    action_type = "stats_percentile"
    display_name = "计算百分位数"
    description = "计算百分位数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            percentiles = params.get("percentiles", [25, 50, 75, 90, 95, 99])

            if not data:
                return ActionResult(success=False, message="data is required")

            values = sorted([d.get(field, 0) for d in data])
            n = len(values)
            result = {}

            for p in percentiles:
                idx = int(n * p / 100)
                if idx >= n:
                    idx = n - 1
                result[f"p{p}"] = values[idx]

            return ActionResult(
                success=True,
                data={"percentiles": result, "count": n, "field": field},
                message=f"Percentiles of {field}: " + ", ".join(f"p{p}={result[f'p{p}']:.2f}" for p in percentiles),
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stats percentile failed: {e}")
