"""Data analytics action module for RabAI AutoClick.

Provides data analytics operations:
- AnalyticsStatsAction: Calculate statistics
- AnalyticsTrendAction: Analyze trends
- AnalyticsPercentileAction: Calculate percentiles
- AnalyticsCorrelationAction: Calculate correlations
"""

import math
from typing import Any, Dict, List, Optional
from collections import Counter


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AnalyticsStatsAction(BaseAction):
    """Calculate statistics."""
    action_type = "analytics_stats"
    display_name = "统计分析"
    description = "计算统计数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                else:
                    v = item
                if v is not None:
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        pass

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            values.sort()
            n = len(values)
            mean = sum(values) / n
            variance = sum((x - mean) ** 2 for x in values) / n
            std = math.sqrt(variance)
            median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2
            min_val = values[0]
            max_val = values[-1]

            return ActionResult(
                success=True,
                message=f"Statistics for {n} values",
                data={
                    "count": n,
                    "mean": mean,
                    "median": median,
                    "std": std,
                    "variance": variance,
                    "min": min_val,
                    "max": max_val,
                    "sum": sum(values)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Analytics stats failed: {str(e)}")


class AnalyticsTrendAction(BaseAction):
    """Analyze trends."""
    action_type = "analytics_trend"
    display_name = "趋势分析"
    description = "分析数据趋势"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            time_field = params.get("time_field", "")

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                    t = item.get(time_field, len(values))
                else:
                    v = item
                    t = len(values)
                if v is not None:
                    try:
                        values.append((t, float(v)))
                    except (ValueError, TypeError):
                        pass

            if len(values) < 2:
                return ActionResult(success=False, message="Not enough data points for trend analysis")

            values.sort(key=lambda x: x[0])
            n = len(values)
            x_vals = [v[0] for v in values]
            y_vals = [v[1] for v in values]
            x_mean = sum(x_vals) / n
            y_mean = sum(y_vals) / n

            numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
            denominator = sum((x - x_mean) ** 2 for x in x_vals)

            if denominator == 0:
                slope = 0
            else:
                slope = numerator / denominator

            intercept = y_mean - slope * x_mean
            trend = "increasing" if slope > 0.001 else "decreasing" if slope < -0.001 else "stable"

            return ActionResult(
                success=True,
                message=f"Trend analysis: {trend} (slope={slope:.4f})",
                data={
                    "trend": trend,
                    "slope": slope,
                    "intercept": intercept,
                    "data_points": n
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Analytics trend failed: {str(e)}")


class AnalyticsPercentileAction(BaseAction):
    """Calculate percentiles."""
    action_type = "analytics_percentile"
    display_name = "百分位数"
    description = "计算百分位数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            percentiles = params.get("percentiles", [25, 50, 75, 90, 95, 99])

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                else:
                    v = item
                if v is not None:
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        pass

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            values.sort()
            n = len(values)
            result = {}

            for p in percentiles:
                if p < 0 or p > 100:
                    continue
                k = (p / 100) * (n - 1)
                f = math.floor(k)
                c = math.ceil(k)
                if f == c:
                    result[p] = values[int(k)]
                else:
                    d0 = values[int(f)] * (c - k)
                    d1 = values[int(c)] * (k - f)
                    result[p] = d0 + d1

            return ActionResult(
                success=True,
                message=f"Percentiles calculated for {n} values",
                data={"percentiles": result, "count": n}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Analytics percentile failed: {str(e)}")


class AnalyticsCorrelationAction(BaseAction):
    """Calculate correlations."""
    action_type = "analytics_correlation"
    display_name = "相关性分析"
    description = "计算数据相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x", "")
            field_y = params.get("field_y", "")
            method = params.get("method", "pearson")

            pairs = []
            for item in data:
                if isinstance(item, dict):
                    x = item.get(field_x)
                    y = item.get(field_y)
                else:
                    x, y = None, None
                if x is not None and y is not None:
                    try:
                        pairs.append((float(x), float(y)))
                    except (ValueError, TypeError):
                        pass

            if len(pairs) < 2:
                return ActionResult(success=False, message="Not enough data pairs")

            n = len(pairs)
            x_vals = [p[0] for p in pairs]
            y_vals = [p[1] for p in pairs]
            x_mean = sum(x_vals) / n
            y_mean = sum(y_vals) / n

            numerator = sum((x - x_mean) * (y - y_mean) for x, y in pairs)
            x_denom = math.sqrt(sum((x - x_mean) ** 2 for x in x_vals))
            y_denom = math.sqrt(sum((y - y_mean) ** 2 for y in y_vals))

            if x_denom == 0 or y_denom == 0:
                correlation = 0
            else:
                correlation = numerator / (x_denom * y_denom)

            strength = "none"
            abs_corr = abs(correlation)
            if abs_corr > 0.7:
                strength = "strong"
            elif abs_corr > 0.4:
                strength = "moderate"
            elif abs_corr > 0.2:
                strength = "weak"

            return ActionResult(
                success=True,
                message=f"Correlation: {correlation:.4f} ({strength})",
                data={
                    "correlation": correlation,
                    "strength": strength,
                    "method": method,
                    "pairs": n
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Analytics correlation failed: {str(e)}")
