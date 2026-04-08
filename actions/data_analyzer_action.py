"""Data analyzer action module for RabAI AutoClick.

Provides data analysis operations:
- AnalyzeDescriptiveAction: Descriptive statistics
- AnalyzeCorrelationAction: Correlation analysis
- AnalyzeOutlierAction: Outlier detection
- AnalyzeTrendAction: Trend analysis
- AnalyzeDistributionAction: Distribution analysis
"""

import math
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AnalyzeDescriptiveAction(BaseAction):
    """Compute descriptive statistics."""
    action_type = "analyze_descriptive"
    display_name = "描述性统计"
    description = "计算描述性统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = sorted([d.get(field, 0) for d in data])
            n = len(values)
            mean = sum(values) / n
            variance = sum((v - mean) ** 2 for v in values) / n
            std = math.sqrt(variance)
            median = values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2
            min_val = values[0]
            max_val = values[-1]

            return ActionResult(
                success=True,
                data={"count": n, "mean": mean, "median": median, "std": std, "min": min_val, "max": max_val, "variance": variance},
                message=f"Descriptive stats: n={n}, mean={mean:.2f}, std={std:.2f}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Analyze descriptive failed: {e}")


class AnalyzeCorrelationAction(BaseAction):
    """Compute correlation between fields."""
    action_type = "analyze_correlation"
    display_name = "相关性分析"
    description = "计算字段相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field_x = params.get("field_x", "x")
            field_y = params.get("field_y", "y")

            if not data:
                return ActionResult(success=False, message="data is required")

            x_vals = [d.get(field_x, 0) for d in data]
            y_vals = [d.get(field_y, 0) for d in data]
            n = len(x_vals)

            mean_x = sum(x_vals) / n
            mean_y = sum(y_vals) / n

            numerator = sum((x_vals[i] - mean_x) * (y_vals[i] - mean_y) for i in range(n))
            denom_x = math.sqrt(sum((v - mean_x) ** 2 for v in x_vals))
            denom_y = math.sqrt(sum((v - mean_y) ** 2 for v in y_vals))

            if denom_x * denom_y == 0:
                correlation = 0.0
            else:
                correlation = numerator / (denom_x * denom_y)

            return ActionResult(
                success=True,
                data={"correlation": correlation, "field_x": field_x, "field_y": field_y, "n": n},
                message=f"Correlation {field_x} vs {field_y}: {correlation:.4f}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Analyze correlation failed: {e}")


class AnalyzeOutlierAction(BaseAction):
    """Detect outliers using IQR method."""
    action_type = "analyze_outlier"
    display_name = "异常值检测"
    description = "IQR方法检测异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            threshold = params.get("threshold", 1.5)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = sorted([d.get(field, 0) for d in data])
            n = len(values)
            q1 = values[n // 4]
            q3 = values[3 * n // 4]
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr

            outliers = [{"index": i, "value": v, "d": data[i]} for i, v in enumerate(values) if v < lower or v > upper]

            return ActionResult(
                success=True,
                data={"outliers": outliers, "outlier_count": len(outliers), "lower_bound": lower, "upper_bound": upper, "q1": q1, "q3": q3},
                message=f"Outliers: {len(outliers)} detected (threshold={threshold})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Analyze outlier failed: {e}")


class AnalyzeTrendAction(BaseAction):
    """Analyze trend in time series data."""
    action_type = "analyze_trend"
    display_name = "趋势分析"
    description = "时间序列趋势分析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            n = len(values)
            x_vals = list(range(n))
            x_mean = sum(x_vals) / n
            y_mean = sum(values) / n

            numerator = sum((x_vals[i] - x_mean) * (values[i] - y_mean) for i in range(n))
            denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))

            slope = numerator / denominator if denominator != 0 else 0
            intercept = y_mean - slope * x_mean

            if slope > 0.1:
                trend = "increasing"
            elif slope < -0.1:
                trend = "decreasing"
            else:
                trend = "stable"

            return ActionResult(
                success=True,
                data={"trend": trend, "slope": slope, "intercept": intercept, "data_points": n},
                message=f"Trend analysis: {trend} (slope={slope:.4f})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Analyze trend failed: {e}")


class AnalyzeDistributionAction(BaseAction):
    """Analyze data distribution."""
    action_type = "analyze_distribution"
    display_name = "分布分析"
    description = "数据分析分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            bin_count = params.get("bin_count", 10)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            min_val = min(values)
            max_val = max(values)
            bin_width = (max_val - min_val) / bin_count if bin_count > 0 else 1

            bins = [0] * bin_count
            for v in values:
                bin_idx = min(int((v - min_val) / bin_width), bin_count - 1) if bin_width > 0 else 0
                bins[bin_idx] += 1

            hist = [{"bin_start": min_val + i * bin_width, "bin_end": min_val + (i + 1) * bin_width, "count": c} for i, c in enumerate(bins)]

            return ActionResult(
                success=True,
                data={"histogram": hist, "bin_count": bin_count, "min": min_val, "max": max_val, "n": len(values)},
                message=f"Distribution: {len(values)} values in {bin_count} bins",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Analyze distribution failed: {e}")
