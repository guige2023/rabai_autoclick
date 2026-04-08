"""Data analyzer action module for RabAI AutoClick.

Provides data analysis operations:
- DataAnalyzerAction: Analyze data statistics
- DataProfilerAction: Profile data characteristics
- DataOutlierAction: Detect data outliers
- DataCorrelationAction: Analyze correlations
"""

import math
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from collections import Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataAnalyzerAction(BaseAction):
    """Analyze data statistics."""
    action_type = "data_analyzer"
    display_name = "数据分析器"
    description = "分析数据统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            analysis_type = params.get("analysis_type", "basic")
            numeric_only = params.get("numeric_only", False)

            if not isinstance(data, list):
                data = [data]

            if numeric_only:
                numeric_data = [x for x in data if isinstance(x, (int, float))]
                data = numeric_data

            if not data:
                return ActionResult(success=False, message="No data to analyze")

            if analysis_type == "basic":
                numeric_data = [x for x in data if isinstance(x, (int, float))]
                non_numeric = [x for x in data if not isinstance(x, (int, float))]

                result = {
                    "count": len(data),
                    "numeric_count": len(numeric_data),
                    "non_numeric_count": len(non_numeric)
                }

                if numeric_data:
                    result.update(self._analyze_numeric(numeric_data))
                if non_numeric:
                    result["most_common"] = Counter(non_numeric).most_common(5)

                return ActionResult(
                    success=True,
                    data={
                        "analysis": result,
                        "analysis_type": "basic"
                    },
                    message=f"Analyzed {len(data)} items: {len(numeric_data)} numeric, {len(non_numeric)} non-numeric"
                )

            elif analysis_type == "detailed":
                numeric_data = [x for x in data if isinstance(x, (int, float))]
                result = self._analyze_numeric(numeric_data) if numeric_data else {}
                result["count"] = len(data)
                result["unique_count"] = len(set(str(x) for x in data))
                result["value_distribution"] = self._get_distribution(data)

                return ActionResult(
                    success=True,
                    data={
                        "analysis": result,
                        "analysis_type": "detailed"
                    },
                    message=f"Detailed analysis of {len(data)} items"
                )

            return ActionResult(success=False, message=f"Unknown analysis_type: {analysis_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data analyzer error: {str(e)}")

    def _analyze_numeric(self, data: List) -> Dict:
        sorted_data = sorted(data)
        n = len(data)

        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / n
        std_dev = math.sqrt(variance)
        median = sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2

        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_data[q1_idx]
        q3 = sorted_data[q3_idx]
        iqr = q3 - q1

        return {
            "mean": mean,
            "median": median,
            "std_dev": std_dev,
            "variance": variance,
            "min": min(data),
            "max": max(data),
            "range": max(data) - min(data),
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "sum": sum(data)
        }

    def _get_distribution(self, data: List) -> Dict:
        counter = Counter(str(x) for x in data)
        total = len(data)
        distribution = {}
        for value, count in counter.most_common(10):
            distribution[value] = {"count": count, "percentage": round(count / total * 100, 2)}
        return distribution

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"analysis_type": "basic", "numeric_only": False}


class DataProfilerAction(BaseAction):
    """Profile data characteristics."""
    action_type = "data_profiler"
    display_name = "数据剖析器"
    description = "剖析数据特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to profile")

            profile = {
                "total_records": len(data),
                "total_fields": 0,
                "field_profiles": {}
            }

            if isinstance(data[0], dict):
                profile["total_fields"] = len(data[0])
                all_fields = set()
                for record in data:
                    all_fields.update(record.keys())

                for field in all_fields:
                    values = [record.get(field) for record in data if field in record]
                    null_count = sum(1 for v in values if v is None or v == "")
                    unique_values = len(set(str(v) for v in values if v is not None))

                    field_profile = {
                        "null_count": null_count,
                        "null_percentage": round(null_count / len(data) * 100, 2) if data else 0,
                        "unique_values": unique_values,
                        "completeness": round((len(data) - null_count) / len(data) * 100, 2) if data else 0
                    }

                    numeric_values = [v for v in values if isinstance(v, (int, float))]
                    if numeric_values:
                        field_profile["min"] = min(numeric_values)
                        field_profile["max"] = max(numeric_values)
                        field_profile["mean"] = sum(numeric_values) / len(numeric_values)

                    profile["field_profiles"][field] = field_profile

            else:
                values = data
                null_count = sum(1 for v in values if v is None or v == "")
                profile["null_count"] = null_count
                profile["null_percentage"] = round(null_count / len(data) * 100, 2) if data else 0
                profile["unique_values"] = len(set(str(v) for v in values if v is not None))

            return ActionResult(
                success=True,
                data={
                    "profile": profile,
                    "profiled_fields": profile["total_fields"]
                },
                message=f"Profiled {profile['total_records']} records with {profile['total_fields']} fields"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data profiler error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DataOutlierAction(BaseAction):
    """Detect data outliers."""
    action_type = "data_outlier"
    display_name = "异常值检测"
    description = "检测数据异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)

            if not isinstance(data, list):
                data = [data]

            numeric_data = [(i, x) for i, x in enumerate(data) if isinstance(x, (int, float))]

            if not numeric_data:
                return ActionResult(success=True, data={"outliers": [], "method": method, "count": 0}, message="No numeric data for outlier detection")

            values = [x for _, x in numeric_data]
            sorted_values = sorted(values)
            n = len(values)

            if method == "iqr":
                q1_idx = n // 4
                q3_idx = 3 * n // 4
                q1 = sorted_values[q1_idx]
                q3 = sorted_values[q3_idx]
                iqr = q3 - q1
                lower_bound = q1 - threshold * iqr
                upper_bound = q3 + threshold * iqr

                outliers = [(idx, val) for idx, val in numeric_data if val < lower_bound or val > upper_bound]

            elif method == "zscore":
                mean = sum(values) / n
                std_dev = math.sqrt(sum((x - mean) ** 2 for x in values) / n)
                z_threshold = threshold

                outliers = []
                for idx, val in numeric_data:
                    z_score = abs((val - mean) / std_dev) if std_dev > 0 else 0
                    if z_score > z_threshold:
                        outliers.append((idx, val, round(z_score, 2)))

            elif method == "mad":
                sorted_values = sorted(values)
                median = sorted_values[n // 2] if n % 2 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
                med_abs_dev = sorted([abs(x - median) for x in values])
                mad = med_abs_dev[n // 2]
                threshold_mad = threshold * mad

                outliers = [(idx, val) for idx, val in numeric_data if abs(val - median) > threshold_mad]

            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            outlier_values = [val for idx, val in outliers]
            outlier_indices = [idx for idx, _ in outliers]

            return ActionResult(
                success=True,
                data={
                    "outliers": outlier_values,
                    "outlier_indices": outlier_indices,
                    "count": len(outliers),
                    "method": method,
                    "threshold": threshold
                },
                message=f"Detected {len(outliers)} outliers using {method} method"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data outlier error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"method": "iqr", "threshold": 1.5}


class DataCorrelationAction(BaseAction):
    """Analyze correlations."""
    action_type = "data_correlation"
    display_name = "相关性分析"
    description = "分析数据相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            columns = params.get("columns", [])
            method = params.get("method", "pearson")

            if not isinstance(data, list) or not data:
                return ActionResult(success=False, message="No data to analyze")

            if not columns:
                if isinstance(data[0], dict):
                    columns = list(data[0].keys())

            if not columns:
                return ActionResult(success=False, message="No columns specified")

            numeric_pairs = []
            for col_a in columns:
                for col_b in columns:
                    if col_a >= col_b:
                        continue

                    values_a = [r.get(col_a) if isinstance(r, dict) else None for r in data]
                    values_b = [r.get(col_b) if isinstance(r, dict) else None for r in data]

                    pairs = [(a, b) for a, b in zip(values_a, values_b) if isinstance(a, (int, float)) and isinstance(b, (int, float))]

                    if len(pairs) < 2:
                        continue

                    corr = self._calculate_correlation([p[0] for p in pairs], [p[1] for p in pairs], method)
                    numeric_pairs.append({
                        "column_a": col_a,
                        "column_b": col_b,
                        "correlation": round(corr, 4),
                        "pairs_count": len(pairs)
                    })

            numeric_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

            return ActionResult(
                success=True,
                data={
                    "correlations": numeric_pairs,
                    "count": len(numeric_pairs),
                    "method": method
                },
                message=f"Computed {len(numeric_pairs)} correlations using {method}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data correlation error: {str(e)}")

    def _calculate_correlation(self, x: List, y: List, method: str) -> float:
        n = len(x)
        if n < 2:
            return 0.0

        if method == "pearson":
            mean_x = sum(x) / n
            mean_y = sum(y) / n
            numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
            denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
            denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))
            if denom_x == 0 or denom_y == 0:
                return 0.0
            return numerator / (denom_x * denom_y)

        elif method == "spearman":
            rank_x = self._rankify(x)
            rank_y = self._rankify(y)
            return self._calculate_correlation(rank_x, rank_y, "pearson")

        return 0.0

    def _rankify(self, values: List) -> List:
        sorted_with_indices = sorted(enumerate(values), key=lambda x: x[1])
        ranks = [0] * len(values)
        for rank, (original_idx, _) in enumerate(sorted_with_indices, 1):
            ranks[original_idx] = rank
        return ranks

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"columns": [], "method": "pearson"}
