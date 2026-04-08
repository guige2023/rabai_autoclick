"""Data profiling action module for RabAI AutoClick.

Provides data profiling operations:
- DataProfilerAction: Profile dataset structure and statistics
- ColumnProfilerAction: Profile individual columns
- DataQualityAction: Assess data quality metrics
"""

import math
from collections import Counter
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataProfilerAction(BaseAction):
    """Profile dataset structure and statistics."""
    action_type = "data_profiler"
    display_name = "数据画像"
    description = "分析数据集结构和统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            profile_type = params.get("profile_type", "basic")

            if data is None:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list):
                return self._profile_list(data, profile_type)
            elif isinstance(data, dict):
                return self._profile_dict(data, profile_type)
            else:
                return ActionResult(success=False, message="Unsupported data type")

        except Exception as e:
            return ActionResult(success=False, message=f"DataProfiler error: {e}")

    def _profile_list(self, data: List, profile_type: str) -> ActionResult:
        if not data:
            return ActionResult(success=True, message="Empty dataset", data={"row_count": 0})

        row_count = len(data)
        types = Counter(type(item).__name__ for item in data)
        type_counts = dict(types)

        profile = {
            "row_count": row_count,
            "type_distribution": type_counts,
            "types": list(types.keys()),
        }

        if profile_type in ("basic", "full"):
            null_count = sum(1 for item in data if item is None)
            profile["null_count"] = null_count
            profile["null_percent"] = round(null_count / row_count * 100, 2)

            if all(isinstance(item, (int, float)) for item in data):
                nums = [item for item in data if item is not None]
                profile["numeric_stats"] = self._numeric_stats(nums)

        if profile_type == "full" and all(isinstance(item, dict) for item in data):
            all_keys = set()
            for item in data:
                all_keys.update(item.keys())
            field_names = sorted(all_keys)
            profile["fields"] = field_names
            profile["field_count"] = len(field_names)

            field_profiles = {}
            for field in field_names:
                values = [item.get(field) for item in data if field in item]
                field_types = Counter(type(v).__name__ for v in values)
                field_profiles[field] = {
                    "count": len(values),
                    "types": dict(field_types),
                    "null_count": len(data) - len(values),
                }
            profile["field_profiles"] = field_profiles

        return ActionResult(success=True, message=f"Profiled {row_count} rows", data=profile)

    def _profile_dict(self, data: Dict, profile_type: str) -> ActionResult:
        key_count = len(data)
        value_types = Counter(type(v).__name__ for v in data.values())
        null_count = sum(1 for v in data.values() if v is None)

        profile = {
            "key_count": key_count,
            "value_type_distribution": dict(value_types),
            "null_count": null_count,
            "null_percent": round(null_count / key_count * 100, 2) if key_count > 0 else 0,
            "keys": list(data.keys()),
        }

        if profile_type == "full":
            numeric_keys = [k for k, v in data.items() if isinstance(v, (int, float))]
            if numeric_keys:
                nums = [data[k] for k in numeric_keys]
                profile["numeric_stats"] = self._numeric_stats(nums)

        return ActionResult(success=True, message=f"Profiled {key_count} keys", data=profile)

    def _numeric_stats(self, nums: List) -> Dict:
        if not nums:
            return {}
        sorted_nums = sorted(nums)
        n = len(sorted_nums)
        mean = sum(sorted_nums) / n
        variance = sum((x - mean) ** 2 for x in sorted_nums) / n
        std_dev = math.sqrt(variance)
        return {
            "min": sorted_nums[0],
            "max": sorted_nums[-1],
            "mean": round(mean, 6),
            "median": sorted_nums[n // 2],
            "std_dev": round(std_dev, 6),
            "variance": round(variance, 6),
            "q1": sorted_nums[n // 4],
            "q3": sorted_nums[3 * n // 4],
        }


class ColumnProfilerAction(BaseAction):
    """Profile individual columns."""
    action_type = "column_profiler"
    display_name = "列数据画像"
    description = "分析单个列的数据特征"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            column_data = params.get("column_data", [])
            column_name = params.get("column_name", "column")
            compute_stats = params.get("compute_stats", True)

            if not isinstance(column_data, list):
                return ActionResult(success=False, message="column_data must be a list")

            total = len(column_data)
            if total == 0:
                return ActionResult(success=True, message="Empty column", data={"name": column_name, "count": 0})

            null_count = sum(1 for v in column_data if v is None)
            non_null = [v for v in column_data if v is not None]
            unique_values = set(non_null)

            profile = {
                "name": column_name,
                "count": total,
                "null_count": null_count,
                "null_percent": round(null_count / total * 100, 2),
                "unique_count": len(unique_values),
                "unique_percent": round(len(unique_values) / len(non_null) * 100, 2) if non_null else 0,
            }

            if compute_stats:
                types = Counter(type(v).__name__ for v in non_null)
                profile["types"] = dict(types)

                numeric_vals = [v for v in non_null if isinstance(v, (int, float))]
                if numeric_vals:
                    profile["numeric_stats"] = DataProfilerAction()._numeric_stats(numeric_vals)

                str_vals = [v for v in non_null if isinstance(v, str)]
                if str_vals:
                    lengths = [len(s) for s in str_vals]
                    profile["string_stats"] = {
                        "min_length": min(lengths),
                        "max_length": max(lengths),
                        "avg_length": round(sum(lengths) / len(lengths), 2),
                        "non_empty": sum(1 for s in str_vals if s.strip()),
                    }

            return ActionResult(success=True, message=f"Profiled column '{column_name}'", data=profile)
        except Exception as e:
            return ActionResult(success=False, message=f"ColumnProfiler error: {e}")


class DataQualityAction(BaseAction):
    """Assess data quality metrics."""
    action_type = "data_quality"
    display_name = "数据质量评估"
    description = "评估数据质量指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", {})

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            total = len(data)
            if total == 0:
                return ActionResult(success=False, message="Empty dataset")

            quality_metrics = {}

            completeness_threshold = rules.get("completeness_threshold", 0.95)
            if isinstance(data[0], dict) if data else False:
                all_keys = set()
                for item in data:
                    all_keys.update(item.keys())
                field_completeness = {}
                for key in all_keys:
                    non_null = sum(1 for item in data if key in item and item[key] is not None)
                    completeness = non_null / total
                    field_completeness[key] = {
                        "completeness": round(completeness, 4),
                        "pass": completeness >= completeness_threshold,
                    }
                quality_metrics["field_completeness"] = field_completeness
                overall_completeness = sum(fc["completeness"] for fc in field_completeness.values()) / len(field_completeness)
                quality_metrics["overall_completeness"] = round(overall_completeness, 4)
            else:
                null_count = sum(1 for v in data if v is None)
                quality_metrics["overall_completeness"] = round((total - null_count) / total, 4)

            uniqueness_threshold = rules.get("uniqueness_threshold", 0.9)
            deduped_count = len(set(str(v) for v in data))
            uniqueness = deduped_count / total
            quality_metrics["uniqueness"] = round(uniqueness, 4)
            quality_metrics["uniqueness_pass"] = uniqueness >= uniqueness_threshold

            validity_rules = rules.get("validity", [])
            validity_results = []
            for item in data:
                item_valid = True
                if isinstance(item, dict):
                    for field, rule in validity_rules:
                        value = item.get(field)
                        if rule.get("type") == "range":
                            min_val = rule.get("min")
                            max_val = rule.get("max")
                            if value is not None and not (min_val <= value <= max_val):
                                item_valid = False
                                break
                validity_results.append(item_valid)
            valid_count = sum(validity_results)
            quality_metrics["validity_rate"] = round(valid_count / total, 4)

            score = (
                quality_metrics.get("overall_completeness", 1.0) * 0.4
                + quality_metrics.get("uniqueness", 1.0) * 0.3
                + quality_metrics.get("validity_rate", 1.0) * 0.3
            )
            quality_metrics["overall_score"] = round(score, 4)
            quality_metrics["quality_grade"] = "A" if score >= 0.9 else "B" if score >= 0.7 else "C" if score >= 0.5 else "D"

            return ActionResult(
                success=True,
                message=f"Data quality score: {quality_metrics['overall_score']} ({quality_metrics['quality_grade']})",
                data={"metrics": quality_metrics, "total_records": total},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DataQuality error: {e}")
