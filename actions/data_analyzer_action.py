"""Data analyzer action module for RabAI AutoClick.

Provides data analysis operations:
- DataAnalyzerAction: Analyze data structure and content
- DataProfilerAction: Profile data quality and characteristics
- DataComparatorAction: Compare datasets
- DataSummarizerAction: Generate data summaries
- DataOutlierAction: Detect outliers
"""

import statistics
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from collections import Counter

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataAnalyzerAction(BaseAction):
    """Analyze data structure and content."""
    action_type = "data_analyzer"
    display_name = "数据分析"
    description = "分析数据结构与内容"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            analysis_type = params.get("analysis_type", "structure")
            include_stats = params.get("include_stats", True)

            if data is None:
                return ActionResult(success=False, message="data is required")

            if analysis_type == "structure":
                result = self._analyze_structure(data)
            elif analysis_type == "content":
                result = self._analyze_content(data)
            elif analysis_type == "type":
                result = self._analyze_types(data)
            else:
                result = self._analyze_structure(data)

            if include_stats and isinstance(data, (list, dict)):
                result["statistics"] = self._compute_statistics(data)

            return ActionResult(
                success=True,
                data=result,
                message=f"Data analysis completed: {result.get('record_count', 0) if isinstance(data, list) else 1} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data analyzer error: {str(e)}")

    def _analyze_structure(self, data: Any) -> Dict:
        if isinstance(data, dict):
            return {
                "data_type": "object",
                "record_count": 1,
                "field_count": len(data),
                "fields": list(data.keys()),
                "nested_depth": self._calculate_depth(data)
            }
        elif isinstance(data, list):
            first_item = data[0] if data else None
            return {
                "data_type": "array",
                "record_count": len(data),
                "field_count": len(first_item) if isinstance(first_item, (dict, list)) else 0,
                "nested_depth": self._calculate_depth(first_item) if first_item else 0
            }
        else:
            return {
                "data_type": type(data).__name__,
                "record_count": 1,
                "field_count": 0
            }

    def _analyze_content(self, data: Any) -> Dict:
        if isinstance(data, str):
            return {
                "content_type": "text",
                "length": len(data),
                "word_count": len(data.split()),
                "has_whitespace": " " in data
            }
        elif isinstance(data, (list, dict)):
            return {
                "content_type": "structured",
                "null_count": self._count_nulls(data),
                "empty_count": self._count_empty(data)
            }
        else:
            return {"content_type": "primitive", "value": str(data)[:100]}

    def _analyze_types(self, data: Any) -> Dict:
        if isinstance(data, list):
            types = [type(item).__name__ for item in data]
            return {
                "types": dict(Counter(types)),
                "dominant_type": Counter(types).most_common(1)[0][0] if types else None,
                "unique_types": len(set(types))
            }
        elif isinstance(data, dict):
            types = {k: type(v).__name__ for k, v in data.items()}
            return {"types": types, "unique_types": len(set(types.values()))}
        else:
            return {"types": [type(data).__name__], "unique_types": 1}

    def _compute_statistics(self, data: Any) -> Dict:
        if isinstance(data, list) and data and isinstance(data[0], (int, float)):
            nums = [x for x in data if isinstance(x, (int, float))]
            if nums:
                return {
                    "min": min(nums),
                    "max": max(nums),
                    "mean": statistics.mean(nums),
                    "median": statistics.median(nums),
                    "stdev": statistics.stdev(nums) if len(nums) > 1 else 0
                }
        return {"statistical_analysis": "not applicable for non-numeric data"}

    def _calculate_depth(self, obj: Any, depth: int = 0) -> int:
        if not isinstance(obj, (dict, list)):
            return depth
        if not obj:
            return depth
        if isinstance(obj, dict):
            return max(self._calculate_depth(v, depth + 1) for v in obj.values()) if obj else depth + 1
        elif isinstance(obj, list):
            return max(self._calculate_depth(item, depth + 1) for item in obj) if obj else depth + 1
        return depth

    def _count_nulls(self, data: Any) -> int:
        if data is None:
            return 1
        if isinstance(data, dict):
            return sum(self._count_nulls(v) for v in data.values())
        if isinstance(data, list):
            return sum(self._count_nulls(item) for item in data)
        return 0

    def _count_empty(self, data: Any) -> int:
        if data == "" or data == [] or data == {}:
            return 1
        if isinstance(data, dict):
            return sum(self._count_empty(v) for v in data.values())
        if isinstance(data, list):
            return sum(self._count_empty(item) for item in data)
        return 0


class DataProfilerAction(BaseAction):
    """Profile data quality and characteristics."""
    action_type = "data_profiler"
    display_name = "数据画像"
    description = "生成数据质量画像"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            profile_level = params.get("profile_level", "basic")

            if data is None:
                return ActionResult(success=False, message="data is required")

            profile = {
                "profiled_at": datetime.now().isoformat(),
                "profile_level": profile_level,
                "size_bytes": len(str(data)),
                "data_type": type(data).__name__
            }

            if isinstance(data, list):
                profile.update({
                    "record_count": len(data),
                    "unique_records": len(set(str(item) for item in data)),
                    "duplicate_count": len(data) - len(set(str(item) for item in data))
                })
            elif isinstance(data, dict):
                profile.update({
                    "field_count": len(data),
                    "null_fields": sum(1 for v in data.values() if v is None)
                })

            if profile_level == "detailed" and isinstance(data, list):
                profile["distribution"] = self._profile_distribution(data)

            return ActionResult(
                success=True,
                data=profile,
                message=f"Data profile generated: {profile.get('record_count', 1)} records"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data profiler error: {str(e)}")

    def _profile_distribution(self, data: List) -> Dict:
        if not data:
            return {}
        types = Counter(type(item).__name__ for item in data)
        return {"types": dict(types)}


class DataComparatorAction(BaseAction):
    """Compare datasets."""
    action_type = "data_comparator"
    display_name = "数据对比"
    description = "对比数据集差异"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data1 = params.get("data1", None)
            data2 = params.get("data2", None)
            comparison_mode = params.get("comparison_mode", "deep")

            if data1 is None or data2 is None:
                return ActionResult(success=False, message="data1 and data2 are required")

            start_time = datetime.now()

            if comparison_mode == "shallow":
                is_equal = data1 == data2
                differences = []
            else:
                is_equal, differences = self._deep_compare(data1, data2)

            comparison_result = {
                "is_equal": is_equal,
                "differences": differences,
                "difference_count": len(differences),
                "comparison_mode": comparison_mode,
                "duration_ms": round((datetime.now() - start_time).total_seconds() * 1000, 2)
            }

            return ActionResult(
                success=True,
                data=comparison_result,
                message=f"Comparison completed: {'IDENTICAL' if is_equal else 'DIFFERENT'} ({len(differences)} differences)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data comparator error: {str(e)}")

    def _deep_compare(self, obj1: Any, obj2: Any, path: str = "") -> tuple:
        differences = []
        if type(obj1) != type(obj2):
            differences.append({
                "path": path or "root",
                "type": "type_mismatch",
                "value1": type(obj1).__name__,
                "value2": type(obj2).__name__
            })
            return False, differences
        if isinstance(obj1, dict):
            all_keys = set(obj1.keys()) | set(obj2.keys())
            for key in all_keys:
                new_path = f"{path}.{key}" if path else key
                if key not in obj1:
                    differences.append({"path": new_path, "type": "missing_in_1", "value": obj2[key]})
                elif key not in obj2:
                    differences.append({"path": new_path, "type": "missing_in_2", "value": obj1[key]})
                else:
                    _, diffs = self._deep_compare(obj1[key], obj2[key], new_path)
                    differences.extend(diffs)
        elif isinstance(obj1, list):
            if len(obj1) != len(obj2):
                differences.append({"path": path, "type": "length_mismatch", "len1": len(obj1), "len2": len(obj2)})
            for i, (item1, item2) in enumerate(zip(obj1, obj2)):
                _, diffs = self._deep_compare(item1, item2, f"{path}[{i}]")
                differences.extend(diffs)
        elif obj1 != obj2:
            differences.append({"path": path, "type": "value_mismatch", "value1": obj1, "value2": obj2})
        return len(differences) == 0, differences


class DataSummarizerAction(BaseAction):
    """Generate data summaries."""
    action_type = "data_summarizer"
    display_name = "数据摘要"
    description = "生成数据摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            summary_type = params.get("summary_type", "full")
            max_items = params.get("max_items", 10)

            if data is None:
                return ActionResult(success=False, message="data is required")

            summary = {
                "summary_type": summary_type,
                "generated_at": datetime.now().isoformat(),
                "data_type": type(data).__name__
            }

            if isinstance(data, list):
                summary["record_count"] = len(data)
                if summary_type in ["full", "head"]:
                    summary["head"] = data[:max_items]
                if summary_type == "full":
                    summary["tail"] = data[-max_items:]
            elif isinstance(data, dict):
                summary["field_count"] = len(data)
                if summary_type in ["full", "head"]:
                    items = list(data.items())[:max_items]
                    summary["head"] = dict(items)

            return ActionResult(
                success=True,
                data=summary,
                message=f"Summary generated: {summary.get('record_count', summary.get('field_count', 'N/A'))} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data summarizer error: {str(e)}")


class DataOutlierAction(BaseAction):
    """Detect outliers in data."""
    action_type = "data_outlier"
    display_name = "异常值检测"
    description = "检测数据中的异常值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "iqr")
            threshold = params.get("threshold", 1.5)

            if not data:
                return ActionResult(success=False, message="data is required")

            numeric_data = [x for x in data if isinstance(x, (int, float))]
            if not numeric_data:
                return ActionResult(
                    success=True,
                    data={"outliers": [], "method": method, "message": "No numeric data to analyze"},
                    message="No outliers detected (no numeric data)"
                )

            if method == "iqr":
                outliers = self._detect_iqr_outliers(numeric_data, threshold)
            elif method == "zscore":
                outliers = self._detect_zscore_outliers(numeric_data, threshold)
            else:
                outliers = []

            return ActionResult(
                success=True,
                data={
                    "outliers": outliers,
                    "outlier_count": len(outliers),
                    "method": method,
                    "threshold": threshold,
                    "total_analyzed": len(numeric_data)
                },
                message=f"Outlier detection completed: {len(outliers)} outliers found"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data outlier error: {str(e)}")

    def _detect_iqr_outliers(self, data: List[float], threshold: float) -> List:
        sorted_data = sorted(data)
        q1_idx = len(sorted_data) // 4
        q3_idx = 3 * len(sorted_data) // 4
        q1 = sorted_data[q1_idx]
        q3 = sorted_data[q3_idx]
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr
        return [x for x in data if x < lower_bound or x > upper_bound]

    def _detect_zscore_outliers(self, data: List[float], threshold: float) -> List:
        if len(data) < 2:
            return []
        mean = statistics.mean(data)
        stdev = statistics.stdev(data)
        if stdev == 0:
            return []
        zscores = [(x - mean) / stdev for x in data]
        return [data[i] for i, z in enumerate(zscores) if abs(z) > threshold]
