"""Data quality action module for RabAI AutoClick.

Provides data quality validation and monitoring:
- DataProfiler: Profile dataset statistics
- NullChecker: Detect and report null values
- DuplicateDetector: Find duplicate records
- SchemaValidator: Validate data against schema
- DataQualityReporter: Generate quality reports
"""

from __future__ import annotations

import sys
import os
import re
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from collections import Counter

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataProfilerAction(BaseAction):
    """Profile dataset statistics and distributions."""
    action_type = "data_profiler"
    display_name = "数据画像"
    description = "生成数据集统计画像"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            compute_histograms = params.get("compute_histograms", True)
            compute_percentiles = params.get("compute_percentiles", True)
            bins = params.get("bins", 10)

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list) or not isinstance(data[0], dict):
                return ActionResult(success=False, message="data must be a list of dictionaries")

            columns = list(data[0].keys())
            total_rows = len(data)

            profile = {
                "total_rows": total_rows,
                "total_columns": len(columns),
                "columns": {},
            }

            for col in columns:
                values = [row.get(col) for row in data if col in row]
                non_null = [v for v in values if v is not None and v != ""]

                col_stats = {
                    "type": self._infer_type(values),
                    "total_count": len(values),
                    "null_count": len(values) - len(non_null),
                    "null_percentage": round((len(values) - len(non_null)) / len(values) * 100, 2) if values else 0,
                    "unique_count": len(set(str(v) for v in non_null)),
                    "unique_percentage": round(len(set(str(v) for v in non_null)) / len(non_null) * 100, 2) if non_null else 0,
                }

                if non_null:
                    try:
                        numeric_vals = [float(v) for v in non_null if self._is_numeric(v)]
                        if numeric_vals:
                            col_stats.update({
                                "min": min(numeric_vals),
                                "max": max(numeric_vals),
                                "mean": sum(numeric_vals) / len(numeric_vals),
                                "sum": sum(numeric_vals),
                            })
                            if compute_percentiles:
                                sorted_vals = sorted(numeric_vals)
                                for p in [25, 50, 75, 90, 95, 99]:
                                    idx = int(len(sorted_vals) * p / 100)
                                    col_stats[f"p{p}"] = sorted_vals[min(idx, len(sorted_vals) - 1)]
                    except:
                        pass

                    col_stats["top_values"] = self._top_values(non_null, 5)

                profile["columns"][col] = col_stats

            return ActionResult(
                success=True,
                message=f"Profiled {total_rows} rows, {len(columns)} columns",
                data=profile
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _infer_type(self, values: List[Any]) -> str:
        non_null = [v for v in values if v is not None and v != ""]
        if not non_null:
            return "unknown"
        sample = non_null[:100]
        if all(self._is_numeric(v) for v in sample):
            return "numeric"
        if all(isinstance(v, bool) or str(v).lower() in ("true", "false") for v in sample):
            return "boolean"
        if all(self._is_datetime(v) for v in sample):
            return "datetime"
        if all(isinstance(v, str) for v in sample):
            return "string"
        return "mixed"

    def _is_numeric(self, v: Any) -> bool:
        try:
            float(v)
            return True
        except:
            return False

    def _is_datetime(self, v: Any) -> bool:
        if isinstance(v, str):
            return bool(re.match(r"\d{4}-\d{2}-\d{2}", str(v)))
        return False

    def _top_values(self, values: List[Any], n: int) -> List[Dict[str, Any]]:
        counter = Counter(str(v) for v in values)
        total = len(values)
        return [
            {"value": v, "count": c, "percentage": round(c / total * 100, 2)}
            for v, c in counter.most_common(n)
        ]


class NullCheckerAction(BaseAction):
    """Detect and report null/missing values."""
    action_type = "null_checker"
    display_name = "空值检测"
    description = "检测并报告空值和缺失数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            threshold_pct = params.get("threshold_pct", 0)
            fail_on_threshold = params.get("fail_on_threshold", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list) or not isinstance(data[0], dict):
                return ActionResult(success=False, message="data must be a list of dictionaries")

            total_rows = len(data)
            columns = list(data[0].keys())

            null_report = {}
            failed_columns = []

            for col in columns:
                values = [row.get(col) for row in data]
                null_count = sum(1 for v in values if v is None or v == "" or v == "null")
                null_pct = null_count / total_rows * 100 if total_rows > 0 else 0

                null_report[col] = {
                    "null_count": null_count,
                    "null_percentage": round(null_pct, 2),
                    "non_null_count": total_rows - null_count,
                }

                if null_pct >= threshold_pct:
                    failed_columns.append(col)

            if failed_columns and fail_on_threshold:
                return ActionResult(
                    success=False,
                    message=f"Null threshold exceeded: {failed_columns}",
                    data={"null_report": null_report, "failed_columns": failed_columns}
                )

            return ActionResult(
                success=True,
                message=f"Checked {len(columns)} columns",
                data={"null_report": null_report, "failed_columns": failed_columns}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DuplicateDetectorAction(BaseAction):
    """Find duplicate records in datasets."""
    action_type = "duplicate_detector"
    display_name = "重复检测"
    description = "检测数据集中的重复记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_columns = params.get("key_columns", None)
            subset = params.get("subset", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            total_rows = len(data)

            if subset:
                keys = [tuple(row.get(c) for c in subset if c in row) for row in data]
            elif key_columns:
                keys = [tuple(row.get(c) for c in key_columns if c in row) for row in data]
            else:
                keys = [tuple(sorted(row.items())) for row in data]

            counter = Counter(keys)
            duplicates = {k: v for k, v in counter.items() if v > 1}

            dup_records = []
            seen_keys = set()
            for i, row in enumerate(data):
                if subset:
                    key = tuple(row.get(c) for c in subset if c in row)
                elif key_columns:
                    key = tuple(row.get(c) for c in key_columns if c in row)
                else:
                    key = tuple(sorted(row.items()))

                if duplicates.get(key) and key not in seen_keys:
                    seen_keys.add(key)
                    dup_records.append({
                        "key": key if len(key) <= 5 else f"({len(key)} columns)",
                        "count": duplicates[key],
                        "first_index": data.index(row) if row in data else -1,
                    })

            dup_count = sum(1 for v in counter.values() if v > 1)
            dup_pct = dup_count / total_rows * 100 if total_rows > 0 else 0

            return ActionResult(
                success=True,
                message=f"Found {len(duplicates)} unique duplicate groups",
                data={
                    "duplicate_groups": len(duplicates),
                    "duplicate_records": dup_count,
                    "duplicate_percentage": round(dup_pct, 2),
                    "duplicate_details": dup_records[:100],
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class SchemaValidatorAction(BaseAction):
    """Validate data against a defined schema."""
    action_type = "schema_validator"
    display_name = "Schema验证"
    description = "验证数据是否符合Schema定义"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            schema = params.get("schema", {})

            if not data:
                return ActionResult(success=False, message="data is required")
            if not schema:
                return ActionResult(success=False, message="schema is required")

            errors = []
            validated = 0

            for i, record in enumerate(data):
                row_errors = []
                for col, col_schema in schema.items():
                    value = record.get(col)
                    col_type = col_schema.get("type", "string")
                    required = col_schema.get("required", False)
                    pattern = col_schema.get("pattern", None)
                    enum_values = col_schema.get("enum", None)

                    if value is None or value == "":
                        if required:
                            row_errors.append(f"Row {i}: '{col}' is required but missing")
                        continue

                    if not self._check_type(value, col_type):
                        row_errors.append(f"Row {i}: '{col}' type mismatch (expected {col_type})")

                    if pattern and isinstance(value, str):
                        if not re.match(pattern, value):
                            row_errors.append(f"Row {i}: '{col}' does not match pattern {pattern}")

                    if enum_values and value not in enum_values:
                        row_errors.append(f"Row {i}: '{col}' value '{value}' not in allowed values")

                if row_errors:
                    errors.extend(row_errors)
                else:
                    validated += 1

            success = len(errors) == 0
            return ActionResult(
                success=success,
                message=f"Validated: {validated} ok, {len(errors)} errors",
                data={"validated": validated, "errors": errors[:50], "total_errors": len(errors)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")

    def _check_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
            "string": lambda v: isinstance(v, str),
            "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
            "boolean": lambda v: isinstance(v, bool),
            "object": lambda v: isinstance(v, dict),
            "array": lambda v: isinstance(v, list),
        }
        checker = type_map.get(expected_type, lambda v: True)
        return checker(value)


class DataQualityReporterAction(BaseAction):
    """Generate comprehensive data quality reports."""
    action_type = "data_quality_reporter"
    display_name = "数据质量报告"
    description = "生成数据质量综合报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            report_format = params.get("format", "summary")

            if not data:
                return ActionResult(success=False, message="data is required")

            profiler = DataProfilerAction()
            null_result = NullCheckerAction().execute(context, {"data": data})
            dup_result = DuplicateDetectorAction().execute(context, {"data": data})
            profile_result = profiler.execute(context, {"data": data, "compute_histograms": False, "compute_percentiles": False})

            quality_score = 100.0
            issues = []

            if null_result.data:
                for col, info in null_result.data.get("null_report", {}).items():
                    if info["null_percentage"] > 10:
                        issues.append(f"High null rate in '{col}': {info['null_percentage']}%")
                        quality_score -= info["null_percentage"] * 0.5

            if dup_result.data:
                dup_pct = dup_result.data.get("duplicate_percentage", 0)
                if dup_pct > 5:
                    issues.append(f"High duplicate rate: {dup_pct}%")
                    quality_score -= dup_pct

            quality_score = max(0, round(quality_score, 2))

            score_grade = "A" if quality_score >= 90 else "B" if quality_score >= 75 else "C" if quality_score >= 60 else "D" if quality_score >= 40 else "F"

            report = {
                "quality_score": quality_score,
                "grade": score_grade,
                "total_rows": len(data),
                "total_columns": profile_result.data.get("total_columns", 0),
                "issues": issues,
                "null_summary": null_result.data,
                "duplicate_summary": dup_result.data,
                "profile_summary": profile_result.data,
                "generated_at": datetime.now().isoformat(),
            }

            return ActionResult(
                success=True,
                message=f"Quality report: {score_grade} ({quality_score}%)",
                data=report
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
