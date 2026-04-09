"""Data quality action module for RabAI AutoClick.

Provides data quality operations:
- QualityCheckerAction: Check data quality rules
- QualityProfilerAction: Profile data quality statistics
- QualityCleanerAction: Clean and fix data quality issues
- QualityReporterAction: Generate data quality reports
"""

import sys
import os
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from collections import Counter

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class QualityIssue:
    """A data quality issue."""
    row: int
    column: str
    issue_type: str
    message: str
    severity: str = "warning"


@dataclass
class QualityReport:
    """Data quality report."""
    total_rows: int
    total_columns: int
    issues: List[QualityIssue]
    completeness: float
    accuracy: float
    consistency: float
    timeliness: float


class QualityRule:
    """A data quality validation rule."""

    def __init__(self, name: str, check_fn: Callable[[Any], tuple[bool, str]]) -> None:
        self.name = name
        self.check_fn = check_fn

    def apply(self, value: Any) -> tuple[bool, str]:
        return self.check_fn(value)


class DataProfiler:
    """Profiles data to generate statistics."""

    def profile_column(self, values: List[Any]) -> Dict[str, Any]:
        non_null = [v for v in values if v is not None and v != ""]
        null_count = len(values) - len(non_null)

        stats: Dict[str, Any] = {
            "total_count": len(values),
            "null_count": null_count,
            "null_percentage": round(null_count / len(values) * 100, 2) if values else 0,
            "unique_count": len(set(str(v) for v in non_null)),
            "filled_count": len(non_null)
        }

        numeric = [v for v in non_null if isinstance(v, (int, float))]
        if numeric:
            stats["min"] = min(numeric)
            stats["max"] = max(numeric)
            stats["avg"] = sum(numeric) / len(numeric)

        str_values = [v for v in non_null if isinstance(v, str)]
        if str_values:
            lengths = [len(v) for v in str_values]
            stats["min_length"] = min(lengths)
            stats["max_length"] = max(lengths)
            stats["avg_length"] = sum(lengths) / len(lengths)
            stats["top_values"] = Counter(str_values).most_common(5)

        return stats

    def profile_dataset(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not data:
            return {}

        columns = list(data[0].keys())
        column_profiles = {}

        for col in columns:
            values = [row.get(col) for row in data]
            column_profiles[col] = self.profile_column(values)

        return {
            "row_count": len(data),
            "column_count": len(columns),
            "columns": column_profiles
        }


class DataCleaner:
    """Cleans data quality issues."""

    def remove_nulls(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if columns:
            return [row for row in data if all(row.get(c) is not None and row.get(c) != "" for c in columns)]
        return [row for row in data if all(row.get(c) is not None and row.get(c) != "" for c in row.keys())]

    def remove_duplicates(self, data: List[Dict[str, Any]], key_columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        if key_columns:
            seen = set()
            result = []
            for row in data:
                key = tuple(row.get(c) for c in key_columns)
                if key not in seen:
                    seen.add(key)
                    result.append(row)
            return result
        else:
            return list({str(row): row for row in data}.values())

    def fill_nulls(self, data: List[Dict[str, Any]], column: str, fill_value: Any, strategy: str = "fixed") -> List[Dict[str, Any]]:
        if strategy == "fixed":
            for row in data:
                if row.get(column) is None or row.get(column) == "":
                    row[column] = fill_value
        elif strategy == "forward":
            last_value: Any = fill_value
            for row in data:
                if row.get(column) is None or row.get(column) == "":
                    row[column] = last_value
                else:
                    last_value = row[column]
        return data

    def trim_strings(self, data: List[Dict[str, Any]], columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        for row in data:
            for key, value in row.items():
                if columns and key not in columns:
                    continue
                if isinstance(value, str):
                    row[key] = value.strip()
        return data


_profiler = DataProfiler()
_cleaner = DataCleaner()
_rules: Dict[str, QualityRule] = {}


class QualityCheckerAction(BaseAction):
    """Check data against quality rules."""
    action_type = "data_quality_checker"
    display_name = "数据质量检查"
    description = "根据质量规则检查数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        rule_name = params.get("rule_name", "")
        column = params.get("column", "")

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        if rule_name:
            rule = _rules.get(rule_name)
            if not rule:
                return ActionResult(success=False, message=f"规则 {rule_name} 不存在")

            issues = []
            for i, row in enumerate(data):
                value = row.get(column) if column else row
                passed, msg = rule.apply(value)
                if not passed:
                    issues.append(QualityIssue(
                        row=i,
                        column=column or "row",
                        issue_type=rule_name,
                        message=msg
                    ))

            return ActionResult(
                success=len(issues) == 0,
                message=f"检查完成，{len(issues)} 个问题",
                data={"issues": [{"row": i.row, "column": i.column, "message": i.message} for i in issues]}
            )

        default_checks = ["null_check", "duplicate_check"]
        issues = []

        seen_rows = set()
        for i, row in enumerate(data):
            row_str = str(sorted(row.items()))
            if row_str in seen_rows:
                issues.append(QualityIssue(row=i, column="*", issue_type="duplicate", message="Duplicate row"))
            seen_rows.add(row_str)

            for col, value in row.items():
                if value is None or value == "":
                    issues.append(QualityIssue(row=i, column=col, issue_type="null", message=f"Null value in {col}"))

        return ActionResult(
            success=len(issues) == 0,
            message=f"质量检查完成，{len(issues)} 个问题",
            data={"issues": [{"row": i.row, "column": i.column, "message": i.message} for i in issues[:100]]}
        )


class QualityProfilerAction(BaseAction):
    """Profile data quality statistics."""
    action_type = "data_quality_profiler"
    display_name = "数据质量分析"
    description = "分析数据的质量统计信息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        if not data:
            return ActionResult(success=False, message="data为空")

        profile = _profiler.profile_dataset(data)

        return ActionResult(
            success=True,
            message=f"数据分析完成，{profile['row_count']} 行 {profile['column_count']} 列",
            data=profile
        )


class QualityCleanerAction(BaseAction):
    """Clean and fix data quality issues."""
    action_type = "data_quality_cleaner"
    display_name = "数据质量清洗"
    description = "清洗和修复数据质量问题"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        operation = params.get("operation", "trim")

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        original_count = len(data)

        if operation == "remove_nulls":
            columns = params.get("columns")
            data = _cleaner.remove_nulls(data, columns)
        elif operation == "remove_duplicates":
            key_columns = params.get("key_columns")
            data = _cleaner.remove_duplicates(data, key_columns)
        elif operation == "fill_nulls":
            column = params.get("column", "")
            fill_value = params.get("fill_value", "")
            strategy = params.get("strategy", "fixed")
            data = _cleaner.fill_nulls(data, column, fill_value, strategy)
        elif operation == "trim":
            columns = params.get("columns")
            data = _cleaner.trim_strings(data, columns)
        else:
            return ActionResult(success=False, message=f"未知操作: {operation}")

        removed = original_count - len(data)

        return ActionResult(
            success=True,
            message=f"清洗完成，移除 {removed} 行",
            data={"original_count": original_count, "cleaned_count": len(data), "removed": removed}
        )


class QualityReporterAction(BaseAction):
    """Generate data quality reports."""
    action_type = "data_quality_reporter"
    display_name = "数据质量报告"
    description = "生成数据质量报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", [])
        format_type = params.get("format", "summary")

        if not isinstance(data, list):
            return ActionResult(success=False, message="data必须是列表")

        profile = _profiler.profile_dataset(data)

        total_cells = profile["row_count"] * profile["column_count"]
        null_cells = sum(p.get("null_count", 0) for p in profile["columns"].values())
        completeness = round((total_cells - null_cells) / total_cells * 100, 2) if total_cells > 0 else 0

        issues = []
        seen_rows = set()
        dup_count = 0
        for i, row in enumerate(data):
            row_str = str(sorted(row.items()))
            if row_str in seen_rows:
                dup_count += 1
                issues.append({"type": "duplicate", "count": 1})
            seen_rows.add(row_str)

        report = {
            "timestamp": datetime.now().isoformat(),
            "total_rows": profile["row_count"],
            "total_columns": profile["column_count"],
            "total_cells": total_cells,
            "null_cells": null_cells,
            "completeness": completeness,
            "duplicate_rows": dup_count,
            "column_profiles": profile["columns"]
        }

        if format_type == "summary":
            return ActionResult(
                success=True,
                message=f"质量报告: 完整度 {completeness}%",
                data={
                    "completeness": completeness,
                    "total_rows": profile["row_count"],
                    "total_columns": profile["column_count"],
                    "duplicate_rows": dup_count
                }
            )

        if format_type == "full":
            return ActionResult(
                success=True,
                message="完整质量报告已生成",
                data=report
            )

        return ActionResult(success=False, message=f"未知格式: {format_type}")
