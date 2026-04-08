"""Data profiler action module for RabAI AutoClick.

Provides data profiling operations:
- ProfileSchemaAction: Profile schema
- ProfileQualityAction: Profile data quality
- ProfileDistributionAction: Profile distributions
- ProfileCardinalityAction: Profile cardinality
- ProfileSummaryAction: Generate profile summary
"""

import math
from collections import Counter
from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ProfileSchemaAction(BaseAction):
    """Profile schema of data."""
    action_type = "profile_schema"
    display_name = "Schema分析"
    description = "分析数据Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            fields = {}
            for item in data:
                for key, value in item.items():
                    if key not in fields:
                        fields[key] = {"type": type(value).__name__, "null_count": 0, "present_count": 0}
                    fields[key]["present_count"] += 1
                    if value is None:
                        fields[key]["null_count"] += 1

            for f in fields.values():
                f["null_pct"] = (f["null_count"] / len(data)) * 100 if len(data) > 0 else 0

            return ActionResult(
                success=True,
                data={"schema": fields, "field_count": len(fields), "row_count": len(data)},
                message=f"Schema profile: {len(fields)} fields, {len(data)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Profile schema failed: {e}")


class ProfileQualityAction(BaseAction):
    """Profile data quality."""
    action_type = "profile_quality"
    display_name = "质量分析"
    description = "分析数据质量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            null_count = sum(1 for item in data for v in item.values() if v is None)
            total_cells = sum(len(item) for item in data)
            empty_count = sum(1 for item in data for v in item.values() if v == "" or v == [])
            duplicate_rows = len(data) - len({tuple(sorted(d.items())) for d in data})

            quality_score = ((total_cells - null_count - empty_count) / total_cells * 100) if total_cells > 0 else 0

            return ActionResult(
                success=True,
                data={"quality_score": quality_score, "null_count": null_count, "empty_count": empty_count, "duplicate_rows": duplicate_rows, "total_cells": total_cells},
                message=f"Data quality score: {quality_score:.1f}% ({null_count} nulls, {empty_count} empty, {duplicate_rows} duplicates)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Profile quality failed: {e}")


class ProfileDistributionAction(BaseAction):
    """Profile value distributions."""
    action_type = "profile_distribution"
    display_name = "分布分析"
    description = "分析值分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            bin_count = params.get("bin_count", 10)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field) for d in data if field in d]
            if not values:
                return ActionResult(success=False, message=f"No values found for field '{field}'")

            counter = Counter(values)
            top_values = counter.most_common(10)

            min_val = min(values)
            max_val = max(values)
            mean_val = sum(values) / len(values)

            return ActionResult(
                success=True,
                data={"field": field, "distinct_count": len(counter), "min": min_val, "max": max_val, "mean": mean_val, "top_values": top_values},
                message=f"Distribution of {field}: {len(counter)} distinct values, mean={mean_val:.2f}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Profile distribution failed: {e}")


class ProfileCardinalityAction(BaseAction):
    """Profile cardinality."""
    action_type = "profile_cardinality"
    display_name = "基数分析"
    description = "分析字段基数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            cardinalities = {}
            for key in data[0].keys():
                distinct = set(item.get(key) for item in data if key in item)
                cardinalities[key] = {"distinct": len(distinct), "pct": (len(distinct) / len(data)) * 100 if len(data) > 0 else 0}

            return ActionResult(
                success=True,
                data={"cardinalities": cardinalities, "field_count": len(cardinalities)},
                message=f"Cardinality profile: {len(cardinalities)} fields analyzed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Profile cardinality failed: {e}")


class ProfileSummaryAction(BaseAction):
    """Generate profile summary."""
    action_type = "profile_summary"
    display_name = "分析摘要"
    description = "生成分析摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            fields = list(data[0].keys()) if data else []
            null_counts = {f: sum(1 for item in data if item.get(f) is None) for f in fields}
            distinct_counts = {f: len(set(item.get(f) for item in data if f in item)) for f in fields}

            summary = {
                "row_count": len(data),
                "field_count": len(fields),
                "null_counts": null_counts,
                "distinct_counts": distinct_counts,
                "complete_pct": {f: ((len(data) - null_counts[f]) / len(data) * 100) for f in fields},
            }

            return ActionResult(
                success=True,
                data={"summary": summary, "row_count": len(data), "field_count": len(fields)},
                message=f"Profile summary: {len(data)} rows x {len(fields)} fields",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Profile summary failed: {e}")
