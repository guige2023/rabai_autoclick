"""Data reader action module for RabAI AutoClick.

Provides data reading operations:
- ReadQueryAction: Query data
- ReadFilterAction: Filter data
- ReadPaginateAction: Paginate data
- ReadAggregateAction: Aggregate read
- ReadExportAction: Export data
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ReadQueryAction(BaseAction):
    """Query data from storage."""
    action_type = "read_query"
    display_name = "查询读取"
    description = "从存储查询数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            storage_key = params.get("storage_key", "default")
            conditions = params.get("conditions", [])

            store = getattr(context, "_data_stores", {}).get(storage_key, [])
            results = store

            if conditions:
                filtered = []
                for item in results:
                    match = True
                    for cond in conditions:
                        field = cond.get("field", "")
                        operator = cond.get("operator", "eq")
                        value = cond.get("value")
                        item_val = item.get(field)

                        if operator == "eq" and item_val != value:
                            match = False
                        elif operator == "ne" and item_val == value:
                            match = False
                        elif operator == "gt" and item_val <= value:
                            match = False
                        elif operator == "lt" and item_val >= value:
                            match = False
                        elif operator == "contains" and isinstance(item_val, str) and value not in item_val:
                            match = False

                        if not match:
                            break
                    if match:
                        filtered.append(item)
                results = filtered

            return ActionResult(
                success=True,
                data={"results": results, "count": len(results), "storage_key": storage_key},
                message=f"Query '{storage_key}': returned {len(results)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read query failed: {e}")


class ReadFilterAction(BaseAction):
    """Filter data during read."""
    action_type = "read_filter"
    display_name = "过滤读取"
    description = "读取时过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filters = params.get("filters", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            results = data
            for f in filters:
                field = f.get("field", "")
                value = f.get("value")
                filter_type = f.get("type", "include")

                if filter_type == "include":
                    results = [d for d in results if d.get(field) == value]
                elif filter_type == "exclude":
                    results = [d for d in results if d.get(field) != value]
                elif filter_type == "range":
                    min_v = f.get("min")
                    max_v = f.get("max")
                    results = [d for d in results if (min_v is None or d.get(field) >= min_v) and (max_v is None or d.get(field) <= max_v)]

            return ActionResult(
                success=True,
                data={"filtered": results, "count": len(results), "original_count": len(data)},
                message=f"Filter read: {len(data)} → {len(results)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read filter failed: {e}")


class ReadPaginateAction(BaseAction):
    """Paginate data."""
    action_type = "read_paginate"
    display_name = "分页读取"
    description = "分页读取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            page = params.get("page", 1)
            page_size = params.get("page_size", 20)
            sort_by = params.get("sort_by", None)
            order = params.get("order", "asc")

            if not data:
                return ActionResult(success=False, message="data is required")

            if sort_by:
                data = sorted(data, key=lambda x: x.get(sort_by, 0), reverse=(order == "desc"))

            total_pages = (len(data) + page_size - 1) // page_size
            start = (page - 1) * page_size
            end = start + page_size
            page_data = data[start:end]

            return ActionResult(
                success=True,
                data={"page_data": page_data, "page": page, "page_size": page_size, "total_pages": total_pages, "total_items": len(data)},
                message=f"Page {page}/{total_pages}: {len(page_data)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read paginate failed: {e}")


class ReadAggregateAction(BaseAction):
    """Aggregate during read."""
    action_type = "read_aggregate"
    display_name = "聚合读取"
    description = "读取时聚合数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            aggregations = params.get("aggregations", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            groups: Dict = {}
            for item in data:
                key = tuple(item.get(g) for g in group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

            results = []
            for key, items in groups.items():
                row = dict(zip(group_by, key))
                for agg in aggregations:
                    field = agg.get("field", "")
                    func = agg.get("func", "sum")
                    values = [i.get(field, 0) for i in items]
                    if func == "sum":
                        row[f"{field}_sum"] = sum(values)
                    elif func == "avg":
                        row[f"{field}_avg"] = sum(values) / len(values) if values else 0
                    elif func == "count":
                        row[f"{field}_count"] = len(values)
                    elif func == "min":
                        row[f"{field}_min"] = min(values) if values else None
                    elif func == "max":
                        row[f"{field}_max"] = max(values) if values else None
                results.append(row)

            return ActionResult(
                success=True,
                data={"results": results, "group_count": len(results), "aggregations": len(aggregations)},
                message=f"Aggregate read: {len(results)} groups with {len(aggregations)} aggregations",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read aggregate failed: {e}")


class ReadExportAction(BaseAction):
    """Export data during read."""
    action_type = "read_export"
    display_name = "导出读取"
    description = "读取时导出数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            format_type = params.get("format", "json")
            fields = params.get("fields", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if fields:
                data = [{k: item.get(k) for k in fields if k in item} for item in data]

            if format_type == "json":
                import json
                content = json.dumps(data, indent=2)
            elif format_type == "csv":
                import csv
                import io
                output = io.StringIO()
                if data:
                    writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
                    writer.writeheader()
                    writer.writerows(data)
                content = output.getvalue()
            else:
                content = str(data)

            return ActionResult(
                success=True,
                data={"content": content, "format": format_type, "count": len(data), "size_bytes": len(content)},
                message=f"Export read: {len(data)} items as {format_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Read export failed: {e}")
