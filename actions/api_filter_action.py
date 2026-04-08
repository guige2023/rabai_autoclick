"""API filter action module for RabAI AutoClick.

Provides API filtering operations:
- QueryFilterAction: Filter API responses by query params
- ResponseFilterAction: Filter response data by field conditions
- HeaderFilterAction: Filter/modify request headers
- FieldSelectorAction: Select specific fields from response
- ResultFilterAction: Filter results by criteria
"""

import re
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueryFilterAction(BaseAction):
    """Filter API requests by query parameters."""
    action_type = "api_query_filter"
    display_name = "查询过滤器"
    description = "通过查询参数过滤API请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            filters = params.get("filters", {})
            exclude_empty = params.get("exclude_empty", True)
            url_encoding = params.get("url_encoding", True)

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            query_parts = []
            for key, value in filters.items():
                if exclude_empty and (value is None or value == ""):
                    continue
                if url_encoding:
                    import urllib.parse
                    key = urllib.parse.quote(str(key))
                    value = urllib.parse.quote(str(value))
                query_parts.append(f"{key}={value}")

            separator = "&" if "?" in base_url else "?"
            filtered_url = base_url + separator + "&".join(query_parts) if query_parts else base_url

            return ActionResult(
                success=True,
                data={
                    "filtered_url": filtered_url,
                    "filters_applied": list(filters.keys()),
                    "filter_count": len(query_parts)
                },
                message=f"Applied {len(query_parts)} filters to URL"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Query filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"filters": {}, "exclude_empty": True, "url_encoding": True}


class ResponseFilterAction(BaseAction):
    """Filter response data by field conditions."""
    action_type = "api_response_filter"
    display_name = "响应过滤器"
    description = "按字段条件过滤响应数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            conditions = params.get("conditions", {})
            operator = params.get("operator", "and")
            case_sensitive = params.get("case_sensitive", False)

            if not isinstance(data, list):
                data = [data]

            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                matches = 0
                for field, value in conditions.items():
                    item_value = item.get(field)
                    if case_sensitive:
                        match = str(item_value) == str(value)
                    else:
                        match = str(item_value).lower() == str(value).lower()

                    if operator == "and" and not match:
                        break
                    elif operator == "or" and match:
                        matches = len(conditions)
                        break
                    if match:
                        matches += 1

                if operator == "and" and matches == len(conditions):
                    filtered.append(item)
                elif operator == "or" and matches > 0:
                    filtered.append(item)

            return ActionResult(
                success=True,
                data={
                    "filtered": filtered,
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "conditions": conditions,
                    "operator": operator
                },
                message=f"Filtered {len(data)} items to {len(filtered)} results"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data", "conditions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"operator": "and", "case_sensitive": False}


class HeaderFilterAction(BaseAction):
    """Filter and modify request headers."""
    action_type = "api_header_filter"
    display_name = "头部过滤器"
    description = "过滤和修改请求头"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            headers = params.get("headers", {})
            include_keys = params.get("include_keys", [])
            exclude_keys = params.get("exclude_keys", [])
            add_headers = params.get("add_headers", {})
            transform_case = params.get("transform_case", None)

            filtered = dict(headers)

            if include_keys:
                filtered = {k: v for k, v in filtered.items() if k in include_keys}

            if exclude_keys:
                filtered = {k: v for k, v in filtered.items() if k not in exclude_keys}

            filtered.update(add_headers)

            if transform_case == "lower":
                filtered = {k.lower(): v for k, v in filtered.items()}
            elif transform_case == "upper":
                filtered = {k.upper(): v for k, v in filtered.items()}

            return ActionResult(
                success=True,
                data={
                    "headers": filtered,
                    "original_count": len(headers),
                    "final_count": len(filtered)
                },
                message=f"Filtered headers: {len(headers)} -> {len(filtered)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Header filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["headers"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"include_keys": [], "exclude_keys": [], "add_headers": {}, "transform_case": None}


class FieldSelectorAction(BaseAction):
    """Select specific fields from response data."""
    action_type = "api_field_selector"
    display_name = "字段选择器"
    description = "从响应数据中选择特定字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            flatten = params.get("flatten", False)
            rename_map = params.get("rename_map", {})

            if not isinstance(data, list):
                data = [data]

            selected = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                record = {}
                for field in fields:
                    value = item.get(field)
                    if flatten and isinstance(value, (list, dict)):
                        import json
                        value = json.dumps(value)
                    final_field = rename_map.get(field, field)
                    record[final_field] = value
                selected.append(record)

            return ActionResult(
                success=True,
                data={
                    "selected": selected,
                    "original_count": len(data),
                    "selected_count": len(selected),
                    "fields": fields
                },
                message=f"Selected {len(fields)} fields from {len(data)} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Field selector error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data", "fields"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"flatten": False, "rename_map": {}}


class ResultFilterAction(BaseAction):
    """Filter results by various criteria."""
    action_type = "api_result_filter"
    display_name = "结果过滤器"
    description = "按各种条件过滤结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filter_type = params.get("filter_type", "unique")
            min_value = params.get("min_value")
            max_value = params.get("max_value")
            pattern = params.get("pattern")
            limit = params.get("limit")

            if not isinstance(data, list):
                data = [data]

            filtered = data

            if filter_type == "unique":
                seen = set()
                filtered = []
                for item in filtered:
                    key = str(item) if not isinstance(item, (str, int, float)) else item
                    if key not in seen:
                        seen.add(key)
                        filtered.append(item)

            elif filter_type == "range" and min_value is not None:
                filtered = [x for x in filtered if isinstance(x, (int, float)) and min_value <= x <= (max_value or float("inf"))]

            elif filter_type == "pattern" and pattern:
                regex = re.compile(pattern, re.IGNORECASE)
                filtered = [x for x in filtered if regex.search(str(x))]

            elif filter_type == "null":
                filtered = [x for x in filtered if x is not None]

            if limit and len(filtered) > limit:
                filtered = filtered[:limit]

            return ActionResult(
                success=True,
                data={
                    "filtered": filtered,
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "filter_type": filter_type
                },
                message=f"Applied {filter_type} filter: {len(data)} -> {len(filtered)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Result filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"filter_type": "unique", "min_value": None, "max_value": None, "pattern": None, "limit": None}
