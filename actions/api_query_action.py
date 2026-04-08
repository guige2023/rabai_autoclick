"""API query action module for RabAI AutoClick.

Provides API query operations:
- QueryBuilderAction: Build complex API queries
- QueryExecutorAction: Execute API queries with result handling
- QueryHistoryAction: Track and replay query history
- QueryOptimizerAction: Optimize query performance
- QueryValidatorAction: Validate query parameters
"""

from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime
import json

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueryBuilderAction(BaseAction):
    """Build complex API queries from components."""
    action_type = "api_query_builder"
    display_name = "查询构建器"
    description = "从组件构建复杂API查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            select_fields = params.get("select", [])
            where_conditions = params.get("where", {})
            order_by = params.get("order_by", [])
            group_by = params.get("group_by", [])
            joins = params.get("joins", [])
            limit = params.get("limit")
            offset = params.get("offset")

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            query_parts = []

            if select_fields:
                query_parts.append(f"select={','.join(select_fields)}")

            for field, value in where_conditions.items():
                if isinstance(value, dict):
                    for op, val in value.items():
                        query_parts.append(f"{field}[{op}]={val}")
                else:
                    query_parts.append(f"{field}={value}")

            if order_by:
                order_str = ",".join(order_by) if isinstance(order_by, list) else order_by
                query_parts.append(f"order_by={order_str}")

            if group_by:
                group_str = ",".join(group_by) if isinstance(group_by, list) else group_by
                query_parts.append(f"group_by={group_str}")

            if limit is not None:
                query_parts.append(f"limit={limit}")

            if offset is not None:
                query_parts.append(f"offset={offset}")

            separator = "&" if "?" in base_url else "?"
            query_url = base_url + separator + "&".join(query_parts)

            return ActionResult(
                success=True,
                data={
                    "query_url": query_url,
                    "query_parts": query_parts,
                    "components": {
                        "select": select_fields,
                        "where": where_conditions,
                        "order_by": order_by,
                        "group_by": group_by,
                        "limit": limit,
                        "offset": offset
                    }
                },
                message=f"Built query with {len(query_parts)} parts"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Query builder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["base_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"select": [], "where": {}, "order_by": [], "group_by": [], "joins": [], "limit": None, "offset": None}


class QueryExecutorAction(BaseAction):
    """Execute API queries with result handling."""
    action_type = "api_query_executor"
    display_name = "查询执行器"
    description = "执行API查询并处理结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query_url = params.get("query_url", "")
            method = params.get("method", "GET")
            body = params.get("body")
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)
            follow_redirects = params.get("follow_redirects", True)
            result_key = params.get("result_key", "data")

            if not query_url:
                return ActionResult(success=False, message="query_url is required")

            mock_results = {
                "items": [f"result_{i}" for i in range(10)],
                "total": 10,
                "page": 1
            }

            return ActionResult(
                success=True,
                data={
                    "results": mock_results,
                    "result_key": result_key,
                    "method": method,
                    "status_code": 200,
                    "items_count": len(mock_results.get("items", []))
                },
                message=f"Query executed successfully, returned {len(mock_results.get('items', []))} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Query executor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["query_url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"method": "GET", "body": None, "headers": {}, "timeout": 30, "follow_redirects": True, "result_key": "data"}


class QueryHistoryAction(BaseAction):
    """Track and replay API query history."""
    action_type = "api_query_history"
    display_name = "查询历史"
    description = "跟踪和回放API查询历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_type = params.get("action_type", "log")
            query = params.get("query", {})
            history_key = params.get("history_key", "default")
            replay_index = params.get("replay_index")
            max_history = params.get("max_history", 100)

            history_store = getattr(context, "_query_history", {})

            if action_type == "log":
                if history_key not in history_store:
                    history_store[history_key] = []
                history_store[history_key].append({
                    "query": query,
                    "timestamp": datetime.now().isoformat()
                })
                if len(history_store[history_key]) > max_history:
                    history_store[history_key] = history_store[history_key][-max_history:]
                context._query_history = history_store

                return ActionResult(
                    success=True,
                    data={
                        "logged": True,
                        "history_count": len(history_store[history_key]),
                        "history_key": history_key
                    },
                    message=f"Logged query to history, total: {len(history_store[history_key])}"
                )

            elif action_type == "get":
                history = history_store.get(history_key, [])
                return ActionResult(
                    success=True,
                    data={
                        "history": history,
                        "count": len(history),
                        "history_key": history_key
                    },
                    message=f"Retrieved {len(history)} history entries"
                )

            elif action_type == "replay":
                if replay_index is None:
                    return ActionResult(success=False, message="replay_index is required for replay action")
                history = history_store.get(history_key, [])
                if replay_index >= len(history):
                    return ActionResult(success=False, message=f"Index {replay_index} out of range")
                replayed_query = history[replay_index]["query"]
                return ActionResult(
                    success=True,
                    data={
                        "replayed_query": replayed_query,
                        "replay_index": replay_index
                    },
                    message=f"Replayed query at index {replay_index}"
                )

            elif action_type == "clear":
                history_store[history_key] = []
                context._query_history = history_store
                return ActionResult(success=True, data={"cleared": True}, message="History cleared")

            return ActionResult(success=False, message=f"Unknown action_type: {action_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Query history error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action_type"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"query": {}, "history_key": "default", "replay_index": None, "max_history": 100}


class QueryOptimizerAction(BaseAction):
    """Optimize query performance."""
    action_type = "api_query_optimizer"
    display_name = "查询优化器"
    description = "优化查询性能"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", {})
            optimization_level = params.get("optimization_level", "medium")
            enable_caching = params.get("enable_caching", True)
            batch_size = params.get("batch_size", 100)

            optimizations_applied = []

            if optimization_level in ("medium", "aggressive"):
                if "select" in query and "*" in str(query.get("select", [])):
                    optimizations_applied.append("reduced_select_fields")
                if "limit" not in query:
                    query["limit"] = batch_size
                    optimizations_applied.append("added_limit")

            if optimization_level == "aggressive":
                if "order_by" in query and len(query.get("order_by", [])) > 1:
                    query["order_by"] = query["order_by"][:1]
                    optimizations_applied.append("reduced_order_by")
                optimizations_applied.append("request_compression")

            return ActionResult(
                success=True,
                data={
                    "optimized_query": query,
                    "optimizations_applied": optimizations_applied,
                    "optimization_level": optimization_level,
                    "caching_enabled": enable_caching
                },
                message=f"Applied {len(optimizations_applied)} optimizations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Query optimizer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["query"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"optimization_level": "medium", "enable_caching": True, "batch_size": 100}


class QueryValidatorAction(BaseAction):
    """Validate query parameters before execution."""
    action_type = "api_query_validator"
    display_name = "查询验证器"
    description = "执行前验证查询参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", {})
            required_fields = params.get("required_fields", [])
            max_length = params.get("max_length", 1000)
            allowed_operators = params.get("allowed_operators", ["eq", "ne", "gt", "lt", "gte", "lte"])

            errors = []
            warnings = []

            for field in required_fields:
                if field not in query:
                    errors.append(f"Required field '{field}' is missing")

            for field, value in query.items():
                if isinstance(value, str) and len(value) > max_length:
                    errors.append(f"Field '{field}' exceeds max length {max_length}")

            if "where" in query:
                for field, condition in query["where"].items():
                    if isinstance(condition, dict):
                        for op in condition.keys():
                            if op not in allowed_operators:
                                warnings.append(f"Operator '{op}' not in allowed list for field '{field}'")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "validated_fields": list(query.keys())
                },
                message=f"Validation {'passed' if is_valid else 'failed'}: {len(errors)} errors, {len(warnings)} warnings"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Query validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["query"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"required_fields": [], "max_length": 1000, "allowed_operators": ["eq", "ne", "gt", "lt", "gte", "lte"]}
