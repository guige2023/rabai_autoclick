"""Data stream processor action module for RabAI AutoClick.

Provides stream data processing operations:
- StreamProcessorAction: Process data streams with transformations
- WindowedProcessorAction: Process data in fixed-size windows
- StreamAggregatorAction: Aggregate streaming data
- StreamFilterAction: Filter streaming data
- StreamJoinerAction: Join multiple data streams
"""

import time
from typing import Any, Dict, List, Optional, Callable, Union, Iterator
from datetime import datetime
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamProcessorAction(BaseAction):
    """Process data streams with transformations."""
    action_type = "data_stream_processor"
    display_name = "流数据处理器"
    description = "使用转换处理数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_data = params.get("stream_data", [])
            transforms = params.get("transforms", [])
            buffer_size = params.get("buffer_size", 100)
            max_items = params.get("max_items")

            if not stream_data:
                return ActionResult(success=False, message="No stream data provided")

            processed = []
            transform_counts = {t: 0 for t in transforms}

            for item in stream_data:
                if max_items and len(processed) >= max_items:
                    break

                current = item
                for transform in transforms:
                    if transform == "uppercase":
                        current = str(current).upper()
                    elif transform == "lowercase":
                        current = str(current).lower()
                    elif transform == "trim":
                        current = str(current).strip()
                    elif transform == "hash":
                        import hashlib
                        current = hashlib.md5(str(current).encode()).hexdigest()
                    elif transform == "length":
                        current = len(str(current))
                    transform_counts[transform] += 1

                processed.append(current)

            return ActionResult(
                success=True,
                data={
                    "processed_items": processed,
                    "total_processed": len(processed),
                    "original_count": len(stream_data),
                    "transform_counts": transform_counts,
                    "transforms_applied": transforms
                },
                message=f"Stream processed: {len(processed)} items with {len(transforms)} transforms"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream processor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["stream_data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"transforms": [], "buffer_size": 100, "max_items": None}


class WindowedProcessorAction(BaseAction):
    """Process data in fixed-size windows."""
    action_type = "data_windowed_processor"
    display_name = "窗口处理器"
    description = "按固定大小窗口处理数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_data = params.get("stream_data", [])
            window_size = params.get("window_size", 10)
            window_type = params.get("window_type", "tumbling")
            overlap = params.get("overlap", 0)
            window_func = params.get("window_func", "sum")

            if not stream_data:
                return ActionResult(success=False, message="No stream data provided")

            windows = []
            current_window = []

            if window_type == "tumbling":
                for i, item in enumerate(stream_data):
                    current_window.append(item)
                    if len(current_window) >= window_size:
                        windows.append(current_window.copy())
                        current_window = []

                if current_window:
                    windows.append(current_window)

            elif window_type == "sliding":
                step = window_size - overlap
                for i in range(0, len(stream_data), step):
                    window = stream_data[i:i + window_size]
                    if len(window) > 0:
                        windows.append(window)

            elif window_type == "session":
                gap_timeout = params.get("gap_timeout", 5)
                session_windows = []
                current_session = []

                for i, item in enumerate(stream_data):
                    if not current_session:
                        current_session.append(item)
                    else:
                        current_session.append(item)

                    if len(current_session) >= window_size:
                        session_windows.append(current_session.copy())
                        current_session = []

                if current_session:
                    session_windows.append(current_session)

                windows = session_windows

            window_results = []
            for window in windows:
                if window_func == "sum" and all(isinstance(x, (int, float)) for x in window):
                    result = sum(window)
                elif window_func == "avg" and all(isinstance(x, (int, float)) for x in window):
                    result = sum(window) / len(window)
                elif window_func == "min":
                    result = min(window)
                elif window_func == "max":
                    result = max(window)
                elif window_func == "count":
                    result = len(window)
                elif window_func == "first":
                    result = window[0]
                elif window_func == "last":
                    result = window[-1]
                else:
                    result = window
                window_results.append(result)

            return ActionResult(
                success=True,
                data={
                    "windows": windows,
                    "window_results": window_results,
                    "window_count": len(windows),
                    "window_size": window_size,
                    "window_type": window_type,
                    "window_func": window_func
                },
                message=f"Created {len(windows)} {window_type} windows of size {window_size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Windowed processor error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["stream_data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"window_size": 10, "window_type": "tumbling", "overlap": 0, "window_func": "sum", "gap_timeout": 5}


class StreamAggregatorAction(BaseAction):
    """Aggregate streaming data."""
    action_type = "data_stream_aggregator"
    display_name = "流数据聚合器"
    description = "聚合流式数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_data = params.get("stream_data", [])
            group_by = params.get("group_by")
            aggregations = params.get("aggregations", ["count"])
            having = params.get("having")

            if not stream_data:
                return ActionResult(success=False, message="No stream data provided")

            groups = {}

            for item in stream_data:
                if not isinstance(item, dict):
                    item = {"value": item}

                if group_by:
                    key = item.get(group_by, "unknown")
                else:
                    key = "all"

                if key not in groups:
                    groups[key] = []
                groups[key].append(item)

            results = {}
            for group_key, group_items in groups.items():
                group_result = {}
                for agg in aggregations:
                    values = [item.get("value", item) for item in group_items]
                    numeric_values = [v for v in values if isinstance(v, (int, float))]

                    if agg == "count":
                        group_result["count"] = len(group_items)
                    elif agg == "sum" and numeric_values:
                        group_result["sum"] = sum(numeric_values)
                    elif agg == "avg" and numeric_values:
                        group_result["avg"] = sum(numeric_values) / len(numeric_values)
                    elif agg == "min" and numeric_values:
                        group_result["min"] = min(numeric_values)
                    elif agg == "max" and numeric_values:
                        group_result["max"] = max(numeric_values)
                    elif agg == "first":
                        group_result["first"] = values[0]
                    elif agg == "last":
                        group_result["last"] = values[-1]

                results[group_key] = group_result

            if having:
                filtered_results = {}
                for group_key, agg_result in results.items():
                    if having.get("min_count") and agg_result.get("count", 0) < having["min_count"]:
                        continue
                    filtered_results[group_key] = agg_result
                results = filtered_results

            return ActionResult(
                success=True,
                data={
                    "aggregated": results,
                    "group_count": len(results),
                    "total_items": len(stream_data),
                    "aggregations": aggregations
                },
                message=f"Aggregated {len(stream_data)} items into {len(results)} groups"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream aggregator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["stream_data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"group_by": None, "aggregations": ["count"], "having": None}


class StreamFilterAction(BaseAction):
    """Filter streaming data."""
    action_type = "data_stream_filter"
    display_name = "流数据过滤器"
    description = "过滤流式数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_data = params.get("stream_data", [])
            filter_conditions = params.get("filter_conditions", [])
            filter_type = params.get("filter_type", "include")
            logical_op = params.get("logical_op", "and")

            if not stream_data:
                return ActionResult(success=False, message="No stream data provided")

            filtered = []

            for item in stream_data:
                if not isinstance(item, dict):
                    item = {"value": item}

                matches = []
                for condition in filter_conditions:
                    field = condition.get("field", "value")
                    operator = condition.get("operator", "eq")
                    value = condition.get("value")

                    item_value = item.get(field, item.get("value"))

                    if operator == "eq":
                        match = item_value == value
                    elif operator == "ne":
                        match = item_value != value
                    elif operator == "gt":
                        match = item_value is not None and item_value > value
                    elif operator == "lt":
                        match = item_value is not None and item_value < value
                    elif operator == "gte":
                        match = item_value is not None and item_value >= value
                    elif operator == "lte":
                        match = item_value is not None and item_value <= value
                    elif operator == "contains":
                        match = value in str(item_value) if item_value else False
                    elif operator == "startswith":
                        match = str(item_value).startswith(str(value)) if item_value else False
                    elif operator == "endswith":
                        match = str(item_value).endswith(str(value)) if item_value else False
                    elif operator == "in":
                        match = item_value in value if isinstance(value, list) else False
                    elif operator == "exists":
                        match = field in item
                    else:
                        match = False

                    matches.append(match)

                if not filter_conditions:
                    passes = True
                elif logical_op == "and":
                    passes = all(matches)
                else:
                    passes = any(matches)

                if filter_type == "include" and passes:
                    filtered.append(item)
                elif filter_type == "exclude" and not passes:
                    filtered.append(item)

            return ActionResult(
                success=True,
                data={
                    "filtered": filtered,
                    "original_count": len(stream_data),
                    "filtered_count": len(filtered),
                    "filter_conditions": filter_conditions
                },
                message=f"Stream filter: {len(stream_data)} -> {len(filtered)} items"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream filter error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["stream_data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"filter_conditions": [], "filter_type": "include", "logical_op": "and"}


class StreamJoinerAction(BaseAction):
    """Join multiple data streams."""
    action_type = "data_stream_joiner"
    display_name = "流数据连接器"
    description = "连接多个数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_a = params.get("stream_a", [])
            stream_b = params.get("stream_b", [])
            join_key_a = params.get("join_key_a", "id")
            join_key_b = params.get("join_key_b", "id")
            join_type = params.get("join_type", "inner")
            join_suffixes = params.get("join_suffixes", ["_a", "_b"])

            if not stream_a and not stream_b:
                return ActionResult(success=False, message="No stream data provided")

            if not stream_a:
                if join_type in ("inner", "left"):
                    return ActionResult(success=True, data={"joined": [], "join_type": join_type, "count": 0}, message="Stream A empty, no joins")
                return ActionResult(success=True, data={"joined": stream_b, "join_type": join_type, "count": len(stream_b)}, message=f"Returned {len(stream_b)} items from stream B")

            if not stream_b:
                if join_type in ("inner", "right"):
                    return ActionResult(success=True, data={"joined": [], "join_type": join_type, "count": 0}, message="Stream B empty, no joins")
                return ActionResult(success=True, data={"joined": stream_a, "join_type": join_type, "count": len(stream_a)}, message=f"Returned {len(stream_a)} items from stream A")

            index_b = {item.get(join_key_b, None): item for item in stream_b}
            joined = []
            unmatched_a = []
            unmatched_b = list(stream_b)

            for item_a in stream_a:
                key_a = item_a.get(join_key_a)
                item_b = index_b.get(key_a)

                if item_b:
                    suffix_a, suffix_b = join_suffixes
                    merged = {}
                    for k, v in item_a.items():
                        merged[f"{k}{suffix_a}"] = v
                    for k, v in item_b.items():
                        if k == join_key_b:
                            merged[k] = v
                        else:
                            merged[f"{k}{suffix_b}"] = v
                    joined.append(merged)
                    if item_b in unmatched_b:
                        unmatched_b.remove(item_b)
                else:
                    unmatched_a.append(item_a)

            result_items = joined
            if join_type == "left":
                result_items = joined + [dict(item, **{f"{join_key_b}_matched": False}) for item in unmatched_a]
            elif join_type == "right":
                result_items = joined + [dict(item, **{f"{join_key_a}_matched": False}) for item in unmatched_b]
            elif join_type == "full":
                result_items = joined + [dict(item, **{f"{join_key_b}_matched": False}) for item in unmatched_a] + [dict(item, **{f"{join_key_a}_matched": False}) for item in unmatched_b]

            return ActionResult(
                success=True,
                data={
                    "joined": result_items,
                    "join_type": join_type,
                    "count": len(result_items),
                    "inner_join_count": len(joined),
                    "unmatched_a_count": len(unmatched_a),
                    "unmatched_b_count": len(unmatched_b)
                },
                message=f"Joined streams: {len(joined)} matches, {len(result_items)} total"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream joiner error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["stream_a", "stream_b"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"join_key_a": "id", "join_key_b": "id", "join_type": "inner", "join_suffixes": ["_a", "_b"]}
