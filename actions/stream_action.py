"""Stream processing action module for RabAI AutoClick.

Provides stream processing operations:
- StreamFilterAction: Filter stream items
- StreamMapAction: Transform stream items
- StreamReduceAction: Reduce stream to aggregate
- StreamWindowAction: Windowed stream processing
- StreamMergeAction: Merge multiple streams
- StreamSplitAction: Split stream
"""

from typing import Any, Callable, Dict, Iterator, List, Optional
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamFilterAction(BaseAction):
    """Filter stream items."""
    action_type = "stream_filter"
    display_name = "流过滤"
    description = "过滤流数据项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            condition = params.get("condition", "")
            field = params.get("field", "")
            operator = params.get("operator", "eq")
            value = params.get("value", None)

            if not items:
                return ActionResult(success=False, message="No items to filter")

            if condition:
                filtered = []
                for item in items:
                    try:
                        for k, v in (item.items() if isinstance(item, dict) else enumerate([item])):
                            condition_copy = condition.replace(k, repr(v))
                        if eval(condition_copy, {"__builtins__": {}}, {}):
                            filtered.append(item)
                    except:
                        pass
            else:
                filtered = []
                for item in items:
                    if isinstance(item, dict) and field:
                        cell = item.get(field, "")
                        if self._compare(cell, operator, value):
                            filtered.append(item)
                    elif callable(value) and value(item):
                        filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Filtered {len(items)} to {len(filtered)} items",
                data={"items": filtered, "original_count": len(items), "filtered_count": len(filtered)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream filter error: {str(e)}")

    def _compare(self, cell: Any, operator: str, value: Any) -> bool:
        ops = {
            "eq": lambda a, b: a == b, "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b, "ge": lambda a, b: a >= b,
            "lt": lambda a, b: a < b, "le": lambda a, b: a <= b,
            "contains": lambda a, b: b in str(a), "in": lambda a, b: a in b if isinstance(b, (list, tuple)) else a == b
        }
        op_func = ops.get(operator, ops["eq"])
        try:
            return op_func(cell, value)
        except:
            return False


class StreamMapAction(BaseAction):
    """Transform stream items."""
    action_type = "stream_map"
    display_name = "流映射"
    description = "映射转换流数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            field = params.get("field", "")
            transform = params.get("transform", "passthrough")

            if not items:
                return ActionResult(success=False, message="No items to map")

            transformed = []

            for item in items:
                if isinstance(item, dict) and field:
                    new_item = dict(item)
                    original = new_item.get(field)

                    if transform == "uppercase":
                        new_item[field] = str(original).upper()
                    elif transform == "lowercase":
                        new_item[field] = str(original).lower()
                    elif transform == "titlecase":
                        new_item[field] = str(original).title()
                    elif transform == "trim":
                        new_item[field] = str(original).strip()
                    elif transform == "abs":
                        try:
                            new_item[field] = abs(float(original))
                        except:
                            pass
                    elif transform == "str":
                        new_item[field] = str(original) if original is not None else ""
                    elif transform == "int":
                        try:
                            new_item[field] = int(float(original))
                        except:
                            pass
                    elif transform == "float":
                        try:
                            new_item[field] = float(original)
                        except:
                            pass
                    elif transform == "length":
                        new_item[field] = len(str(original)) if original is not None else 0
                    elif transform == "hash":
                        import hashlib
                        new_item[field] = hashlib.md5(str(original).encode()).hexdigest()
                    else:
                        pass

                    transformed.append(new_item)
                else:
                    transformed.append(item)

            return ActionResult(
                success=True,
                message=f"Mapped {len(items)} items",
                data={"items": transformed, "count": len(transformed)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream map error: {str(e)}")


class StreamReduceAction(BaseAction):
    """Reduce stream to aggregate."""
    action_type = "stream_reduce"
    display_name = "流聚合"
    description = "聚合流数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            field = params.get("field", "")
            reduce_func = params.get("reduce_func", "sum")
            initial = params.get("initial", 0)

            if not items:
                return ActionResult(success=False, message="No items to reduce")

            if field:
                values = []
                for item in items:
                    if isinstance(item, dict):
                        val = item.get(field, 0)
                        try:
                            values.append(float(val))
                        except:
                            pass
                    else:
                        try:
                            values.append(float(item))
                        except:
                            pass

                if reduce_func == "sum":
                    result = sum(values) if values else initial
                elif reduce_func == "avg" or reduce_func == "mean":
                    result = sum(values) / len(values) if values else initial
                elif reduce_func == "min":
                    result = min(values) if values else initial
                elif reduce_func == "max":
                    result = max(values) if values else initial
                elif reduce_func == "count":
                    result = len(values)
                elif reduce_func == "first":
                    result = values[0] if values else initial
                elif reduce_func == "last":
                    result = values[-1] if values else initial
                elif reduce_func == "median":
                    values.sort()
                    n = len(values)
                    result = values[n // 2] if n > 0 else initial
                else:
                    result = sum(values) if values else initial
            else:
                result = initial

            return ActionResult(
                success=True,
                message=f"Reduced to {result}",
                data={"result": result, "type": reduce_func}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream reduce error: {str(e)}")


class StreamWindowAction(BaseAction):
    """Windowed stream processing."""
    action_type = "stream_window"
    display_name = "流窗口"
    description = "窗口化流处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            window_size = params.get("window_size", 3)
            window_type = params.get("type", "fixed")
            operation = params.get("operation", "mean")
            step = params.get("step", 1)

            if not items:
                return ActionResult(success=False, message="No items to window")

            windows = []

            if window_type == "fixed":
                for i in range(0, len(items), step):
                    window = items[i:i + window_size]
                    if len(window) == window_size:
                        windows.append(window)

            elif window_type == "sliding":
                for i in range(len(items)):
                    start = max(0, i - window_size + 1)
                    window = items[start:i + 1]
                    windows.append(window)

            elif window_type == "tumbling":
                for i in range(0, len(items), window_size):
                    window = items[i:i + window_size]
                    windows.append(window)

            window_results = []
            for window in windows:
                if operation == "mean":
                    try:
                        values = [float(v) for v in window]
                        result = sum(values) / len(values)
                    except:
                        result = window
                elif operation == "sum":
                    try:
                        values = [float(v) for v in window]
                        result = sum(values)
                    except:
                        result = window
                elif operation == "min":
                    try:
                        values = [float(v) for v in window]
                        result = min(values)
                    except:
                        result = window
                elif operation == "max":
                    try:
                        values = [float(v) for v in window]
                        result = max(values)
                    except:
                        result = window
                elif operation == "count":
                    result = len(window)
                elif operation == "first":
                    result = window[0]
                elif operation == "last":
                    result = window[-1]
                else:
                    result = window

                window_results.append({"window": window, "result": result})

            return ActionResult(
                success=True,
                message=f"Created {len(windows)} windows with {operation}",
                data={"windows": window_results, "window_count": len(windows), "operation": operation}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream window error: {str(e)}")


class StreamMergeAction(BaseAction):
    """Merge multiple streams."""
    action_type = "stream_merge"
    display_name = "合并流"
    description = "合并多个数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            streams = params.get("streams", [])
            merge_type = params.get("type", "concat")
            key_field = params.get("key_field", "")

            if not streams:
                return ActionResult(success=False, message="No streams to merge")

            if merge_type == "concat":
                merged = []
                for stream in streams:
                    if isinstance(stream, list):
                        merged.extend(stream)
                    else:
                        merged.append(stream)

            elif merge_type == "union":
                seen = set()
                merged = []
                for stream in streams:
                    if isinstance(stream, list):
                        for item in stream:
                            key = str(item) if not key_field else str(item.get(key_field, ""))
                            if key not in seen:
                                seen.add(key)
                                merged.append(item)
                    else:
                        key = str(stream) if not key_field else str(stream.get(key_field, ""))
                        if key not in seen:
                            seen.add(key)
                            merged.append(stream)

            elif merge_type == "zip":
                max_len = max(len(s) for s in streams if isinstance(s, list)) if streams else 0
                merged = []
                for i in range(max_len):
                    row = []
                    for stream in streams:
                        if isinstance(stream, list) and i < len(stream):
                            row.append(stream[i])
                        else:
                            row.append(None)
                    merged.append(row)

            elif merge_type == "interleave":
                merged = []
                max_len = max(len(s) for s in streams if isinstance(s, list)) if streams else 0
                for i in range(max_len):
                    for stream in streams:
                        if isinstance(stream, list) and i < len(stream):
                            merged.append(stream[i])

            else:
                merged = []

            return ActionResult(
                success=True,
                message=f"Merged {len(streams)} streams into {len(merged)} items",
                data={"merged": merged, "merged_count": len(merged), "stream_count": len(streams)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream merge error: {str(e)}")


class StreamSplitAction(BaseAction):
    """Split stream."""
    action_type = "stream_split"
    display_name = "分割流"
    description = "分割数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            split_by = params.get("split_by", "count")
            count = params.get("count", 2)
            field = params.get("field", "")
            condition = params.get("condition", "")

            if not items:
                return ActionResult(success=False, message="No items to split")

            if split_by == "count":
                chunk_size = max(1, len(items) // count)
                chunks = []
                for i in range(0, len(items), chunk_size):
                    chunks.append(items[i:i + chunk_size])

            elif split_by == "field":
                buckets = {}
                for item in items:
                    if isinstance(item, dict) and field:
                        key = str(item.get(field, "unknown"))
                    else:
                        key = str(item)
                    if key not in buckets:
                        buckets[key] = []
                    buckets[key].append(item)
                chunks = list(buckets.values())

            elif split_by == "condition":
                true_items = []
                false_items = []
                for item in items:
                    try:
                        if condition:
                            for k, v in (item.items() if isinstance(item, dict) else enumerate([item])):
                                condition_copy = condition.replace(k, repr(v))
                            if eval(condition_copy, {"__builtins__": {}}, {}):
                                true_items.append(item)
                            else:
                                false_items.append(item)
                        else:
                            false_items.append(item)
                    except:
                        false_items.append(item)
                chunks = [true_items, false_items]

            else:
                chunks = [items]

            return ActionResult(
                success=True,
                message=f"Split into {len(chunks)} streams",
                data={"streams": chunks, "stream_count": len(chunks)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Stream split error: {str(e)}")
