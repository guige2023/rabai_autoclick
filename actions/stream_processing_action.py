"""Stream processing action module for RabAI AutoClick.

Provides stream processing operations:
- StreamFilterAction: Filter stream elements
- StreamMapAction: Transform stream elements
- StreamReduceAction: Reduce stream to aggregate
- StreamWindowAction: Windowed stream processing
- StreamJoinAction: Join two streams
"""

from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamState:
    """Maintains stream processing state."""

    def __init__(self):
        self._lock = Lock()
        self._buffers: Dict[str, deque] = {}
        self._counters: Dict[str, int] = {}
        self._windows: Dict[str, List] = {}

    def get_buffer(self, name: str, maxlen: int = 1000) -> deque:
        with self._lock:
            if name not in self._buffers:
                self._buffers[name] = deque(maxlen=maxlen)
            return self._buffers[name]

    def increment(self, counter: str, delta: int = 1) -> int:
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + delta
            return self._counters[counter]

    def get_counter(self, counter: str) -> int:
        with self._lock:
            return self._counters.get(counter, 0)

    def get_window(self, name: str) -> List:
        with self._lock:
            if name not in self._windows:
                self._windows[name] = []
            return self._windows[name]


class StreamFilterAction(BaseAction):
    """Filter stream elements based on predicates."""
    action_type = "stream_filter"
    display_name = "流数据过滤"
    description = "基于谓词过滤流数据元素"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            predicate_type = params.get("predicate_type", "include")
            field = params.get("field", None)
            operator = params.get("operator", "eq")
            value = params.get("value", None)

            if not isinstance(items, list):
                items = [items]

            filtered = []
            for item in items:
                if field is None:
                    check_value = item
                elif isinstance(item, dict):
                    check_value = item.get(field)
                else:
                    check_value = None

                matched = False
                if operator == "eq":
                    matched = check_value == value
                elif operator == "ne":
                    matched = check_value != value
                elif operator == "gt":
                    matched = check_value is not None and check_value > value
                elif operator == "ge":
                    matched = check_value is not None and check_value >= value
                elif operator == "lt":
                    matched = check_value is not None and check_value < value
                elif operator == "le":
                    matched = check_value is not None and check_value <= value
                elif operator == "in":
                    matched = check_value in value if isinstance(value, (list, tuple, set)) else False
                elif operator == "contains":
                    matched = value in check_value if check_value is not None else False
                elif operator == "startswith":
                    matched = isinstance(check_value, str) and check_value.startswith(str(value))
                elif operator == "endswith":
                    matched = isinstance(check_value, str) and check_value.endswith(str(value))
                elif operator == "regex":
                    import re
                    matched = isinstance(check_value, str) and re.search(str(value), check_value) is not None
                elif operator == "exists":
                    matched = check_value is not None

                if predicate_type == "include" and matched:
                    filtered.append(item)
                elif predicate_type == "exclude" and not matched:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Filtered {len(items)} -> {len(filtered)} items",
                data={"filtered": filtered, "input_count": len(items), "output_count": len(filtered)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StreamFilter error: {e}")


class StreamMapAction(BaseAction):
    """Transform stream elements."""
    action_type = "stream_map"
    display_name = "流数据映射"
    description = "对流数据元素进行转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            map_type = params.get("map_type", "field")
            field = params.get("field", None)
            expression = params.get("expression", None)
            rename_map = params.get("rename_map", {})

            if not isinstance(items, list):
                items = [items]

            mapped = []
            for item in items:
                if not isinstance(item, dict):
                    if map_type == "passthrough":
                        mapped.append(item)
                    continue

                if map_type == "field":
                    result = item.get(field, None) if field else item
                elif map_type == "pick":
                    picked = {k: item.get(k) for k in expression if k in item}
                    result = picked
                elif map_type == "omit":
                    omitted = {k: v for k, v in item.items() if k not in (expression or [])}
                    result = omitted
                elif map_type == "rename":
                    renamed = {rename_map.get(k, k): v for k, v in item.items()}
                    result = renamed
                elif map_type == "compute":
                    result = {**item}
                    for target_field, expr in (expression or {}).items():
                        try:
                            result[target_field] = eval(expr, {"item": item, "__builtins__": {}})
                        except Exception:
                            result[target_field] = None
                else:
                    result = item
                mapped.append(result)

            return ActionResult(
                success=True,
                message=f"Mapped {len(items)} items",
                data={"mapped": mapped, "count": len(mapped)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StreamMap error: {e}")


class StreamReduceAction(BaseAction):
    """Reduce stream to aggregate values."""
    action_type = "stream_reduce"
    display_name = "流数据聚合"
    description = "将流数据聚合为汇总值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            reduce_type = params.get("reduce_type", "sum")
            field = params.get("field", None)
            group_by = params.get("group_by", None)
            initial = params.get("initial", 0)

            if not isinstance(items, list):
                items = [items]

            if group_by:
                groups: Dict[str, List] = {}
                for item in items:
                    if isinstance(item, dict):
                        key = str(item.get(group_by, "unknown"))
                    else:
                        key = "default"
                    if key not in groups:
                        groups[key] = []
                    groups[key].append(item)

                results = {}
                for group_key, group_items in groups.items():
                    values = [item.get(field, 0) if isinstance(item, dict) else item for item in group_items]
                    if reduce_type == "sum":
                        results[group_key] = sum(values)
                    elif reduce_type == "count":
                        results[group_key] = len(values)
                    elif reduce_type == "avg":
                        results[group_key] = sum(values) / len(values) if values else 0
                    elif reduce_type == "min":
                        results[group_key] = min(values) if values else None
                    elif reduce_type == "max":
                        results[group_key] = max(values) if values else None
                    elif reduce_type == "product":
                        result = 1
                        for v in values:
                            result *= v
                        results[group_key] = result
                return ActionResult(
                    success=True,
                    message=f"Grouped reduce: {len(groups)} groups",
                    data={"results": results, "group_count": len(groups)},
                )
            else:
                values = []
                for item in items:
                    if isinstance(item, dict) and field:
                        values.append(item.get(field, 0))
                    else:
                        values.append(item)

                if reduce_type == "sum":
                    result = sum(values)
                elif reduce_type == "count":
                    result = len(values)
                elif reduce_type == "avg":
                    result = sum(values) / len(values) if values else 0
                elif reduce_type == "min":
                    result = min(values) if values else None
                elif reduce_type == "max":
                    result = max(values) if values else None
                elif reduce_type == "product":
                    result = 1
                    for v in values:
                        result *= v
                elif reduce_type == "first":
                    result = values[0] if values else None
                elif reduce_type == "last":
                    result = values[-1] if values else None
                elif reduce_type == "collect":
                    result = values
                else:
                    result = sum(values)

                return ActionResult(
                    success=True,
                    message=f"Reduced to {result}",
                    data={"result": result, "reduce_type": reduce_type, "item_count": len(items)},
                )
        except Exception as e:
            return ActionResult(success=False, message=f"StreamReduce error: {e}")


class StreamWindowAction(BaseAction):
    """Windowed stream processing."""
    action_type = "stream_window"
    display_name = "流数据窗口"
    description = "窗口化流数据处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            window_type = params.get("window_type", "tumbling")
            size = params.get("size", 5)
            slide = params.get("slide", None)
            window_func = params.get("window_func", "avg")

            if not isinstance(items, list):
                items = [items]

            if slide is None:
                slide = size

            windows = []
            n = len(items)

            if window_type == "tumbling":
                for start in range(0, n, slide):
                    end = start + size
                    window_items = items[start:min(end, n)]
                    windows.append({"start": start, "end": min(end, n), "items": window_items})
            elif window_type == "sliding":
                for start in range(0, n, slide):
                    end = start + size
                    window_items = items[start:min(end, n)]
                    windows.append({"start": start, "end": min(end, n), "items": window_items})
            elif window_type == "session":
                gap = params.get("gap", 2)
                session_windows: List[List] = []
                current_session = []
                last_ts = None
                for i, item in enumerate(items):
                    current_session.append(item)
                    if i - (len(session_windows) - 1 if session_windows else 0) > gap:
                        if current_session:
                            session_windows.append(current_session)
                        current_session = []
                if current_session:
                    session_windows.append(current_session)
                windows = [{"items": w} for w in session_windows]

            window_results = []
            for w in windows:
                w_items = w["items"]
                if not w_items:
                    continue
                if isinstance(w_items[0], dict) and window_func:
                    field = params.get("field", "value")
                    values = [item.get(field, 0) for item in w_items]
                    if window_func == "avg":
                        agg = sum(values) / len(values)
                    elif window_func == "sum":
                        agg = sum(values)
                    elif window_func == "min":
                        agg = min(values)
                    elif window_func == "max":
                        agg = max(values)
                    elif window_func == "count":
                        agg = len(values)
                    else:
                        agg = values
                    window_results.append({"window": w, "result": agg})
                else:
                    window_results.append({"window": w, "result": w_items})

            return ActionResult(
                success=True,
                message=f"Windowed: {len(windows)} windows",
                data={"windows": window_results, "window_count": len(windows)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StreamWindow error: {e}")


class StreamJoinAction(BaseAction):
    """Join two streams."""
    action_type = "stream_join"
    display_name = "流数据连接"
    description = "连接两个流的数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left = params.get("left", [])
            right = params.get("right", [])
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            join_type = params.get("join_type", "inner")
            left_name = params.get("left_name", "left")
            right_name = params.get("right_name", "right")

            if not isinstance(left, list):
                left = [left]
            if not isinstance(right, list):
                right = [right]

            right_index: Dict[str, List] = {}
            for r_item in right:
                if isinstance(r_item, dict):
                    key = str(r_item.get(right_key, ""))
                else:
                    key = str(r_item)
                if key not in right_index:
                    right_index[key] = []
                right_index[key].append(r_item)

            joined = []
            left_unmatched = []

            for l_item in left:
                if isinstance(l_item, dict):
                    l_key = str(l_item.get(left_key, ""))
                else:
                    l_key = str(l_item)

                if l_key in right_index:
                    for r_item in right_index[l_key]:
                        merged = {f"{left_name}_{k}": v for k, v in l_item.items()} if isinstance(l_item, dict) else {left_name: l_item}
                        merged.update({f"{right_name}_{k}": v for k, v in r_item.items()} if isinstance(r_item, dict) else {right_name: r_item})
                        joined.append(merged)
                elif join_type in ("left", "outer"):
                    left_unmatched.append(l_item)

            if join_type in ("right", "outer"):
                used_keys = set()
                for l_item in left:
                    if isinstance(l_item, dict):
                        used_keys.add(str(l_item.get(left_key, "")))
                for r_item in right:
                    if isinstance(r_item, dict):
                        r_key = str(r_item.get(right_key, ""))
                    else:
                        r_key = str(r_item)
                    if r_key not in used_keys:
                        merged = {left_name: None}
                        merged.update({f"{right_name}_{k}": v for k, v in r_item.items()} if isinstance(r_item, dict) else {right_name: r_item})
                        joined.append(merged)

            return ActionResult(
                success=True,
                message=f"Joined: {len(joined)} results",
                data={
                    "joined": joined,
                    "join_count": len(joined),
                    "left_count": len(left),
                    "right_count": len(right),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StreamJoin error: {e}")
