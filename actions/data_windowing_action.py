"""Data windowing action module for RabAI AutoClick.

Provides data windowing operations:
- RollingWindowAction: Rolling window aggregation
- TumblingWindowAction: Tumbling window aggregation
- SessionWindowAction: Session window detection
- TimeWindowAction: Time-based windows
"""

from typing import Any, Callable, Dict, List, Optional
from collections import deque
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RollingWindowAction(BaseAction):
    """Rolling window aggregation."""
    action_type = "rolling_window"
    display_name = "滚动窗口"
    description = "滚动窗口聚合"

    def __init__(self):
        super().__init__()
        self._windows: Dict[str, deque] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "add")
            window_id = params.get("window_id", "default")
            window_size = params.get("window_size", 10)
            items = params.get("items", [])
            aggregation = params.get("aggregation", "avg")

            if not isinstance(items, list):
                items = [items]

            if action == "add":
                if window_id not in self._windows:
                    self._windows[window_id] = deque(maxlen=window_size)

                for item in items:
                    self._windows[window_id].append(item)

                window = list(self._windows[window_id])
                agg_result = self._aggregate(window, aggregation)

                return ActionResult(
                    success=True,
                    message=f"Added {len(items)} items to window '{window_id}' (size={len(window)}/{window_size})",
                    data={
                        "window_id": window_id,
                        "window_size": len(window),
                        "aggregation": agg_result,
                        "items": window,
                    },
                )

            elif action == "get":
                if window_id not in self._windows:
                    return ActionResult(success=True, message=f"Window '{window_id}' is empty", data={"window": [], "size": 0})
                window = list(self._windows[window_id])
                return ActionResult(success=True, message=f"Window '{window_id}': {len(window)} items", data={"window": window, "size": len(window)})

            elif action == "clear":
                if window_id in self._windows:
                    self._windows[window_id].clear()
                return ActionResult(success=True, message=f"Window '{window_id}' cleared")

            elif action == "stats":
                if window_id not in self._windows:
                    return ActionResult(success=True, message=f"Window '{window_id}' is empty", data={"size": 0})
                window = list(self._windows[window_id])
                results = {}
                for agg in ["avg", "sum", "min", "max", "count"]:
                    results[agg] = self._aggregate(window, agg)
                return ActionResult(success=True, message=f"Window '{window_id}' stats", data={"stats": results, "size": len(window)})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"RollingWindow error: {e}")

    def _aggregate(self, items: List, aggregation: str) -> Any:
        if not items:
            return None
        numeric_items = [i for i in items if isinstance(i, (int, float))]
        if not numeric_items:
            return items[-1] if items else None
        if aggregation == "avg":
            return round(sum(numeric_items) / len(numeric_items), 6)
        elif aggregation == "sum":
            return round(sum(numeric_items), 4)
        elif aggregation == "min":
            return min(numeric_items)
        elif aggregation == "max":
            return max(numeric_items)
        elif aggregation == "count":
            return len(numeric_items)
        elif aggregation == "first":
            return items[0]
        elif aggregation == "last":
            return items[-1]
        return items


class TumblingWindowAction(BaseAction):
    """Tumbling window aggregation."""
    action_type = "tumbling_window"
    display_name = "滚动窗口(固定)"
    description = "固定大小滚动窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            aggregation = params.get("aggregation", "avg")
            offset = params.get("offset", 0)

            if not isinstance(data, list):
                data = [data]

            windows = []
            for i in range(offset, len(data), window_size):
                window_items = data[i : i + window_size]
                agg_result = RollingWindowAction()._aggregate(window_items, aggregation)
                windows.append({
                    "window_id": len(windows),
                    "start_index": i,
                    "end_index": min(i + window_size, len(data)),
                    "size": len(window_items),
                    "items": window_items,
                    "aggregation": agg_result,
                })

            return ActionResult(
                success=True,
                message=f"Created {len(windows)} tumbling windows (size={window_size})",
                data={
                    "windows": windows,
                    "window_count": len(windows),
                    "window_size": window_size,
                    "aggregation": aggregation,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TumblingWindow error: {e}")


class SessionWindowAction(BaseAction):
    """Session window detection."""
    action_type = "session_window"
    display_name = "会话窗口"
    description = "会话窗口检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            gap_threshold = params.get("gap_threshold", 5)
            timestamp_field = params.get("timestamp_field", "timestamp")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            sessions = []
            current_session = [data[0]]
            last_ts = None

            for i in range(1, len(data)):
                item = data[i]
                if not isinstance(item, dict):
                    continue

                ts = item.get(timestamp_field)
                if ts is None:
                    continue

                if last_ts is None:
                    last_ts = ts
                    continue

                gap = abs(ts - last_ts) if isinstance(ts, (int, float)) else 0

                if gap > gap_threshold:
                    sessions.append(current_session)
                    current_session = [item]
                else:
                    current_session.append(item)
                last_ts = ts

            if current_session:
                sessions.append(current_session)

            session_results = []
            for i, session in enumerate(sessions):
                agg_result = RollingWindowAction()._aggregate(session, "count")
                session_results.append({
                    "session_id": i,
                    "size": len(session),
                    "items": session,
                })

            return ActionResult(
                success=True,
                message=f"Detected {len(sessions)} sessions (gap_threshold={gap_threshold})",
                data={
                    "sessions": session_results,
                    "session_count": len(sessions),
                    "gap_threshold": gap_threshold,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SessionWindow error: {e}")


class TimeWindowAction(BaseAction):
    """Time-based windows."""
    action_type = "time_window"
    display_name: "时间窗口"
    description: "基于时间的窗口"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_seconds = params.get("window_seconds", 60)
            timestamp_field = params.get("timestamp_field", "timestamp")
            aggregation = params.get("aggregation", "avg")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            time_windows: Dict[int, List] = {}
            for item in data:
                if not isinstance(item, dict):
                    continue
                ts = item.get(timestamp_field)
                if ts is None:
                    continue
                if isinstance(ts, (int, float)):
                    window_key = int(ts // window_seconds)
                    if window_key not in time_windows:
                        time_windows[window_key] = []
                    time_windows[window_key].append(item)

            sorted_keys = sorted(time_windows.keys())
            results = []
            for key in sorted_keys:
                items = time_windows[key]
                from datetime import datetime
                window_start = datetime.fromtimestamp(key * window_seconds)
                agg_result = RollingWindowAction()._aggregate(items, aggregation)
                results.append({
                    "window_start": window_start.isoformat(),
                    "timestamp": key * window_seconds,
                    "size": len(items),
                    "items": items,
                    "aggregation": agg_result,
                })

            return ActionResult(
                success=True,
                message=f"Created {len(results)} time windows (window={window_seconds}s)",
                data={
                    "windows": results,
                    "window_count": len(results),
                    "window_seconds": window_seconds,
                    "aggregation": aggregation,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TimeWindow error: {e}")
