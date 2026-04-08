"""Data windowing action module for RabAI AutoClick.

Provides windowing operations:
- WindowTumblingAction: Tumbling window
- WindowSlidingAction: Sliding window
- WindowSessionAction: Session window
- WindowHoppingAction: Hopping window
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WindowTumblingAction(BaseAction):
    """Tumbling window operation."""
    action_type = "window_tumbling"
    display_name = "翻滚窗口"
    description = "翻滚窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            windows = []
            for i in range(0, len(data), window_size):
                window_items = data[i : i + window_size]
                values = [item.get(field, 0) for item in window_items]
                windows.append({
                    "window_id": len(windows),
                    "start_idx": i,
                    "end_idx": i + len(window_items) - 1,
                    "items": window_items,
                    "count": len(window_items),
                    "sum": sum(values),
                    "avg": sum(values) / len(values) if values else 0,
                })

            return ActionResult(
                success=True,
                data={"windows": windows, "window_count": len(windows), "window_size": window_size},
                message=f"Created {len(windows)} tumbling windows of size {window_size}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window tumbling failed: {e}")


class WindowSlidingAction(BaseAction):
    """Sliding window operation."""
    action_type = "window_sliding"
    display_name = "滑动窗口"
    description = "滑动窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            slide_size = params.get("slide_size", 1)
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            windows = []
            for i in range(0, len(data) - window_size + 1, slide_size):
                window_items = data[i : i + window_size]
                values = [item.get(field, 0) for item in window_items]
                windows.append({
                    "window_id": len(windows),
                    "start_idx": i,
                    "end_idx": i + window_size - 1,
                    "items": window_items,
                    "count": len(window_items),
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                })

            return ActionResult(
                success=True,
                data={"windows": windows, "window_count": len(windows), "window_size": window_size, "slide_size": slide_size},
                message=f"Created {len(windows)} sliding windows (size={window_size}, slide={slide_size})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window sliding failed: {e}")


class WindowSessionAction(BaseAction):
    """Session window operation."""
    action_type = "window_session"
    display_name = "会话窗口"
    description = "会话窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            gap = params.get("gap", 10)
            field = params.get("field", "timestamp")

            if not data:
                return ActionResult(success=False, message="data is required")

            windows = []
            current_window = []
            last_val = None

            for item in data:
                val = item.get(field, 0)
                if current_window and last_val is not None and abs(val - last_val) > gap:
                    windows.append({"session_id": len(windows), "items": current_window, "count": len(current_window)})
                    current_window = []
                current_window.append(item)
                last_val = val

            if current_window:
                windows.append({"session_id": len(windows), "items": current_window, "count": len(current_window)})

            return ActionResult(
                success=True,
                data={"windows": windows, "session_count": len(windows), "gap_threshold": gap},
                message=f"Created {len(windows)} session windows (gap={gap})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window session failed: {e}")


class WindowHoppingAction(BaseAction):
    """Hopping window operation."""
    action_type = "window_hopping"
    display_name = "跳跃窗口"
    description = "跳跃窗口操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            window_size = params.get("window_size", 5)
            hop_size = params.get("hop_size", 3)
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            windows = []
            for i in range(0, len(data), hop_size):
                window_items = data[i : i + window_size]
                if len(window_items) < window_size:
                    break
                values = [item.get(field, 0) for item in window_items]
                windows.append({
                    "window_id": len(windows),
                    "start_idx": i,
                    "end_idx": i + window_size - 1,
                    "items": window_items,
                    "count": len(window_items),
                    "sum": sum(values),
                })

            return ActionResult(
                success=True,
                data={"windows": windows, "window_count": len(windows), "window_size": window_size, "hop_size": hop_size},
                message=f"Created {len(windows)} hopping windows (size={window_size}, hop={hop_size})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Window hopping failed: {e}")
