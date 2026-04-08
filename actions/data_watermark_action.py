"""Data watermark action module for RabAI AutoClick.

Provides watermark operations:
- WatermarkSetAction: Set watermark on stream
- WatermarkQueryAction: Query watermark
- WatermarkAdvanceAction: Advance watermark
- WatermarkLagAction: Calculate lag
- WatermarkGCAction: Garbage collect old records
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WatermarkSetAction(BaseAction):
    """Set watermark on a stream."""
    action_type = "watermark_set"
    display_name = "设置水位线"
    description = "设置数据流水位线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            watermark_time = params.get("watermark_time", int(time.time()))
            watermark_type = params.get("type", "event_time")

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "watermarks"):
                context.watermarks = {}
            context.watermarks[stream_id] = {
                "stream_id": stream_id,
                "watermark_time": watermark_time,
                "type": watermark_type,
                "set_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "watermark_time": watermark_time, "type": watermark_type},
                message=f"Watermark set to {watermark_time} for stream {stream_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watermark set failed: {e}")


class WatermarkQueryAction(BaseAction):
    """Query current watermark."""
    action_type = "watermark_query"
    display_name = "查询水位线"
    description = "查询当前水位线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            watermarks = getattr(context, "watermarks", {})
            wm = watermarks.get(stream_id)

            if not wm:
                return ActionResult(success=False, message=f"No watermark for stream {stream_id}")

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "watermark_time": wm["watermark_time"], "type": wm["type"]},
                message=f"Watermark: {wm['watermark_time']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watermark query failed: {e}")


class WatermarkAdvanceAction(BaseAction):
    """Advance watermark to new time."""
    action_type = "watermark_advance"
    display_name = "推进水位线"
    description = "推进水位线时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            new_time = params.get("new_time", int(time.time()))

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            watermarks = getattr(context, "watermarks", {})
            if stream_id not in watermarks:
                return ActionResult(success=False, message=f"No watermark for stream {stream_id}")

            old_time = watermarks[stream_id]["watermark_time"]
            watermarks[stream_id]["watermark_time"] = new_time
            watermarks[stream_id]["advanced_at"] = time.time()

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "old_time": old_time, "new_time": new_time},
                message=f"Watermark advanced: {old_time} -> {new_time}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watermark advance failed: {e}")


class WatermarkLagAction(BaseAction):
    """Calculate watermark lag."""
    action_type = "watermark_lag"
    display_name = "水位线延迟"
    description = "计算水位线延迟"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            current_time = params.get("current_time", int(time.time()))

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            watermarks = getattr(context, "watermarks", {})
            wm = watermarks.get(stream_id)
            if not wm:
                return ActionResult(success=False, message=f"No watermark for stream {stream_id}")

            lag = current_time - wm["watermark_time"]

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "watermark_time": wm["watermark_time"], "current_time": current_time, "lag_s": lag},
                message=f"Lag: {lag}s",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watermark lag failed: {e}")


class WatermarkGCAction(BaseAction):
    """Garbage collect old records below watermark."""
    action_type = "watermark_gc"
    display_name = "水位线清理"
    description = "清理水位线以下的旧记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            watermarks = getattr(context, "watermarks", {})
            streams = getattr(context, "data_streams", {})
            wm = watermarks.get(stream_id)
            stream = streams.get(stream_id, {})

            if not wm:
                return ActionResult(success=False, message=f"No watermark for stream {stream_id}")

            wm_time = wm["watermark_time"]
            buffer = stream.get("buffer", [])
            original_count = len(buffer)
            retained = [r for r in buffer if r.get("timestamp", 0) >= wm_time]

            streams[stream_id]["buffer"] = retained
            removed = original_count - len(retained)

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "removed": removed, "retained": len(retained)},
                message=f"GC: removed {removed} records, retained {len(retained)}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Watermark GC failed: {e}")
