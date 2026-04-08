"""Data stream processing action module for RabAI AutoClick.

Provides data stream operations:
- StreamCreateAction: Create a data stream
- StreamWriteAction: Write to stream
- StreamReadAction: Read from stream
- StreamTransformAction: Transform stream data
- StreamWindowAction: Windowed stream processing
- StreamCloseAction: Close a stream
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamCreateAction(BaseAction):
    """Create a data stream."""
    action_type = "stream_create"
    display_name = "创建数据流"
    description = "创建数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            stream_type = params.get("type", " unbounded")
            buffer_size = params.get("buffer_size", 1000)

            if not name:
                return ActionResult(success=False, message="name is required")

            stream_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "data_streams"):
                context.data_streams = {}
            context.data_streams[stream_id] = {
                "stream_id": stream_id,
                "name": name,
                "type": stream_type,
                "buffer_size": buffer_size,
                "status": "active",
                "created_at": time.time(),
                "buffer": [],
            }

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "name": name, "buffer_size": buffer_size},
                message=f"Stream {stream_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream create failed: {e}")


class StreamWriteAction(BaseAction):
    """Write data to stream."""
    action_type = "stream_write"
    display_name = "流写入"
    description = "向数据流写入数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            data = params.get("data", None)
            batch = params.get("batch", False)

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "data_streams") or stream_id not in context.data_streams:
                return ActionResult(success=False, message=f"Stream {stream_id} not found")

            stream = context.data_streams[stream_id]
            records = data if isinstance(data, list) else [data]

            for record in records:
                stream["buffer"].append({"data": record, "timestamp": time.time()})
            while len(stream["buffer"]) > stream["buffer_size"]:
                stream["buffer"].pop(0)

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "written": len(records), "buffer_size": len(stream["buffer"])},
                message=f"Wrote {len(records)} records to stream {stream_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream write failed: {e}")


class StreamReadAction(BaseAction):
    """Read from stream."""
    action_type = "stream_read"
    display_name = "流读取"
    description = "从数据流读取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            max_records = params.get("max_records", 100)
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 5)

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "data_streams") or stream_id not in context.data_streams:
                return ActionResult(success=False, message=f"Stream {stream_id} not found")

            stream = context.data_streams[stream_id]
            records = stream["buffer"][:max_records]

            if not records and blocking:
                time.sleep(min(timeout, 2))
                records = stream["buffer"][:max_records]

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "records": records, "count": len(records)},
                message=f"Read {len(records)} records from stream {stream_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream read failed: {e}")


class StreamTransformAction(BaseAction):
    """Transform stream data."""
    action_type = "stream_transform"
    display_name = "流转换"
    description = "转换流数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            transform_type = params.get("transform_type", "map")
            expression = params.get("expression", "")

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "data_streams") or stream_id not in context.data_streams:
                return ActionResult(success=False, message=f"Stream {stream_id} not found")

            stream = context.data_streams[stream_id]
            records = stream["buffer"]

            transformed = []
            for record in records:
                r = record.copy()
                if transform_type == "map" and expression:
                    r["data"] = f"transformed({r['data']})"
                elif transform_type == "filter":
                    if expression and "filter" in str(r["data"]):
                        transformed.append(r)
                    continue
                elif transform_type == "flatmap":
                    r["data"] = [r["data"], r["data"]]
                transformed.append(r)

            if transform_type != "filter":
                transformed = transformed

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "transform_type": transform_type, "output_count": len(transformed)},
                message=f"Transformed {len(records)} -> {len(transformed)} records",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream transform failed: {e}")


class StreamWindowAction(BaseAction):
    """Windowed stream processing."""
    action_type = "stream_window"
    display_name = "流窗口"
    description = "滑动窗口流处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            window_type = params.get("window_type", "tumbling")
            window_size = params.get("window_size", 60)
            slide_interval = params.get("slide_interval", 30)

            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "data_streams") or stream_id not in context.data_streams:
                return ActionResult(success=False, message=f"Stream {stream_id} not found")

            stream = context.data_streams[stream_id]
            records = stream["buffer"]

            if window_type == "tumbling":
                window_count = max(1, len(records) // max(window_size, 1))
            elif window_type == "sliding":
                window_count = max(1, (len(records) - window_size) // max(slide_interval, 1) + 1)
            else:
                window_count = 1

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "window_type": window_type, "window_count": window_count},
                message=f"{window_count} {window_type} windows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream window failed: {e}")


class StreamCloseAction(BaseAction):
    """Close a data stream."""
    action_type = "stream_close"
    display_name = "关闭数据流"
    description = "关闭数据流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_id = params.get("stream_id", "")
            if not stream_id:
                return ActionResult(success=False, message="stream_id is required")

            if not hasattr(context, "data_streams") or stream_id not in context.data_streams:
                return ActionResult(success=False, message=f"Stream {stream_id} not found")

            stream = context.data_streams[stream_id]
            stream["status"] = "closed"
            record_count = len(stream["buffer"])
            del context.data_streams[stream_id]

            return ActionResult(
                success=True,
                data={"stream_id": stream_id, "records_flushed": record_count},
                message=f"Stream {stream_id} closed, {record_count} records flushed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Stream close failed: {e}")
