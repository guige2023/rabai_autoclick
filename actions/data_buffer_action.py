"""Data buffer action module for RabAI AutoClick.

Provides buffering operations:
- DataBufferAction: Buffer data for batch processing
- BufferFlushAction: Flush buffer contents
- BufferManagerAction: Manage multiple buffers
- RingBufferAction: Ring buffer implementation
- SlidingWindowAction: Sliding window buffer
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataBufferAction(BaseAction):
    """Buffer data for batch processing."""
    action_type = "data_buffer"
    display_name = "数据缓冲"
    description = "缓冲数据用于批处理"

    def __init__(self):
        super().__init__()
        self._buffers = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            buffer_name = params.get("buffer_name", "default")
            data = params.get("data", None)
            max_size = params.get("max_size", 1000)
            flush_threshold = params.get("flush_threshold", 100)
            ttl_seconds = params.get("ttl_seconds", 0)

            if operation == "add":
                return self._add_to_buffer(buffer_name, data, max_size, ttl_seconds)
            elif operation == "flush":
                return self._flush_buffer(buffer_name)
            elif operation == "get":
                return self._get_buffer(buffer_name)
            elif operation == "clear":
                return self._clear_buffer(buffer_name)
            elif operation == "stats":
                return self._get_buffer_stats(buffer_name)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data buffer error: {str(e)}")

    def _add_to_buffer(self, name: str, data: Any, max_size: int, ttl: int) -> ActionResult:
        if name not in self._buffers:
            self._buffers[name] = {
                "data": [],
                "max_size": max_size,
                "ttl_seconds": ttl,
                "created_at": time.time(),
                "item_count": 0
            }

        buffer = self._buffers[name]
        current_time = time.time()

        if ttl > 0 and (current_time - buffer["created_at"]) > ttl:
            buffer["data"] = []
            buffer["created_at"] = current_time

        if len(buffer["data"]) >= max_size:
            buffer["data"].pop(0)

        buffer["data"].append({
            "value": data,
            "timestamp": current_time
        })
        buffer["item_count"] += 1

        return ActionResult(
            success=True,
            data={
                "buffer_name": name,
                "current_size": len(buffer["data"]),
                "max_size": max_size,
                "total_items_added": buffer["item_count"],
                "flush_recommended": len(buffer["data"]) >= max_size * 0.8
            },
            message=f"Added to buffer '{name}': {len(buffer['data'])}/{max_size} items"
        )

    def _flush_buffer(self, name: str) -> ActionResult:
        if name not in self._buffers or not self._buffers[name]["data"]:
            return ActionResult(
                success=True,
                data={"buffer_name": name, "flushed": 0},
                message=f"Buffer '{name}' is empty"
            )

        buffer = self._buffers[name]
        flushed_data = buffer["data"].copy()
        buffer["data"] = []

        return ActionResult(
            success=True,
            data={
                "buffer_name": name,
                "flushed_count": len(flushed_data),
                "flushed_items": flushed_data
            },
            message=f"Flushed {len(flushed_data)} items from buffer '{name}'"
        )

    def _get_buffer(self, name: str) -> ActionResult:
        if name not in self._buffers:
            return ActionResult(
                success=True,
                data={"buffer_name": name, "exists": False, "items": []},
                message=f"Buffer '{name}' does not exist"
            )

        buffer = self._buffers[name]
        return ActionResult(
            success=True,
            data={
                "buffer_name": name,
                "exists": True,
                "current_size": len(buffer["data"]),
                "max_size": buffer["max_size"],
                "items": [item["value"] for item in buffer["data"]]
            },
            message=f"Retrieved buffer '{name}': {len(buffer['data'])} items"
        )

    def _clear_buffer(self, name: str) -> ActionResult:
        if name in self._buffers:
            self._buffers[name]["data"] = []
        return ActionResult(
            success=True,
            data={"buffer_name": name, "cleared": True},
            message=f"Buffer '{name}' cleared"
        )

    def _get_buffer_stats(self, name: str) -> ActionResult:
        if name not in self._buffers:
            return ActionResult(success=False, message=f"Buffer '{name}' does not exist")

        buffer = self._buffers[name]
        current_time = time.time()
        age = current_time - buffer["created_at"]

        return ActionResult(
            success=True,
            data={
                "buffer_name": name,
                "current_size": len(buffer["data"]),
                "max_size": buffer["max_size"],
                "total_items_added": buffer["item_count"],
                "utilization": round(len(buffer["data"]) / buffer["max_size"] * 100, 1),
                "age_seconds": round(age, 1),
                "ttl_seconds": buffer["ttl_seconds"]
            },
            message=f"Buffer '{name}' stats: {len(buffer['data'])}/{buffer['max_size']} ({age:.1f}s old)"
        )


class BufferFlushAction(BaseAction):
    """Flush buffer contents to destination."""
    action_type = "buffer_flush"
    display_name = "缓冲刷新"
    description = "刷新缓冲内容到目标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            buffer_name = params.get("buffer_name", "default")
            destination = params.get("destination", {})
            flush_mode = params.get("flush_mode", "all")
            batch_size = params.get("batch_size", 100)

            return ActionResult(
                success=True,
                data={
                    "buffer_name": buffer_name,
                    "destination": destination,
                    "flush_mode": flush_mode,
                    "batch_size": batch_size,
                    "flushed_at": datetime.now().isoformat()
                },
                message=f"Buffer flush configured: mode={flush_mode}, batch_size={batch_size}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Buffer flush error: {str(e)}")


class BufferManagerAction(BaseAction):
    """Manage multiple buffers."""
    action_type = "buffer_manager"
    display_name = "缓冲管理器"
    description = "管理多个缓冲区"

    def __init__(self):
        super().__init__()
        self._managers = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            manager_id = params.get("manager_id", "default")
            buffer_configs = params.get("buffer_configs", [])

            if operation == "create":
                self._managers[manager_id] = {
                    "buffers": {cfg["name"]: cfg for cfg in buffer_configs},
                    "created_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "manager_id": manager_id,
                        "buffer_count": len(buffer_configs),
                        "buffers": [cfg["name"] for cfg in buffer_configs]
                    },
                    message=f"Buffer manager '{manager_id}' created with {len(buffer_configs)} buffers"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "managers": list(self._managers.keys()),
                        "manager_count": len(self._managers)
                    },
                    message=f"Buffer managers: {list(self._managers.keys())}"
                )

            elif operation == "status":
                if manager_id not in self._managers:
                    return ActionResult(success=False, message=f"Manager '{manager_id}' not found")
                manager = self._managers[manager_id]
                return ActionResult(
                    success=True,
                    data={
                        "manager_id": manager_id,
                        "buffers": manager["buffers"],
                        "created_at": manager["created_at"]
                    },
                    message=f"Manager '{manager_id}' status retrieved"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Buffer manager error: {str(e)}")


class RingBufferAction(BaseAction):
    """Ring buffer implementation."""
    action_type = "ring_buffer"
    display_name = "环形缓冲"
    description = "环形缓冲区实现"

    def __init__(self):
        super().__init__()
        self._ring_buffers = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "push")
            buffer_id = params.get("buffer_id", "default")
            capacity = params.get("capacity", 100)
            item = params.get("item", None)

            if operation == "create":
                self._ring_buffers[buffer_id] = {
                    "capacity": capacity,
                    "buffer": [None] * capacity,
                    "head": 0,
                    "tail": 0,
                    "size": 0
                }
                return ActionResult(
                    success=True,
                    data={"buffer_id": buffer_id, "capacity": capacity},
                    message=f"Ring buffer '{buffer_id}' created with capacity {capacity}"
                )

            elif operation == "push":
                if buffer_id not in self._ring_buffers:
                    return ActionResult(success=False, message=f"Buffer '{buffer_id}' not found")
                rb = self._ring_buffers[buffer_id]
                if item is None:
                    return ActionResult(success=False, message="item is required for push")
                overwritten = None
                if rb["size"] == rb["capacity"]:
                    overwritten = rb["buffer"][rb["tail"]]
                    rb["tail"] = (rb["tail"] + 1) % rb["capacity"]
                else:
                    rb["size"] += 1
                rb["buffer"][rb["head"]] = item
                rb["head"] = (rb["head"] + 1) % rb["capacity"]
                return ActionResult(
                    success=True,
                    data={
                        "buffer_id": buffer_id,
                        "overwritten": overwritten,
                        "current_size": rb["size"]
                    },
                    message=f"Pushed to ring buffer: size={rb['size']}/{rb['capacity']}"
                )

            elif operation == "pop":
                if buffer_id not in self._ring_buffers:
                    return ActionResult(success=False, message=f"Buffer '{buffer_id}' not found")
                rb = self._ring_buffers[buffer_id]
                if rb["size"] == 0:
                    return ActionResult(success=False, message="Buffer is empty")
                item = rb["buffer"][rb["tail"]]
                rb["buffer"][rb["tail"]] = None
                rb["tail"] = (rb["tail"] + 1) % rb["capacity"]
                rb["size"] -= 1
                return ActionResult(
                    success=True,
                    data={"buffer_id": buffer_id, "item": item, "current_size": rb["size"]},
                    message=f"Popped from ring buffer: size={rb['size']}/{rb['capacity']}"
                )

            elif operation == "peek":
                if buffer_id not in self._ring_buffers:
                    return ActionResult(success=False, message=f"Buffer '{buffer_id}' not found")
                rb = self._ring_buffers[buffer_id]
                if rb["size"] == 0:
                    return ActionResult(success=True, data={"buffer_id": buffer_id, "item": None, "empty": True})
                item = rb["buffer"][rb["tail"]]
                return ActionResult(
                    success=True,
                    data={"buffer_id": buffer_id, "item": item, "current_size": rb["size"]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Ring buffer error: {str(e)}")


class SlidingWindowAction(BaseAction):
    """Sliding window buffer implementation."""
    action_type = "sliding_window"
    display_name = "滑动窗口"
    description = "滑动窗口缓冲区"

    def __init__(self):
        super().__init__()
        self._windows = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            window_id = params.get("window_id", "default")
            window_size = params.get("window_size", 10)
            item = params.get("item", None)
            timestamp = params.get("timestamp", time.time())

            if operation == "create":
                self._windows[window_id] = {
                    "size": window_size,
                    "items": deque(maxlen=window_size),
                    "timestamps": deque(maxlen=window_size)
                }
                return ActionResult(
                    success=True,
                    data={"window_id": window_id, "window_size": window_size},
                    message=f"Sliding window '{window_id}' created with size {window_size}"
                )

            elif operation == "add":
                if window_id not in self._windows:
                    return ActionResult(success=False, message=f"Window '{window_id}' not found")
                window = self._windows[window_id]
                window["items"].append(item)
                window["timestamps"].append(timestamp)
                return ActionResult(
                    success=True,
                    data={
                        "window_id": window_id,
                        "current_size": len(window["items"]),
                        "window_size": window["size"],
                        "oldest": window["timestamps"][0] if window["timestamps"] else None,
                        "newest": timestamp
                    },
                    message=f"Added to sliding window: {len(window['items'])}/{window['size']}"
                )

            elif operation == "get":
                if window_id not in self._windows:
                    return ActionResult(success=False, message=f"Window '{window_id}' not found")
                window = self._windows[window_id]
                return ActionResult(
                    success=True,
                    data={
                        "window_id": window_id,
                        "items": list(window["items"]),
                        "timestamps": list(window["timestamps"]),
                        "current_size": len(window["items"])
                    },
                    message=f"Retrieved sliding window: {len(window['items'])} items"
                )

            elif operation == "aggregate":
                if window_id not in self._windows:
                    return ActionResult(success=False, message=f"Window '{window_id}' not found")
                window = self._windows[window_id]
                items = list(window["items"])
                if all(isinstance(x, (int, float)) for x in items):
                    return ActionResult(
                        success=True,
                        data={
                            "window_id": window_id,
                            "sum": sum(items),
                            "avg": sum(items) / len(items) if items else 0,
                            "min": min(items) if items else None,
                            "max": max(items) if items else None,
                            "count": len(items)
                        },
                        message=f"Sliding window aggregate: sum={sum(items)}, avg={sum(items)/len(items):.2f}"
                    )
                return ActionResult(
                    success=True,
                    data={"window_id": window_id, "count": len(items)},
                    message=f"Sliding window: {len(items)} items"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sliding window error: {str(e)}")
