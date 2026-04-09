"""Stream processing action module for RabAI AutoClick.

Provides:
- StreamProcessingAction: Process data streams
- StreamProcessorAction: Stream processor
- StreamAction: Stream operations
- StreamingAction: Streaming utilities
"""

import time
import json
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import deque
from datetime import datetime
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StreamProcessingAction(BaseAction):
    """Process data streams with windowing."""
    action_type = "stream_processing"
    display_name = "流处理"
    description = "数据流处理"

    def __init__(self):
        super().__init__()
        self._streams: Dict[str, Dict] = {}
        self._buffers: Dict[str, deque] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            stream_name = params.get("stream_name", "")

            if operation == "create":
                if not stream_name:
                    return ActionResult(success=False, message="stream_name required")

                self._streams[stream_name] = {
                    "name": stream_name,
                    "type": params.get("type", "infinite"),
                    "window_size": params.get("window_size", 100),
                    "window_type": params.get("window_type", "tumbling"),
                    "created_at": time.time(),
                    "events_received": 0,
                    "events_processed": 0
                }
                self._buffers[stream_name] = deque(maxlen=params.get("buffer_size", 1000))

                return ActionResult(success=True, data={"stream": stream_name}, message=f"Stream '{stream_name}' created")

            elif operation == "publish":
                if stream_name not in self._streams:
                    return ActionResult(success=False, message=f"Stream '{stream_name}' not found")

                event = {
                    "data": params.get("data", {}),
                    "timestamp": time.time(),
                    "key": params.get("key", "")
                }

                self._buffers[stream_name].append(event)
                self._streams[stream_name]["events_received"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "stream": stream_name,
                        "buffer_size": len(self._buffers[stream_name]),
                        "events_received": self._streams[stream_name]["events_received"]
                    }
                )

            elif operation == "consume":
                if stream_name not in self._buffers:
                    return ActionResult(success=False, message=f"Stream '{stream_name}' not found")

                max_events = params.get("max_events", 10)
                events = []
                for _ in range(min(max_events, len(self._buffers[stream_name]))):
                    if self._buffers[stream_name]:
                        events.append(self._buffers[stream_name].popleft())
                        self._streams[stream_name]["events_processed"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "stream": stream_name,
                        "consumed": len(events),
                        "remaining": len(self._buffers.get(stream_name, []))
                    }
                )

            elif operation == "window":
                if stream_name not in self._buffers:
                    return ActionResult(success=False, message=f"Stream '{stream_name}' not found")

                window_type = params.get("window_type", "tumbling")
                window_size = params.get("window_size", 10)

                events = list(self._buffers[stream_name])
                if window_type == "tumbling":
                    windows = [events[i:i+window_size] for i in range(0, len(events), window_size)]
                elif window_type == "sliding":
                    slide_size = params.get("slide_size", 5)
                    windows = [events[i:i+window_size] for i in range(0, max(1, len(events) - window_size + 1), slide_size)]
                else:
                    windows = [events]

                return ActionResult(
                    success=True,
                    data={
                        "stream": stream_name,
                        "windows": len(windows),
                        "window_type": window_type,
                        "events_in_windows": sum(len(w) for w in windows)
                    }
                )

            elif operation == "aggregate":
                if stream_name not in self._buffers:
                    return ActionResult(success=False, message=f"Stream '{stream_name}' not found")

                events = list(self._buffers[stream_name])
                agg_type = params.get("agg_type", "count")
                field = params.get("field", "")

                if agg_type == "count":
                    result = len(events)
                elif agg_type == "sum" and field:
                    result = sum(e.get("data", {}).get(field, 0) for e in events)
                elif agg_type == "avg" and field:
                    values = [e.get("data", {}).get(field, 0) for e in events]
                    result = sum(values) / len(values) if values else 0
                elif agg_type == "min" and field:
                    values = [e.get("data", {}).get(field, 0) for e in events]
                    result = min(values) if values else 0
                elif agg_type == "max" and field:
                    values = [e.get("data", {}).get(field, 0) for e in events]
                    result = max(values) if values else 0
                else:
                    result = len(events)

                return ActionResult(
                    success=True,
                    data={
                        "stream": stream_name,
                        "aggregation": result,
                        "type": agg_type,
                        "events": len(events)
                    }
                )

            elif operation == "stats":
                if stream_name not in self._streams:
                    return ActionResult(success=False, message=f"Stream '{stream_name}' not found")

                s = self._streams[stream_name]
                return ActionResult(
                    success=True,
                    data={
                        "stream": stream_name,
                        "received": s["events_received"],
                        "processed": s["events_processed"],
                        "buffer_size": len(self._buffers.get(stream_name, []))
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream processing error: {str(e)}")


class StreamProcessorAction(BaseAction):
    """Stream processor with transformations."""
    action_type = "stream_processor"
    display_name = "流处理器"
    description = "流处理器"

    def __init__(self):
        super().__init__()
        self._processors: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "process")
            processor_name = params.get("processor_name", "")

            if operation == "register":
                if not processor_name:
                    return ActionResult(success=False, message="processor_name required")

                self._processors[processor_name] = {
                    "name": processor_name,
                    "transform_type": params.get("transform_type", "passthrough"),
                    "config": params.get("config", {}),
                    "created_at": time.time(),
                    "processed_count": 0
                }
                return ActionResult(success=True, data={"processor": processor_name})

            elif operation == "process":
                if not processor_name:
                    return ActionResult(success=False, message="processor_name required")

                data = params.get("data")
                processor = self._processors.get(processor_name, {})
                transform_type = processor.get("transform_type", "passthrough")

                result = self._transform(data, transform_type, processor.get("config", {}))

                return ActionResult(
                    success=True,
                    data={
                        "processed": result,
                        "processor": processor_name
                    }
                )

            elif operation == "list":
                return ActionResult(success=True, data={"processors": list(self._processors.keys())})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream processor error: {str(e)}")

    def _transform(self, data: Any, transform_type: str, config: Dict) -> Any:
        if transform_type == "passthrough":
            return data
        elif transform_type == "filter":
            condition = config.get("condition", "")
            if isinstance(data, list):
                return [x for x in data if x]
            return data
        elif transform_type == "map":
            field = config.get("field", "")
            func = config.get("func", "upper")
            if isinstance(data, dict):
                if func == "upper" and field in data:
                    data[field] = str(data[field]).upper()
            return data
        elif transform_type == "flatten":
            if isinstance(data, list):
                flat = []
                for item in data:
                    if isinstance(item, (list, dict)):
                        flat.extend(item.values() if isinstance(item, dict) else item)
                    else:
                        flat.append(item)
                return flat
            return data
        elif transform_type == "enrich":
            if isinstance(data, dict):
                data["_enriched"] = True
                data["_processed_at"] = time.time()
            return data
        return data


class StreamAction(BaseAction):
    """Stream operations."""
    action_type = "stream_action"
    display_name = "流操作"
    description = "流操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            stream_name = params.get("stream_name", "")

            if operation == "create":
                if not stream_name:
                    return ActionResult(success=False, message="stream_name required")

                return ActionResult(success=True, data={"stream": stream_name}, message=f"Stream '{stream_name}' created")

            elif operation == "push":
                return ActionResult(success=True, data={"pushed": True})

            elif operation == "pull":
                return ActionResult(success=True, data={"pulled": []})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream error: {str(e)}")


class StreamingAction(BaseAction):
    """Streaming utilities."""
    action_type = "streaming_action"
    display_name = "流式处理"
    description = "流式处理工具"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "stream")

            if operation == "stream":
                data = params.get("data", [])
                chunk_size = params.get("chunk_size", 100)

                chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

                return ActionResult(
                    success=True,
                    data={
                        "chunks": len(chunks),
                        "chunk_size": chunk_size,
                        "total_items": len(data)
                    }
                )

            elif operation == "buffer":
                return ActionResult(success=True, data={"buffered": True})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Streaming error: {str(e)}")
