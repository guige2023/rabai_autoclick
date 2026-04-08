"""Data feeder action module for RabAI AutoClick.

Provides data feeding operations:
- DataFeederAction: Feed data to destinations
- BatchFeederAction: Batch feed data
- StreamFeederAction: Stream data to destinations
- ParallelFeederAction: Feed data in parallel
- ThrottledFeederAction: Throttle data feeding
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataFeederAction(BaseAction):
    """Feed data to destinations."""
    action_type = "data_feeder"
    display_name = "数据推送"
    description = "推送数据到目标"

    def __init__(self):
        super().__init__()
        self._feeders = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "feed")
            feeder_id = params.get("feeder_id", "default")
            destination = params.get("destination", {})
            data = params.get("data", None)
            batch_size = params.get("batch_size", 100)

            if operation == "feed":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                feed_result = {
                    "feeder_id": feeder_id,
                    "destination": destination,
                    "data": data,
                    "fed_at": datetime.now().isoformat(),
                    "size": len(str(data))
                }

                return ActionResult(
                    success=True,
                    data={
                        "feeder_id": feeder_id,
                        "destination": destination,
                        "fed": True,
                        "fed_at": feed_result["fed_at"],
                        "data_size": feed_result["size"]
                    },
                    message=f"Data fed to {destination.get('type', 'unknown')}: {feed_result['size']} bytes"
                )

            elif operation == "create":
                self._feeders[feeder_id] = {
                    "destination": destination,
                    "batch_size": batch_size,
                    "created_at": datetime.now().isoformat(),
                    "feed_count": 0
                }
                return ActionResult(
                    success=True,
                    data={
                        "feeder_id": feeder_id,
                        "destination": destination,
                        "batch_size": batch_size
                    },
                    message=f"Feeder '{feeder_id}' created"
                )

            elif operation == "stats":
                if feeder_id not in self._feeders:
                    return ActionResult(success=False, message=f"Feeder '{feeder_id}' not found")

                stats = self._feeders[feeder_id]
                return ActionResult(
                    success=True,
                    data={
                        "feeder_id": feeder_id,
                        "destination": stats["destination"],
                        "feed_count": stats["feed_count"],
                        "batch_size": stats["batch_size"]
                    },
                    message=f"Feeder '{feeder_id}' stats: {stats['feed_count']} feeds"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data feeder error: {str(e)}")


class BatchFeederAction(BaseAction):
    """Batch feed data."""
    action_type = "batch_feeder"
    display_name = "批量推送"
    description = "批量推送数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            destination = params.get("destination", {})
            batch_size = params.get("batch_size", 10)
            batch_delay_ms = params.get("batch_delay_ms", 0)

            if not items:
                return ActionResult(success=False, message="items is required")

            batches = []
            for i in range(0, len(items), batch_size):
                batch = items[i:i + batch_size]
                batches.append(batch)

            batch_results = []
            for i, batch in enumerate(batches):
                batch_results.append({
                    "batch_index": i,
                    "items_count": len(batch),
                    "fed": True,
                    "fed_at": datetime.now().isoformat()
                })
                if batch_delay_ms > 0 and i < len(batches) - 1:
                    time.sleep(batch_delay_ms / 1000)

            return ActionResult(
                success=True,
                data={
                    "destination": destination,
                    "total_items": len(items),
                    "batch_count": len(batches),
                    "batch_size": batch_size,
                    "batch_results": batch_results
                },
                message=f"Batch feed completed: {len(items)} items in {len(batches)} batches"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Batch feeder error: {str(e)}")


class StreamFeederAction(BaseAction):
    """Stream data to destinations."""
    action_type = "stream_feeder"
    display_name = "流式推送"
    description = "流式推送数据"

    def __init__(self):
        super().__init__()
        self._streams = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "stream")
            stream_id = params.get("stream_id", "default")
            data = params.get("data", None)
            destination = params.get("destination", {})

            if operation == "start":
                self._streams[stream_id] = {
                    "destination": destination,
                    "buffer": deque(maxlen=1000),
                    "started_at": datetime.now().isoformat(),
                    "streamed_count": 0
                }
                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "started": True
                    },
                    message=f"Stream feeder '{stream_id}' started"
                )

            elif operation == "send":
                if stream_id not in self._streams:
                    return ActionResult(success=False, message=f"Stream '{stream_id}' not found")

                stream = self._streams[stream_id]
                stream["buffer"].append(data)
                stream["streamed_count"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "buffered_count": len(stream["buffer"]),
                        "streamed_count": stream["streamed_count"]
                    },
                    message=f"Data sent to stream '{stream_id}'"
                )

            elif operation == "flush":
                if stream_id not in self._streams:
                    return ActionResult(success=False, message=f"Stream '{stream_id}' not found")

                stream = self._streams[stream_id]
                flushed = list(stream["buffer"])
                stream["buffer"].clear()

                return ActionResult(
                    success=True,
                    data={
                        "stream_id": stream_id,
                        "flushed_count": len(flushed),
                        "flushed_data": flushed
                    },
                    message=f"Flushed {len(flushed)} items from stream '{stream_id}'"
                )

            elif operation == "stop":
                if stream_id in self._streams:
                    del self._streams[stream_id]
                return ActionResult(
                    success=True,
                    data={"stream_id": stream_id, "stopped": True},
                    message=f"Stream feeder '{stream_id}' stopped"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Stream feeder error: {str(e)}")


class ParallelFeederAction(BaseAction):
    """Feed data in parallel."""
    action_type = "parallel_feeder"
    display_name = "并行推送"
    description = "并行推送数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            destinations = params.get("destinations", [])
            max_parallel = params.get("max_parallel", 3)

            if not items:
                return ActionResult(success=False, message="items is required")

            if not destinations:
                destinations = [{"type": "default", "id": f"dest_{i}"} for i in range(max_parallel)]

            dest_index = 0
            results = []
            for item in items:
                dest = destinations[dest_index % len(destinations)]
                results.append({
                    "item": item,
                    "destination": dest,
                    "fed": True,
                    "fed_at": datetime.now().isoformat()
                })
                dest_index += 1

            return ActionResult(
                success=True,
                data={
                    "items_count": len(items),
                    "destinations_count": len(destinations),
                    "max_parallel": max_parallel,
                    "results": results
                },
                message=f"Parallel feed: {len(items)} items to {len(destinations)} destinations"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Parallel feeder error: {str(e)}")


class ThrottledFeederAction(BaseAction):
    """Throttle data feeding."""
    action_type = "throttled_feeder"
    display_name": "限速推送"
    description = "限速推送数据"

    def __init__(self):
        super().__init__()
        self._last_feed_times = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            feeder_id = params.get("feeder_id", "default")
            data = params.get("data", None)
            max_rate = params.get("max_rate", 100)
            rate_unit = params.get("rate_unit", "per_second")
            burst_size = params.get("burst_size", 10)

            if data is None:
                return ActionResult(success=False, message="data is required")

            now = time.time()
            if feeder_id not in self._last_feed_times:
                self._last_feed_times[feeder_id] = deque(maxlen=burst_size)

            times = self._last_feed_times[feeder_id]

            if rate_unit == "per_second":
                min_interval = 1.0 / max_rate
            elif rate_unit == "per_minute":
                min_interval = 60.0 / max_rate
            else:
                min_interval = 1.0 / max_rate

            while times and now - times[0] < min_interval:
                if not times:
                    break
                wait_time = min_interval - (now - times[0])
                if wait_time > 0:
                    time.sleep(wait_time)
                now = time.time()
                while times and now - times[0] >= min_interval:
                    times.popleft()

            times.append(now)

            return ActionResult(
                success=True,
                data={
                    "feeder_id": feeder_id,
                    "throttled": True,
                    "max_rate": max_rate,
                    "rate_unit": rate_unit,
                    "fed_at": datetime.now().isoformat()
                },
                message=f"Throttled feed: rate={max_rate}/{rate_unit}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Throttled feeder error: {str(e)}")
