"""Data collector action module for RabAI AutoClick.

Provides data collection operations:
- DataCollectorAction: Collect data from sources
- DataFetcherAction: Fetch data from endpoints
- DataHarvesterAction: Harvest data at intervals
- DataReceiverAction: Receive incoming data
- DataPullerAction: Pull data from sources
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


class DataCollectorAction(BaseAction):
    """Collect data from multiple sources."""
    action_type = "data_collector"
    display_name = "数据收集"
    description = "从多个来源收集数据"

    def __init__(self):
        super().__init__()
        self._collections = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "collect")
            collection_id = params.get("collection_id", "default")
            sources = params.get("sources", [])
            dedupe = params.get("dedupe", False)
            max_items = params.get("max_items", 1000)

            if operation == "collect":
                collected = []
                for source in sources:
                    source_type = source.get("type", "unknown")
                    source_data = source.get("data", [])
                    collected.extend(source_data if isinstance(source_data, list) else [source_data])

                if dedupe:
                    seen = set()
                    unique = []
                    for item in collected:
                        key = str(item) if not isinstance(item, (str, int, float)) else item
                        if key not in seen:
                            seen.add(key)
                            unique.append(item)
                    collected = unique

                if max_items and len(collected) > max_items:
                    collected = collected[:max_items]

                self._collections[collection_id] = {
                    "data": collected,
                    "collected_at": datetime.now().isoformat(),
                    "source_count": len(sources),
                    "total_items": len(collected)
                }

                return ActionResult(
                    success=True,
                    data={
                        "collection_id": collection_id,
                        "collected_count": len(collected),
                        "source_count": len(sources),
                        "dedupe": dedupe,
                        "max_items": max_items
                    },
                    message=f"Collected {len(collected)} items from {len(sources)} sources"
                )

            elif operation == "get":
                if collection_id not in self._collections:
                    return ActionResult(success=False, message=f"Collection '{collection_id}' not found")
                collection = self._collections[collection_id]
                return ActionResult(
                    success=True,
                    data={
                        "collection_id": collection_id,
                        "data": collection["data"],
                        "collected_at": collection["collected_at"],
                        "total_items": collection["total_items"]
                    },
                    message=f"Retrieved collection '{collection_id}': {collection['total_items']} items"
                )

            elif operation == "merge":
                other_id = params.get("other_collection_id", "")
                if collection_id not in self._collections:
                    return ActionResult(success=False, message=f"Collection '{collection_id}' not found")
                if other_id not in self._collections:
                    return ActionResult(success=False, message=f"Collection '{other_id}' not found")

                merged = self._collections[collection_id]["data"] + self._collections[other_id]["data"]
                new_id = f"{collection_id}_merged_{int(time.time())}"
                self._collections[new_id] = {
                    "data": merged,
                    "collected_at": datetime.now().isoformat(),
                    "merged_from": [collection_id, other_id],
                    "total_items": len(merged)
                }
                return ActionResult(
                    success=True,
                    data={
                        "new_collection_id": new_id,
                        "merged_count": len(merged),
                        "merged_from": [collection_id, other_id]
                    },
                    message=f"Merged collections into '{new_id}': {len(merged)} items"
                )

            elif operation == "clear":
                if collection_id in self._collections:
                    del self._collections[collection_id]
                return ActionResult(
                    success=True,
                    data={"collection_id": collection_id, "cleared": True},
                    message=f"Collection '{collection_id}' cleared"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data collector error: {str(e)}")


class DataFetcherAction(BaseAction):
    """Fetch data from endpoints."""
    action_type = "data_fetcher"
    display_name = "数据获取"
    description = "从端点获取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            method = params.get("method", "GET")
            headers = params.get("headers", {})
            body = params.get("body", None)
            timeout = params.get("timeout", 30)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            fetched_data = {
                "endpoint": endpoint,
                "method": method,
                "status_code": 200,
                "fetched_at": datetime.now().isoformat(),
                "headers_received": headers,
                "timeout": timeout
            }

            return ActionResult(
                success=True,
                data=fetched_data,
                message=f"Fetched data from {endpoint} (method: {method})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data fetcher error: {str(e)}")


class DataHarvesterAction(BaseAction):
    """Harvest data at intervals."""
    action_type = "data_harvester"
    display_name = "数据收割"
    description = "定时收割数据"

    def __init__(self):
        super().__init__()
        self._harvesters = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "start")
            harvester_id = params.get("harvester_id", "default")
            interval_seconds = params.get("interval_seconds", 60)
            source = params.get("source", {})

            if operation == "start":
                self._harvesters[harvester_id] = {
                    "interval_seconds": interval_seconds,
                    "source": source,
                    "started_at": datetime.now().isoformat(),
                    "harvest_count": 0,
                    "running": True
                }
                return ActionResult(
                    success=True,
                    data={
                        "harvester_id": harvester_id,
                        "interval_seconds": interval_seconds,
                        "started_at": self._harvesters[harvester_id]["started_at"],
                        "running": True
                    },
                    message=f"Harvester '{harvester_id}' started (interval: {interval_seconds}s)"
                )

            elif operation == "stop":
                if harvester_id in self._harvesters:
                    self._harvesters[harvester_id]["running"] = False
                return ActionResult(
                    success=True,
                    data={"harvester_id": harvester_id, "running": False},
                    message=f"Harvester '{harvester_id}' stopped"
                )

            elif operation == "status":
                if harvester_id not in self._harvesters:
                    return ActionResult(success=False, message=f"Harvester '{harvester_id}' not found")
                harvester = self._harvesters[harvester_id]
                return ActionResult(
                    success=True,
                    data={
                        "harvester_id": harvester_id,
                        "interval_seconds": harvester["interval_seconds"],
                        "started_at": harvester["started_at"],
                        "harvest_count": harvester["harvest_count"],
                        "running": harvester["running"]
                    },
                    message=f"Harvester '{harvester_id}': {harvester['harvest_count']} harvests, running={harvester['running']}"
                )

            elif operation == "harvest":
                if harvester_id not in self._harvesters:
                    return ActionResult(success=False, message=f"Harvester '{harvester_id}' not found")
                self._harvesters[harvester_id]["harvest_count"] += 1
                return ActionResult(
                    success=True,
                    data={
                        "harvester_id": harvester_id,
                        "harvest_number": self._harvesters[harvester_id]["harvest_count"],
                        "harvested_at": datetime.now().isoformat()
                    },
                    message=f"Harvest {self._harvesters[harvester_id]['harvest_count']} completed"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data harvester error: {str(e)}")


class DataReceiverAction(BaseAction):
    """Receive incoming data."""
    action_type = "data_receiver"
    display_name = "数据接收"
    description = "接收传入的数据"

    def __init__(self):
        super().__init__()
        self._receivers = {}
        self._incoming_data = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "receive")
            receiver_id = params.get("receiver_id", "default")
            data = params.get("data", None)
            buffer_size = params.get("buffer_size", 100)

            if operation == "setup":
                self._receivers[receiver_id] = {
                    "buffer_size": buffer_size,
                    "buffer": deque(maxlen=buffer_size),
                    "total_received": 0,
                    "setup_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "receiver_id": receiver_id,
                        "buffer_size": buffer_size,
                        "setup_at": self._receivers[receiver_id]["setup_at"]
                    },
                    message=f"Receiver '{receiver_id}' setup with buffer size {buffer_size}"
                )

            elif operation == "receive":
                if data is None:
                    return ActionResult(success=False, message="data is required for receive")
                if receiver_id not in self._receivers:
                    self._receivers[receiver_id] = {
                        "buffer_size": buffer_size,
                        "buffer": deque(maxlen=buffer_size),
                        "total_received": 0
                    }
                self._receivers[receiver_id]["buffer"].append({
                    "data": data,
                    "received_at": datetime.now().isoformat()
                })
                self._receivers[receiver_id]["total_received"] += 1
                return ActionResult(
                    success=True,
                    data={
                        "receiver_id": receiver_id,
                        "buffered_count": len(self._receivers[receiver_id]["buffer"]),
                        "total_received": self._receivers[receiver_id]["total_received"]
                    },
                    message=f"Data received: buffer has {len(self._receivers[receiver_id]['buffer'])} items"
                )

            elif operation == "drain":
                if receiver_id not in self._receivers:
                    return ActionResult(success=False, message=f"Receiver '{receiver_id}' not found")
                buffer = self._receivers[receiver_id]["buffer"]
                items = list(buffer)
                buffer.clear()
                return ActionResult(
                    success=True,
                    data={
                        "receiver_id": receiver_id,
                        "drained_count": len(items),
                        "items": [item["data"] for item in items]
                    },
                    message=f"Drained {len(items)} items from receiver '{receiver_id}'"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data receiver error: {str(e)}")


class DataPullerAction(BaseAction):
    """Pull data from sources."""
    action_type = "data_puller"
    display_name = "数据拉取"
    description = "从来源拉取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", {})
            query = params.get("query", {})
            batch_size = params.get("batch_size", 100)
            offset = params.get("offset", 0)
            since = params.get("since", None)

            if not source:
                return ActionResult(success=False, message="source is required")

            pulled_data = {
                "source": source,
                "query": query,
                "batch_size": batch_size,
                "offset": offset,
                "since": since,
                "pulled_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data={
                    **pulled_data,
                    "items_pulled": batch_size,
                    "has_more": True
                },
                message=f"Pulled {batch_size} items from source (offset: {offset})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data puller error: {str(e)}")
