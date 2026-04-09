"""Event gateway action module for RabAI AutoClick.

Provides event gateway operations:
- EventPublisher: Publish events to event bus
- EventSubscriber: Subscribe to event streams
- EventRouter: Route events based on rules
- EventAggregator: Aggregate events over windows
- EventReplay: Replay historical events
"""

from __future__ import annotations

import json
import sys
import os
import time
import uuid
from typing import Any, Dict, List, Optional
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventPublisherAction(BaseAction):
    """Publish events to event bus."""
    action_type = "event_publisher"
    display_name = "事件发布"
    description = "向事件总线发布事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bus_path = params.get("bus_path", "/tmp/event_bus")
            topic = params.get("topic", "default")
            event_type = params.get("event_type", "")
            event_data = params.get("event_data", {})
            event_id = params.get("event_id", str(uuid.uuid4())[:12])

            os.makedirs(bus_path, exist_ok=True)
            topic_dir = os.path.join(bus_path, topic)
            os.makedirs(topic_dir, exist_ok=True)

            event = {
                "event_id": event_id,
                "event_type": event_type,
                "data": event_data,
                "timestamp": datetime.now().isoformat(),
                "topic": topic,
            }

            event_file = os.path.join(topic_dir, f"{event_id}.json")
            with open(event_file, "w") as f:
                json.dump(event, f, indent=2)

            index_file = os.path.join(topic_dir, "_index.json")
            index = []
            if os.path.exists(index_file):
                with open(index_file) as f:
                    index = json.load(f)
            index.append({"event_id": event_id, "timestamp": event["timestamp"]})
            with open(index_file, "w") as f:
                json.dump(index, f)

            return ActionResult(
                success=True,
                message=f"Published event: {event_type} to {topic}",
                data={"event_id": event_id, "topic": topic, "timestamp": event["timestamp"]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class EventSubscriberAction(BaseAction):
    """Subscribe to event streams."""
    action_type = "event_subscriber"
    display_name = "事件订阅"
    description = "订阅事件流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bus_path = params.get("bus_path", "/tmp/event_bus")
            topic = params.get("topic", "default")
            filter_type = params.get("filter_type", None)
            since = params.get("since", None)
            limit = params.get("limit", 100)

            topic_dir = os.path.join(bus_path, topic)
            if not os.path.exists(topic_dir):
                return ActionResult(success=True, message="No events", data={"events": []})

            index_file = os.path.join(topic_dir, "_index.json")
            if not os.path.exists(index_file):
                return ActionResult(success=True, message="No events", data={"events": []})

            with open(index_file) as f:
                index = json.load(f)

            if since:
                since_dt = datetime.fromisoformat(since) if isinstance(since, str) else since
                index = [e for e in index if datetime.fromisoformat(e["timestamp"]) >= since_dt]

            events = []
            for entry in index[-limit:]:
                event_file = os.path.join(topic_dir, f"{entry['event_id']}.json")
                if os.path.exists(event_file):
                    with open(event_file) as f:
                        event = json.load(f)
                    if filter_type and event.get("event_type") != filter_type:
                        continue
                    events.append(event)

            return ActionResult(success=True, message=f"Subscribed: {len(events)} events", data={"events": events, "count": len(events), "topic": topic})

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class EventRouterAction(BaseAction):
    """Route events based on rules."""
    action_type = "event_router"
    display_name = "事件路由"
    description = "基于规则路由事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bus_path = params.get("bus_path", "/tmp/event_bus")
            rules = params.get("rules", [])
            event = params.get("event", {})

            if not rules or not event:
                return ActionResult(success=False, message="rules and event required")

            matched_routes = []
            for rule in rules:
                rule_name = rule.get("name", "")
                match_type = rule.get("match_type", "event_type")
                match_value = rule.get("match_value", "")
                destination = rule.get("destination", "")

                if match_type == "event_type" and event.get("event_type") == match_value:
                    matched_routes.append({"rule": rule_name, "destination": destination})
                elif match_type == "field":
                    field_path = rule.get("field", "")
                    field_value = event.get("data", {}).get(field_path)
                    if field_value == match_value:
                        matched_routes.append({"rule": rule_name, "destination": destination})

            routed_events = []
            for route in matched_routes:
                topic_dir = os.path.join(bus_path, route["destination"])
                os.makedirs(topic_dir, exist_ok=True)

                event_id = str(uuid.uuid4())[:12]
                routed_event = {**event, "event_id": event_id, "routed_by": route["rule"], "routed_at": datetime.now().isoformat()}

                event_file = os.path.join(topic_dir, f"{event_id}.json")
                with open(event_file, "w") as f:
                    json.dump(routed_event, f, indent=2)
                routed_events.append(routed_event)

            return ActionResult(
                success=True,
                message=f"Routed to {len(routed_events)} destinations",
                data={"routed_events": routed_events, "routes": matched_routes, "count": len(routed_events)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class EventAggregatorAction(BaseAction):
    """Aggregate events over time windows."""
    action_type = "event_aggregator"
    display_name = "事件聚合"
    description = "按时间窗口聚合事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            events = params.get("events", [])
            window_seconds = params.get("window_seconds", 60)
            aggregation = params.get("aggregation", "count")
            group_by = params.get("group_by", None)

            if not events:
                return ActionResult(success=False, message="events required")

            windows = defaultdict(list)
            for event in events:
                ts = datetime.fromisoformat(event.get("timestamp", datetime.now().isoformat()))
                window_key = int(ts.timestamp() / window_seconds) * window_seconds
                windows[window_key].append(event)

            results = []
            for window_ts, window_events in sorted(windows.items()):
                window_start = datetime.fromtimestamp(window_ts).isoformat()
                window_end = datetime.fromtimestamp(window_ts + window_seconds).isoformat()

                if aggregation == "count":
                    result = {"window_start": window_start, "window_end": window_end, "count": len(window_events)}
                elif aggregation == "sum":
                    field = params.get("field", "value")
                    result = {"window_start": window_start, "window_end": window_end, "sum": sum(e.get("data", {}).get(field, 0) for e in window_events)}
                elif aggregation == "distinct":
                    field = params.get("field", "event_type")
                    result = {"window_start": window_start, "window_end": window_end, "distinct": len(set(e.get("data", {}).get(field) for e in window_events))}
                else:
                    result = {"window_start": window_start, "window_end": window_end, "count": len(window_events)}

                results.append(result)

            return ActionResult(
                success=True,
                message=f"Aggregated into {len(results)} windows",
                data={"windows": results, "count": len(results), "aggregation": aggregation}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
