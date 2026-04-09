"""Automation event bus action module for RabAI AutoClick.

Provides publish/subscribe event handling for decoupling
automation tasks and enabling reactive workflows.
"""

import time
import uuid
import json
from typing import Any, Dict, List, Optional, Union, Callable
from collections import defaultdict
from core.base_action import BaseAction, ActionResult


class EventBusPublishAction(BaseAction):
    """Publish events to a shared event bus.
    
    Events are delivered to all registered subscribers.
    Supports event filtering, priorities, and delivery confirmation.
    """
    action_type = "event_bus_publish"
    display_name = "事件发布"
    description = "向共享事件总线发布事件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Publish event to bus.
        
        Args:
            context: Execution context.
            params: Dict with keys: event_type, payload, topic, id, timestamp.
        
        Returns:
            ActionResult with delivery confirmation.
        """
        event_type = params.get("event_type", "custom")
        payload = params.get("payload")
        topic = params.get("topic", "default")
        event_id = params.get("id") or uuid.uuid4().hex[:12]
        timestamp = params.get("timestamp") or time.time()
        
        if not event_type:
            return ActionResult(success=False, message="event_type is required")
        
        try:
            event = {
                "id": event_id,
                "type": event_type,
                "topic": topic,
                "payload": payload,
                "timestamp": timestamp,
                "published_at": time.time()
            }
            
            bus = getattr(context, "_event_bus", None)
            if bus is None:
                bus = defaultdict(list)
                setattr(context, "_event_bus", bus)
            
            bus[topic].append(event)
            
            subscriber_count = len(getattr(context, "_event_subscribers", {}).get(topic, []))
            
            return ActionResult(
                success=True,
                message=f"Event '{event_type}' published to topic '{topic}'",
                data={
                    "event_id": event_id,
                    "topic": topic,
                    "subscribers_notified": subscriber_count
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Event publish failed: {e}")


class EventBusSubscribeAction(BaseAction):
    """Subscribe to events from the event bus.
    
    Registers handlers for specific event types or topics.
    Handlers are called when matching events are published.
    """
    action_type = "event_bus_subscribe"
    display_name = "事件订阅"
    description = "从事件总线订阅特定类型的事件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Subscribe to events.
        
        Args:
            context: Execution context.
            params: Dict with keys: topic, event_type, handler_id,
                   filter_expr, auto_ack.
        
        Returns:
            ActionResult with subscription confirmation.
        """
        topic = params.get("topic", "default")
        event_type = params.get("event_type", "*")
        handler_id = params.get("handler_id") or uuid.uuid4().hex[:8]
        filter_expr = params.get("filter_expr")
        auto_ack = params.get("auto_ack", True)
        
        try:
            subscribers = getattr(context, "_event_subscribers", None)
            if subscribers is None:
                subscribers = defaultdict(list)
                setattr(context, "_event_subscribers", subscribers)
            
            subscription = {
                "id": handler_id,
                "topic": topic,
                "event_type": event_type,
                "filter_expr": filter_expr,
                "auto_ack": auto_ack,
                "subscribed_at": time.time()
            }
            
            subscribers[topic].append(subscription)
            
            return ActionResult(
                success=True,
                message=f"Subscribed to topic '{topic}' with handler '{handler_id}'",
                data={
                    "handler_id": handler_id,
                    "topic": topic,
                    "event_type": event_type
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Subscribe failed: {e}")


class EventBusConsumeAction(BaseAction):
    """Consume and process events from the event bus.
    
    Retrieves pending events matching subscription criteria
    and optionally acknowledges successful processing.
    """
    action_type = "event_bus_consume"
    display_name = "事件消费"
    description = "消费事件总线中的待处理事件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Consume events from bus.
        
        Args:
            context: Execution context.
            params: Dict with keys: topic, handler_id, max_events,
                   timeout, acknowledge.
        
        Returns:
            ActionResult with consumed events.
        """
        topic = params.get("topic", "default")
        handler_id = params.get("handler_id")
        max_events = params.get("max_events", 10)
        timeout = params.get("timeout", 5.0)
        acknowledge = params.get("acknowledge", True)
        
        start_time = time.time()
        consumed = []
        
        try:
            bus = getattr(context, "_event_bus", {})
            subscribers = getattr(context, "_event_subscribers", {})
            
            topic_events = bus.get(topic, [])
            topic_subscribers = subscribers.get(topic, [])
            
            if not topic_subscribers:
                return ActionResult(
                    success=False,
                    message=f"No subscribers for topic '{topic}'",
                    data={"events": [], "count": 0}
                )
            
            target_handler = None
            if handler_id:
                for sub in topic_subscribers:
                    if sub["id"] == handler_id:
                        target_handler = sub
                        break
            
            for event in topic_events:
                if len(consumed) >= max_events:
                    break
                
                if time.time() - start_time > timeout:
                    break
                
                if target_handler:
                    event_type = event.get("type")
                    sub_type = target_handler.get("event_type")
                    
                    if sub_type != "*" and event_type != sub_type:
                        continue
                    
                    filter_expr = target_handler.get("filter_expr")
                    if filter_expr:
                        try:
                            if not eval(filter_expr, {"event": event}):
                                continue
                        except Exception:
                            pass
                
                consumed.append(event)
                
                if acknowledge:
                    pass
            
            if acknowledge and consumed:
                bus[topic] = [e for e in topic_events if e not in consumed]
            
            return ActionResult(
                success=True,
                message=f"Consumed {len(consumed)} events from topic '{topic}'",
                data={
                    "events": consumed,
                    "count": len(consumed),
                    "topic": topic
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Consume failed: {e}")


class EventBusBridgeAction(BaseAction):
    """Bridge events between different topics or systems.
    
    Forwards events from source topic to destination topics
    with optional transformation and filtering.
    """
    action_type = "event_bus_bridge"
    display_name = "事件桥接"
    description = "在不同主题或系统之间桥接事件"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Bridge events between topics.
        
        Args:
            context: Execution context.
            params: Dict with keys: source_topic, dest_topics, transform_expr,
                   filter_expr, copy.
        
        Returns:
            ActionResult with bridging result.
        """
        source_topic = params.get("source_topic", "default")
        dest_topics = params.get("dest_topics", [])
        transform_expr = params.get("transform_expr")
        filter_expr = params.get("filter_expr")
        copy = params.get("copy", True)
        
        if not dest_topics:
            return ActionResult(success=False, message="At least one destination topic required")
        
        if isinstance(dest_topics, str):
            dest_topics = [dest_topics]
        
        try:
            bus = getattr(context, "_event_bus", {})
            source_events = bus.get(source_topic, [])
            
            if not source_events:
                return ActionResult(
                    success=True,
                    message=f"No events in source topic '{source_topic}'",
                    data={"bridged": 0}
                )
            
            forwarded = 0
            for event in source_events:
                if filter_expr:
                    try:
                        if not eval(filter_expr, {"event": event}):
                            continue
                    except Exception:
                        pass
                
                for dest in dest_topics:
                    transformed = event
                    if transform_expr:
                        try:
                            transformed = eval(transform_expr, {"event": event})
                        except Exception:
                            transformed = event
                    
                    new_event = {
                        **transformed,
                        "bridged_from": source_topic,
                        "bridged_at": time.time()
                    }
                    
                    bus[dest].append(new_event)
                    forwarded += 1
            
            if not copy:
                bus[source_topic] = []
            
            return ActionResult(
                success=True,
                message=f"Bridged {forwarded} events to {len(dest_topics)} topics",
                data={
                    "source": source_topic,
                    "destinations": dest_topics,
                    "bridged": forwarded
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bridge failed: {e}")
