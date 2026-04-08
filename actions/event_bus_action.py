"""Event bus action module for RabAI AutoClick.

Provides event-driven automation with pub/sub messaging,
event filtering, and event handling.
"""

import time
import json
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventBusPublishAction(BaseAction):
    """Publish event to event bus.
    
    Sends events to subscribed handlers.
    """
    action_type = "event_bus_publish"
    display_name = "发布事件"
    description = "发布事件到事件总线"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Publish event.
        
        Args:
            context: Execution context.
            params: Dict with keys: event_type, event_data, topic,
                   timestamp.
        
        Returns:
            ActionResult with publish status.
        """
        event_type = params.get('event_type', 'generic')
        event_data = params.get('event_data', {})
        topic = params.get('topic', 'default')
        timestamp = params.get('timestamp', None)

        if not event_type:
            return ActionResult(success=False, message="event_type is required")

        try:
            event = {
                'type': event_type,
                'data': event_data,
                'topic': topic,
                'timestamp': timestamp or time.time(),
                'published_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }

            event_bus = getattr(context, '_event_bus', None)
            if event_bus is None:
                context._event_bus = defaultdict(list)
                event_bus = context._event_bus

            event_bus[topic].append(event)

            subscribers = event_bus.get(f'_subscribers_{topic}', [])
            for handler in subscribers:
                try:
                    if callable(handler):
                        handler(event)
                except:
                    pass

            return ActionResult(
                success=True,
                message=f"Published event: {event_type} to {topic}",
                data={
                    'event': event,
                    'topic': topic,
                    'subscribers_notified': len(subscribers)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Publish failed: {str(e)}")


class EventBusSubscribeAction(BaseAction):
    """Subscribe to events from event bus.
    
    Registers handler for specific event types.
    """
    action_type = "event_bus_subscribe"
    display_name = "订阅事件"
    description = "订阅事件总线事件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Subscribe to events.
        
        Args:
            context: Execution context.
            params: Dict with keys: topic, event_types, handler,
                   filter_func.
        
        Returns:
            ActionResult with subscription status.
        """
        topic = params.get('topic', 'default')
        event_types = params.get('event_types', [])
        handler = params.get('handler', None)
        filter_func = params.get('filter_func', None)

        if not event_types and not filter_func:
            return ActionResult(success=False, message="event_types or filter_func required")

        try:
            event_bus = getattr(context, '_event_bus', None)
            if event_bus is None:
                context._event_bus = defaultdict(list)
                event_bus = context._event_bus

            sub_key = f'_subscribers_{topic}'
            if sub_key not in event_bus:
                event_bus[sub_key] = []

            handler_info = {
                'types': event_types,
                'filter': filter_func,
                'handler': handler,
                'subscribed_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }

            event_bus[sub_key].append(handler_info)

            return ActionResult(
                success=True,
                message=f"Subscribed to {topic}: {event_types}",
                data={
                    'topic': topic,
                    'event_types': event_types,
                    'subscription_id': len(event_bus[sub_key])
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Subscribe failed: {str(e)}")


class EventBusQueryAction(BaseAction):
    """Query events from event bus.
    
    Retrieves past events matching criteria.
    """
    action_type = "event_bus_query"
    display_name = "查询事件"
    description = "查询事件总线历史"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query events.
        
        Args:
            context: Execution context.
            params: Dict with keys: topic, event_types, since, until,
                   limit, offset.
        
        Returns:
            ActionResult with matching events.
        """
        topic = params.get('topic', 'default')
        event_types = params.get('event_types', [])
        since = params.get('since', None)
        until = params.get('until', None)
        limit = params.get('limit', 100)
        offset = params.get('offset', 0)

        try:
            event_bus = getattr(context, '_event_bus', None)
            if event_bus is None:
                return ActionResult(
                    success=True,
                    message="No events found",
                    data={'events': [], 'total': 0}
                )

            events = event_bus.get(topic, [])

            filtered = []
            for event in events:
                if event_types and event.get('type') not in event_types:
                    continue
                if since and event.get('timestamp', 0) < since:
                    continue
                if until and event.get('timestamp', float('inf')) > until:
                    continue
                filtered.append(event)

            total = len(filtered)
            paginated = filtered[offset:offset + limit]

            return ActionResult(
                success=True,
                message=f"Found {total} events",
                data={
                    'events': paginated,
                    'total': total,
                    'returned': len(paginated),
                    'offset': offset,
                    'limit': limit
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Query failed: {str(e)}")


class EventBusClearAction(BaseAction):
    """Clear events from event bus.
    
    Removes events by topic or criteria.
    """
    action_type = "event_bus_clear"
    display_name = "清除事件"
    description = "清除事件总线事件"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear events.
        
        Args:
            context: Execution context.
            params: Dict with keys: topic, event_types, before.
        
        Returns:
            ActionResult with clear status.
        """
        topic = params.get('topic', None)
        event_types = params.get('event_types', None)
        before = params.get('before', None)

        try:
            event_bus = getattr(context, '_event_bus', None)
            if event_bus is None:
                return ActionResult(success=True, message="Event bus already empty")

            if topic is None:
                cleared = sum(len(events) for events in event_bus.values() if isinstance(events, list))
                event_bus.clear()
                return ActionResult(
                    success=True,
                    message=f"Cleared all events: {cleared}",
                    data={'cleared_count': cleared}
                )

            if topic in event_bus:
                original_count = len(event_bus[topic])
                
                if event_types or before:
                    filtered = []
                    for event in event_bus[topic]:
                        if event_types and event.get('type') in event_types:
                            continue
                        if before and event.get('timestamp', 0) >= before:
                            filtered.append(event)
                    event_bus[topic] = filtered
                    cleared = original_count - len(filtered)
                else:
                    cleared = len(event_bus[topic])
                    del event_bus[topic]
            else:
                cleared = 0

            return ActionResult(
                success=True,
                message=f"Cleared {cleared} events from {topic}",
                data={'cleared_count': cleared, 'topic': topic}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Clear failed: {str(e)}")


class EventBusWaitAction(BaseAction):
    """Wait for specific event.
    
    Blocks until event occurs or timeout.
    """
    action_type = "event_bus_wait"
    display_name = "等待事件"
    description = "等待特定事件发生"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Wait for event.
        
        Args:
            context: Execution context.
            params: Dict with keys: event_type, topic, timeout,
                   poll_interval.
        
        Returns:
            ActionResult with first matching event.
        """
        event_type = params.get('event_type', '')
        topic = params.get('topic', 'default')
        timeout = params.get('timeout', 30)
        poll_interval = params.get('poll_interval', 0.5)

        if not event_type:
            return ActionResult(success=False, message="event_type required")

        try:
            event_bus = getattr(context, '_event_bus', None)
            if event_bus is None:
                context._event_bus = defaultdict(list)

            start_time = time.time()
            initial_count = len(event_bus.get(topic, []))

            while time.time() - start_time < timeout:
                events = event_bus.get(topic, [])
                
                if len(events) > initial_count:
                    for event in events[initial_count:]:
                        if event.get('type') == event_type:
                            return ActionResult(
                                success=True,
                                message=f"Event received: {event_type}",
                                data={'event': event}
                            )

                time.sleep(poll_interval)

            return ActionResult(
                success=False,
                message=f"Timeout waiting for {event_type}",
                data={'timeout': timeout}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Wait failed: {str(e)}")
