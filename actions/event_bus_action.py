"""Event bus action module for RabAI AutoClick.

Provides pub/sub event bus for decoupled communication
between components with support for filtering and async delivery.
"""

import time
import sys
import os
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Event:
    """An event in the event bus."""
    id: str
    topic: str
    data: Any
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None


@dataclass 
class Subscription:
    """Event subscription."""
    id: str
    topic: str
    handler: Callable
    filter_func: Optional[Callable] = None
    async_delivery: bool = False


class EventBusAction(BaseAction):
    """Event bus action for publish/subscribe communication.
    
    Supports topic-based routing, filtering, async delivery,
    and pattern matching subscriptions.
    """
    action_type = "event_bus"
    display_name = "事件总线"
    description = "发布订阅事件总线"
    
    def __init__(self):
        super().__init__()
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._pattern_subscriptions: List[Subscription] = []
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history: int = 1000
    
    def subscribe(
        self,
        topic: str,
        handler: Callable,
        filter_func: Optional[Callable] = None,
        async_delivery: bool = False
    ) -> str:
        """Subscribe to an event topic.
        
        Args:
            topic: Topic pattern to subscribe to (supports * wildcards).
            handler: Callable to invoke when event matches.
            filter_func: Optional filter function (event -> bool).
            async_delivery: If True, deliver asynchronously.
            
        Returns:
            Subscription ID.
        """
        sub_id = str(uuid.uuid4())
        
        with self._lock:
            if '*' in topic or '?' in topic:
                sub = Subscription(
                    id=sub_id,
                    topic=topic,
                    handler=handler,
                    filter_func=filter_func,
                    async_delivery=async_delivery
                )
                self._pattern_subscriptions.append(sub)
            else:
                sub = Subscription(
                    id=sub_id,
                    topic=topic,
                    handler=handler,
                    filter_func=filter_func,
                    async_delivery=async_delivery
                )
                self._subscriptions[topic].append(sub)
        
        return sub_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from a topic.
        
        Args:
            subscription_id: ID returned from subscribe().
            
        Returns:
            True if unsubscribed, False if not found.
        """
        with self._lock:
            for topic, subs in self._subscriptions.items():
                for sub in subs:
                    if sub.id == subscription_id:
                        subs.remove(sub)
                        return True
            
            for sub in self._pattern_subscriptions:
                if sub.id == subscription_id:
                    self._pattern_subscriptions.remove(sub)
                    return True
        
        return False
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute event bus operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: publish|subscribe|unsubscribe|history
                topic: Event topic (for publish/subscribe)
                data: Event data (for publish)
                handler: Handler function (for subscribe).
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'publish')
        
        if operation == 'publish':
            return self._publish(params)
        elif operation == 'subscribe':
            return self._subscribe(params)
        elif operation == 'unsubscribe':
            return self._unsubscribe(params)
        elif operation == 'history':
            return self._history(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _publish(self, params: Dict[str, Any]) -> ActionResult:
        """Publish an event."""
        topic = params.get('topic', '')
        data = params.get('data')
        metadata = params.get('metadata', {})
        source = params.get('source')
        
        if not topic:
            return ActionResult(success=False, message="Topic is required")
        
        event = Event(
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            timestamp=time.time(),
            metadata=metadata,
            source=source
        )
        
        delivered = 0
        errors = []
        
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]
        
            matching_subs = []
            
            if topic in self._subscriptions:
                matching_subs.extend(self._subscriptions[topic])
            
            for sub in self._pattern_subscriptions:
                if self._topic_matches(topic, sub.topic):
                    matching_subs.append(sub)
        
        for sub in matching_subs:
            if sub.filter_func and not sub.filter_func(event):
                continue
            
            try:
                if sub.async_delivery:
                    thread = threading.Thread(target=self._deliver, args=(sub, event))
                    thread.daemon = True
                    thread.start()
                else:
                    self._deliver(sub, event)
                delivered += 1
            except Exception as e:
                errors.append({'subscription_id': sub.id, 'error': str(e)})
        
        return ActionResult(
            success=True,
            message=f"Published to {topic}: {delivered} delivered",
            data={
                'event_id': event.id,
                'topic': topic,
                'delivered': delivered,
                'errors': errors
            }
        )
    
    def _deliver(self, sub: Subscription, event: Event) -> None:
        """Deliver event to subscription handler."""
        try:
            result = sub.handler(event)
            if isinstance(result, ActionResult) and not result.success:
                pass
        except Exception:
            pass
    
    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Subscribe to a topic."""
        topic = params.get('topic', '')
        handler = params.get('handler')
        filter_expr = params.get('filter')
        async_delivery = params.get('async_delivery', False)
        
        if not topic or not handler:
            return ActionResult(success=False, message="Topic and handler are required")
        
        filter_func = None
        if filter_expr:
            filter_func = self._compile_filter(filter_expr)
        
        sub_id = self.subscribe(topic, handler, filter_func, async_delivery)
        
        return ActionResult(
            success=True,
            message=f"Subscribed to {topic}",
            data={'subscription_id': sub_id, 'topic': topic}
        )
    
    def _unsubscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Unsubscribe from a topic."""
        subscription_id = params.get('subscription_id', '')
        
        if not subscription_id:
            return ActionResult(success=False, message="Subscription ID is required")
        
        success = self.unsubscribe(subscription_id)
        
        return ActionResult(
            success=success,
            message=f"{'Unsubscribed' if success else 'Subscription not found'}"
        )
    
    def _history(self, params: Dict[str, Any]) -> ActionResult:
        """Get event history."""
        topic = params.get('topic')
        limit = params.get('limit', 100)
        
        with self._lock:
            events = self._event_history
            
            if topic:
                events = [e for e in events if self._topic_matches(e.topic, topic)]
            
            events = events[-limit:]
        
        return ActionResult(
            success=True,
            message=f"{len(events)} events in history",
            data={
                'events': [
                    {
                        'id': e.id,
                        'topic': e.topic,
                        'data': e.data,
                        'timestamp': e.timestamp,
                        'metadata': e.metadata
                    }
                    for e in events
                ]
            }
        )
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches pattern."""
        import fnmatch
        
        if '*' in pattern or '?' in pattern:
            return fnmatch.fnmatch(topic, pattern)
        
        return topic == pattern
    
    def _compile_filter(self, filter_expr: str) -> Optional[Callable]:
        """Compile filter expression to callable."""
        if filter_expr.startswith('${') and filter_expr.endswith('}'):
            field_path = filter_expr[2:-1]
            
            def filter_by_field(event: Event) -> bool:
                parts = field_path.split('.')
                value = event.data
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        return False
                return bool(value)
            
            return filter_by_field
        
        return None
