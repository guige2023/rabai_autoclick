"""Event bus action module for RabAI AutoClick.

Provides pub/sub event bus with topic filtering,
async delivery, and event ordering.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class Event:
    """Represents an event on the bus."""
    
    def __init__(
        self,
        topic: str,
        data: Any,
        event_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.event_id = event_id or f"evt_{int(time.time() * 1000000)}"
        self.topic = topic
        self.data = data
        self.metadata = metadata or {}
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'event_id': self.event_id,
            'topic': self.topic,
            'data': self.data,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }


class EventBusAction(BaseAction):
    """Pub/sub event bus for decoupled event handling.
    
    Supports topic-based routing, async delivery,
    wildcard subscriptions, and event history.
    """
    action_type = "event_bus"
    display_name = "事件总线"
    description = "发布/订阅事件总线"
    
    def __init__(self):
        super().__init__()
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_history: List[Event] = []
        self._max_history = 1000
        self._lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=4)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute event bus operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (publish, subscribe,
                   unsubscribe, get_history), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'publish')
        
        if action == 'publish':
            return self._publish_event(params)
        elif action == 'subscribe':
            return self._subscribe(params)
        elif action == 'unsubscribe':
            return self._unsubscribe(params)
        elif action == 'publish_batch':
            return self._publish_batch(params)
        elif action == 'get_history':
            return self._get_history(params)
        elif action == 'clear_history':
            return self._clear_history(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _publish_event(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Publish an event to the bus."""
        topic = params.get('topic')
        if not topic:
            return ActionResult(success=False, message="topic is required")
        
        data = params.get('data')
        event_id = params.get('event_id')
        metadata = params.get('metadata', {})
        async_delivery = params.get('async', True)
        
        event = Event(
            topic=topic,
            data=data,
            event_id=event_id,
            metadata=metadata
        )
        
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)
        
        handlers = self._get_matching_handlers(topic)
        
        delivered = 0
        failed = []
        
        for handler in handlers:
            try:
                if async_delivery:
                    self._executor.submit(handler, event)
                else:
                    handler(event)
                delivered += 1
            except Exception as e:
                failed.append({'handler': str(handler), 'error': str(e)})
        
        return ActionResult(
            success=delivered > 0,
            message=f"Published to topic '{topic}': {delivered} delivered",
            data={
                'event': event.to_dict(),
                'delivered': delivered,
                'failed': len(failed),
                'failures': failed
            }
        )
    
    def _get_matching_handlers(self, topic: str) -> List[Callable]:
        """Get handlers that match the topic."""
        handlers = []
        
        with self._lock:
            for pattern, pattern_handlers in self._subscribers.items():
                if self._topic_matches(topic, pattern):
                    handlers.extend(pattern_handlers)
        
        return handlers
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches a pattern (supports wildcards)."""
        if pattern == '#':
            return True
        
        if '*' in pattern:
            import re
            regex_pattern = pattern.replace('.', r'\.').replace('*', '[^.]+')
            return bool(re.match(f"^{regex_pattern}$", topic))
        
        return topic == pattern
    
    def _subscribe(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Subscribe to a topic."""
        topic = params.get('topic')
        if not topic:
            return ActionResult(success=False, message="topic is required")
        
        handler = params.get('handler')
        callback_url = params.get('callback_url')
        
        if not handler and not callback_url:
            return ActionResult(
                success=False,
                message="handler or callback_url is required"
            )
        
        def wrapped_handler(event: Event):
            """Wrapped handler for async execution."""
            try:
                if callback_url:
                    import urllib.request
                    data = json.dumps({'event': event.to_dict()}).encode()
                    req = urllib.request.Request(
                        callback_url,
                        data=data,
                        headers={'Content-Type': 'application/json'}
                    )
                    urllib.request.urlopen(req, timeout=5)
                elif handler:
                    handler(event)
            except Exception:
                pass
        
        with self._lock:
            self._subscribers[topic].append(wrapped_handler)
        
        return ActionResult(
            success=True,
            message=f"Subscribed to topic '{topic}'",
            data={
                'topic': topic,
                'subscriber_count': len(self._subscribers[topic])
            }
        )
    
    def _unsubscribe(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unsubscribe from a topic."""
        topic = params.get('topic')
        if not topic:
            return ActionResult(success=False, message="topic is required")
        
        with self._lock:
            if topic in self._subscribers:
                del self._subscribers[topic]
        
        return ActionResult(
            success=True,
            message=f"Unsubscribed from topic '{topic}'"
        )
    
    def _publish_batch(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Publish multiple events."""
        events = params.get('events', [])
        if not events:
            return ActionResult(success=False, message="No events provided")
        
        results = []
        for event_config in events:
            topic = event_config.get('topic')
            data = event_config.get('data')
            
            if topic and data is not None:
                result = self._publish_event({
                    'topic': topic,
                    'data': data,
                    'async': False
                })
                results.append(result.data)
        
        return ActionResult(
            success=True,
            message=f"Published {len(results)} events",
            data={
                'published': len(results),
                'results': results
            }
        )
    
    def _get_history(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get event history."""
        topic = params.get('topic')
        since = params.get('since')
        limit = params.get('limit', 100)
        
        with self._lock:
            events = self._event_history
            
            if topic:
                events = [e for e in events if self._topic_matches(e.topic, topic)]
            if since:
                events = [e for e in events if e.timestamp >= since]
            
            events = events[-limit:]
        
        return ActionResult(
            success=True,
            message=f"Retrieved {len(events)} events from history",
            data={
                'events': [e.to_dict() for e in events],
                'count': len(events)
            }
        )
    
    def _clear_history(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear event history."""
        with self._lock:
            count = len(self._event_history)
            self._event_history.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared {count} events from history"
        )
