"""
Event Bus Utilities for UI Automation.

This module provides a publish-subscribe event bus for communication
between components in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """
    An event in the event bus.
    
    Attributes:
        event_id: Unique event identifier
        event_type: Event type/name
        data: Event payload
        priority: Event priority
        timestamp: When event was published
        source: Event source identifier
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    data: Any = None
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Subscription:
    """
    An event subscription.
    
    Attributes:
        subscription_id: Unique subscription identifier
        event_type: Event type to subscribe to
        handler: Handler function
        priority: Handler priority
        filter_func: Optional filter function
    """
    subscription_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    handler: Callable[[Event], None] = field(repr=False)
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Optional[Callable[[Event], bool]] = None


class EventBus:
    """
    Publish-subscribe event bus.
    
    Example:
        bus = EventBus()
        
        # Subscribe
        sub_id = bus.subscribe("click", lambda e: handle_click(e))
        
        # Publish
        bus.publish(Event(event_type="click", data={"x": 100, "y": 200}))
        
        # Unsubscribe
        bus.unsubscribe(sub_id)
    """
    
    def __init__(self):
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._global_handlers: list[Subscription] = []
        self._lock = threading.RLock()
        self._event_history: list[Event] = []
        self._max_history = 1000
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None
    ) -> str:
        """
        Subscribe to an event type.
        
        Args:
            event_type: Event type to subscribe to
            handler: Handler function
            priority: Handler priority
            filter_func: Optional filter function
            
        Returns:
            Subscription ID for later unsubscription
        """
        subscription = Subscription(
            event_type=event_type,
            handler=handler,
            priority=priority,
            filter_func=filter_func
        )
        
        with self._lock:
            if event_type not in self._subscriptions:
                self._subscriptions[event_type] = []
            
            self._subscriptions[event_type].append(subscription)
            
            # Sort by priority (highest first)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value,
                reverse=True
            )
        
        return subscription.subscription_id
    
    def subscribe_all(
        self,
        handler: Callable[[Event], None],
        priority: EventPriority = EventPriority.NORMAL
    ) -> str:
        """
        Subscribe to all events.
        
        Args:
            handler: Handler function
            priority: Handler priority
            
        Returns:
            Subscription ID
        """
        subscription = Subscription(
            event_type="*",
            handler=handler,
            priority=priority
        )
        
        with self._lock:
            self._global_handlers.append(subscription)
            self._global_handlers.sort(key=lambda s: s.priority.value, reverse=True)
        
        return subscription.subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from an event.
        
        Args:
            subscription_id: Subscription ID to remove
            
        Returns:
            True if unsubscribed, False if not found
        """
        with self._lock:
            # Check specific subscriptions
            for event_type, subs in self._subscriptions.items():
                for i, sub in enumerate(subs):
                    if sub.subscription_id == subscription_id:
                        del subs[i]
                        return True
            
            # Check global handlers
            for i, sub in enumerate(self._global_handlers):
                if sub.subscription_id == subscription_id:
                    del self._global_handlers[i]
                    return True
        
        return False
    
    def publish(self, event: Event) -> int:
        """
        Publish an event to all subscribers.
        
        Args:
            event: Event to publish
            
        Returns:
            Number of handlers that received the event
        """
        self._add_to_history(event)
        
        handlers_called = 0
        
        with self._lock:
            # Get subscriptions for this event type
            subs = self._subscriptions.get(event.event_type, [])
            
            # Call handlers
            for subscription in subs:
                if self._should_handle(subscription, event):
                    try:
                        subscription.handler(event)
                        handlers_called += 1
                    except Exception:
                        pass
            
            # Call global handlers
            for subscription in self._global_handlers:
                try:
                    subscription.handler(event)
                    handlers_called += 1
                except Exception:
                    pass
        
        return handlers_called
    
    def _should_handle(self, subscription: Subscription, event: Event) -> bool:
        """Check if subscription should handle the event."""
        if subscription.filter_func:
            try:
                return subscription.filter_func(event)
            except Exception:
                return False
        return True
    
    def _add_to_history(self, event: Event) -> None:
        """Add event to history."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)
    
    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> list[Event]:
        """
        Get event history.
        
        Args:
            event_type: Optional filter by event type
            limit: Maximum events to return
            
        Returns:
            List of events
        """
        history = self._event_history
        
        if event_type:
            history = [e for e in history if e.event_type == event_type]
        
        return history[-limit:]
    
    def get_subscription_count(self, event_type: Optional[str] = None) -> int:
        """Get number of subscriptions."""
        with self._lock:
            if event_type:
                return len(self._subscriptions.get(event_type, []))
            return sum(len(subs) for subs in self._subscriptions.values()) + len(self._global_handlers)
    
    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()


class EventBusBuilder:
    """
    Builder for creating configured event buses.
    
    Example:
        bus = (EventBusBuilder()
            .with_history_size(500)
            .with_default_handlers()
            .build())
    """
    
    def __init__(self):
        self._max_history = 1000
        self._error_handler: Optional[Callable[[Exception, Event], None]] = None
    
    def with_history_size(self, size: int) -> 'EventBusBuilder':
        """Set maximum history size."""
        self._max_history = size
        return self
    
    def with_error_handler(
        self,
        handler: Callable[[Exception, Event], None]
    ) -> 'EventBusBuilder':
        """Set error handler for subscriber exceptions."""
        self._error_handler = handler
        return self
    
    def build(self) -> EventBus:
        """Build the event bus."""
        return EventBus()
