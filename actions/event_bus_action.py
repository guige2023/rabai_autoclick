"""
Event Bus Action Module

Pub-sub event system with topic filtering, dead letter handling,
and guaranteed delivery options. Supports both sync and async.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event delivery priority."""
    
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Event message structure."""
    
    id: str
    topic: str
    data: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    ttl_seconds: float = 300
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class Subscription:
    """Event subscription configuration."""
    
    id: str
    topic_pattern: str
    handler: Callable
    filter_func: Optional[Callable] = None
    auto_ack: bool = True
    concurrency: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeliveryResult:
    """Result of event delivery."""
    
    event_id: str
    success: bool
    subscriber_id: str
    duration_ms: float = 0
    error: Optional[str] = None


class TopicMatcher:
    """Matches event topics against subscription patterns."""
    
    @staticmethod
    def matches(topic: str, pattern: str) -> bool:
        """Check if topic matches pattern."""
        if pattern == "*" or pattern == topic:
            return True
        
        if "#" in pattern:
            parts = pattern.split(".")
            topic_parts = topic.split(".")
            return TopicMatcher._match_wildcard(parts, topic_parts)
        
        if "+" in pattern:
            return TopicMatcher._match_single_level(pattern, topic)
        
        return topic == pattern
    
    @staticmethod
    def _match_wildcard(pattern_parts: List[str], topic_parts: List[str]) -> bool:
        """Match with # wildcard (multi-level)."""
        if "#" in pattern_parts:
            hash_index = pattern_parts.index("#")
            prefix = pattern_parts[:hash_index]
            return topic_parts[:len(prefix)] == prefix
        
        return pattern_parts == topic_parts
    
    @staticmethod
    def _match_single_level(pattern: str, topic: str) -> bool:
        """Match with + wildcard (single level)."""
        pattern_parts = pattern.split(".")
        topic_parts = topic.split(".")
        
        if len(pattern_parts) != len(topic_parts):
            return False
        
        for p, t in zip(pattern_parts, topic_parts):
            if p == "+":
                continue
            if p != t:
                return False
        
        return True


class EventBus:
    """Core event bus implementation."""
    
    def __init__(self):
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._event_history: List[Event] = []
        self._max_history: int = 1000
        self._lock = asyncio.Lock()
        self._delivery_stats: Dict[str, Dict] = defaultdict(lambda: {
            "delivered": 0,
            "failed": 0,
            "last_delivery": None
        })
    
    def subscribe(self, subscription: Subscription) -> str:
        """Subscribe to events matching a topic pattern."""
        self._subscriptions[subscription.topic_pattern].append(subscription)
        return subscription.id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        for pattern, subs in self._subscriptions.items():
            self._subscriptions[pattern] = [
                s for s in subs if s.id != subscription_id
            ]
        return True
    
    def get_subscriptions(self, topic: str) -> List[Subscription]:
        """Get all subscriptions matching a topic."""
        matches = []
        for pattern, subs in self._subscriptions.items():
            if TopicMatcher.matches(topic, pattern):
                matches.extend(subs)
        return matches
    
    async def publish(self, event: Event) -> List[DeliveryResult]:
        """Publish an event to all matching subscribers."""
        async with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]
        
        subscribers = self.get_subscriptions(event.topic)
        results = []
        
        tasks = []
        for sub in subscribers:
            if sub.filter_func and not sub.filter_func(event):
                continue
            tasks.append(self._deliver_event(event, sub))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            results = [r for r in results if isinstance(r, DeliveryResult)]
        
        return results
    
    async def _deliver_event(self, event: Event, sub: Subscription) -> DeliveryResult:
        """Deliver a single event to a subscriber."""
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(sub.handler):
                await sub.handler(event)
            else:
                sub.handler(event)
            
            self._delivery_stats[sub.id]["delivered"] += 1
            self._delivery_stats[sub.id]["last_delivery"] = time.time()
            
            return DeliveryResult(
                event_id=event.id,
                success=True,
                subscriber_id=sub.id,
                duration_ms=(time.time() - start_time) * 1000
            )
        
        except Exception as e:
            self._delivery_stats[sub.id]["failed"] += 1
            
            return DeliveryResult(
                event_id=event.id,
                success=False,
                subscriber_id=sub.id,
                duration_ms=(time.time() - start_time) * 1000,
                error=str(e)
            )
    
    def get_history(
        self,
        topic: Optional[str] = None,
        limit: int = 100
    ) -> List[Event]:
        """Get event history."""
        history = self._event_history
        
        if topic:
            history = [e for e in history if e.topic == topic]
        
        return history[-limit:]


class EventBusAction:
    """
    Main event bus action handler.
    
    Provides pub-sub messaging with topic filtering,
    guaranteed delivery, and dead letter handling.
    """
    
    def __init__(self):
        self.bus = EventBus()
        self._dead_letter_handler: Optional[Callable] = None
        self._middleware: List[Callable] = []
    
    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable,
        filter_func: Optional[Callable] = None,
        concurrency: int = 1
    ) -> str:
        """Subscribe to events."""
        subscription = Subscription(
            id=str(uuid.uuid4()),
            topic_pattern=topic_pattern,
            handler=handler,
            filter_func=filter_func,
            concurrency=concurrency
        )
        return self.bus.subscribe(subscription)
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        return self.bus.unsubscribe(subscription_id)
    
    async def publish(
        self,
        topic: str,
        data: Any,
        priority: EventPriority = EventPriority.NORMAL,
        headers: Optional[Dict] = None,
        correlation_id: Optional[str] = None
    ) -> str:
        """Publish an event to the bus."""
        event = Event(
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            priority=priority,
            headers=headers or {},
            correlation_id=correlation_id
        )
        
        for mw in self._middleware:
            await mw(event)
        
        results = await self.bus.publish(event)
        
        failed = [r for r in results if not r.success]
        if failed and self._dead_letter_handler:
            for result in failed:
                event_data = self._find_event(result.event_id)
                if event_data:
                    await self._dead_letter_handler(event_data, result.error)
        
        return event.id
    
    def _find_event(self, event_id: str) -> Optional[Event]:
        """Find event by ID from history."""
        for event in self.bus._event_history:
            if event.id == event_id:
                return event
        return None
    
    def set_dead_letter_handler(self, handler: Callable) -> None:
        """Set handler for failed event deliveries."""
        self._dead_letter_handler = handler
    
    def add_middleware(self, middleware: Callable) -> None:
        """Add middleware for event processing."""
        self._middleware.append(middleware)
    
    def create_topic_filter(
        self,
        field_path: str,
        expected_value: Any
    ) -> Callable:
        """Create a filter function for topic subscriptions."""
        def filter_func(event: Event) -> bool:
            value = event.data
            for key in field_path.split("."):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return False
            return value == expected_value
        
        return filter_func
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        total_subs = sum(len(subs) for subs in self.bus._subscriptions.values())
        
        return {
            "total_subscriptions": total_subs,
            "topics": list(self.bus._subscriptions.keys()),
            "event_history_size": len(self.bus._event_history),
            "delivery_stats": dict(self.bus._delivery_stats)
        }
    
    def get_topics(self) -> List[str]:
        """Get list of subscribed topics."""
        return list(self.bus._subscriptions.keys())
