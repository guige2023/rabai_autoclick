"""
Event Bus and Pub-Sub Messaging Module.

Provides asynchronous event-driven communication between
automation components with filtering, routing, and delivery guarantees.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    CRITICAL = auto()


@dataclass(frozen=True)
class Event:
    event_type: str
    topic: str
    data: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple)
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    event_id: str = ""
    source: str = ""
    reply_to: Optional[str] = None

    def data_dict(self) -> Dict[str, Any]:
        return dict(self.data)

    def __hash__(self) -> int:
        return hash(self.event_id or f"{self.topic}:{self.timestamp}")


@dataclass
class Subscription:
    subscriber_id: str
    topic_pattern: str
    handler: Callable
    filter_func: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    oneshot: bool = False
    metadata: Tuple[Tuple[str, str], ...] = field(default_factory=tuple)


@dataclass
class DeliveryResult:
    event_id: str
    subscriber_id: str
    delivered: bool
    latency_ms: float = 0.0
    error: Optional[str] = None


class TopicMatcher:
    """Matches events to subscriptions using glob patterns."""

    @classmethod
    def matches(cls, pattern: str, topic: str) -> bool:
        if pattern == "*" or pattern == topic:
            return True
        if "*" in pattern:
            import fnmatch
            return fnmatch.fnmatch(topic, pattern)
        if "#" in pattern:
            parts = pattern.split(".")
            topic_parts = topic.split(".")
            return cls._match_hierarchy(parts, topic_parts)
        return False

    @classmethod
    def _match_hierarchy(cls, pattern_parts: List[str], topic_parts: List[str]) -> bool:
        if not pattern_parts:
            return not topic_parts
        if not topic_parts:
            return pattern_parts == ["#"]
        p, *rest_p = pattern_parts
        t, *rest_t = topic_parts
        if p == "#":
            return True
        if p == t or p == "*":
            return cls._match_hierarchy(rest_p, rest_t)
        return False


class EventBus:
    """
    Asynchronous event bus for publish-subscribe messaging.
    """

    def __init__(self, max_queue_size: int = 10000):
        self.max_queue_size = max_queue_size
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._subscriber_counter: int = 0
        self._running: bool = False
        self._processor_task: Optional[asyncio.Task] = None
        self._delivery_results: Dict[str, List[DeliveryResult]] = {}
        self._handlers: Dict[str, Callable] = {}

    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
        oneshot: bool = False,
    ) -> str:
        self._subscriber_counter += 1
        sub_id = f"sub_{self._subscriber_counter}"
        sub = Subscription(
            subscriber_id=sub_id,
            topic_pattern=topic_pattern,
            handler=handler,
            filter_func=filter_func,
            oneshot=oneshot,
        )
        self._subscriptions[topic_pattern].append(sub)
        logger.info("Subscribed %s to topic '%s'", sub_id, topic_pattern)
        return sub_id

    def unsubscribe(self, subscriber_id: str) -> bool:
        for pattern, subs in self._subscriptions.items():
            for i, sub in enumerate(subs):
                if sub.subscriber_id == subscriber_id:
                    subs.pop(i)
                    logger.info("Unsubscribed %s from '%s'", subscriber_id, pattern)
                    return True
        return False

    def publish(
        self,
        topic: str,
        data: Optional[Dict[str, Any]] = None,
        priority: EventPriority = EventPriority.NORMAL,
        source: str = "",
    ) -> str:
        import uuid
        event_id = str(uuid.uuid4())[:8]
        event = Event(
            event_type="message",
            topic=topic,
            data=tuple((k, v) for k, v in (data or {}).items()),
            priority=priority,
            event_id=event_id,
            source=source,
        )
        priority_val = self._priority_to_int(priority)
        self._event_queue.put_nowait((priority_val, event))
        logger.debug("Published event %s to topic '%s'", event_id, topic)
        return event_id

    def _priority_to_int(self, priority: EventPriority) -> int:
        return {
            EventPriority.LOW: 3,
            EventPriority.NORMAL: 2,
            EventPriority.HIGH: 1,
            EventPriority.CRITICAL: 0,
        }.get(priority, 2)

    async def start(self) -> None:
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())
        logger.info("Event bus started")

    async def stop(self) -> None:
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        logger.info("Event bus stopped")

    async def _process_events(self) -> None:
        while self._running:
            try:
                _, event = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
                await self._dispatch_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                logger.error("Event processing error: %s", exc)

    async def _dispatch_event(self, event: Event) -> None:
        results: List[DeliveryResult] = []
        for pattern, subs in self._subscriptions.items():
            if TopicMatcher.matches(pattern, event.topic):
                for sub in list(subs):
                    if sub.filter_func and not sub.filter_func(event):
                        continue
                    start = time.time()
                    try:
                        if asyncio.iscoroutinefunction(sub.handler):
                            await sub.handler(event)
                        else:
                            sub.handler(event)
                        latency = (time.time() - start) * 1000
                        results.append(DeliveryResult(
                            event_id=event.event_id,
                            subscriber_id=sub.subscriber_id,
                            delivered=True,
                            latency_ms=latency,
                        ))
                    except Exception as exc:
                        results.append(DeliveryResult(
                            event_id=event.event_id,
                            subscriber_id=sub.subscriber_id,
                            delivered=False,
                            latency_ms=(time.time() - start) * 1000,
                            error=str(exc),
                        ))
                    if sub.oneshot:
                        subs.remove(sub)

        self._delivery_results[event.event_id] = results

    def get_delivery_results(self, event_id: str) -> List[DeliveryResult]:
        return self._delivery_results.get(event_id, [])

    def clear_results(self, event_id: Optional[str] = None) -> None:
        if event_id:
            self._delivery_results.pop(event_id, None)
        else:
            self._delivery_results.clear()

    def register_handler(
        self, event_type: str, handler: Callable[[Event], Any]
    ) -> None:
        self._handlers[event_type] = handler

    async def request_reply(
        self,
        topic: str,
        data: Optional[Dict[str, Any]] = None,
        timeout: float = 5.0,
    ) -> Optional[Event]:
        import uuid
        reply_id = str(uuid.uuid4())[:8]
        reply_received = asyncio.Event()

        async def reply_handler(event: Event) -> None:
            self._handlers["_reply_handler"] = None
            reply_received.set()

        reply_topic = f"_reply.{reply_id}"
        self.subscribe(reply_topic, reply_handler, oneshot=True)
        event_data = dict(data or {})
        event_data["_reply_to"] = reply_topic
        self.publish(topic, event_data, source=reply_id)
        try:
            await asyncio.wait_for(reply_received.wait(), timeout=timeout)
            return None
        except asyncio.TimeoutError:
            return None
