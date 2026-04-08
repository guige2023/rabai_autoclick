# Copyright (c) 2024. coded by claude
"""API Observer Pattern Action Module.

Implements observer pattern for API event notification system
with support for async observers and event filtering.
"""
from typing import Optional, Dict, Any, List, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)


class EventType(Enum):
    REQUEST_SENT = "request_sent"
    REQUEST_RECEIVED = "request_received"
    RESPONSE_SENT = "response_sent"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"
    AUTH_FAILURE = "auth_failure"


@dataclass
class APIEvent:
    event_id: str
    event_type: EventType
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None


class Observer:
    def __init__(self, name: str, event_types: Optional[Set[EventType]] = None):
        self.name = name
        self.event_types = event_types or set(EventType)
        self.observer_id = str(uuid.uuid4())

    async def on_event(self, event: APIEvent) -> None:
        pass


class EventBus:
    def __init__(self):
        self._observers: Dict[str, Observer] = {}
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    def subscribe(self, observer: Observer) -> str:
        self._observers[observer.observer_id] = observer
        logger.info(f"Observer '{observer.name}' subscribed")
        return observer.observer_id

    def unsubscribe(self, observer_id: str) -> bool:
        if observer_id in self._observers:
            del self._observers[observer_id]
            return True
        return False

    async def publish(self, event: APIEvent) -> None:
        await self._event_queue.put(event)

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())

    async def stop(self) -> None:
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass

    async def _process_events(self) -> None:
        while self._running:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
                await self._dispatch_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing event: {e}")

    async def _dispatch_event(self, event: APIEvent) -> None:
        tasks = []
        for observer in self._observers.values():
            if event.event_type in observer.event_types:
                tasks.append(self._safe_notify(observer, event))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_notify(self, observer: Observer, event: APIEvent) -> None:
        try:
            await observer.on_event(event)
        except Exception as e:
            logger.error(f"Observer '{observer.name}' failed to handle event: {e}")
