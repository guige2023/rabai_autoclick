"""Message queue utilities: pub/sub, queues, message handling with acknowledgment."""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Message",
    "MessageQueue",
    "PubSub",
    "MessageConsumer",
]


@dataclass
class Message:
    """A message in the queue."""

    id: str
    topic: str
    payload: Any
    timestamp: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    delivery_count: int = 0
    acknowledged: bool = False

    @classmethod
    def create(cls, topic: str, payload: Any, **kwargs: Any) -> "Message":
        return cls(id=uuid.uuid4().hex, topic=topic, payload=payload, **kwargs)


class MessageQueue:
    """Thread-safe in-memory message queue with FIFO processing."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._queue: list[Message] = []
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._callbacks: dict[str, Callable[[Message], None]] = {}
        self._running = False
        self._consumer_thread: threading.Thread | None = None

    def enqueue(self, message: Message) -> None:
        with self._not_empty:
            self._queue.append(message)
            self._not_empty.notify()

    def dequeue(self, timeout: float | None = None) -> Message | None:
        with self._not_empty:
            while True:
                if self._queue:
                    return self._queue.pop(0)
                if timeout and timeout > 0:
                    remaining = self._not_empty.wait(timeout)
                    if not remaining:
                        return None
                elif timeout == 0:
                    return None
                else:
                    self._not_empty.wait()

    def subscribe(self, callback: Callable[[Message], None]) -> None:
        self._callbacks["default"] = callback

    def start_consuming(self) -> None:
        if self._running:
            return
        self._running = True

        def consumer():
            while self._running:
                msg = self.dequeue(timeout=1.0)
                if msg and "default" in self._callbacks:
                    try:
                        self._callbacks["default"](msg)
                        msg.acknowledged = True
                    except Exception:
                        pass

        self._consumer_thread = threading.Thread(target=consumer, daemon=True)
        self._consumer_thread.start()

    def stop_consuming(self) -> None:
        self._running = False


class PubSub:
    """Publish/Subscribe message broker."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Message], None]]] = defaultdict(list)
        self._lock = threading.RLock()

    def publish(self, topic: str, payload: Any, **kwargs: Any) -> Message:
        msg = Message.create(topic=topic, payload=payload, **kwargs)
        with self._lock:
            callbacks = list(self._subscribers.get(topic, []))
        for callback in callbacks:
            try:
                callback(msg)
            except Exception:
                pass
        return msg

    def subscribe(
        self,
        topic: str,
        callback: Callable[[Message], None],
    ) -> None:
        with self._lock:
            self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable[[Message], None]) -> bool:
        with self._lock:
            if topic in self._subscribers:
                try:
                    self._subscribers[topic].remove(callback)
                    return True
                except ValueError:
                    pass
        return False

    def topics(self) -> list[str]:
        with self._lock:
            return list(self._subscribers.keys())


class MessageConsumer:
    """Message consumer with manual acknowledgment support."""

    def __init__(self, queue: MessageQueue) -> None:
        self.queue = queue
        self._handlers: dict[str, Callable[[Message], None]] = {}
        self._running = False

    def register(self, topic: str, handler: Callable[[Message], None]) -> None:
        self._handlers[topic] = handler

    def start(self) -> None:
        self._running = True
        thread = threading.Thread(target=self._consume, daemon=True)
        thread.start()

    def stop(self) -> None:
        self._running = False

    def _consume(self) -> None:
        while self._running:
            msg = self.queue.dequeue(timeout=1.0)
            if msg is None:
                continue
            handler = self._handlers.get(msg.topic)
            if handler:
                try:
                    handler(msg)
                    msg.acknowledged = True
                except Exception:
                    msg.delivery_count += 1
