"""
Queue and message broker utilities - RabbitMQ, Kafka, Redis queue, task queue patterns.
"""
from typing import Any, Dict, List, Optional, Callable
import json
import logging
import time
from collections import deque
from threading import Lock

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class InMemoryQueue:
    """Simple in-memory FIFO queue with optional persistence simulation."""

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._queue: deque = deque()
        self._dead_letter: deque = deque()
        self._lock = Lock()
        self._max_size = 10000
        self._retry_limit = 3

    def enqueue(self, item: Any, priority: int = 0) -> bool:
        with self._lock:
            if len(self._queue) >= self._max_size:
                return False
            if priority != 0:
                self._queue.appendleft((priority, item))
                self._queue = deque(sorted(self._queue, key=lambda x: x[0], reverse=True))
            else:
                self._queue.append(item)
            return True

    def dequeue(self) -> Optional[Any]:
        with self._lock:
            if self._queue:
                item = self._queue.popleft()
                if isinstance(item, tuple):
                    return item[1]
                return item
            return None

    def peek(self) -> Optional[Any]:
        with self._lock:
            if self._queue:
                item = self._queue[0]
                if isinstance(item, tuple):
                    return item[1]
                return item
            return None

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def is_empty(self) -> bool:
        return self.size() == 0

    def clear(self) -> int:
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count

    def to_list(self) -> List[Any]:
        with self._lock:
            return [item[1] if isinstance(item, tuple) else item for item in self._queue]


class MessageBus:
    """In-memory message bus with pub/sub pattern."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable]] = {}
        self._lock = Lock()
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._max_history = 100

    def publish(self, topic: str, message: Dict[str, Any]) -> int:
        with self._lock:
            if topic not in self._subscribers:
                return 0
            count = 0
            for callback in self._subscribers[topic]:
                try:
                    callback(message)
                    count += 1
                except Exception as e:
                    logger.error(f"Subscriber error on topic {topic}: {e}")
            if topic not in self._history:
                self._history[topic] = []
            self._history[topic].append(message)
            if len(self._history[topic]) > self._max_history:
                self._history[topic].pop(0)
            return count

    def subscribe(self, topic: str, callback: Callable) -> None:
        with self._lock:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable) -> bool:
        with self._lock:
            if topic in self._subscribers:
                try:
                    self._subscribers[topic].remove(callback)
                    return True
                except ValueError:
                    pass
            return False

    def history(self, topic: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history.get(topic, []))


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.time()
        self._lock = Lock()

    def allow_request(self, tokens: int = 1) -> bool:
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_update = now
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        with self._lock:
            if self.tokens >= tokens:
                return 0.0
            return (tokens - self.tokens) / self.rate


class QueueAction(BaseAction):
    """Queue and message broker operations.

    Provides in-memory queue, message bus with pub/sub, rate limiting.
    """

    def __init__(self) -> None:
        self._queues: Dict[str, InMemoryQueue] = {}
        self._bus = MessageBus()
        self._rate_limiters: Dict[str, RateLimiter] = {}

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "enqueue")
        queue_name = params.get("queue", "default")
        topic = params.get("topic", "default")
        item = params.get("item")
        limiter_name = params.get("limiter", "default")

        try:
            if operation == "enqueue":
                if queue_name not in self._queues:
                    self._queues[queue_name] = InMemoryQueue(queue_name)
                priority = int(params.get("priority", 0))
                success = self._queues[queue_name].enqueue(item, priority)
                return {"success": success, "queue": queue_name, "size": self._queues[queue_name].size()}

            elif operation == "dequeue":
                if queue_name not in self._queues:
                    return {"success": True, "item": None, "queue": queue_name}
                item = self._queues[queue_name].dequeue()
                return {"success": True, "item": item, "queue": queue_name, "size": self._queues[queue_name].size()}

            elif operation == "peek":
                if queue_name not in self._queues:
                    return {"success": True, "item": None, "queue": queue_name}
                item = self._queues[queue_name].peek()
                return {"success": True, "item": item, "queue": queue_name}

            elif operation == "size":
                if queue_name not in self._queues:
                    return {"success": True, "size": 0, "queue": queue_name}
                return {"success": True, "size": self._queues[queue_name].size(), "queue": queue_name}

            elif operation == "clear":
                if queue_name not in self._queues:
                    return {"success": True, "cleared": 0}
                cleared = self._queues[queue_name].clear()
                return {"success": True, "cleared": cleared, "queue": queue_name}

            elif operation == "list_queues":
                return {"success": True, "queues": list(self._queues.keys()), "counts": {k: v.size() for k, v in self._queues.items()}}

            elif operation == "publish":
                message = params.get("message", {})
                count = self._bus.publish(topic, message)
                return {"success": True, "topic": topic, "subscribers_notified": count}

            elif operation == "history":
                messages = self._bus.history(topic)
                return {"success": True, "topic": topic, "messages": messages, "count": len(messages)}

            elif operation == "rate_limit_create":
                rate = float(params.get("rate", 10))
                capacity = int(params.get("capacity", 10))
                self._rate_limiters[limiter_name] = RateLimiter(rate, capacity)
                return {"success": True, "limiter": limiter_name, "rate": rate, "capacity": capacity}

            elif operation == "rate_limit_allow":
                if limiter_name not in self._rate_limiters:
                    return {"success": False, "error": f"Limiter {limiter_name} not found"}
                tokens = int(params.get("tokens", 1))
                allowed = self._rate_limiters[limiter_name].allow_request(tokens)
                wait_time = self._rate_limiters[limiter_name].wait_time(tokens) if not allowed else 0.0
                return {"success": True, "allowed": allowed, "limiter": limiter_name, "wait_time": wait_time}

            elif operation == "batch":
                items = params.get("items", [])
                results = []
                for it in items:
                    if queue_name not in self._queues:
                        self._queues[queue_name] = InMemoryQueue(queue_name)
                    self._queues[queue_name].enqueue(it)
                    results.append(it)
                return {"success": True, "enqueued": len(results), "queue": queue_name, "items": results}

            elif operation == "drain":
                if queue_name not in self._queues:
                    return {"success": True, "items": [], "drained": 0}
                items = []
                q = self._queues[queue_name]
                while not q.is_empty():
                    item = q.dequeue()
                    if item:
                        items.append(item)
                return {"success": True, "items": items, "drained": len(items), "queue": queue_name}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"QueueAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for queue operations."""
    return QueueAction().execute(context, params)
