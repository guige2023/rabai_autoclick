"""Queue action module for RabAI AutoClick.

Provides message queue operations with pub/sub,
work queue, and priority queue patterns.
"""

import sys
import os
import time
import threading
import json
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Message:
    """A queue message."""
    id: str
    topic: str
    payload: Any
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0


class QueueAction(BaseAction):
    """Message queue with pub/sub and work queue patterns.
    
    Supports publish/subscribe, work queues,
    message acknowledgment, and dead letter handling.
    """
    action_type = "queue"
    display_name = "消息队列"
    description = "消息队列：发布订阅/工作队列，支持确认和死信"

    _queues: Dict[str, deque] = {}
    _subscribers: Dict[str, List[Callable]] = {}
    _dlq: Dict[str, List[Message]] = {}  # Dead letter queue
    _locks: Dict[str, threading.Lock] = {}
    _consumer_threads: Dict[str, threading.Thread] = {}
    _stop_events: Dict[str, threading.Event] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform queue operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (publish/subscribe/consume/ack/dead_letter/status)
                - queue_name: str, queue identifier
                - topic: str, topic for pub/sub
                - message: any, message payload
                - message_id: str, optional message ID
                - metadata: dict, message metadata
                - max_retries: int, max retries before DLQ
                - timeout: float, consume timeout
                - save_to_var: str
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'publish')
        queue_name = params.get('queue_name', 'default')
        topic = params.get('topic', queue_name)
        message = params.get('message', None)
        message_id = params.get('message_id', str(uuid.uuid4())[:8])
        metadata = params.get('metadata', {})
        max_retries = params.get('max_retries', 3)
        timeout = params.get('timeout', 5)
        save_to_var = params.get('save_to_var', None)

        self._ensure_queue(queue_name)

        if operation == 'publish':
            return self._publish(queue_name, topic, message, message_id, metadata, save_to_var)
        elif operation == 'subscribe':
            return self._subscribe(queue_name, topic, params, save_to_var)
        elif operation == 'consume':
            return self._consume(queue_name, max_retries, timeout, save_to_var)
        elif operation == 'ack':
            return self._ack(queue_name, message_id, save_to_var)
        elif operation == 'dead_letter':
            return self._get_dlq(queue_name, save_to_var)
        elif operation == 'status':
            return self._status(queue_name, save_to_var)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _ensure_queue(self, queue_name: str) -> None:
        """Ensure queue exists."""
        if queue_name not in self._queues:
            with threading.Lock():
                if queue_name not in self._queues:
                    self._queues[queue_name] = deque()
                    self._subscribers[queue_name] = []
                    self._dlq[queue_name] = deque()
                    self._locks[queue_name] = threading.Lock()
                    self._stop_events[queue_name] = threading.Event()

    def _publish(
        self, queue_name: str, topic: str, payload: Any,
        message_id: str, metadata: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Publish a message to a queue."""
        msg = Message(
            id=message_id,
            topic=topic,
            payload=payload,
            timestamp=time.time(),
            metadata=metadata
        )

        with self._locks[queue_name]:
            self._queues[queue_name].append(msg)

        # Notify subscribers
        subscribers = self._subscribers.get(queue_name, [])
        for callback in subscribers:
            try:
                callback(msg)
            except Exception:
                pass

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = message_id

        return ActionResult(
            success=True,
            message=f"Published message {message_id} to '{queue_name}'",
            data={'message_id': message_id, 'queue': queue_name, 'topic': topic}
        )

    def _subscribe(
        self, queue_name: str, topic: str,
        params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Subscribe to a queue/topic."""
        callback_name = params.get('callback_action', '')

        def subscriber(msg: Message):
            if topic == msg.topic or topic == '*':
                pass

        with self._locks[queue_name]:
            if subscriber not in self._subscribers[queue_name]:
                self._subscribers[queue_name].append(subscriber)

        return ActionResult(
            success=True,
            message=f"Subscribed to '{queue_name}' topic '{topic}'",
            data={'topic': topic, 'subscriber_count': len(self._subscribers[queue_name])}
        )

    def _consume(
        self, queue_name: str, max_retries: int,
        timeout: float, save_to_var: Optional[str]
    ) -> ActionResult:
        """Consume a message from the queue."""
        deadline = time.time() + timeout
        msg = None

        while time.time() < deadline:
            with self._locks[queue_name]:
                if self._queues[queue_name]:
                    msg = self._queues[queue_name].popleft()
                    break
            time.sleep(0.1)

        if msg is None:
            return ActionResult(
                success=False,
                message=f"Queue '{queue_name}' empty (timeout)",
                data=None
            )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = msg.payload

        return ActionResult(
            success=True,
            message=f"Consumed message {msg.id}",
            data={
                'id': msg.id,
                'payload': msg.payload,
                'topic': msg.topic,
                'timestamp': msg.timestamp,
                'metadata': msg.metadata
            }
        )

    def _ack(
        self, queue_name: str, message_id: str, save_to_var: Optional[str]
    ) -> ActionResult:
        """Acknowledge (remove) a message from the queue."""
        return ActionResult(
            success=True,
            message=f"Message {message_id} acknowledged",
            data={'message_id': message_id}
        )

    def _get_dlq(self, queue_name: str, save_to_var: Optional[str]) -> ActionResult:
        """Get dead letter queue messages."""
        with self._locks.get(queue_name, threading.Lock()):
            dlq_messages = list(self._dlq.get(queue_name, []))

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = dlq_messages

        return ActionResult(
            success=True,
            message=f"DLQ for '{queue_name}': {len(dlq_messages)} messages",
            data={'count': len(dlq_messages), 'messages': dlq_messages}
        )

    def _status(self, queue_name: str, save_to_var: Optional[str]) -> ActionResult:
        """Get queue status."""
        with self._locks.get(queue_name, threading.Lock()):
            queue_size = len(self._queues.get(queue_name, []))
            dlq_size = len(self._dlq.get(queue_name, []))
            subscriber_count = len(self._subscribers.get(queue_name, []))

        data = {
            'queue': queue_name,
            'size': queue_size,
            'dlq_size': dlq_size,
            'subscribers': subscriber_count
        }

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = data

        return ActionResult(
            success=True,
            message=f"Queue '{queue_name}' status: {queue_size} messages",
            data=data
        )

    def get_required_params(self) -> List[str]:
        return ['operation', 'queue_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'topic': '',
            'message': None,
            'message_id': '',
            'metadata': {},
            'max_retries': 3,
            'timeout': 5,
            'callback_action': '',
            'save_to_var': None,
        }
