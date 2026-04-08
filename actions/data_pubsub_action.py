"""Data PubSub Action Module for RabAI AutoClick.

Publish/subscribe messaging for inter-module communication
with topic filtering and message acknowledgment.
"""

import time
import uuid
import threading
import sys
import os
from typing import Any, Callable, Dict, List, Optional
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataPubSubAction(BaseAction):
    """Publish/subscribe messaging for module communication.

    Provides a message bus for inter-module communication with
    topic-based routing, message persistence, and delivery
    acknowledgment support.
    """
    action_type = "data_pubsub"
    display_name = "数据发布订阅"
    description = "发布订阅消息系统，模块间通信"

    _subscribers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    _messages: deque = deque(maxlen=10000)
    _pending_ack: Dict[str, Dict[str, Any]] = {}
    _lock = threading.RLock()
    _delivery_threads: Dict[str, threading.Thread] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pub/sub operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'publish', 'subscribe', 'unsubscribe',
                               'receive', 'ack', 'list_topics', 'stats'
                - topic: str - message topic
                - message: Any (optional) - message payload
                - subscriber_id: str (optional) - subscriber identifier
                - handler: str (optional) - message handler name
                - ack_required: bool (optional) - require acknowledgment
                - timeout: float (optional) - receive timeout

        Returns:
            ActionResult with pub/sub operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'publish')

            if operation == 'publish':
                return self._publish(params, start_time)
            elif operation == 'subscribe':
                return self._subscribe(params, start_time)
            elif operation == 'unsubscribe':
                return self._unsubscribe(params, start_time)
            elif operation == 'receive':
                return self._receive(params, start_time)
            elif operation == 'ack':
                return self._ack_message(params, start_time)
            elif operation == 'list_topics':
                return self._list_topics(start_time)
            elif operation == 'stats':
                return self._get_stats(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"PubSub action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _publish(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Publish a message to a topic."""
        topic = params.get('topic', 'default')
        message = params.get('message')
        headers = params.get('headers', {})
        ack_required = params.get('ack_required', False)

        message_id = str(uuid.uuid4())

        pub_message = {
            'message_id': message_id,
            'topic': topic,
            'payload': message,
            'headers': headers,
            'timestamp': time.time(),
            'publisher': 'pubsub',
            'ack_required': ack_required,
            'delivered': False,
            'acknowledged': False
        }

        self._messages.append(pub_message)

        if ack_required:
            self._pending_ack[message_id] = pub_message

        delivered_count = self._deliver_to_subscribers(topic, pub_message)

        return ActionResult(
            success=True,
            message=f"Published to {topic}: {delivered_count} subscribers",
            data={
                'message_id': message_id,
                'topic': topic,
                'subscribers_reached': delivered_count,
                'ack_required': ack_required
            },
            duration=time.time() - start_time
        )

    def _subscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Subscribe to a topic."""
        topic = params.get('topic', 'default')
        subscriber_id = params.get('subscriber_id', str(uuid.uuid4()))
        handler = params.get('handler', 'log')

        with self._lock:
            subscriber = {
                'subscriber_id': subscriber_id,
                'topic': topic,
                'handler': handler,
                'subscribed_at': time.time(),
                'messages_received': 0,
                'messages_acked': 0
            }
            self._subscribers[topic].append(subscriber)

        return ActionResult(
            success=True,
            message=f"Subscribed to {topic}",
            data={
                'subscriber_id': subscriber_id,
                'topic': topic,
                'total_subscribers': len(self._subscribers[topic])
            },
            duration=time.time() - start_time
        )

    def _unsubscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Unsubscribe from a topic."""
        topic = params.get('topic', 'default')
        subscriber_id = params.get('subscriber_id', '')

        with self._lock:
            original = len(self._subscribers[topic])
            self._subscribers[topic] = [
                s for s in self._subscribers[topic]
                if s['subscriber_id'] != subscriber_id
            ]
            removed = original - len(self._subscribers[topic])

        return ActionResult(
            success=True,
            message=f"Unsubscribed {subscriber_id} from {topic}",
            data={
                'removed': removed,
                'remaining': len(self._subscribers[topic])
            },
            duration=time.time() - start_time
        )

    def _receive(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Receive a message for a subscriber."""
        subscriber_id = params.get('subscriber_id', '')
        topic = params.get('topic', 'default')
        timeout = params.get('timeout', 1.0)
        poll_interval = 0.1

        elapsed = 0.0
        while elapsed < timeout:
            for msg in reversed(list(self._messages)):
                if msg['topic'] == topic:
                    return ActionResult(
                        success=True,
                        message=f"Message received: {msg['message_id']}",
                        data={
                            'message_id': msg['message_id'],
                            'topic': msg['topic'],
                            'payload': msg['payload'],
                            'timestamp': msg['timestamp'],
                            'ack_required': msg['ack_required']
                        },
                        duration=time.time() - start_time
                    )
            time.sleep(poll_interval)
            elapsed += poll_interval

        return ActionResult(
            success=False,
            message="No message received within timeout",
            data={'timeout': timeout, 'topic': topic},
            duration=time.time() - start_time
        )

    def _ack_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Acknowledge a message."""
        message_id = params.get('message_id', '')

        if message_id in self._pending_ack:
            msg = self._pending_ack[message_id]
            msg['acknowledged'] = True
            del self._pending_ack[message_id]

            return ActionResult(
                success=True,
                message=f"Message acknowledged: {message_id}",
                data={'message_id': message_id},
                duration=time.time() - start_time
            )

        return ActionResult(
            success=False,
            message=f"Pending message not found: {message_id}",
            duration=time.time() - start_time
        )

    def _list_topics(self, start_time: float) -> ActionResult:
        """List all active topics."""
        with self._lock:
            topics = {}
            for topic, subscribers in self._subscribers.items():
                if subscribers:
                    topics[topic] = {
                        'subscriber_count': len(subscribers),
                        'subscriber_ids': [s['subscriber_id'] for s in subscribers]
                    }

        return ActionResult(
            success=True,
            message=f"Active topics: {len(topics)}",
            data={'topics': topics, 'count': len(topics)},
            duration=time.time() - start_time
        )

    def _get_stats(self, start_time: float) -> ActionResult:
        """Get pub/sub statistics."""
        total_messages = len(self._messages)
        pending_ack = len(self._pending_ack)
        total_subscribers = sum(len(s) for s in self._subscribers.values())

        return ActionResult(
            success=True,
            message="PubSub statistics",
            data={
                'total_messages': total_messages,
                'pending_ack': pending_ack,
                'total_subscribers': total_subscribers,
                'topics_with_subscribers': sum(
                    1 for s in self._subscribers.values() if s
                )
            },
            duration=time.time() - start_time
        )

    def _deliver_to_subscribers(self, topic: str, message: Dict[str, Any]) -> int:
        """Deliver message to all matching subscribers."""
        delivered = 0

        for subscriber in self._subscribers[topic]:
            subscriber['messages_received'] += 1
            delivered += 1

        return delivered
