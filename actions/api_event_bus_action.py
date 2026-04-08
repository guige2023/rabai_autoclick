"""API Event Bus Action Module for RabAI AutoClick.

Pub/sub event bus for inter-service API event routing
with topic filtering and message persistence.
"""

import time
import json
import uuid
import threading
import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiEventBusAction(BaseAction):
    """Publish/subscribe event bus for API event routing.

    Routes events between API services using topic-based
    pub/sub. Supports wildcards, message filtering, and
    guaranteed delivery options.
    """
    action_type = "api_event_bus"
    display_name = "API事件总线"
    description = "发布/订阅事件总线，主题过滤和消息路由"

    _subscribers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    _event_history: List[Dict[str, Any]] = []
    _max_history = 1000
    _locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute event bus operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'publish', 'subscribe', 'unsubscribe',
                               'list_topics', 'history', 'clear'
                - topic: str - event topic (e.g., 'api.request', 'user.*')
                - subscriber_id: str (optional) - unique subscriber identifier
                - callback: callable (optional) - callback function
                - handler: str (optional) - handler name
                - message: Any (optional) - message to publish
                - headers: dict (optional) - message headers
                - persistent: bool (optional) - store in history
                - limit: int (optional) - history limit

        Returns:
            ActionResult with event bus operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'publish')

            if operation == 'publish':
                return self._publish_event(params, start_time)
            elif operation == 'subscribe':
                return self._subscribe(params, start_time)
            elif operation == 'unsubscribe':
                return self._unsubscribe(params, start_time)
            elif operation == 'list_topics':
                return self._list_topics(start_time)
            elif operation == 'history':
                return self._get_history(params, start_time)
            elif operation == 'clear':
                return self._clear_history(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Event bus action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _publish_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Publish an event to a topic."""
        topic = params.get('topic', '')
        message = params.get('message')
        headers = params.get('headers', {})
        persistent = params.get('persistent', True)
        correlation_id = params.get('correlation_id', str(uuid.uuid4()))

        if not topic:
            return ActionResult(
                success=False,
                message="topic is required",
                duration=time.time() - start_time
            )

        event = {
            'event_id': str(uuid.uuid4()),
            'topic': topic,
            'message': message,
            'headers': headers,
            'correlation_id': correlation_id,
            'timestamp': time.time(),
            'published_by': 'api_event_bus'
        }

        if persistent:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)

        matching_topics = self._find_matching_topics(topic)
        delivered = 0

        for match_topic in matching_topics:
            with self._locks[match_topic]:
                for subscriber in self._subscribers[match_topic]:
                    try:
                        self._deliver_event(subscriber, event)
                        delivered += 1
                    except Exception as e:
                        subscriber['error_count'] = subscriber.get('error_count', 0) + 1

        return ActionResult(
            success=True,
            message=f"Event published to {topic}",
            data={
                'event_id': event['event_id'],
                'topic': topic,
                'delivered_to': delivered,
                'matching_topics': len(matching_topics)
            },
            duration=time.time() - start_time
        )

    def _subscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Subscribe to a topic."""
        topic = params.get('topic', '')
        subscriber_id = params.get('subscriber_id', str(uuid.uuid4()))
        handler = params.get('handler', 'log')
        queue_size = params.get('queue_size', 100)

        if not topic:
            return ActionResult(
                success=False,
                message="topic is required",
                duration=time.time() - start_time
            )

        subscriber = {
            'subscriber_id': subscriber_id,
            'topic': topic,
            'handler': handler,
            'queue_size': queue_size,
            'subscribed_at': time.time(),
            'messages_received': 0,
            'error_count': 0
        }

        with self._locks[topic]:
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
        topic = params.get('topic', '')
        subscriber_id = params.get('subscriber_id', '')

        if not topic or not subscriber_id:
            return ActionResult(
                success=False,
                message="topic and subscriber_id are required",
                duration=time.time() - start_time
            )

        with self._locks[topic]:
            original = len(self._subscribers[topic])
            self._subscribers[topic] = [
                s for s in self._subscribers[topic]
                if s['subscriber_id'] != subscriber_id
            ]
            removed = original - len(self._subscribers[topic])

        return ActionResult(
            success=removed > 0,
            message=f"Unsubscribed {subscriber_id} from {topic}" if removed else "Subscriber not found",
            data={'removed': removed},
            duration=time.time() - start_time
        )

    def _list_topics(self, start_time: float) -> ActionResult:
        """List all topics with subscribers."""
        topics = {}
        for topic, subscribers in self._subscribers.items():
            if subscribers:
                topics[topic] = {
                    'subscriber_count': len(subscribers),
                    'subscriber_ids': [s['subscriber_id'] for s in subscribers]
                }

        return ActionResult(
            success=True,
            message=f"Topics with subscribers: {len(topics)}",
            data={'topics': topics, 'count': len(topics)},
            duration=time.time() - start_time
        )

    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get event history."""
        topic = params.get('topic')
        limit = params.get('limit', 100)

        events = self._event_history
        if topic:
            events = [e for e in events if self._topic_matches(e['topic'], topic)]

        return ActionResult(
            success=True,
            message=f"History: {len(events)} events",
            data={
                'events': events[-limit:],
                'total_in_history': len(self._event_history)
            },
            duration=time.time() - start_time
        )

    def _clear_history(self, start_time: float) -> ActionResult:
        """Clear event history."""
        cleared = len(self._event_history)
        self._event_history.clear()
        return ActionResult(
            success=True,
            message=f"Cleared {cleared} events",
            data={'cleared': cleared},
            duration=time.time() - start_time
        )

    def _find_matching_topics(self, topic: str) -> Set[str]:
        """Find all topics that match the given pattern."""
        matching = {topic}

        for registered_topic in self._subscribers.keys():
            if self._topic_matches(registered_topic, topic):
                matching.add(registered_topic)
            if self._topic_matches(topic, registered_topic):
                matching.add(registered_topic)

        return matching

    def _topic_matches(self, pattern: str, topic: str) -> bool:
        """Check if topic matches a wildcard pattern."""
        if pattern == topic:
            return True

        if '*' in pattern:
            import re
            regex = pattern.replace('.', r'\.').replace('*', '[^.]+')
            return bool(re.match(f'^{regex}$', topic))

        return False

    def _deliver_event(self, subscriber: Dict[str, Any], event: Dict[str, Any]) -> None:
        """Deliver an event to a subscriber."""
        handler = subscriber['handler']
        subscriber['messages_received'] += 1

        if handler == 'log':
            pass
        elif handler == 'count':
            pass
        elif handler == 'store':
            if 'messages' not in subscriber:
                subscriber['messages'] = []
            subscriber['messages'].append(event)
