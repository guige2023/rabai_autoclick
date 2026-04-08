"""
Message Bus Utilities

Provides utilities for a message bus pattern
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class Message:
    """Represents a message on the bus."""
    topic: str
    payload: dict[str, Any]
    timestamp: float = 0.0


class MessageBus:
    """
    Pub/sub message bus for inter-component communication.
    
    Supports topic-based message routing
    and subscription management.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Message], None]]] = {}
        self._message_history: list[Message] = []
        self._max_history = 100

    def publish(
        self,
        topic: str,
        payload: dict[str, Any] | None = None,
    ) -> Message:
        """
        Publish a message to a topic.
        
        Args:
            topic: Message topic.
            payload: Optional message payload.
            
        Returns:
            Published Message.
        """
        import time
        message = Message(
            topic=topic,
            payload=payload or {},
            timestamp=time.time(),
        )
        self._message_history.append(message)
        if len(self._message_history) > self._max_history:
            self._message_history.pop(0)
        self._deliver_message(message)
        return message

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
    ) -> None:
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic to subscribe to.
            handler: Callback for messages.
        """
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(handler)

    def unsubscribe(
        self,
        topic: str,
        handler: Callable[[Message], None],
    ) -> None:
        """Unsubscribe a handler from a topic."""
        if topic in self._subscribers:
            self._subscribers[topic] = [
                h for h in self._subscribers[topic] if h != handler
            ]

    def _deliver_message(self, message: Message) -> None:
        """Deliver message to all subscribers."""
        if message.topic in self._subscribers:
            for handler in self._subscribers[message.topic]:
                handler(message)

    def get_topics(self) -> list[str]:
        """Get list of subscribed topics."""
        return list(self._subscribers.keys())

    def clear_topic(self, topic: str) -> None:
        """Clear all subscribers for a topic."""
        self._subscribers.pop(topic, None)
