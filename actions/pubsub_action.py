"""Pub/Sub action module for RabAI AutoClick.

Provides publish-subscribe messaging with topic management,
message filtering, dead letter queue, and multiple subscriber support.
"""

import sys
import os
import json
import time
import uuid
import asyncio
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Message:
    """Represents a pub/sub message."""
    message_id: str
    topic: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3
    is_dead_letter: bool = False
    error: Optional[str] = None


@dataclass
class Subscription:
    """Represents a topic subscription."""
    subscription_id: str
    topic: str
    subscriber_func: Optional[Callable] = None
    filter_expression: Optional[str] = None
    ack_timeout_seconds: float = 30.0
    max_retries: int = 3
    dead_letter_topic: Optional[str] = None
    is_async: bool = True


@dataclass
class TopicConfig:
    """Configuration for a pub/sub topic."""
    name: str
    description: str = ""
    retention_seconds: float = 86400.0  # 24 hours
    max_message_size: int = 1024 * 1024  # 1MB
    deliveryGuarantee: str = "at_least_once"  # at_least_once, exactly_once
    ordering_key: Optional[str] = None


class PubSubBroker:
    """In-memory pub/sub message broker."""
    
    def __init__(self):
        self._topics: Dict[str, TopicConfig] = {}
        self._subscriptions: Dict[str, Subscription] = {}
        self._messages: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._pending_acks: Dict[str, Dict[str, float]] = defaultdict(dict)  # sub_id -> msg_id -> expiry
        self._dead_letter_topics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._subscriber_callbacks: Dict[str, Callable] = {}
        self._message_routes: Dict[str, List[str]] = defaultdict(list)  # topic -> [subscription_ids]
        self._lock = asyncio.Lock()
    
    def create_topic(self, config: TopicConfig) -> None:
        """Create a new topic."""
        self._topics[config.name] = config
        self._messages[config.name] = deque(maxlen=10000)
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic and all its messages."""
        if topic_name in self._topics:
            del self._topics[topic_name]
            self._messages[topic_name].clear()
            # Remove subscriptions for this topic
            to_remove = [sid for sid, sub in self._subscriptions.items() if sub.topic == topic_name]
            for sid in to_remove:
                del self._subscriptions[sid]
                self._subscriber_callbacks.pop(sid, None)
            return True
        return False
    
    def list_topics(self) -> List[str]:
        """List all topic names."""
        return list(self._topics.keys())
    
    def get_topic_stats(self, topic_name: str) -> Dict[str, Any]:
        """Get statistics for a topic."""
        if topic_name not in self._topics:
            return {}
        return {
            "name": topic_name,
            "message_count": len(self._messages[topic_name]),
            "subscription_count": len(self._message_routes[topic_name]),
            "config": {
                "retention_seconds": self._topics[topic_name].retention_seconds,
                "max_message_size": self._topics[topic_name].max_message_size,
                "delivery_guarantee": self._topics[topic_name].deliveryGuarantee
            }
        }
    
    def create_subscription(self, subscription: Subscription) -> None:
        """Create a subscription for a topic."""
        self._subscriptions[subscription.subscription_id] = subscription
        self._message_routes[subscription.topic].append(subscription.subscription_id)
    
    def delete_subscription(self, subscription_id: str) -> bool:
        """Delete a subscription."""
        if subscription_id in self._subscriptions:
            sub = self._subscriptions[subscription_id]
            if sub.topic in self._message_routes:
                self._message_routes[sub.topic] = [
                    sid for sid in self._message_routes[sub.topic]
                    if sid != subscription_id
                ]
            del self._subscriptions[subscription_id]
            self._subscriber_callbacks.pop(subscription_id, None)
            return True
        return False
    
    def list_subscriptions(self, topic_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List subscriptions, optionally filtered by topic."""
        subs = self._subscriptions.values()
        if topic_name:
            subs = [s for s in subs if s.topic == topic_name]
        return [
            {"subscription_id": s.subscription_id, "topic": s.topic, "filter": s.filter_expression}
            for s in subs
        ]
    
    def register_callback(self, subscription_id: str, 
                          callback: Callable) -> None:
        """Register a callback for a subscription."""
        self._subscriber_callbacks[subscription_id] = callback
        if subscription_id in self._subscriptions:
            self._subscriptions[subscription_id].subscriber_func = callback
    
    async def publish(self, topic_name: str, data: Any,
                      headers: Optional[Dict[str, str]] = None,
                      attributes: Optional[Dict[str, Any]] = None,
                      ordering_key: Optional[str] = None) -> str:
        """Publish a message to a topic.
        
        Args:
            topic_name: Target topic name.
            data: Message data.
            headers: Optional message headers.
            attributes: Optional message attributes for filtering.
            ordering_key: Optional ordering key for FIFO within key.
        
        Returns:
            The published message ID.
        """
        if topic_name not in self._topics:
            raise ValueError(f"Topic '{topic_name}' does not exist")
        
        message_id = str(uuid.uuid4())
        message = Message(
            message_id=message_id,
            topic=topic_name,
            data=data,
            headers=headers or {},
            attributes=attributes or {},
            max_retries=self._topics[topic_name].deliveryGuarantee == "at_least_once" and 3 or 0
        )
        
        async with self._lock:
            self._messages[topic_name].append(message)
        
        # Deliver to subscribers
        await self._deliver_to_subscribers(topic_name, message)
        
        return message_id
    
    async def _deliver_to_subscribers(self, topic_name: str, 
                                      message: Message) -> None:
        """Deliver a message to all matching subscribers."""
        subscription_ids = self._message_routes.get(topic_name, [])
        
        for sub_id in subscription_ids:
            sub = self._subscriptions.get(sub_id)
            if not sub:
                continue
            
            # Check filter expression
            if sub.filter_expression and not self._matches_filter(message, sub.filter_expression):
                continue
            
            # Track pending ack
            async with self._lock:
                self._pending_acks[sub_id][message.message_id] = time.time() + sub.ack_timeout_seconds
            
            # Deliver to callback
            callback = self._subscriber_callbacks.get(sub_id) or sub.subscriber_func
            if callback:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(message)
                    else:
                        callback(message)
                    # Auto-ack on success
                    await self.acknowledge(sub_id, message.message_id)
                except Exception as e:
                    # Handle failure
                    message.retry_count += 1
                    if message.retry_count >= sub.max_retries:
                        # Send to dead letter
                        await self._send_to_dead_letter(sub, message, str(e))
                    else:
                        # Requeue
                        async with self._lock:
                            self._messages[topic_name].append(message)
    
    def _matches_filter(self, message: Message, 
                        filter_expr: str) -> bool:
        """Check if message matches filter expression."""
        # Simple attribute-based filtering
        # Format: "attribute.key == value" or "attribute.key > value"
        try:
            if "==" in filter_expr:
                key, value = filter_expr.split("==")
                key = key.strip().replace("attribute.", "")
                return str(message.attributes.get(key, "")) == value.strip().strip('"\'')
            elif "!=" in filter_expr:
                key, value = filter_expr.split("!=")
                key = key.strip().replace("attribute.", "")
                return str(message.attributes.get(key, "")) != value.strip().strip('"\'')
        except Exception:
            pass
        return True  # Default to match if parsing fails
    
    async def _send_to_dead_letter(self, subscription: Subscription,
                                   message: Message, error: str) -> None:
        """Send a failed message to dead letter topic."""
        dl_topic = subscription.dead_letter_topic or f"{subscription.topic}.dead-letter"
        
        message.is_dead_letter = True
        message.error = error
        
        async with self._lock:
            self._dead_letter_topics[dl_topic].append(message)
    
    async def acknowledge(self, subscription_id: str, 
                          message_id: str) -> bool:
        """Acknowledge a message delivery.
        
        Returns True if ack was successful.
        """
        async with self._lock:
            if subscription_id in self._pending_acks:
                if message_id in self._pending_acks[subscription_id]:
                    del self._pending_acks[subscription_id][message_id]
                    return True
        return False
    
    def pull(self, subscription_id: str, 
             max_messages: int = 10) -> List[Message]:
        """Pull messages for a subscription (synchronous pull model)."""
        if subscription_id not in self._subscriptions:
            return []
        
        sub = self._subscriptions[subscription_id]
        messages = []
        
        for _ in range(max_messages):
            try:
                async with asyncio.Lock():
                    msg = self._messages[sub.topic].popleft()
                messages.append(msg)
                # Track for ack
                self._pending_acks[subscription_id][msg.message_id] = time.time() + sub.ack_timeout_seconds
            except IndexError:
                break
        
        return messages
    
    def get_dead_letter_messages(self, topic_name: str,
                                  count: int = 10) -> List[Message]:
        """Get messages from a dead letter topic."""
        dl_topic = f"{topic_name}.dead-letter"
        messages = list(self._dead_letter_topics.get(dl_topic, deque()))
        return messages[-count:]
    
    def republish_dead_letter(self, topic_name: str,
                              message_id: str) -> bool:
        """Republish a dead letter message to its original topic."""
        dl_topic = f"{topic_name}.dead-letter"
        dl_messages = self._dead_letter_topics.get(dl_topic, deque())
        
        for i, msg in enumerate(dl_messages):
            if msg.message_id == message_id:
                msg.is_dead_letter = False
                msg.error = None
                msg.retry_count = 0
                del dl_messages[i]
                self._messages[topic_name].append(msg)
                return True
        return False
    
    def clear_dead_letter(self, topic_name: str) -> int:
        """Clear all messages from a dead letter topic."""
        dl_topic = f"{topic_name}.dead-letter"
        count = len(self._dead_letter_topics.get(dl_topic, deque()))
        self._dead_letter_topics[dl_topic].clear()
        return count


class PubSubAction(BaseAction):
    """Publish and subscribe to message topics.
    
    Supports topic management, message publishing, subscription
    with filtering, dead letter queue, and acknowledgment.
    """
    action_type = "pubsub"
    display_name = "消息订阅发布"
    description = "发布订阅消息系统，支持主题管理、死信队列和消息过滤"
    
    def __init__(self):
        super().__init__()
        self._broker = PubSubBroker()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pub/sub operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_topic", "delete_topic", "list_topics",
                  "get_stats", "create_subscription", "delete_subscription",
                  "list_subscriptions", "publish", "pull", "acknowledge",
                  "get_dead_letter", "republish_dead_letter", "clear_dead_letter"
                - For topic ops: name, description
                - For subscription: topic_name, subscription_id, filter_expression
                - For publish: topic_name, data, headers, attributes
                - For pull: subscription_id, max_messages
                - For acknowledge: subscription_id, message_id
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_topic":
                return self._create_topic(params)
            elif operation == "delete_topic":
                return self._delete_topic(params)
            elif operation == "list_topics":
                return self._list_topics(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            elif operation == "create_subscription":
                return self._create_subscription(params)
            elif operation == "delete_subscription":
                return self._delete_subscription(params)
            elif operation == "list_subscriptions":
                return self._list_subscriptions(params)
            elif operation == "publish":
                return self._publish_message(params)
            elif operation == "pull":
                return self._pull_messages(params)
            elif operation == "acknowledge":
                return self._acknowledge(params)
            elif operation == "get_dead_letter":
                return self._get_dead_letter(params)
            elif operation == "republish_dead_letter":
                return self._republish_dead_letter(params)
            elif operation == "clear_dead_letter":
                return self._clear_dead_letter(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pub/Sub error: {str(e)}")
    
    def _create_topic(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new topic."""
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Topic name is required")
        
        config = TopicConfig(
            name=name,
            description=params.get("description", ""),
            retention_seconds=params.get("retention_seconds", 86400.0),
            max_message_size=params.get("max_message_size", 1024 * 1024),
            deliveryGuarantee=params.get("delivery_guarantee", "at_least_once")
        )
        self._broker.create_topic(config)
        return ActionResult(
            success=True,
            message=f"Topic '{name}' created",
            data={"name": name}
        )
    
    def _delete_topic(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a topic."""
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Topic name is required")
        
        deleted = self._broker.delete_topic(name)
        return ActionResult(
            success=deleted,
            message=f"Topic '{name}' deleted" if deleted else f"Topic '{name}' not found"
        )
    
    def _list_topics(self, params: Dict[str, Any]) -> ActionResult:
        """List all topics."""
        topics = self._broker.list_topics()
        return ActionResult(
            success=True,
            message=f"Found {len(topics)} topics",
            data={"topics": topics}
        )
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get topic statistics."""
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="Topic name is required")
        
        stats = self._broker.get_topic_stats(name)
        if not stats:
            return ActionResult(success=False, message=f"Topic '{name}' not found")
        return ActionResult(success=True, message="Stats retrieved", data=stats)
    
    def _create_subscription(self, params: Dict[str, Any]) -> ActionResult:
        """Create a subscription."""
        topic_name = params.get("topic_name", "")
        subscription_id = params.get("subscription_id", str(uuid.uuid4()))
        
        if not topic_name:
            return ActionResult(success=False, message="topic_name is required")
        
        subscription = Subscription(
            subscription_id=subscription_id,
            topic=topic_name,
            filter_expression=params.get("filter_expression"),
            ack_timeout_seconds=params.get("ack_timeout_seconds", 30.0),
            max_retries=params.get("max_retries", 3),
            dead_letter_topic=params.get("dead_letter_topic")
        )
        self._broker.create_subscription(subscription)
        return ActionResult(
            success=True,
            message=f"Subscription '{subscription_id}' created for topic '{topic_name}'",
            data={"subscription_id": subscription_id, "topic": topic_name}
        )
    
    def _delete_subscription(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a subscription."""
        subscription_id = params.get("subscription_id", "")
        if not subscription_id:
            return ActionResult(success=False, message="subscription_id is required")
        
        deleted = self._broker.delete_subscription(subscription_id)
        return ActionResult(
            success=deleted,
            message=f"Subscription '{subscription_id}' deleted" if deleted else f"Subscription '{subscription_id}' not found"
        )
    
    def _list_subscriptions(self, params: Dict[str, Any]) -> ActionResult:
        """List subscriptions."""
        topic_name = params.get("topic_name")
        subs = self._broker.list_subscriptions(topic_name)
        return ActionResult(
            success=True,
            message=f"Found {len(subs)} subscriptions",
            data={"subscriptions": subs}
        )
    
    def _publish_message(self, params: Dict[str, Any]) -> ActionResult:
        """Publish a message."""
        topic_name = params.get("topic_name", "")
        if not topic_name:
            return ActionResult(success=False, message="topic_name is required")
        
        data = params.get("data")
        headers = params.get("headers", {})
        attributes = params.get("attributes", {})
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            message_id = loop.run_until_complete(
                self._broker.publish(topic_name, data, headers, attributes)
            )
            return ActionResult(
                success=True,
                message=f"Message published to '{topic_name}'",
                data={"message_id": message_id, "topic": topic_name}
            )
        finally:
            loop.close()
    
    def _pull_messages(self, params: Dict[str, Any]) -> ActionResult:
        """Pull messages from a subscription."""
        subscription_id = params.get("subscription_id", "")
        max_messages = params.get("max_messages", 10)
        
        if not subscription_id:
            return ActionResult(success=False, message="subscription_id is required")
        
        messages = self._broker.pull(subscription_id, max_messages)
        return ActionResult(
            success=True,
            message=f"Pulled {len(messages)} messages",
            data={
                "messages": [
                    {"message_id": m.message_id, "data": m.data,
                     "timestamp": m.timestamp, "attributes": m.attributes}
                    for m in messages
                ]
            }
        )
    
    def _acknowledge(self, params: Dict[str, Any]) -> ActionResult:
        """Acknowledge a message."""
        subscription_id = params.get("subscription_id", "")
        message_id = params.get("message_id", "")
        
        if not subscription_id or not message_id:
            return ActionResult(success=False, message="subscription_id and message_id are required")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            success = loop.run_until_complete(
                self._broker.acknowledge(subscription_id, message_id)
            )
            return ActionResult(
                success=success,
                message="Message acknowledged" if success else "Ack failed - message not found"
            )
        finally:
            loop.close()
    
    def _get_dead_letter(self, params: Dict[str, Any]) -> ActionResult:
        """Get dead letter messages."""
        topic_name = params.get("topic_name", "")
        count = params.get("count", 10)
        
        if not topic_name:
            return ActionResult(success=False, message="topic_name is required")
        
        messages = self._broker.get_dead_letter_messages(topic_name, count)
        return ActionResult(
            success=True,
            message=f"Found {len(messages)} dead letter messages",
            data={
                "messages": [
                    {"message_id": m.message_id, "data": m.data,
                     "error": m.error, "retry_count": m.retry_count}
                    for m in messages
                ]
            }
        )
    
    def _republish_dead_letter(self, params: Dict[str, Any]) -> ActionResult:
        """Republish a dead letter message."""
        topic_name = params.get("topic_name", "")
        message_id = params.get("message_id", "")
        
        if not topic_name or not message_id:
            return ActionResult(success=False, message="topic_name and message_id are required")
        
        republished = self._broker.republish_dead_letter(topic_name, message_id)
        return ActionResult(
            success=republished,
            message="Message republished" if republished else "Dead letter message not found"
        )
    
    def _clear_dead_letter(self, params: Dict[str, Any]) -> ActionResult:
        """Clear dead letter topic."""
        topic_name = params.get("topic_name", "")
        if not topic_name:
            return ActionResult(success=False, message="topic_name is required")
        
        count = self._broker.clear_dead_letter(topic_name)
        return ActionResult(
            success=True,
            message=f"Cleared {count} messages from dead letter topic"
        )
