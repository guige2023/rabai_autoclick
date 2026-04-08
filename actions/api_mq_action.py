"""API message queue action module for RabAI AutoClick.

Provides message queue operations:
- MQConnectAction: Connect to message queue
- MQPublishAction: Publish message
- MQSubscribeAction: Subscribe to topic
- MQConsumeAction: Consume messages
- MQDisconnectAction: Disconnect from queue
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MQConnectAction(BaseAction):
    """Connect to message queue."""
    action_type = "mq_connect"
    display_name = "MQ连接"
    description = "连接消息队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "localhost")
            port = params.get("port", 5672)
            vhost = params.get("vhost", "/")
            username = params.get("username", "guest")
            password = params.get("password", "guest")

            conn_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "mq_connections"):
                context.mq_connections = {}
            context.mq_connections[conn_id] = {
                "conn_id": conn_id,
                "host": host,
                "port": port,
                "vhost": vhost,
                "status": "connected",
                "connected_at": time.time(),
                "channels": [],
            }

            return ActionResult(
                success=True,
                data={"conn_id": conn_id, "host": host, "port": port, "vhost": vhost},
                message=f"MQ connected to {host}:{port}{vhost}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MQ connect failed: {e}")


class MQPublishAction(BaseAction):
    """Publish message to queue."""
    action_type = "mq_publish"
    display_name = "MQ发布"
    description = "向消息队列发布消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conn_id = params.get("conn_id", "")
            topic = params.get("topic", "")
            message = params.get("message", "")
            properties = params.get("properties", {})

            if not conn_id or not topic:
                return ActionResult(success=False, message="conn_id and topic are required")

            msg_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "mq_messages"):
                context.mq_messages = {}
            if topic not in context.mq_messages:
                context.mq_messages[topic] = []
            context.mq_messages[topic].append({
                "msg_id": msg_id,
                "message": message,
                "properties": properties,
                "published_at": time.time(),
            })

            return ActionResult(
                success=True,
                data={"conn_id": conn_id, "topic": topic, "msg_id": msg_id},
                message=f"Published {msg_id} to topic {topic}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MQ publish failed: {e}")


class MQSubscribeAction(BaseAction):
    """Subscribe to topic."""
    action_type = "mq_subscribe"
    display_name = "MQ订阅"
    description = "订阅消息主题"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conn_id = params.get("conn_id", "")
            topic = params.get("topic", "")
            group_id = params.get("group_id", str(uuid.uuid4())[:8])
            auto_ack = params.get("auto_ack", True)

            if not conn_id or not topic:
                return ActionResult(success=False, message="conn_id and topic are required")

            sub_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "mq_subscriptions"):
                context.mq_subscriptions = {}
            context.mq_subscriptions[sub_id] = {
                "sub_id": sub_id,
                "conn_id": conn_id,
                "topic": topic,
                "group_id": group_id,
                "auto_ack": auto_ack,
                "subscribed_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"sub_id": sub_id, "topic": topic, "group_id": group_id},
                message=f"Subscribed {sub_id} to topic {topic}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MQ subscribe failed: {e}")


class MQConsumeAction(BaseAction):
    """Consume messages from topic."""
    action_type = "mq_consume"
    display_name = "MQ消费"
    description = "消费消息队列消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            topic = params.get("topic", "")
            max_messages = params.get("max_messages", 10)
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 5)

            if not topic:
                return ActionResult(success=False, message="topic is required")

            messages = getattr(context, "mq_messages", {}).get(topic, [])
            consumed = messages[:max_messages]
            if messages:
                context.mq_messages[topic] = messages[max_messages:]

            return ActionResult(
                success=True,
                data={"topic": topic, "consumed": len(consumed), "messages": [{"msg_id": m["msg_id"]} for m in consumed]},
                message=f"Consumed {len(consumed)} messages from {topic}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MQ consume failed: {e}")


class MQDisconnectAction(BaseAction):
    """Disconnect from message queue."""
    action_type = "mq_disconnect"
    display_name = "MQ断开"
    description = "断开消息队列连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            conn_id = params.get("conn_id", "")
            if not conn_id:
                return ActionResult(success=False, message="conn_id is required")

            connections = getattr(context, "mq_connections", {})
            if conn_id in connections:
                connections[conn_id]["status"] = "disconnected"
                del connections[conn_id]

            return ActionResult(success=True, data={"conn_id": conn_id}, message=f"MQ connection {conn_id} disconnected")
        except Exception as e:
            return ActionResult(success=False, message=f"MQ disconnect failed: {e}")
