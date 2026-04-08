"""API queue/action module for RabAI AutoClick.

Provides message queue operations:
- QueuePublishAction: Publish message to queue
- QueueConsumeAction: Consume messages from queue
- QueueCreateAction: Create a queue
- QueueDeleteAction: Delete a queue
- QueueStatusAction: Get queue status
- QueuePurgeAction: Purge all messages
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class QueuePublishAction(BaseAction):
    """Publish a message to queue."""
    action_type = "queue_publish"
    display_name = "队列发布"
    description = "向队列发布消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            message = params.get("message", "")
            priority = params.get("priority", 0)
            headers = params.get("headers", {})

            if not queue_name or not message:
                return ActionResult(success=False, message="queue_name and message are required")

            msg_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "message_queues"):
                context.message_queues = {}
            if queue_name not in context.message_queues:
                context.message_queues[queue_name] = {"messages": [], "stats": {"published": 0, "consumed": 0}}

            context.message_queues[queue_name]["messages"].append({
                "msg_id": msg_id,
                "message": message,
                "priority": priority,
                "headers": headers,
                "published_at": time.time(),
            })
            context.message_queues[queue_name]["stats"]["published"] += 1

            return ActionResult(
                success=True,
                data={"msg_id": msg_id, "queue_name": queue_name, "priority": priority},
                message=f"Message {msg_id} published to {queue_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue publish failed: {e}")


class QueueConsumeAction(BaseAction):
    """Consume messages from queue."""
    action_type = "queue_consume"
    display_name = "队列消费"
    description = "从队列消费消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            max_messages = params.get("max_messages", 1)
            blocking = params.get("blocking", False)
            timeout = params.get("timeout", 5)

            if not queue_name:
                return ActionResult(success=False, message="queue_name is required")

            queues = getattr(context, "message_queues", {})
            if queue_name not in queues:
                return ActionResult(success=False, message=f"Queue {queue_name} not found")

            messages = queues[queue_name]["messages"][:max_messages]
            queues[queue_name]["messages"] = queues[queue_name]["messages"][max_messages:]
            queues[queue_name]["stats"]["consumed"] += len(messages)

            return ActionResult(
                success=True,
                data={"queue_name": queue_name, "consumed": len(messages), "messages": [{"msg_id": m["msg_id"]} for m in messages]},
                message=f"Consumed {len(messages)} messages from {queue_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue consume failed: {e}")


class QueueCreateAction(BaseAction):
    """Create a queue."""
    action_type = "queue_create"
    display_name = "创建队列"
    description = "创建消息队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            queue_type = params.get("type", "fifo")
            max_size = params.get("max_size", 10000)

            if not queue_name:
                return ActionResult(success=False, message="queue_name is required")

            if not hasattr(context, "message_queues"):
                context.message_queues = {}

            context.message_queues[queue_name] = {
                "type": queue_type,
                "max_size": max_size,
                "messages": [],
                "stats": {"published": 0, "consumed": 0},
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"queue_name": queue_name, "type": queue_type, "max_size": max_size},
                message=f"Queue {queue_name} created ({queue_type})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue create failed: {e}")


class QueueDeleteAction(BaseAction):
    """Delete a queue."""
    action_type = "queue_delete"
    display_name = "删除队列"
    description = "删除消息队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            if not queue_name:
                return ActionResult(success=False, message="queue_name is required")

            queues = getattr(context, "message_queues", {})
            if queue_name not in queues:
                return ActionResult(success=False, message=f"Queue {queue_name} not found")

            stats = queues[queue_name]["stats"]
            del queues[queue_name]

            return ActionResult(
                success=True,
                data={"queue_name": queue_name, "messages_published": stats["published"], "messages_consumed": stats["consumed"]},
                message=f"Queue {queue_name} deleted",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue delete failed: {e}")


class QueueStatusAction(BaseAction):
    """Get queue status."""
    action_type = "queue_status"
    display_name = "队列状态"
    description = "获取队列状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            if not queue_name:
                return ActionResult(success=False, message="queue_name is required")

            queues = getattr(context, "message_queues", {})
            if queue_name not in queues:
                return ActionResult(success=False, message=f"Queue {queue_name} not found")

            q = queues[queue_name]
            return ActionResult(
                success=True,
                data={
                    "queue_name": queue_name,
                    "type": q["type"],
                    "message_count": len(q["messages"]),
                    "max_size": q["max_size"],
                    "stats": q["stats"],
                },
                message=f"Queue {queue_name}: {len(q['messages'])} messages",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue status failed: {e}")


class QueuePurgeAction(BaseAction):
    """Purge all messages from queue."""
    action_type = "queue_purge"
    display_name = "清空队列"
    description = "清空队列所有消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            queue_name = params.get("queue_name", "")
            if not queue_name:
                return ActionResult(success=False, message="queue_name is required")

            queues = getattr(context, "message_queues", {})
            if queue_name not in queues:
                return ActionResult(success=False, message=f"Queue {queue_name} not found")

            count = len(queues[queue_name]["messages"])
            queues[queue_name]["messages"].clear()

            return ActionResult(
                success=True,
                data={"queue_name": queue_name, "purged_count": count},
                message=f"Purged {count} messages from {queue_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Queue purge failed: {e}")
