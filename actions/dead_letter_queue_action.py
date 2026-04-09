"""Dead letter queue action module for RabAI AutoClick.

Provides DLQ operations:
- DLQReader: Read messages from dead letter queues
- DLQProcessor: Process and handle dead letter messages
- DLQRequeue: Requeue messages from DLQ to source queue
- DLQManager: Manage DLQ configuration
- DLQMonitor: Monitor DLQ health and metrics
"""

from __future__ import annotations

import json
import sys
import os
import time
import hashlib
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class DLQReaderAction(BaseAction):
    """Read messages from dead letter queues."""
    action_type = "dlq_reader"
    display_name = "DLQ读取"
    description = "读取死信队列中的消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dlq_path = params.get("dlq_path", "/tmp/dead_letter_queue")
            queue_name = params.get("queue_name", "default")
            limit = params.get("limit", 100)
            since = params.get("since", None)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            queue_dir = os.path.join(dlq_path, queue_name)
            if not os.path.exists(queue_dir):
                return ActionResult(success=True, message="DLQ empty", data={"messages": [], "count": 0})

            messages = []
            for filename in sorted(os.listdir(queue_dir)):
                if not filename.endswith(".json"):
                    continue
                filepath = os.path.join(queue_dir, filename)
                with open(filepath) as f:
                    msg = json.load(f)

                if since:
                    msg_time = datetime.fromisoformat(msg.get("failed_at", ""))
                    since_dt = datetime.fromisoformat(since) if isinstance(since, str) else since
                    if msg_time < since_dt:
                        continue

                messages.append(msg)
                if len(messages) >= limit:
                    break

            return ActionResult(
                success=True,
                message=f"Read {len(messages)} DLQ messages",
                data={"messages": messages, "count": len(messages), "queue": queue_name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DLQProcessorAction(BaseAction):
    """Process and handle dead letter messages."""
    action_type = "dlq_processor"
    display_name = "DLQ处理"
    description = "处理死信队列消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dlq_path = params.get("dlq_path", "/tmp/dead_letter_queue")
            queue_name = params.get("queue_name", "default")
            max_retries = params.get("max_retries", 3)
            handler_fn = params.get("handler_fn", "discard")
            message_id = params.get("message_id", None)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            if handler_fn == "discard":
                if message_id:
                    msg_file = os.path.join(dlq_path, queue_name, f"{message_id}.json")
                    if os.path.exists(msg_file):
                        os.remove(msg_file)
                        return ActionResult(success=True, message=f"Discarded: {message_id}")

            elif handler_fn == "retry":
                if not message_id:
                    return ActionResult(success=False, message="message_id required for retry handler")

                msg_file = os.path.join(dlq_path, queue_name, f"{message_id}.json")
                if not os.path.exists(msg_file):
                    return ActionResult(success=False, message=f"Message not found: {message_id}")

                with open(msg_file) as f:
                    msg = json.load(f)

                retry_count = msg.get("retry_count", 0) + 1
                if retry_count >= max_retries:
                    msg["final_failure"] = True
                    msg["failed_at"] = datetime.now().isoformat()
                    with open(msg_file, "w") as f:
                        json.dump(msg, f, indent=2)
                    return ActionResult(success=False, message=f"Max retries reached: {message_id}", data={"retry_count": retry_count})

                msg["retry_count"] = retry_count
                msg["last_retry_at"] = datetime.now().isoformat()
                msg["status"] = "retry_scheduled"

                with open(msg_file, "w") as f:
                    json.dump(msg, f, indent=2)

                return ActionResult(
                    success=True,
                    message=f"Scheduled retry {retry_count}/{max_retries}: {message_id}",
                    data={"message_id": message_id, "retry_count": retry_count}
                )

            elif handler_fn == "archive":
                archive_path = params.get("archive_path", os.path.join(dlq_path, "archive"))
                if not message_id:
                    return ActionResult(success=False, message="message_id required for archive handler")

                msg_file = os.path.join(dlq_path, queue_name, f"{message_id}.json")
                if not os.path.exists(msg_file):
                    return ActionResult(success=False, message=f"Message not found: {message_id}")

                os.makedirs(archive_path, exist_ok=True)
                archive_file = os.path.join(archive_path, f"{message_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")

                with open(msg_file) as f:
                    msg = json.load(f)
                msg["archived_at"] = datetime.now().isoformat()

                with open(archive_file, "w") as f:
                    json.dump(msg, f, indent=2)

                os.remove(msg_file)

                return ActionResult(success=True, message=f"Archived: {message_id}", data={"archive_file": archive_file})

            else:
                return ActionResult(success=False, message=f"Unknown handler: {handler_fn}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DLQRequeueAction(BaseAction):
    """Requeue messages from DLQ to source queue."""
    action_type = "dlq_requeue"
    display_name = "DLQ重新入队"
    description = "将死信队列消息重新入队"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dlq_path = params.get("dlq_path", "/tmp/dead_letter_queue")
            queue_name = params.get("queue_name", "default")
            source_queue = params.get("source_queue", queue_name)
            target_queue = params.get("target_queue", source_queue)
            message_ids = params.get("message_ids", None)
            all_messages = params.get("all_messages", False)
            delay_seconds = params.get("delay_seconds", 0)
            redis_url = params.get("redis_url", "redis://localhost:6379/0")
            use_redis = params.get("use_redis", False) and REDIS_AVAILABLE

            queue_dir = os.path.join(dlq_path, queue_name)
            if not os.path.exists(queue_dir):
                return ActionResult(success=False, message="DLQ empty")

            requeued = []
            failed = []

            if all_messages:
                message_ids = [f.replace(".json", "") for f in os.listdir(queue_dir) if f.endswith(".json")]
            elif message_ids is None:
                return ActionResult(success=False, message="message_ids or all_messages=true required")

            for msg_id in message_ids:
                msg_file = os.path.join(queue_dir, f"{msg_id}.json")
                if not os.path.exists(msg_file):
                    failed.append(msg_id)
                    continue

                with open(msg_file) as f:
                    msg = json.load(f)

                msg["requeued_at"] = datetime.now().isoformat()
                msg["requeued_to"] = target_queue
                msg["original_queue"] = source_queue

                target_dir = os.path.join(dlq_path, target_queue)
                os.makedirs(target_dir, exist_ok=True)

                target_file = os.path.join(target_dir, f"{msg_id}.json")
                with open(target_file, "w") as f:
                    json.dump(msg, f, indent=2)

                os.remove(msg_file)
                requeued.append(msg_id)

            return ActionResult(
                success=True,
                message=f"Requeued {len(requeued)} messages to {target_queue}",
                data={"requeued": requeued, "failed": failed, "count": len(requeued)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DLQManagerAction(BaseAction):
    """Manage DLQ configuration."""
    action_type = "dlq_manager"
    display_name = "DLQ管理"
    description = "管理死信队列配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "status")
            dlq_path = params.get("dlq_path", "/tmp/dead_letter_queue")
            queue_name = params.get("queue_name", "")
            config = params.get("config", {})

            os.makedirs(dlq_path, exist_ok=True)
            config_file = os.path.join(dlq_path, "_dlq_config.json")

            dlq_config = {}
            if os.path.exists(config_file):
                with open(config_file) as f:
                    dlq_config = json.load(f)

            if operation == "configure":
                if not queue_name:
                    return ActionResult(success=False, message="queue_name required")

                dlq_config[queue_name] = {
                    "max_retries": config.get("max_retries", 3),
                    "retry_delay_seconds": config.get("retry_delay_seconds", 60),
                    "archive_after_days": config.get("archive_after_days", 7),
                    "dead_letter_threshold": config.get("dead_letter_threshold", 5),
                    "notification_webhook": config.get("notification_webhook", ""),
                    "updated_at": datetime.now().isoformat(),
                }

                with open(config_file, "w") as f:
                    json.dump(dlq_config, f, indent=2)

                return ActionResult(success=True, message=f"Configured DLQ: {queue_name}")

            elif operation == "status":
                queues = []
                for qname in os.listdir(dlq_path):
                    qdir = os.path.join(dlq_path, qname)
                    if os.path.isdir(qdir) and not qname.startswith("_"):
                        msg_count = len([f for f in os.listdir(qdir) if f.endswith(".json")])
                        queues.append({
                            "queue": qname,
                            "message_count": msg_count,
                            "config": dlq_config.get(qname, {}),
                        })

                total_msgs = sum(q["message_count"] for q in queues)
                return ActionResult(
                    success=True,
                    message=f"DLQ Status: {total_msgs} total messages across {len(queues)} queues",
                    data={"queues": queues, "total_messages": total_msgs}
                )

            elif operation == "clear":
                if not queue_name:
                    return ActionResult(success=False, message="queue_name required")

                queue_dir = os.path.join(dlq_path, queue_name)
                if not os.path.exists(queue_dir):
                    return ActionResult(success=False, message=f"Queue not found: {queue_name}")

                import shutil
                count = len(os.listdir(queue_dir))
                shutil.rmtree(queue_dir)
                os.makedirs(queue_dir)

                return ActionResult(success=True, message=f"Cleared {count} messages from {queue_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class DLQMonitorAction(BaseAction):
    """Monitor DLQ health and metrics."""
    action_type = "dlq_monitor"
    display_name = "DLQ监控"
    description = "监控死信队列健康状况"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dlq_path = params.get("dlq_path", "/tmp/dead_letter_queue")
            alert_threshold = params.get("alert_threshold", 10)

            if not os.path.exists(dlq_path):
                return ActionResult(success=True, message="DLQ not initialized", data={"healthy": True, "total_messages": 0})

            queues = []
            total_msgs = 0
            alert_queues = []

            for qname in os.listdir(dlq_path):
                qdir = os.path.join(dlq_path, qname)
                if os.path.isdir(qdir) and not qname.startswith("_"):
                    msgs = [f for f in os.listdir(qdir) if f.endswith(".json")]
                    msg_count = len(msgs)
                    total_msgs += msg_count

                    oldest_msg_age = None
                    if msgs:
                        oldest = sorted(msgs)[0]
                        with open(os.path.join(qdir, oldest)) as f:
                            msg_data = json.load(f)
                            failed_at = msg_data.get("failed_at", "")
                            if failed_at:
                                age = (datetime.now() - datetime.fromisoformat(failed_at)).total_seconds()
                                oldest_msg_age = age

                    queues.append({
                        "queue": qname,
                        "message_count": msg_count,
                        "oldest_message_age_seconds": oldest_msg_age,
                    })

                    if msg_count >= alert_threshold:
                        alert_queues.append(qname)

            healthy = len(alert_queues) == 0
            health_status = "HEALTHY" if healthy else "ALERT"

            return ActionResult(
                success=healthy,
                message=f"DLQ Health: {health_status} - {total_msgs} total messages",
                data={
                    "health_status": health_status,
                    "total_messages": total_msgs,
                    "queues": queues,
                    "alert_queues": alert_queues,
                    "alert_threshold": alert_threshold,
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
