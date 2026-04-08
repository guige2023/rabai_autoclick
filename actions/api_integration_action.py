"""API integration action module for RabAI AutoClick.

Provides API integration operations:
- GraphQLAction: GraphQL client operations
- WebhookHandlerAction: Webhook receiving and processing
- ApiRateLimitAction: Rate limit handling
- ApiKeyRotationAction: API key rotation
"""

import hashlib
import hmac
import json
import time
import urllib.request
import urllib.parse
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphQLAction(BaseAction):
    """GraphQL client operations."""
    action_type = "graphql"
    display_name = "GraphQL客户端"
    description = "GraphQL查询和变更操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            query = params.get("query", "")
            variables = params.get("variables", {})
            operation_name = params.get("operation_name", None)
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")
            if not query:
                return ActionResult(success=False, message="query is required")

            payload: Dict[str, Any] = {"query": query}
            if variables:
                payload["variables"] = variables
            if operation_name:
                payload["operationName"] = operation_name

            req = urllib.request.Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                method="POST",
            )
            req.add_header("Content-Type", "application/json")
            for key, value in headers.items():
                req.add_header(key, value)

            with urllib.request.urlopen(req, timeout=timeout) as response:
                content = response.read().decode("utf-8")
                result = json.loads(content)

            if "errors" in result:
                return ActionResult(
                    success=False,
                    message=f"GraphQL errors: {result['errors']}",
                    data={"errors": result["errors"]},
                )

            return ActionResult(
                success=True,
                message="GraphQL query executed",
                data={"data": result.get("data", {}), "raw": result},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL error: {e}")


class WebhookHandlerAction(BaseAction):
    """Webhook receiving and processing."""
    action_type = "webhook_handler"
    display_name = "Webhook处理器"
    description = "接收和处理Webhook请求"

    def __init__(self):
        super().__init__()
        self._webhooks: Dict[str, Callable] = {}
        self._webhook_history: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "register")
            webhook_id = params.get("webhook_id", "default")
            event_type = params.get("event_type", "*")
            secret = params.get("secret", None)

            if action == "register":
                self._webhooks[webhook_id] = {
                    "event_type": event_type,
                    "secret": secret,
                    "registered_at": time.time(),
                }
                return ActionResult(
                    success=True,
                    message=f"Webhook {webhook_id} registered for {event_type}",
                    data={"webhook_id": webhook_id, "event_type": event_type},
                )

            elif action == "trigger":
                payload = params.get("payload", {})
                verify = params.get("verify_signature", True)

                webhook = self._webhooks.get(webhook_id, {})
                webhook_secret = webhook.get("secret")

                if verify and webhook_secret and secret:
                    signature = params.get("signature", "")
                    expected = self._compute_signature(webhook_secret, json.dumps(payload))
                    if signature != expected:
                        return ActionResult(success=False, message="Signature verification failed")

                entry = {
                    "webhook_id": webhook_id,
                    "payload": payload,
                    "timestamp": time.time(),
                    "verified": verify and (not webhook_secret or signature == self._compute_signature(webhook_secret, json.dumps(payload))),
                }
                self._webhook_history.append(entry)

                return ActionResult(
                    success=True,
                    message=f"Webhook {webhook_id} triggered",
                    data={"entry": entry, "webhook_id": webhook_id},
                )

            elif action == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._webhooks)} webhooks registered",
                    data={"webhooks": list(self._webhooks.keys()), "history_count": len(self._webhook_history)},
                )

            elif action == "history":
                limit = params.get("limit", 100)
                return ActionResult(
                    success=True,
                    message=f"{len(self._webhook_history)} webhook events",
                    data={"history": self._webhook_history[-limit:]},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"WebhookHandler error: {e}")

    def _compute_signature(self, secret: str, payload: str) -> str:
        return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


class ApiRateLimitAction(BaseAction):
    """Rate limit handling and throttling."""
    action_type = "api_rate_limit"
    display_name = "API限流"
    description = "API限流处理和节流"

    def __init__(self):
        super().__init__()
        self._buckets: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "check")
            bucket_id = params.get("bucket_id", "default")
            tokens = params.get("tokens", 1)
            capacity = params.get("capacity", 100)
            refill_rate = params.get("refill_rate", 10)
            refill_period = params.get("refill_period", 60)

            if action == "check":
                if bucket_id not in self._buckets:
                    self._buckets[bucket_id] = {
                        "tokens": capacity,
                        "last_refill": time.time(),
                        "capacity": capacity,
                        "refill_rate": refill_rate,
                        "refill_period": refill_period,
                    }

                bucket = self._buckets[bucket_id]
                self._refill_bucket(bucket)

                if bucket["tokens"] >= tokens:
                    bucket["tokens"] -= tokens
                    return ActionResult(
                        success=True,
                        message="Rate limit check passed",
                        data={"tokens_remaining": bucket["tokens"], "bucket_id": bucket_id},
                    )
                else:
                    wait_time = (tokens - bucket["tokens"]) / bucket["refill_rate"] * bucket["refill_period"]
                    return ActionResult(
                        success=False,
                        message=f"Rate limit exceeded, wait {wait_time:.2f}s",
                        data={"tokens_remaining": bucket["tokens"], "wait_seconds": wait_time},
                    )

            elif action == "status":
                if bucket_id not in self._buckets:
                    return ActionResult(success=True, message="Bucket not initialized", data={"tokens": capacity, "capacity": capacity})
                bucket = self._buckets[bucket_id]
                self._refill_bucket(bucket)
                return ActionResult(
                    success=True,
                    message=f"Bucket {bucket_id}: {bucket['tokens']}/{bucket['capacity']} tokens",
                    data={"tokens": bucket["tokens"], "capacity": bucket["capacity"], "refill_rate": bucket["refill_rate"]},
                )

            elif action == "reset":
                if bucket_id in self._buckets:
                    del self._buckets[bucket_id]
                return ActionResult(success=True, message=f"Bucket {bucket_id} reset")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"ApiRateLimit error: {e}")

    def _refill_bucket(self, bucket: Dict):
        now = time.time()
        elapsed = now - bucket["last_refill"]
        refill_period = bucket["refill_period"]
        tokens_to_add = (elapsed / refill_period) * bucket["refill_rate"]
        bucket["tokens"] = min(bucket["capacity"], bucket["tokens"] + tokens_to_add)
        bucket["last_refill"] = now


class ApiKeyRotationAction(BaseAction):
    """API key rotation management."""
    action_type = "api_key_rotation"
    display_name = "API密钥轮换"
    description = "API密钥轮换管理"

    def __init__(self):
        super().__init__()
        self._key_store: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "register")
            key_id = params.get("key_id", "")
            api_key = params.get("api_key", "")
            service = params.get("service", "default")

            if action == "register":
                self._key_store[key_id] = {
                    "api_key": api_key,
                    "service": service,
                    "created_at": time.time(),
                    "last_rotated": None,
                    "rotation_count": 0,
                    "active": True,
                }
                return ActionResult(
                    success=True,
                    message=f"API key {key_id} registered for {service}",
                    data={"key_id": key_id, "service": service},
                )

            elif action == "rotate":
                new_key = params.get("new_key", "")
                if not new_key:
                    import secrets
                    new_key = secrets.token_urlsafe(32)

                if key_id in self._key_store:
                    self._key_store[key_id]["api_key"] = new_key
                    self._key_store[key_id]["last_rotated"] = time.time()
                    self._key_store[key_id]["rotation_count"] += 1
                else:
                    return ActionResult(success=False, message=f"Key {key_id} not found")

                return ActionResult(
                    success=True,
                    message=f"Key {key_id} rotated",
                    data={"key_id": key_id, "new_key": new_key, "rotation_count": self._key_store[key_id]["rotation_count"]},
                )

            elif action == "get":
                if key_id in self._key_store:
                    key_info = {**self._key_store[key_id]}
                    key_info["api_key"] = "***" + key_info["api_key"][-4:] if len(key_info["api_key"]) > 4 else "***"
                    return ActionResult(success=True, message=f"Key {key_id} retrieved", data={"key": key_info})
                return ActionResult(success=False, message=f"Key {key_id} not found")

            elif action == "list":
                keys = [
                    {"key_id": k, "service": v["service"], "active": v["active"], "rotation_count": v["rotation_count"]}
                    for k, v in self._key_store.items()
                ]
                return ActionResult(success=True, message=f"{len(keys)} keys", data={"keys": keys})

            elif action == "deactivate":
                if key_id in self._key_store:
                    self._key_store[key_id]["active"] = False
                return ActionResult(success=True, message=f"Key {key_id} deactivated")

            elif action == "activate":
                if key_id in self._key_store:
                    self._key_store[key_id]["active"] = True
                return ActionResult(success=True, message=f"Key {key_id} activated")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"ApiKeyRotation error: {e}")
