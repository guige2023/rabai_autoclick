"""Webhook dispatcher action module for RabAI AutoClick.

Provides webhook operations:
- WebhookDispatchAction: Dispatch webhook to multiple endpoints
- WebhookFilterAction: Filter webhook payloads
- WebhookTransformAction: Transform webhook payloads
- WebhookRetryAction: Retry failed webhook deliveries
"""

import json
import hashlib
import hmac
import time
import urllib.request
import urllib.parse
import urllib.error
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class WebhookDelivery:
    """Represents a webhook delivery attempt."""
    delivery_id: str
    url: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    status: str = "pending"
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    response_code: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None


class WebhookDeliveryStore:
    """Store for webhook deliveries."""
    def __init__(self):
        self._deliveries: Dict[str, WebhookDelivery] = {}

    def add(self, delivery: WebhookDelivery) -> str:
        self._deliveries[delivery.delivery_id] = delivery
        return delivery.delivery_id

    def get(self, delivery_id: str) -> Optional[WebhookDelivery]:
        return self._deliveries.get(delivery_id)

    def update(self, delivery_id: str, **kwargs) -> bool:
        if delivery_id in self._deliveries:
            for k, v in kwargs.items():
                if hasattr(self._deliveries[delivery_id], k):
                    setattr(self._deliveries[delivery_id], k, v)
            return True
        return False

    def get_failed(self) -> List[WebhookDelivery]:
        return [d for d in self._deliveries.values() if d.status == "failed"]

    def get_pending(self) -> List[WebhookDelivery]:
        return [d for d in self._deliveries.values() if d.status == "pending"]


_store = WebhookDeliveryStore()


class WebhookDispatchAction(BaseAction):
    """Dispatch webhook to one or more endpoints."""
    action_type = "webhook_dispatch"
    display_name = "Webhook发送"
    description = "向多个端点发送Webhook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            urls = params.get("urls", [])
            payload = params.get("payload", {})
            headers = params.get("headers", {"Content-Type": "application/json"})
            secret = params.get("secret", None)
            method = params.get("method", "POST")
            timeout = params.get("timeout", 30)

            if not urls:
                return ActionResult(success=False, message="urls are required")
            if not payload:
                return ActionResult(success=False, message="payload is required")

            results = []
            for url in urls:
                delivery_id = str(uuid.uuid4())

                delivery_headers = dict(headers)
                delivery_headers["X-Webhook-ID"] = delivery_id
                delivery_headers["X-Webhook-Timestamp"] = str(int(time.time()))

                if secret:
                    signature = hmac.new(
                        secret.encode(),
                        json.dumps(payload).encode(),
                        hashlib.sha256
                    ).hexdigest()
                    delivery_headers["X-Webhook-Signature"] = f"sha256={signature}"

                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(url, data=data, headers=delivery_headers, method=method)

                try:
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        response_body = response.read().decode("utf-8")
                        _store.add(WebhookDelivery(
                            delivery_id=delivery_id,
                            url=url,
                            payload=payload,
                            headers=delivery_headers,
                            status="delivered",
                            attempts=1,
                            last_attempt=datetime.utcnow(),
                            response_code=response.getcode(),
                            response_body=response_body[:500]
                        ))
                        results.append({
                            "delivery_id": delivery_id,
                            "url": url,
                            "status": "delivered",
                            "response_code": response.getcode()
                        })
                except urllib.error.HTTPError as e:
                    error_body = e.read().decode("utf-8")[:500] if e.fp else ""
                    _store.add(WebhookDelivery(
                        delivery_id=delivery_id,
                        url=url,
                        payload=payload,
                        headers=delivery_headers,
                        status="failed",
                        attempts=1,
                        last_attempt=datetime.utcnow(),
                        response_code=e.code,
                        error=error_body
                    ))
                    results.append({
                        "delivery_id": delivery_id,
                        "url": url,
                        "status": "failed",
                        "response_code": e.code,
                        "error": error_body
                    })
                except urllib.error.URLError as e:
                    _store.add(WebhookDelivery(
                        delivery_id=delivery_id,
                        url=url,
                        payload=payload,
                        headers=delivery_headers,
                        status="failed",
                        attempts=1,
                        last_attempt=datetime.utcnow(),
                        error=str(e.reason)
                    ))
                    results.append({
                        "delivery_id": delivery_id,
                        "url": url,
                        "status": "failed",
                        "error": str(e.reason)
                    })

            failed = [r for r in results if r["status"] == "failed"]
            succeeded = [r for r in results if r["status"] == "delivered"]

            return ActionResult(
                success=len(failed) == 0,
                message=f"Webhook dispatched: {len(succeeded)} succeeded, {len(failed)} failed",
                data={"results": results, "succeeded": len(succeeded), "failed": len(failed)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook dispatch failed: {str(e)}")


class WebhookFilterAction(BaseAction):
    """Filter webhook payloads based on conditions."""
    action_type = "webhook_filter"
    display_name = "Webhook过滤"
    description = "根据条件过滤Webhook载荷"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            payload = params.get("payload", {})
            filters = params.get("filters", [])
            filter_mode = params.get("filter_mode", "include")

            if not payload:
                return ActionResult(success=False, message="payload is required")
            if not filters:
                return ActionResult(success=True, message="No filters applied", data=payload)

            filtered = {}
            for f in filters:
                field_path = f.get("field", "")
                operator = f.get("operator", "==")
                value = f.get("value", None)
                inverse = f.get("inverse", False)

                parts = field_path.split(".")
                current = payload
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        current = None
                        break

                match = False
                if current is not None:
                    if operator == "==":
                        match = current == value
                    elif operator == "!=":
                        match = current != value
                    elif operator == ">":
                        match = current > value
                    elif operator == "<":
                        match = current < value
                    elif operator == "contains":
                        match = str(value) in str(current)
                    elif operator == "exists":
                        match = True

                if filter_mode == "include":
                    if (match and not inverse) or (not match and inverse):
                        filtered[field_path] = current
                else:
                    if not match:
                        filtered[field_path] = current

            return ActionResult(
                success=True,
                message=f"Filtered payload with {len(filters)} conditions",
                data={"filtered": filtered, "original": payload}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook filter failed: {str(e)}")


class WebhookTransformAction(BaseAction):
    """Transform webhook payloads."""
    action_type = "webhook_transform"
    display_name = "Webhook转换"
    description = "转换Webhook载荷"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            payload = params.get("payload", {})
            field_mappings = params.get("field_mappings", {})
            computed_fields = params.get("computed_fields", [])
            remove_fields = params.get("remove_fields", [])

            transformed = dict(payload)

            for old_field, new_field in field_mappings.items():
                if old_field in transformed:
                    transformed[new_field] = transformed.pop(old_field)

            for cf in computed_fields:
                field_name = cf.get("field", "")
                expression = cf.get("expression", "")
                try:
                    transformed[field_name] = eval(expression, {"__builtins__": {}}, {"payload": transformed})
                except Exception:
                    transformed[field_name] = None

            for rf in remove_fields:
                parts = rf.split(".")
                current = transformed
                for part in parts[:-1]:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        break
                else:
                    if isinstance(current, dict) and parts[-1] in current:
                        del current[parts[-1]]

            return ActionResult(
                success=True,
                message=f"Transformed webhook payload",
                data={"transformed": transformed, "original": payload}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook transform failed: {str(e)}")


class WebhookRetryAction(BaseAction):
    """Retry failed webhook deliveries."""
    action_type = "webhook_retry"
    display_name = "Webhook重试"
    description = "重试失败的Webhook投递"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_attempts = params.get("max_attempts", 3)
            backoff_multiplier = params.get("backoff_multiplier", 2)
            delivery_ids = params.get("delivery_ids", None)

            if delivery_ids:
                deliveries = [_store.get(did) for did in delivery_ids if _store.get(did)]
            else:
                deliveries = _store.get_failed()

            results = []
            for delivery in deliveries:
                if delivery.attempts >= max_attempts:
                    results.append({
                        "delivery_id": delivery.delivery_id,
                        "status": "skipped",
                        "reason": f"Max attempts ({max_attempts}) reached"
                    })
                    continue

                delay = (backoff_multiplier ** delivery.attempts)
                headers = dict(delivery.headers)
                headers["X-Webhook-Retry"] = str(delivery.attempts + 1)

                data = json.dumps(delivery.payload).encode("utf-8")
                req = urllib.request.Request(delivery.url, data=data, headers=headers, method="POST")

                try:
                    with urllib.request.urlopen(req, timeout=30) as response:
                        _store.update(delivery.delivery_id,
                            status="delivered",
                            attempts=delivery.attempts + 1,
                            last_attempt=datetime.utcnow(),
                            response_code=response.getcode()
                        )
                        results.append({
                            "delivery_id": delivery.delivery_id,
                            "status": "delivered",
                            "attempts": delivery.attempts + 1
                        })
                except Exception as ex:
                    _store.update(delivery.delivery_id,
                        status="failed",
                        attempts=delivery.attempts + 1,
                        last_attempt=datetime.utcnow(),
                        error=str(ex)
                    )
                    results.append({
                        "delivery_id": delivery.delivery_id,
                        "status": "failed",
                        "error": str(ex)
                    })

            succeeded = [r for r in results if r["status"] == "delivered"]
            return ActionResult(
                success=len(succeeded) == len(results),
                message=f"Retry complete: {len(succeeded)}/{len(results)} succeeded",
                data={"results": results}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook retry failed: {str(e)}")
