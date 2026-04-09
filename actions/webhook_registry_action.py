"""Webhook registry action module for RabAI AutoClick.

Provides webhook management:
- WebhookRegistry: Register and manage webhooks
- WebhookDispatcher: Dispatch events to webhooks
- WebhookHealthMonitor: Monitor webhook health
- WebhookRetryHandler: Handle webhook retries with backoff
- WebhookSignatureValidator: Validate webhook signatures
"""

from __future__ import annotations

import json
import sys
import os
import time
import hmac
import hashlib
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebhookRegistryAction(BaseAction):
    """Register and manage webhooks."""
    action_type = "webhook_registry"
    display_name = "Webhook注册"
    description = "注册和管理Webhook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            registry_path = params.get("registry_path", "/tmp/webhook_registry")
            webhook_url = params.get("webhook_url", "")
            webhook_name = params.get("webhook_name", "")
            events = params.get("events", ["*"])
            secret = params.get("secret", "")
            metadata = params.get("metadata", {})

            os.makedirs(registry_path, exist_ok=True)
            registry_file = os.path.join(registry_path, "registry.json")

            registry = {}
            if os.path.exists(registry_file):
                with open(registry_file) as f:
                    registry = json.load(f)

            if operation == "register":
                if not webhook_url or not webhook_name:
                    return ActionResult(success=False, message="webhook_url and webhook_name required")

                webhook_id = hashlib.sha256(webhook_url.encode()).hexdigest()[:16]
                entry = {
                    "id": webhook_id,
                    "name": webhook_name,
                    "url": webhook_url,
                    "events": events,
                    "secret": secret,
                    "metadata": metadata,
                    "registered_at": datetime.now().isoformat(),
                    "active": True,
                    "consecutive_failures": 0,
                    "last_triggered": None,
                    "last_success": None,
                    "last_failure": None,
                }

                registry[webhook_id] = entry
                with open(registry_file, "w") as f:
                    json.dump(registry, f, indent=2)

                return ActionResult(success=True, message=f"Registered: {webhook_name}", data={"id": webhook_id, "name": webhook_name})

            elif operation == "list":
                active_only = params.get("active_only", False)
                event_filter = params.get("event_filter", None)

                webhooks = list(registry.values())
                if active_only:
                    webhooks = [w for w in webhooks if w.get("active", True)]
                if event_filter:
                    webhooks = [w for w in webhooks if event_filter in w.get("events", []) or "*" in w.get("events", [])]

                return ActionResult(success=True, message=f"{len(webhooks)} webhooks", data={"webhooks": webhooks, "count": len(webhooks)})

            elif operation == "delete":
                if not webhook_name:
                    return ActionResult(success=False, message="webhook_name required")

                to_delete = [k for k, v in registry.items() if v.get("name") == webhook_name]
                for k in to_delete:
                    del registry[k]

                with open(registry_file, "w") as f:
                    json.dump(registry, f, indent=2)

                return ActionResult(success=True, message=f"Deleted {len(to_delete)} webhooks")

            elif operation == "toggle":
                if not webhook_name:
                    return ActionResult(success=False, message="webhook_name required")

                active = params.get("active", True)
                for entry in registry.values():
                    if entry.get("name") == webhook_name:
                        entry["active"] = active

                with open(registry_file, "w") as f:
                    json.dump(registry, f, indent=2)

                return ActionResult(success=True, message=f"Toggled {webhook_name} to {active}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class WebhookDispatcherAction(BaseAction):
    """Dispatch events to registered webhooks."""
    action_type = "webhook_dispatcher"
    display_name = "Webhook触发"
    description = "向Webhook发送事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            registry_path = params.get("registry_path", "/tmp/webhook_registry")
            event = params.get("event", "")
            payload = params.get("payload", {})
            timeout = params.get("timeout", 30)

            registry_file = os.path.join(registry_path, "registry.json")
            if not os.path.exists(registry_file):
                return ActionResult(success=False, message="No webhook registry found")

            with open(registry_file) as f:
                registry = json.load(f)

            matched = []
            for webhook_id, entry in registry.items():
                if not entry.get("active", True):
                    continue
                events = entry.get("events", [])
                if event not in events and "*" not in events:
                    continue
                matched.append(entry)

            results = []
            for webhook in matched:
                url = webhook["url"]
                secret = webhook.get("secret", "")

                body = json.dumps(payload).encode("utf-8")
                headers = {"Content-Type": "application/json"}

                if secret:
                    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
                    headers["X-Webhook-Signature"] = signature

                try:
                    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
                    with urllib.request.urlopen(req, timeout=timeout) as response:
                        status = response.status
                        webhook["last_triggered"] = datetime.now().isoformat()
                        webhook["last_success"] = datetime.now().isoformat()
                        webhook["consecutive_failures"] = 0
                        results.append({"name": webhook["name"], "status": status, "success": True})
                except urllib.error.HTTPError as e:
                    webhook["consecutive_failures"] = webhook.get("consecutive_failures", 0) + 1
                    webhook["last_failure"] = datetime.now().isoformat()
                    results.append({"name": webhook["name"], "status": e.code, "success": False})
                except Exception as e:
                    webhook["consecutive_failures"] = webhook.get("consecutive_failures", 0) + 1
                    webhook["last_failure"] = datetime.now().isoformat()
                    results.append({"name": webhook["name"], "error": str(e), "success": False})

            with open(registry_file, "w") as f:
                json.dump(registry, f, indent=2)

            success_count = sum(1 for r in results if r.get("success"))
            return ActionResult(
                success=True,
                message=f"Dispatched: {success_count}/{len(results)} successful",
                data={"results": results, "total": len(results), "success_count": success_count}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class WebhookHealthMonitorAction(BaseAction):
    """Monitor webhook health status."""
    action_type = "webhook_health_monitor"
    display_name = "Webhook健康监控"
    description = "监控Webhook健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            registry_path = params.get("registry_path", "/tmp/webhook_registry")
            registry_file = os.path.join(registry_path, "registry.json")

            if not os.path.exists(registry_file):
                return ActionResult(success=False, message="No registry found")

            with open(registry_file) as f:
                registry = json.load(f)

            unhealthy = []
            healthy = []
            for entry in registry.values():
                failures = entry.get("consecutive_failures", 0)
                if failures >= 3:
                    unhealthy.append({
                        "name": entry.get("name"),
                        "url": entry.get("url"),
                        "consecutive_failures": failures,
                        "last_failure": entry.get("last_failure"),
                    })
                else:
                    healthy.append(entry.get("name"))

            return ActionResult(
                success=True,
                message=f"Health: {len(healthy)} healthy, {len(unhealthy)} unhealthy",
                data={"healthy": healthy, "unhealthy": unhealthy, "total": len(registry)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
