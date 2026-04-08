"""Webhook action module for RabAI AutoClick.

Provides webhook utilities:
- WebhookSender: Send webhooks
- WebhookServer: Receive webhooks
- WebhookRouter: Route webhooks
"""

from typing import Any, Callable, Dict, List, Optional, Dict
import threading
import json
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebhookSender:
    """Send webhooks to endpoints."""

    def __init__(self, timeout: float = 10.0):
        self.timeout = timeout
        self._hooks: Dict[str, Dict[str, Any]] = {}

    def register(self, name: str, url: str, secret: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> str:
        """Register a webhook."""
        hook_id = str(uuid.uuid4())
        self._hooks[name] = {
            "id": hook_id,
            "url": url,
            "secret": secret,
            "headers": headers or {},
        }
        return hook_id

    def unregister(self, name: str) -> bool:
        """Unregister a webhook."""
        if name in self._hooks:
            del self._hooks[name]
            return True
        return False

    def send(self, name: str, payload: Dict[str, Any], event: str = "event") -> Dict[str, Any]:
        """Send webhook (simulated)."""
        if name not in self._hooks:
            return {"success": False, "error": "Webhook not found"}

        hook = self._hooks[name]

        headers = dict(hook["headers"])
        headers["Content-Type"] = "application/json"
        headers["X-Webhook-Event"] = event
        headers["X-Webhook-ID"] = hook["id"]

        return {
            "success": True,
            "hook_id": hook["id"],
            "event": event,
            "url": hook["url"],
            "payload": payload,
            "sent_at": time.time(),
        }

    def list_hooks(self) -> List[Dict[str, Any]]:
        """List all webhooks."""
        return [
            {"name": name, "id": h["id"], "url": h["url"]}
            for name, h in self._hooks.items()
        ]


class WebhookRouter:
    """Route incoming webhooks to handlers."""

    def __init__(self):
        self._routes: Dict[str, Callable] = {}

    def register(self, event: str, handler: Callable) -> None:
        """Register a handler for an event."""
        self._routes[event] = handler

    def route(self, event: str, payload: Dict[str, Any]) -> Any:
        """Route a webhook to its handler."""
        handler = self._routes.get(event)
        if not handler:
            return None
        return handler(payload)


class WebhookAction(BaseAction):
    """Webhook management action."""
    action_type = "webhook"
    display_name = "Webhook管理"
    description = "Webhook发送"

    def __init__(self):
        super().__init__()
        self._sender = WebhookSender()
        self._router = WebhookRouter()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "send")

            if operation == "register":
                return self._register(params)
            elif operation == "unregister":
                return self._unregister(params)
            elif operation == "send":
                return self._send(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Webhook error: {str(e)}")

    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a webhook."""
        name = params.get("name")
        url = params.get("url")
        secret = params.get("secret")
        headers = params.get("headers")

        if not name or not url:
            return ActionResult(success=False, message="name and url are required")

        hook_id = self._sender.register(name, url, secret, headers)

        return ActionResult(success=True, message=f"Webhook registered: {name}", data={"hook_id": hook_id})

    def _unregister(self, params: Dict[str, Any]) -> ActionResult:
        """Unregister a webhook."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        success = self._sender.unregister(name)

        return ActionResult(success=success, message="Unregistered" if success else "Webhook not found")

    def _send(self, params: Dict[str, Any]) -> ActionResult:
        """Send a webhook."""
        name = params.get("name")
        payload = params.get("payload", {})
        event = params.get("event", "event")

        if not name:
            return ActionResult(success=False, message="name is required")

        result = self._sender.send(name, payload, event)

        return ActionResult(success=result["success"], message=result.get("error", "Sent"), data=result)

    def _list(self) -> ActionResult:
        """List all webhooks."""
        hooks = self._sender.list_hooks()

        return ActionResult(success=True, message=f"{len(hooks)} webhooks", data={"hooks": hooks})
