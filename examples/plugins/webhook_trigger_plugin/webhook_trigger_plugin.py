"""
Webhook Trigger Plugin implementation.
"""

import time
import hashlib
import hmac
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from ....src.plugin_system import PluginBase, PluginAPI, TriggerType


class WebhookTriggerPlugin(PluginBase):
    """Plugin that handles webhook triggers for workflows."""
    
    name = "webhook_trigger_plugin"
    version = "1.0.0"
    description = "Triggers workflows via incoming webhooks"
    author = "RabAI Team"
    
    def __init__(self, api: PluginAPI):
        super().__init__(api)
        self._webhook_count = 0
        self._webhook_handlers: Dict[str, Dict[str, Any]] = {}
        self._webhook_history: list = []
        self._registered_routes: list = []
    
    def on_load(self) -> bool:
        """Initialize the webhook trigger plugin."""
        self._api.logger.info(f"Loading {self.name} v{self.version}")
        
        # Register custom actions
        self._api.register_action(
            action_type="register_webhook_handler",
            handler=self._register_webhook_handler,
            description="Register a custom webhook endpoint handler"
        )
        
        self._api.register_action(
            action_type="send_webhook",
            handler=self._send_webhook_handler,
            description="Send an outgoing webhook request"
        )
        
        # Register the main webhook trigger
        default_route = self._api.get_config("default_route", "/webhook/rabai")
        self._api.register_trigger(
            trigger_type=TriggerType.WEBHOOK,
            handler=self._webhook_handler,
            config={
                "route": default_route,
                "method": "POST"
            }
        )
        
        self._registered_routes.append(default_route)
        
        return True
    
    def on_unload(self) -> bool:
        """Clean up when plugin is unloaded."""
        self._api.logger.info(f"Unloading {self.name}")
        
        # Unregister actions
        self._api.unregister_action("register_webhook_handler")
        self._api.unregister_action("send_webhook")
        
        return True
    
    def on_enable(self) -> None:
        """Called when plugin is enabled."""
        self._api.logger.info(f"{self.name} enabled")
    
    def on_disable(self) -> None:
        """Called when plugin is disabled."""
        self._api.logger.info(f"{self.name} disabled")
    
    def _webhook_handler(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handler for incoming webhooks."""
        self._webhook_count += 1
        
        webhook_id = self._webhook_count
        route = event_data.get("route", "unknown")
        method = event_data.get("method", "UNKNOWN")
        data = event_data.get("data", {})
        timestamp = event_data.get("webhook_time", time.time())
        
        webhook_record = {
            "id": webhook_id,
            "route": route,
            "method": method,
            "data": data,
            "timestamp": timestamp,
            "processed": False
        }
        
        self._api.logger.info(f"Received webhook #{webhook_id}: {method} {route}")
        
        # Find and invoke registered handlers for this route
        for handler_info in self._webhook_handlers.values():
            if handler_info["route"] == route:
                try:
                    result = handler_info["handler"](data)
                    webhook_record["processed"] = True
                    webhook_record["handler_result"] = result
                    self._api.logger.info(f"Webhook #{webhook_id} processed by handler")
                except Exception as e:
                    webhook_record["error"] = str(e)
                    self._api.logger.error(f"Error processing webhook #{webhook_id}: {e}")
        
        # Emit event for workflow triggers
        self._api.emit_event("webhook_received", webhook_record)
        
        # Store in history
        self._webhook_history.append(webhook_record)
        
        return {
            "success": True,
            "webhook_id": webhook_id,
            "route": route,
            "processed": webhook_record["processed"]
        }
    
    def _register_webhook_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the register_webhook_handler action."""
        route = kwargs.get("route", "/webhook/custom")
        handler_func = kwargs.get("handler")
        description = kwargs.get("description", "")
        
        if handler_func is None:
            return {
                "success": False,
                "error": "handler function is required"
            }
        
        handler_id = hashlib.md5(f"{route}_{time.time()}".encode()).hexdigest()[:8]
        
        self._webhook_handlers[handler_id] = {
            "id": handler_id,
            "route": route,
            "handler": handler_func,
            "description": description,
            "registered_at": time.time()
        }
        
        self._api.logger.info(f"Registered webhook handler '{handler_id}' for route {route}")
        
        return {
            "success": True,
            "handler_id": handler_id,
            "route": route
        }
    
    def _send_webhook_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the send_webhook action."""
        url = kwargs.get("url", "")
        method = kwargs.get("method", "POST")
        headers = kwargs.get("headers", {})
        body = kwargs.get("body", {})
        
        if not url:
            return {
                "success": False,
                "error": "URL is required"
            }
        
        # Parse URL to validate
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return {
                    "success": False,
                    "error": f"Invalid URL: {url}"
                }
        except Exception as e:
            return {
                "success": False,
                "error": f"Invalid URL: {e}"
            }
        
        # In a real implementation, this would make an HTTP request
        # For demo purposes, we just log it
        self._api.logger.info(f"Would send webhook to {url} ({method})")
        
        return {
            "success": True,
            "message": f"Webhook would be sent to {url}",
            "url": url,
            "method": method,
            "headers": headers,
            "body": body,
            "timestamp": time.time()
        }
    
    def execute(self, action: str, **kwargs) -> Dict[str, Any]:
        """Execute a custom action provided by this plugin."""
        if action == "register_webhook_handler":
            return self._register_webhook_handler(**kwargs)
        elif action == "send_webhook":
            return self._send_webhook_handler(**kwargs)
        elif action == "get_history":
            return {
                "success": True,
                "count": self._webhook_count,
                "history": self._webhook_history[-100:]  # Last 100 webhooks
            }
        elif action == "get_routes":
            return {
                "success": True,
                "routes": self._registered_routes
            }
        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}"
            }


def register() -> WebhookTriggerPlugin:
    """Register the webhook trigger plugin."""
    def _create_plugin(api: PluginAPI) -> WebhookTriggerPlugin:
        return WebhookTriggerPlugin(api)
    return _create_plugin
