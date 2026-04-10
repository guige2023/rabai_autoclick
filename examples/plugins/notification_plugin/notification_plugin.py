"""
Notification Plugin implementation.
"""

import time
from typing import Any, Dict, Optional

from ....src.plugin_system import PluginBase, PluginAPI, TriggerType


class NotificationPlugin(PluginBase):
    """Plugin that sends notifications for workflow events."""
    
    name = "notification_plugin"
    version = "1.0.0"
    description = "Sends system notifications for workflow events"
    author = "RabAI Team"
    
    def __init__(self, api: PluginAPI):
        super().__init__(api)
        self._notification_count = 0
        self._notification_history: list = []
        self._urgency_default = "normal"
    
    def on_load(self) -> bool:
        """Initialize the notification plugin."""
        self._api.logger.info(f"Loading {self.name} v{self.version}")
        
        # Register custom action
        self._api.register_action(
            action_type="send_notification",
            handler=self._send_notification_handler,
            description="Send a system notification",
            schema={
                "title": {"type": "string", "required": True, "description": "Notification title"},
                "message": {"type": "string", "required": True, "description": "Notification message"},
                "urgency": {
                    "type": "string",
                    "required": False,
                    "default": "normal",
                    "enum": ["low", "normal", "critical"],
                    "description": "Urgency level"
                }
            }
        )
        
        # Register workflow event triggers
        self._api.register_trigger(
            trigger_type=TriggerType.WORKFLOW_EVENT,
            handler=self._workflow_event_handler,
            config={
                "events": ["workflow_complete", "workflow_error", "workflow_start"]
            }
        )
        
        # Load config
        self._urgency_default = self._api.get_config("urgency_default", "normal")
        
        return True
    
    def on_unload(self) -> bool:
        """Clean up when plugin is unloaded."""
        self._api.logger.info(f"Unloading {self.name}")
        
        # Unregister action
        self._api.unregister_action("send_notification")
        
        return True
    
    def on_enable(self) -> None:
        """Called when plugin is enabled."""
        self._api.logger.info(f"{self.name} enabled")
    
    def on_disable(self) -> None:
        """Called when plugin is disabled."""
        self._api.logger.info(f"{self.name} disabled")
    
    def _send_notification_handler(self, **kwargs) -> Dict[str, Any]:
        """Handler for the send_notification action."""
        title = kwargs.get("title", "Notification")
        message = kwargs.get("message", "")
        urgency = kwargs.get("urgency", self._urgency_default)
        
        self._notification_count += 1
        
        notification = {
            "id": self._notification_count,
            "title": title,
            "message": message,
            "urgency": urgency,
            "timestamp": time.time(),
            "plugin": self.name
        }
        
        self._notification_history.append(notification)
        
        # Log the notification
        urgency_emoji = {
            "low": "ℹ️",
            "normal": "🔔",
            "critical": "🚨"
        }
        emoji = urgency_emoji.get(urgency, "🔔")
        self._api.logger.info(f"{emoji} Notification [{urgency}]: {title} - {message}")
        
        # Emit event
        self._api.emit_event("notification_sent", notification)
        
        return {
            "success": True,
            "notification_id": self._notification_count,
            "title": title,
            "message": message,
            "urgency": urgency,
            "total_notifications": self._notification_count
        }
    
    def _workflow_event_handler(self, event_data: Dict[str, Any]) -> None:
        """Handler for workflow events."""
        event_type = event_data.get("type", "unknown")
        workflow_id = event_data.get("workflow_id", "unknown")
        
        # Map workflow events to notifications
        notification_map = {
            "workflow_complete": ("Workflow Complete", f"Workflow {workflow_id} completed successfully"),
            "workflow_error": ("Workflow Error", f"Workflow {workflow_id} encountered an error"),
            "workflow_start": ("Workflow Started", f"Workflow {workflow_id} has started")
        }
        
        if event_type in notification_map:
            title, message = notification_map[event_type]
            self._send_notification_handler(
                title=title,
                message=message,
                urgency="normal" if event_type == "workflow_complete" else "critical"
            )
    
    def execute(self, action: str, **kwargs) -> Dict[str, Any]:
        """Execute a custom action provided by this plugin."""
        if action == "send_notification":
            return self._send_notification_handler(**kwargs)
        elif action == "get_history":
            return {
                "success": True,
                "count": self._notification_count,
                "history": self._notification_history[-50:]  # Last 50 notifications
            }
        elif action == "clear_history":
            self._notification_history.clear()
            return {
                "success": True,
                "message": "Notification history cleared"
            }
        else:
            return {
                "success": False,
                "error": f"Unknown action: {action}"
            }


def register() -> NotificationPlugin:
    """Register the notification plugin."""
    def _create_plugin(api: PluginAPI) -> NotificationPlugin:
        return NotificationPlugin(api)
    return _create_plugin
