"""Pushover notification action module for RabAI AutoClick.

Provides Pushover API operations for sending push notifications.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PushoverNotifyAction(BaseAction):
    """Send push notifications via Pushover API."""
    action_type = "pushover_notify"
    display_name = "Pushover通知"
    description = "Pushover推送通知"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Pushover notification.

        Args:
            context: Execution context.
            params: Dict with keys:
                - user_key: Pushover user key
                - api_token: Pushover API token
                - message: Notification message
                - title: Optional notification title
                - priority: Priority (-2 to 2)
                - device: Optional device name
                - sound: Optional notification sound

        Returns:
            ActionResult with notification result.
        """
        user_key = params.get('user_key', '')
        api_token = params.get('api_token', '') or os.environ.get('PUSHOVER_API_TOKEN')
        message = params.get('message', '')
        title = params.get('title', 'RabAI AutoClick')
        priority = params.get('priority', 0)
        device = params.get('device', '')
        sound = params.get('sound', '')

        if not user_key or not api_token:
            return ActionResult(success=False, message="user_key and api_token are required")
        if not message:
            return ActionResult(success=False, message="message is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.post(
                'https://api.pushover.net/1/messages.json',
                data={
                    'token': api_token,
                    'user': user_key,
                    'message': message,
                    'title': title,
                    'priority': priority,
                    'device': device,
                    'sound': sound,
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Pushover notification sent",
                data={'request_id': data.get('request')}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pushover error: {str(e)}")
