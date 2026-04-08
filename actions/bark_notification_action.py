"""Bark notification action module for RabAI AutoClick.

Provides Bark push notification service for iOS.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class BarkNotifyAction(BaseAction):
    """Send push notifications via Bark server."""
    action_type = "bark_notify"
    display_name = "Bark通知"
    description = "Bark iOS推送通知"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Bark notification.

        Args:
            context: Execution context.
            params: Dict with keys:
                - server_url: Bark server URL (e.g. https://api.day.app/your_key)
                - title: Notification title
                - body: Notification body
                - sound: Notification sound name
                - icon: Optional icon URL
                - group: Optional group name
                - url: Optional URL to open

        Returns:
            ActionResult with notification result.
        """
        server_url = params.get('server_url', '')
        title = params.get('title', 'RabAI')
        body = params.get('body', '')
        sound = params.get('sound', 'alarm')
        icon = params.get('icon', '')
        group = params.get('group', 'RabAI AutoClick')
        url = params.get('url', '')

        if not server_url:
            return ActionResult(success=False, message="server_url is required")
        if not body:
            return ActionResult(success=False, message="body is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            import urllib.parse
            params_encoded = []
            if title:
                params_encoded.append(f"title={urllib.parse.quote(title)}")
            if body:
                params_encoded.append(f"body={urllib.parse.quote(body)}")
            if sound:
                params_encoded.append(f"sound={urllib.parse.quote(sound)}")
            if icon:
                params_encoded.append(f"icon={urllib.parse.quote(icon)}")
            if group:
                params_encoded.append(f"group={urllib.parse.quote(group)}")
            if url:
                params_encoded.append(f"url={urllib.parse.quote(url)}")
            full_url = f"{server_url.rstrip('/')}/{'/'.join(params_encoded)}"
            response = requests.get(full_url, timeout=30)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Bark notification sent",
                data={'response': data}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Bark error: {str(e)}")
