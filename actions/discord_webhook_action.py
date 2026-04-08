"""Discord webhook action module for RabAI AutoClick.

Provides Discord webhook operations for sending messages to channels.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DiscordWebhookAction(BaseAction):
    """Send messages to Discord channels via webhook."""
    action_type = "discord_webhook"
    display_name = "Discord消息"
    description = "Discord Webhook消息发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Discord webhook message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: Discord webhook URL
                - content: Message content
                - username: Override webhook username
                - avatar_url: Override avatar URL
                - embeds: Optional embed objects

        Returns:
            ActionResult with send result.
        """
        webhook_url = params.get('webhook_url', '')
        content = params.get('content', '')
        username = params.get('username', '')
        avatar_url = params.get('avatar_url', '')
        embeds = params.get('embeds', [])

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")
        if not content:
            return ActionResult(success=False, message="content is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            payload: Dict[str, Any] = {'content': content}
            if username:
                payload['username'] = username
            if avatar_url:
                payload['avatar_url'] = avatar_url
            if embeds:
                payload['embeds'] = embeds
            response = requests.post(webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Discord message sent",
                data={'status': response.status_code}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Discord error: {str(e)}")
