"""DingTalk webhook action module for RabAI AutoClick.

Provides DingTalk custom chatbot operations for sending messages.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DingTalkWebhookAction(BaseAction):
    """Send messages to DingTalk group via custom webhook."""
    action_type = "dingtalk_webhook"
    display_name = "钉钉消息"
    description = "钉钉群机器人消息"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send DingTalk webhook message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: DingTalk webhook URL
                - secret: Optional webhook secret for signature
                - msg_type: Message type (text, markdown, link, etc.)
                - content: Message content (text or markdown)
                - title: Optional title for link/markdown messages
                - message_url: Optional URL for link messages

        Returns:
            ActionResult with send result.
        """
        webhook_url = params.get('webhook_url', '')
        secret = params.get('secret', '')
        msg_type = params.get('msg_type', 'text')
        content = params.get('content', '')
        title = params.get('title', '')
        message_url = params.get('message_url', '')

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")
        if not content:
            return ActionResult(success=False, message="content is required")

        start = time.time()
        try:
            import requests
            import hmac
            import hashlib
            import base64
            import time as time_module
            import urllib.parse

            # Add timestamp and sign if secret is provided
            if secret:
                timestamp = str(int(time_module.time() * 1000))
                sign_str = f"{timestamp}\n{secret}"
                sign = base64.b64encode(
                    hmac.new(secret.encode('utf-8'), sign_str.encode('utf-8'), hashlib.sha256).digest()
                ).decode('utf-8')
                separator = '&' if '?' in webhook_url else '?'
                webhook_url = f"{webhook_url}{separator}timestamp={urllib.parse.quote(timestamp)}&sign={urllib.parse.quote(sign)}"

            # Build message
            if msg_type == 'text':
                payload = {'msgtype': 'text', 'text': {'content': content}}
            elif msg_type == 'markdown':
                payload = {
                    'msgtype': 'markdown',
                    'markdown': {'title': title or content[:50], 'text': content}
                }
            elif msg_type == 'link':
                payload = {
                    'msgtype': 'link',
                    'link': {'text': content, 'title': title, 'messageUrl': message_url}
                }
            else:
                payload = {'msgtype': 'text', 'text': {'content': content}}

            response = requests.post(webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            if data.get('errcode') == 0:
                return ActionResult(success=True, message="DingTalk message sent", data=data, duration=duration)
            else:
                return ActionResult(success=False, message=f"DingTalk error: {data.get('errmsg')}", data=data)
        except Exception as e:
            return ActionResult(success=False, message=f"DingTalk error: {str(e)}")
