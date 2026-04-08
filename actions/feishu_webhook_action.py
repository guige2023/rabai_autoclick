"""Feishu webhook action module for RabAI AutoClick.

Provides Feishu (Lark) custom chatbot webhook operations.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FeishuWebhookAction(BaseAction):
    """Send messages to Feishu group via custom webhook."""
    action_type = "feishu_webhook"
    display_name = "飞书消息"
    description = "飞书群机器人Webhook"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Feishu webhook message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: Feishu webhook URL
                - msg_type: Message type (text, post, image, etc.)
                - content: Message content dict
                - secret: Optional signing secret

        Returns:
            ActionResult with send result.
        """
        webhook_url = params.get('webhook_url', '')
        msg_type = params.get('msg_type', 'text')
        content = params.get('content', {})
        secret = params.get('secret', '')

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")

        start = time.time()
        try:
            import requests
            import hmac
            import hashlib
            import time as time_module
            import base64

            # Sign if secret is provided
            if secret:
                timestamp = int(time_module.time())
                string_to_sign = f"{timestamp}\n{secret}"
                sign = base64.b64encode(
                    hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha256).digest()
                ).decode('utf-8')
                webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

            # Build message
            if msg_type == 'text' and isinstance(content, str):
                payload = {'msg_type': 'text', 'content': {'text': content}}
            else:
                payload = {'msg_type': msg_type, 'content': content}

            response = requests.post(webhook_url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            if data.get('code') == 0 or data.get('StatusCode') == 0:
                return ActionResult(success=True, message="Feishu message sent", data=data, duration=duration)
            else:
                return ActionResult(success=False, message=f"Feishu error: {data.get('msg')}", data=data)
        except Exception as e:
            return ActionResult(success=False, message=f"Feishu error: {str(e)}")
