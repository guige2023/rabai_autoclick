"""Telegram bot action module for RabAI AutoClick.

Provides Telegram Bot API operations for sending messages and media.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TelegramSendAction(BaseAction):
    """Send messages via Telegram Bot API."""
    action_type = "telegram_send"
    display_name = "Telegram发送"
    description = "Telegram机器人消息发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Telegram message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - bot_token: Telegram bot token
                - chat_id: Target chat ID
                - text: Message text
                - parse_mode: 'Markdown' or 'HTML'
                - reply_to_message_id: Optional reply ID

        Returns:
            ActionResult with message result.
        """
        bot_token = params.get('bot_token', '') or os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = params.get('chat_id', '')
        text = params.get('text', '')
        parse_mode = params.get('parse_mode', '')
        reply_to = params.get('reply_to_message_id', '')

        if not bot_token:
            return ActionResult(success=False, message="bot_token is required")
        if not chat_id:
            return ActionResult(success=False, message="chat_id is required")
        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload: Dict[str, Any] = {'chat_id': chat_id, 'text': text}
            if parse_mode:
                payload['parse_mode'] = parse_mode
            if reply_to:
                payload['reply_to_message_id'] = reply_to
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="Telegram message sent",
                data={
                    'message_id': data.get('result', {}).get('message_id'),
                    'chat_id': chat_id,
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Telegram error: {str(e)}")
