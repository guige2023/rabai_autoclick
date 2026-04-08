"""Slack message action module for RabAI AutoClick.

Provides Slack API operations for sending messages and files.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SlackSendAction(BaseAction):
    """Send messages and files to Slack channels."""
    action_type = "slack_send"
    display_name = "Slack发送消息"
    description = "Slack消息发送"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Send Slack message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - bot_token: Slack bot token
                - channel: Channel ID or name
                - text: Message text
                - blocks: Optional Slack blocks
                - thread_ts: Optional thread timestamp

        Returns:
            ActionResult with message result.
        """
        bot_token = params.get('bot_token') or os.environ.get('SLACK_BOT_TOKEN')
        channel = params.get('channel', '')
        text = params.get('text', '')
        blocks = params.get('blocks', None)
        thread_ts = params.get('thread_ts', None)

        if not bot_token:
            return ActionResult(success=False, message="SLACK_BOT_TOKEN is required")
        if not channel:
            return ActionResult(success=False, message="channel is required")
        if not text:
            return ActionResult(success=False, message="text is required")

        try:
            from slack_sdk import WebClient
            from slack_sdk.errors import SlackApiError
        except ImportError:
            return ActionResult(success=False, message="slack-sdk not installed. Run: pip install slack-sdk")

        client = WebClient(token=bot_token)
        start = time.time()
        try:
            kwargs: Dict[str, Any] = {'channel': channel, 'text': text}
            if blocks:
                kwargs['blocks'] = blocks
            if thread_ts:
                kwargs['thread_ts'] = thread_ts
            response = client.chat_postMessage(**kwargs)
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Message sent to {channel}",
                data={
                    'ts': response.get('ts'),
                    'channel': response.get('channel'),
                },
                duration=duration
            )
        except SlackApiError as e:
            return ActionResult(success=False, message=f"Slack API error: {e.response['error']}")
        except Exception as e:
            return ActionResult(success=False, message=f"Slack error: {str(e)}")
