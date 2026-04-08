"""Slack action module for RabAI AutoClick.

Provides Slack API operations for messaging and workspace management.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SlackAction(BaseAction):
    """Slack API operations.
    
    Supports posting messages, uploading files, managing channels,
    and workspace administration via the Slack API.
    """
    action_type = "slack"
    display_name = "Slack通知"
    description = "Slack消息发送与工作区管理"
    
    def __init__(self) -> None:
        super().__init__()
    
    def _get_slack_client(self):
        """Get Slack client."""
        try:
            from slack_sdk import WebClient
            return WebClient
        except ImportError:
            try:
                import slacker
                return slacker.Slacker
            except ImportError:
                return None
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Slack operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'message', 'upload', 'create_channel', 'invite', 'react', 'info'
                - token: Slack API token (or env SLACK_BOT_TOKEN)
                - channel: Channel name or ID
                - message: Message text
                - file_path: File to upload
                - user: User ID to invite
        
        Returns:
            ActionResult with operation result.
        """
        client_class = self._get_slack_client()
        if client_class is None:
            return ActionResult(
                success=False,
                message="Requires slack-sdk. Install: pip install slack-sdk"
            )
        
        command = params.get('command', 'message')
        token = params.get('token') or os.environ.get('SLACK_BOT_TOKEN')
        channel = params.get('channel')
        message = params.get('message')
        file_path = params.get('file_path')
        user = params.get('user')
        
        if not token:
            return ActionResult(success=False, message="Slack token required")
        
        try:
            if hasattr(client_class, '__init__') and 'WebClient' in str(client_class):
                client = client_class(token=token)
            else:
                client = client_class(token)
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to initialize Slack client: {e}")
        
        if command == 'message':
            if not channel or not message:
                return ActionResult(success=False, message="channel and message required for message")
            return self._slack_message(client, channel, message)
        
        if command == 'upload':
            if not channel or not file_path:
                return ActionResult(success=False, message="channel and file_path required for upload")
            return self._slack_upload(client, channel, file_path)
        
        if command == 'create_channel':
            if not channel:
                return ActionResult(success=False, message="channel name required for create_channel")
            return self._slack_create_channel(client, channel)
        
        if command == 'invite':
            if not channel or not user:
                return ActionResult(success=False, message="channel and user required for invite")
            return self._slack_invite(client, channel, user)
        
        if command == 'react':
            if not channel or not message:
                return ActionResult(success=False, message="channel and message required for react")
            return self._slack_react(client, channel, message, params.get('emoji', 'white_check_mark'))
        
        if command == 'info':
            return self._slack_info(client, channel)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _slack_message(self, client: Any, channel: str, message: str) -> ActionResult:
        """Send Slack message."""
        try:
            response = client.chat_postMessage(channel=channel, text=message)
            ts = response.get('ts', '')
            return ActionResult(
                success=True,
                message=f"Message sent to #{channel}",
                data={'channel': channel, 'ts': ts}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to send message: {e}")
    
    def _slack_upload(self, client: Any, channel: str, file_path: str) -> ActionResult:
        """Upload file to Slack."""
        try:
            import mimetypes
            mime = mimetypes.guess_type(file_path)[0] or 'text/plain'
            response = client.files_upload(channels=channel, file=file_path, filename=os.path.basename(file_path), mimetype=mime)
            file_id = response.get('file', {}).get('id', '')
            return ActionResult(
                success=True,
                message=f"Uploaded {os.path.basename(file_path)} to #{channel}",
                data={'file_id': file_id, 'filename': os.path.basename(file_path)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to upload: {e}")
    
    def _slack_create_channel(self, client: Any, channel: str) -> ActionResult:
        """Create Slack channel."""
        try:
            response = client.conversations_create(name=channel)
            channel_id = response.get('channel', {}).get('id', '')
            return ActionResult(
                success=True,
                message=f"Created channel #{channel}",
                data={'channel_id': channel_id, 'name': channel}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create channel: {e}")
    
    def _slack_invite(self, client: Any, channel: str, user: str) -> ActionResult:
        """Invite user to channel."""
        try:
            channel_id = self._resolve_channel(client, channel)
            client.conversations_invite(channel=channel_id, users=[user])
            return ActionResult(success=True, message=f"Invited {user} to #{channel}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to invite: {e}")
    
    def _slack_react(self, client: Any, channel: str, message: str, emoji: str) -> ActionResult:
        """React to a message with emoji."""
        try:
            response = client.conversations_history(channel=channel, limit=10)
            messages = response.get('messages', [])
            if messages:
                ts = messages[0].get('ts')
                client.reactions_add(channel=channel, timestamp=ts, name=emoji)
                return ActionResult(success=True, message=f"Added {emoji} reaction")
            return ActionResult(success=False, message="No messages found to react to")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to react: {e}")
    
    def _slack_info(self, client: Any, channel: str) -> ActionResult:
        """Get channel info."""
        try:
            channel_id = self._resolve_channel(client, channel)
            info = client.conversations_info(channel=channel_id)
            return ActionResult(
                success=True,
                message=f"Channel info for #{channel}",
                data={'info': info.data}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get info: {e}")
    
    def _resolve_channel(self, client: Any, channel: str) -> str:
        """Resolve channel name to ID."""
        try:
            response = client.conversations_list()
            for c in response.get('channels', []):
                if c.get('name') == channel or c.get('id') == channel:
                    return c.get('id')
            return channel
        except Exception:
            return channel
