"""Slack action module for RabAI AutoClick.

Provides Slack API operations including messaging, channel management,
and webhook-based notifications.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SlackClient:
    """Slack API client with bot token authentication.
    
    Provides methods for sending messages, managing channels,
    and interacting with the Slack Web API.
    """
    
    API_BASE = "https://slack.com/api"
    
    def __init__(self, token: str) -> None:
        """Initialize Slack client.
        
        Args:
            token: Slack bot token (xoxb-...) or user token (xoxp-...).
        """
        self.token = token
    
    def _headers(self) -> Dict[str, str]:
        """Build request headers with authentication."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def _request(
        self,
        method: str,
        path: str,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make an authenticated API request.
        
        Args:
            method: HTTP method (GET or POST).
            path: API path (e.g., '/chat.postMessage').
            data: Optional request body data.
            
        Returns:
            Parsed JSON response from Slack API.
            
        Raises:
            Exception: If the API request fails or returns an error.
        """
        url = f"{self.API_BASE}{path}"
        body = json.dumps(data).encode("utf-8") if data else None
        
        req = Request(
            url,
            data=body,
            headers=self._headers(),
            method=method
        )
        
        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                
                if not result.get("ok", False):
                    raise Exception(f"Slack API error: {result.get('error', 'Unknown error')}")
                
                return result
        
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            try:
                error_data = json.loads(error_body)
                raise Exception(f"Slack API HTTP error: {error_data.get('error', str(e))}")
            except json.JSONDecodeError:
                raise Exception(f"Slack API HTTP error: {error_body[:500]}")
    
    def chat_post_message(
        self,
        channel: str,
        text: str,
        username: Optional[str] = None,
        icon_emoji: Optional[str] = None,
        thread_ts: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        blocks: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Send a message to a Slack channel.
        
        Args:
            channel: Channel ID or name (e.g., '#general' or 'C123456').
            text: Message text (supports Slack Markdown-like formatting).
            username: Override bot username.
            icon_emoji: Override bot icon (e.g., ':robot_face:').
            thread_ts: Thread timestamp to reply in a thread.
            attachments: Optional list of attachment dictionaries.
            blocks: Optional list of Slack block kit blocks.
            
        Returns:
            API response with message timestamp.
        """
        data: Dict[str, Any] = {
            "channel": channel,
            "text": text
        }
        
        if username:
            data["username"] = username
        if icon_emoji:
            data["icon_emoji"] = icon_emoji
        if thread_ts:
            data["thread_ts"] = thread_ts
        if attachments:
            data["attachments"] = attachments
        if blocks:
            data["blocks"] = blocks
        
        return self._request("POST", "/chat.postMessage", data=data)
    
    def chat_update(
        self,
        channel: str,
        ts: str,
        text: str,
        attachments: Optional[List[Dict[str, Any]]] = None,
        blocks: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Update an existing message.
        
        Args:
            channel: Channel ID where the message exists.
            ts: Timestamp of the message to update.
            text: New message text.
            attachments: Optional new attachments.
            blocks: Optional new block kit blocks.
            
        Returns:
            API response confirming the update.
        """
        data: Dict[str, Any] = {
            "channel": channel,
            "ts": ts,
            "text": text
        }
        
        if attachments:
            data["attachments"] = attachments
        if blocks:
            data["blocks"] = blocks
        
        return self._request("POST", "/chat.update", data=data)
    
    def chat_delete(self, channel: str, ts: str) -> Dict[str, Any]:
        """Delete a message.
        
        Args:
            channel: Channel ID where the message exists.
            ts: Timestamp of the message to delete.
            
        Returns:
            API response confirming deletion.
        """
        return self._request("POST", "/chat.delete", data={
            "channel": channel,
            "ts": ts
        })
    
    def conversations_list(
        self,
        types: str = "public_channel,private_channel",
        limit: int = 100,
        cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """List all channels the bot has access to.
        
        Args:
            types: Comma-separated channel types ('public_channel', 'private_channel', 'im', 'mpim').
            limit: Maximum number of channels to return.
            cursor: Pagination cursor for next page.
            
        Returns:
            API response with channel list and pagination info.
        """
        data: Dict[str, Any] = {
            "types": types,
            "limit": limit
        }
        if cursor:
            data["cursor"] = cursor
        
        return self._request("POST", "/conversations.list", data=data)
    
    def conversations_info(self, channel: str) -> Dict[str, Any]:
        """Get information about a channel.
        
        Args:
            channel: Channel ID.
            
        Returns:
            Channel information.
        """
        return self._request("POST", "/conversations.info", data={
            "channel": channel
        })
    
    def users_list(self, limit: int = 100, cursor: Optional[str] = None) -> Dict[str, Any]:
        """List all users in the workspace.
        
        Args:
            limit: Maximum number of users to return.
            cursor: Pagination cursor for next page.
            
        Returns:
            API response with user list and pagination info.
        """
        data: Dict[str, Any] = {"limit": limit}
        if cursor:
            data["cursor"] = cursor
        
        return self._request("POST", "/users.list", data=data)
    
    def users_info(self, user: str) -> Dict[str, Any]:
        """Get information about a user.
        
        Args:
            user: User ID.
            
        Returns:
            User information.
        """
        return self._request("POST", "/users.info", data={"user": user})
    
    def files_upload(
        self,
        file_path: Optional[str] = None,
        content: Optional[str] = None,
        filename: Optional[str] = None,
        channel: Optional[str] = None,
        title: Optional[str] = None,
        initial_comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """Upload a file to Slack.
        
        Args:
            file_path: Path to the file to upload.
            content: File content as string (alternative to file_path).
            filename: Filename to use in Slack.
            channel: Optional channel to share the file to.
            title: File title.
            initial_comment: Optional comment when sharing.
            
        Returns:
            API response with file information.
        """
        if not file_path and not content:
            raise ValueError("Either file_path or content is required")
        
        if file_path and not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        from urllib.parse import urlencode
        
        if file_path:
            with open(file_path, "rb") as f:
                file_content = f.read()
            actual_filename = filename or os.path.basename(file_path)
        else:
            file_content = content.encode("utf-8") if content else b""
            actual_filename = filename or "file"
        
        boundary = "----SlackBoundary" + str(int(time.time()))
        
        body_parts = []
        
        body_parts.append(f"--{boundary}\r\n".encode("utf-8"))
        body_parts.append(
            f'Content-Disposition: form-data; name="filename"\r\n\r\n{actual_filename}\r\n'.encode("utf-8")
        )
        
        if title:
            body_parts.append(f"--{boundary}\r\n".encode("utf-8"))
            body_parts.append(
                f'Content-Disposition: form-data; name="title"\r\n\r\n{title}\r\n'.encode("utf-8")
            )
        
        if channel:
            body_parts.append(f"--{boundary}\r\n".encode("utf-8"))
            body_parts.append(
                f'Content-Disposition: form-data; name="channels"\r\n\r\n{channel}\r\n'.encode("utf-8")
            )
        
        if initial_comment:
            body_parts.append(f"--{boundary}\r\n".encode("utf-8"))
            body_parts.append(
                f'Content-Disposition: form-data; name="initial_comment"\r\n\r\n{initial_comment}\r\n'.encode("utf-8")
            )
        
        body_parts.append(f"--{boundary}\r\n".encode("utf-8"))
        body_parts.append(
            f'Content-Disposition: form-data; name="file"; filename="{actual_filename}"\r\n'.encode("utf-8")
        )
        body_parts.append(b"Content-Type: application/octet-stream\r\n\r\n")
        body_parts.append(file_content)
        body_parts.append(f"\r\n--{boundary}--\r\n".encode("utf-8"))
        
        body = b"".join(body_parts)
        
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}"
        }
        
        req = Request(
            f"{self.API_BASE}/files.upload",
            data=body,
            headers=headers,
            method="POST"
        )
        
        try:
            with urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
                
                if not result.get("ok", False):
                    raise Exception(f"Slack API error: {result.get('error', 'Unknown error')}")
                
                return result
        
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            raise Exception(f"Slack API error: {error_body[:500]}")


class SlackWebhookClient:
    """Slack webhook client for simple notifications.
    
    Provides a lightweight way to send messages via incoming webhooks
    without requiring a bot token.
    """
    
    def __init__(self, webhook_url: str) -> None:
        """Initialize Slack webhook client.
        
        Args:
            webhook_url: Full webhook URL from Slack app configuration.
        """
        self.webhook_url = webhook_url
    
    def send(
        self,
        text: str,
        username: Optional[str] = None,
        icon_emoji: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        blocks: Optional[List[Dict[str, Any]]] = None
    ) -> bool:
        """Send a message via webhook.
        
        Args:
            text: Message text.
            username: Override webhook username.
            icon_emoji: Override webhook icon.
            attachments: Optional attachment list.
            blocks: Optional block kit blocks.
            
        Returns:
            True if message sent successfully.
        """
        payload: Dict[str, Any] = {"text": text}
        
        if username:
            payload["username"] = username
        if icon_emoji:
            payload["icon_emoji"] = icon_emoji
        if attachments:
            payload["attachments"] = attachments
        if blocks:
            payload["blocks"] = blocks
        
        body = json.dumps(payload).encode("utf-8")
        
        req = Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        
        try:
            with urlopen(req, timeout=10) as response:
                return response.status == 200
        except Exception:
            return False


class SlackAction(BaseAction):
    """Slack action for messaging and channel management.
    
    Supports bot token and webhook authentication modes.
    """
    action_type: str = "slack"
    display_name: str = "Slack动作"
    description: str = "Slack消息发送和频道管理，支持机器人令牌和Webhook"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[SlackClient] = None
        self._webhook_client: Optional[SlackWebhookClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Slack operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "webhook":
                return self._webhook(params, start_time)
            elif operation == "send":
                return self._send_message(params, start_time)
            elif operation == "update":
                return self._update_message(params, start_time)
            elif operation == "delete":
                return self._delete_message(params, start_time)
            elif operation == "list_channels":
                return self._list_channels(params, start_time)
            elif operation == "channel_info":
                return self._channel_info(params, start_time)
            elif operation == "list_users":
                return self._list_users(params, start_time)
            elif operation == "upload":
                return self._upload_file(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Slack operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Slack with bot token."""
        token = params.get("token", "")
        
        if not token:
            return ActionResult(
                success=False,
                message="Slack token is required",
                duration=time.time() - start_time
            )
        
        self._client = SlackClient(token=token)
        
        try:
            result = self._client.users_list(limit=1)
            team_info = result.get("team_id", "unknown")
            
            return ActionResult(
                success=True,
                message=f"Connected to Slack (team: {team_info})",
                data={"team_id": team_info},
                duration=time.time() - start_time
            )
        except Exception as e:
            self._client = None
            return ActionResult(
                success=False,
                message=f"Failed to connect: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _webhook(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send message via webhook."""
        webhook_url = params.get("webhook_url", "")
        text = params.get("text", "")
        
        if not webhook_url or not text:
            return ActionResult(
                success=False,
                message="webhook_url and text are required",
                duration=time.time() - start_time
            )
        
        client = SlackWebhookClient(webhook_url=webhook_url)
        success = client.send(
            text=text,
            username=params.get("username"),
            icon_emoji=params.get("icon_emoji"),
            attachments=params.get("attachments"),
            blocks=params.get("blocks")
        )
        
        return ActionResult(
            success=success,
            message="Message sent via webhook" if success else "Failed to send webhook message",
            duration=time.time() - start_time
        )
    
    def _require_client(self) -> SlackClient:
        """Ensure a Slack client exists."""
        if not self._client:
            raise RuntimeError("Not connected to Slack. Use 'connect' operation first.")
        return self._client
    
    def _send_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a message to a channel."""
        client = self._require_client()
        channel = params.get("channel", "")
        text = params.get("text", "")
        
        if not channel or not text:
            return ActionResult(
                success=False,
                message="channel and text are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.chat_post_message(
                channel=channel,
                text=text,
                username=params.get("username"),
                icon_emoji=params.get("icon_emoji"),
                thread_ts=params.get("thread_ts"),
                attachments=params.get("attachments"),
                blocks=params.get("blocks")
            )
            
            return ActionResult(
                success=True,
                message=f"Message sent to {channel}",
                data={
                    "channel": result.get("channel"),
                    "ts": result.get("ts")
                },
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to send message: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _update_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update an existing message."""
        client = self._require_client()
        channel = params.get("channel", "")
        ts = params.get("ts", "")
        text = params.get("text", "")
        
        if not channel or not ts or not text:
            return ActionResult(
                success=False,
                message="channel, ts, and text are required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.chat_update(
                channel=channel,
                ts=ts,
                text=text,
                attachments=params.get("attachments"),
                blocks=params.get("blocks")
            )
            
            return ActionResult(
                success=True,
                message=f"Message updated in {channel}",
                data={"ts": result.get("ts")},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to update message: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _delete_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a message."""
        client = self._require_client()
        channel = params.get("channel", "")
        ts = params.get("ts", "")
        
        if not channel or not ts:
            return ActionResult(
                success=False,
                message="channel and ts are required",
                duration=time.time() - start_time
            )
        
        try:
            client.chat_delete(channel=channel, ts=ts)
            
            return ActionResult(
                success=True,
                message=f"Message deleted from {channel}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to delete message: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_channels(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all accessible channels."""
        client = self._require_client()
        
        try:
            result = client.conversations_list(
                types=params.get("types", "public_channel,private_channel"),
                limit=params.get("limit", 100)
            )
            
            channels = result.get("channels", [])
            
            return ActionResult(
                success=True,
                message=f"Found {len(channels)} channels",
                data={"channels": channels, "count": len(channels)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list channels: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _channel_info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get channel information."""
        client = self._require_client()
        channel = params.get("channel", "")
        
        if not channel:
            return ActionResult(
                success=False,
                message="channel is required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.conversations_info(channel=channel)
            
            return ActionResult(
                success=True,
                message=f"Retrieved info for channel",
                data=result.get("channel", {}),
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to get channel info: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_users(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List workspace users."""
        client = self._require_client()
        
        try:
            result = client.users_list(limit=params.get("limit", 100))
            
            members = result.get("members", [])
            
            return ActionResult(
                success=True,
                message=f"Found {len(members)} users",
                data={"users": members, "count": len(members)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list users: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _upload_file(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Upload a file to Slack."""
        client = self._require_client()
        file_path = params.get("file_path")
        content = params.get("content")
        filename = params.get("filename")
        channel = params.get("channel")
        
        if not file_path and not content:
            return ActionResult(
                success=False,
                message="file_path or content is required",
                duration=time.time() - start_time
            )
        
        try:
            result = client.files_upload(
                file_path=file_path,
                content=content,
                filename=filename,
                channel=channel,
                title=params.get("title"),
                initial_comment=params.get("initial_comment")
            )
            
            file_info = result.get("file", {})
            
            return ActionResult(
                success=True,
                message=f"Uploaded file: {file_info.get('name', 'unknown')}",
                data=file_info,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to upload file: {str(e)}",
                duration=time.time() - start_time
            )
