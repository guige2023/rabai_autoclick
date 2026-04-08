"""Slack Integration Action Module.

Provides Slack messaging, channel management, user lookup,
and webhook-based automation capabilities.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class MessageType(Enum):
    """Slack message type."""
    TEXT = "text"
    BLOCK = "block"
    ATTACHMENT = "attachment"
    EPHEMERAL = "ephemeral"


@dataclass
class SlackUser:
    """Slack user information."""
    id: str
    name: str
    real_name: str
    email: str
    team_id: str
    status: str = "active"
    is_bot: bool = False


@dataclass
class SlackChannel:
    """Slack channel information."""
    id: str
    name: str
    is_private: bool
    topic: str
    purpose: str
    member_count: int
    created_at: float


@dataclass
class SlackMessage:
    """Slack message structure."""
    channel: str
    text: str
    message_type: MessageType
    blocks: List[Dict[str, Any]] = field(default_factory=list)
    attachments: List[Dict[str, Any]] = field(default_factory=list)
    thread_ts: Optional[str] = None
    user: Optional[str] = None
    bot: bool = False


@dataclass
class MessageResult:
    """Result of sending a message."""
    success: bool
    ts: Optional[str]
    channel: str
    message: str
    error: Optional[str] = None
    duration_ms: float = 0.0


class SlackStore:
    """In-memory Slack data store (simulated)."""

    def __init__(self):
        self._users: Dict[str, SlackUser] = {}
        self._channels: Dict[str, SlackChannel] = {}
        self._messages: Dict[str, List[SlackMessage]] = {}
        self._webhooks: Dict[str, Dict[str, Any]] = {}

    def add_user(self, user: SlackUser) -> None:
        """Add user to store."""
        self._users[user.id] = user

    def get_user(self, user_id: str) -> Optional[SlackUser]:
        """Get user by ID."""
        return self._users.get(user_id)

    def get_user_by_email(self, email: str) -> Optional[SlackUser]:
        """Get user by email."""
        for user in self._users.values():
            if user.email == email:
                return user
        return None

    def list_users(self) -> List[SlackUser]:
        """List all users."""
        return list(self._users.values())

    def add_channel(self, channel: SlackChannel) -> None:
        """Add channel to store."""
        self._channels[channel.id] = channel
        self._messages[channel.id] = []

    def get_channel(self, channel_id: str) -> Optional[SlackChannel]:
        """Get channel by ID."""
        return self._channels.get(channel_id)

    def get_channel_by_name(self, name: str) -> Optional[SlackChannel]:
        """Get channel by name."""
        for channel in self._channels.values():
            if channel.name == name:
                return channel
        return None

    def list_channels(self) -> List[SlackChannel]:
        """List all channels."""
        return list(self._channels.values())

    def add_message(self, channel_id: str, message: SlackMessage) -> None:
        """Add message to channel."""
        if channel_id not in self._messages:
            self._messages[channel_id] = []
        self._messages[channel_id].append(message)

    def get_messages(self, channel_id: str, limit: int = 100) -> List[SlackMessage]:
        """Get channel messages."""
        messages = self._messages.get(channel_id, [])
        return messages[-limit:]

    def add_webhook(self, webhook_url: str, channel: str, token: str) -> None:
        """Add incoming webhook."""
        self._webhooks[webhook_url] = {
            "channel": channel,
            "token": token
        }


_global_store = SlackStore()


class SlackAction:
    """Slack integration action.

    Example:
        action = SlackAction()

        action.send_message("#general", "Hello from Aito!")
        action.send_block_message("#alerts", blocks=[...])
        users = action.list_users()
    """

    def __init__(self, store: Optional[SlackStore] = None):
        self._store = store or _global_store
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        """Initialize demo users and channels."""
        if not self._store._users:
            demo_users = [
                SlackUser("U001", "alice", "Alice Chen", "alice@company.com", "T001"),
                SlackUser("U002", "bob", "Bob Smith", "bob@company.com", "T001"),
                SlackUser("U003", "carol", "Carol Wang", "carol@company.com", "T001", is_bot=True),
            ]
            for user in demo_users:
                self._store.add_user(user)

        if not self._store._channels:
            demo_channels = [
                SlackChannel("C001", "general", False, "General discussion", "General chat", 150, time.time()),
                SlackChannel("C002", "alerts", False, "System alerts", "Alerts and notifications", 25, time.time()),
                SlackChannel("C003", "engineering", True, "Engineering team", "Engineering discussions", 50, time.time()),
            ]
            for channel in demo_channels:
                self._store.add_channel(channel)

    def send_message(self, channel: str, text: str,
                     username: Optional[str] = None,
                     icon_emoji: Optional[str] = None) -> MessageResult:
        """Send text message to channel.

        Args:
            channel: Channel name or ID
            text: Message text
            username: Override sender username
            icon_emoji: Override sender icon

        Returns:
            MessageResult with send status
        """
        start = time.time()

        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        if not channel_obj:
            return MessageResult(
                success=False,
                ts=None,
                channel=channel,
                message="",
                error=f"Channel not found: {channel}",
                duration_ms=(time.time() - start) * 1000
            )

        msg = SlackMessage(
            channel=channel_obj.id,
            text=text,
            message_type=MessageType.TEXT,
            bot=username is not None
        )
        self._store.add_message(channel_obj.id, msg)

        return MessageResult(
            success=True,
            ts=f"{int(time.time())}.{uuid.uuid4().hex[:8]}",
            channel=channel,
            message=f"Message sent to {channel}",
            duration_ms=(time.time() - start) * 1000
        )

    def send_block_message(self, channel: str,
                           blocks: List[Dict[str, Any]],
                           text: str = "") -> MessageResult:
        """Send block kit message to channel.

        Args:
            channel: Channel name or ID
            blocks: Block Kit blocks
            text: Fallback text

        Returns:
            MessageResult with send status
        """
        start = time.time()

        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        if not channel_obj:
            return MessageResult(
                success=False,
                ts=None,
                channel=channel,
                message="",
                error=f"Channel not found: {channel}",
                duration_ms=(time.time() - start) * 1000
            )

        msg = SlackMessage(
            channel=channel_obj.id,
            text=text or "Block message",
            message_type=MessageType.BLOCK,
            blocks=blocks
        )
        self._store.add_message(channel_obj.id, msg)

        return MessageResult(
            success=True,
            ts=f"{int(time.time())}.{uuid.uuid4().hex[:8]}",
            channel=channel,
            message=f"Block message sent to {channel}",
            duration_ms=(time.time() - start) * 1000
        )

    def send_reply(self, channel: str, thread_ts: str,
                   text: str) -> MessageResult:
        """Reply to thread.

        Args:
            channel: Channel name or ID
            thread_ts: Parent message timestamp
            text: Reply text

        Returns:
            MessageResult with send status
        """
        start = time.time()

        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        if not channel_obj:
            return MessageResult(
                success=False,
                ts=None,
                channel=channel,
                message="",
                error=f"Channel not found: {channel}",
                duration_ms=(time.time() - start) * 1000
            )

        msg = SlackMessage(
            channel=channel_obj.id,
            text=text,
            message_type=MessageType.TEXT,
            thread_ts=thread_ts
        )
        self._store.add_message(channel_obj.id, msg)

        return MessageResult(
            success=True,
            ts=f"{int(time.time())}.{uuid.uuid4().hex[:8]}",
            channel=channel,
            message=f"Reply sent to thread",
            duration_ms=(time.time() - start) * 1000
        )

    def list_channels(self, include_private: bool = True) -> Dict[str, Any]:
        """List all channels.

        Args:
            include_private: Include private channels

        Returns:
            Dict with channel list
        """
        channels = self._store.list_channels()
        if not include_private:
            channels = [c for c in channels if not c.is_private]
        return {
            "success": True,
            "channels": [
                {
                    "id": c.id,
                    "name": c.name,
                    "is_private": c.is_private,
                    "topic": c.topic,
                    "member_count": c.member_count
                }
                for c in channels
            ],
            "count": len(channels)
        }

    def get_channel(self, channel: str) -> Dict[str, Any]:
        """Get channel info.

        Args:
            channel: Channel name or ID

        Returns:
            Dict with channel info
        """
        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        if channel_obj:
            return {
                "success": True,
                "channel": {
                    "id": channel_obj.id,
                    "name": channel_obj.name,
                    "is_private": channel_obj.is_private,
                    "topic": channel_obj.topic,
                    "purpose": channel_obj.purpose,
                    "member_count": channel_obj.member_count
                }
            }
        return {"success": False, "message": "Channel not found"}

    def list_users(self, include_bots: bool = False) -> Dict[str, Any]:
        """List all users.

        Args:
            include_bots: Include bot users

        Returns:
            Dict with user list
        """
        users = self._store.list_users()
        if not include_bots:
            users = [u for u in users if not u.is_bot]
        return {
            "success": True,
            "users": [
                {
                    "id": u.id,
                    "name": u.name,
                    "real_name": u.real_name,
                    "email": u.email,
                    "status": u.status
                }
                for u in users
            ],
            "count": len(users)
        }

    def get_user(self, user: str) -> Dict[str, Any]:
        """Get user info.

        Args:
            user: User ID, name, or email

        Returns:
            Dict with user info
        """
        user_obj = self._store.get_user(user)
        if not user_obj:
            user_obj = self._store.get_user_by_email(user)
        if not user_obj:
            for u in self._store._users.values():
                if u.name == user or u.real_name == user:
                    user_obj = u
                    break
        if user_obj:
            return {
                "success": True,
                "user": {
                    "id": user_obj.id,
                    "name": user_obj.name,
                    "real_name": user_obj.real_name,
                    "email": user_obj.email,
                    "status": user_obj.status,
                    "is_bot": user_obj.is_bot
                }
            }
        return {"success": False, "message": "User not found"}

    def get_messages(self, channel: str, limit: int = 100) -> Dict[str, Any]:
        """Get channel messages.

        Args:
            channel: Channel name or ID
            limit: Maximum messages to return

        Returns:
            Dict with message list
        """
        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        if not channel_obj:
            return {"success": False, "message": "Channel not found"}

        messages = self._store.get_messages(channel_obj.id, limit)
        return {
            "success": True,
            "channel": channel_obj.name,
            "messages": [
                {
                    "ts": f"{int(messages[i].created_at)}.{i:08d}",
                    "text": m.text,
                    "type": m.message_type.value,
                    "thread_ts": m.thread_ts,
                    "bot": m.bot
                }
                for i, m in enumerate(messages)
            ],
            "count": len(messages)
        }

    def invite_to_channel(self, channel: str, user: str) -> Dict[str, Any]:
        """Invite user to channel.

        Args:
            channel: Channel name or ID
            user: User ID

        Returns:
            Dict with operation result
        """
        channel_obj = self._store.get_channel_by_name(channel.lstrip("#"))
        if not channel_obj:
            channel_obj = self._store.get_channel(channel)
        user_obj = self._store.get_user(user)

        if not channel_obj:
            return {"success": False, "message": "Channel not found"}
        if not user_obj:
            return {"success": False, "message": "User not found"}

        channel_obj.member_count += 1
        return {
            "success": True,
            "message": f"Invited {user_obj.name} to #{channel_obj.name}"
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Slack action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "send", "send_block", "send_reply", "list_channels",
                         "get_channel", "list_users", "get_user", "get_messages",
                         "invite"
            - channel: Channel name or ID
            - text: Message text
            - blocks: Block Kit blocks (for send_block)
            - thread_ts: Thread timestamp (for send_reply)
            - user: User ID, name, or email
            - username: Sender username (for send)
            - icon_emoji: Sender icon (for send)
            - limit: Message limit (for get_messages)
            - include_private: Include private channels
            - include_bots: Include bot users

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "send")
    action = SlackAction()

    try:
        if operation == "send":
            channel = params.get("channel", "")
            text = params.get("text", "")
            if not channel or not text:
                return {"success": False, "message": "channel and text required"}
            result = action.send_message(
                channel=channel,
                text=text,
                username=params.get("username"),
                icon_emoji=params.get("icon_emoji")
            )
            return {
                "success": result.success,
                "ts": result.ts,
                "channel": result.channel,
                "message": result.message,
                "error": result.error,
                "duration_ms": result.duration_ms
            }

        elif operation == "send_block":
            channel = params.get("channel", "")
            blocks = params.get("blocks", [])
            text = params.get("text", "")
            if not channel:
                return {"success": False, "message": "channel required"}
            result = action.send_block_message(channel, blocks, text)
            return {
                "success": result.success,
                "ts": result.ts,
                "channel": result.channel,
                "message": result.message,
                "error": result.error,
                "duration_ms": result.duration_ms
            }

        elif operation == "send_reply":
            channel = params.get("channel", "")
            thread_ts = params.get("thread_ts", "")
            text = params.get("text", "")
            if not channel or not thread_ts or not text:
                return {"success": False, "message": "channel, thread_ts, and text required"}
            result = action.send_reply(channel, thread_ts, text)
            return {
                "success": result.success,
                "ts": result.ts,
                "channel": result.channel,
                "message": result.message,
                "duration_ms": result.duration_ms
            }

        elif operation == "list_channels":
            return action.list_channels(params.get("include_private", True))

        elif operation == "get_channel":
            channel = params.get("channel", "")
            if not channel:
                return {"success": False, "message": "channel required"}
            return action.get_channel(channel)

        elif operation == "list_users":
            return action.list_users(params.get("include_bots", False))

        elif operation == "get_user":
            user = params.get("user", "")
            if not user:
                return {"success": False, "message": "user required"}
            return action.get_user(user)

        elif operation == "get_messages":
            channel = params.get("channel", "")
            limit = params.get("limit", 100)
            if not channel:
                return {"success": False, "message": "channel required"}
            return action.get_messages(channel, limit)

        elif operation == "invite":
            channel = params.get("channel", "")
            user = params.get("user", "")
            if not channel or not user:
                return {"success": False, "message": "channel and user required"}
            return action.invite_to_channel(channel, user)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Slack error: {str(e)}"}
