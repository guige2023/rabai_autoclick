"""Slack API integration for messaging and workspace operations.

Handles Slack API operations including sending messages to channels/DM,
file uploads, reactions, thread replies, and channel management.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


@dataclass
class SlackConfig:
    """Configuration for Slack API client."""
    bot_token: str
    signing_secret: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class SlackMessage:
    """Represents a Slack message."""
    channel: str
    ts: str
    text: str
    user: Optional[str] = None
    thread_ts: Optional[str] = None
    reactions: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)


@dataclass
class SlackChannel:
    """Represents a Slack channel."""
    id: str
    name: str
    is_channel: bool = True
    is_group: bool = False
    is_im: bool = False
    is_private: bool = False
    num_members: int = 0
    topic: str = ""
    purpose: str = ""


class SlackAPIError(Exception):
    """Raised when Slack API returns an error."""
    def __init__(self, message: str, code: Optional[str] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.code = code
        self.response = response or {}


class SlackAction:
    """Slack API client for messaging and workspace operations."""

    BASE_URL = "https://slack.com/api"

    def __init__(self, config: SlackConfig):
        """Initialize Slack client with bot token.

        Args:
            config: SlackConfig with bot token and settings
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {config.bot_token}",
            "Content-Type": "application/json"
        })

    def _request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make authenticated request to Slack API.

        Args:
            method: HTTP method
            endpoint: API endpoint (e.g., 'chat.postMessage')
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON response

        Raises:
            SlackAPIError: On API error or Slack error response
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        retries = self.config.max_retries

        while retries > 0:
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.config.timeout,
                    **kwargs
                )

                data = response.json()

                if not data.get("ok", False):
                    raise SlackAPIError(
                        message=data.get("error", "Unknown error"),
                        code=data.get("error"),
                        response=data
                    )

                return data

            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise SlackAPIError(f"Request failed: {e}")

    def post_message(self, channel: str, text: str,
                      username: Optional[str] = None,
                      icon_emoji: Optional[str] = None,
                      thread_ts: Optional[str] = None,
                      reply_broadcast: bool = False,
                      unfurl_links: bool = False,
                      blocks: Optional[list[dict]] = None) -> SlackMessage:
        """Send a message to a channel or user.

        Args:
            channel: Channel ID, name, or DM user ID
            text: Message text (may include Slack markdown)
            username: Override bot display name
            icon_emoji: Override bot icon (e.g., ':bot:')
            thread_ts: Parent message ts to reply in thread
            reply_broadcast: Also send to parent channel
            unfurl_links: Auto-unfold URLs
            blocks: Optional Block Kit blocks

        Returns:
            SlackMessage with sent message details
        """
        payload: dict[str, Any] = {
            "channel": channel,
            "text": text
        }

        if username:
            payload["username"] = username

        if icon_emoji:
            payload["icon_emoji"] = icon_emoji

        if thread_ts:
            payload["thread_ts"] = thread_ts
            payload["reply_broadcast"] = reply_broadcast

        if unfurl_links:
            payload["unfurl_links"] = True

        if blocks:
            payload["blocks"] = blocks

        data = self._request("POST", "chat.postMessage", json=payload)

        return SlackMessage(
            channel=data["channel"],
            ts=data["ts"],
            text=text,
            user=data.get("user"),
            thread_ts=thread_ts
        )

    def update_message(self, channel: str, ts: str, text: str,
                       blocks: Optional[list[dict]] = None) -> SlackMessage:
        """Update an existing message.

        Args:
            channel: Channel ID where message exists
            ts: Message timestamp
            text: New message text
            blocks: Optional new Block Kit blocks

        Returns:
            Updated SlackMessage
        """
        payload: dict[str, Any] = {
            "channel": channel,
            "ts": ts,
            "text": text
        }

        if blocks:
            payload["blocks"] = blocks

        data = self._request("POST", "chat.update", json=payload)

        return SlackMessage(
            channel=data["channel"],
            ts=data["ts"],
            text=text
        )

    def delete_message(self, channel: str, ts: str) -> bool:
        """Delete a message.

        Args:
            channel: Channel ID
            ts: Message timestamp

        Returns:
            True if deletion successful
        """
        payload = {"channel": channel, "ts": ts}
        data = self._request("POST", "chat.delete", json=payload)
        return data.get("ok", False)

    def upload_file(self, channels: str, file_path: str,
                    title: Optional[str] = None,
                    initial_comment: Optional[str] = None,
                    thread_ts: Optional[str] = None) -> dict:
        """Upload a file to a channel.

        Args:
            channels: Comma-separated channel IDs
            file_path: Local file path to upload
            title: File title
            initial_comment: Comment on the uploaded file
            thread_ts: Post file in thread

        Returns:
            Upload response with file info
        """
        url = f"{self.BASE_URL}/files.upload"

        with open(file_path, "rb") as f:
            files = {"file": f}
            data_payload: dict[str, Any] = {"channels": channels}

            if title:
                data_payload["title"] = title

            if initial_comment:
                data_payload["initial_comment"] = initial_comment

            if thread_ts:
                data_payload["thread_ts"] = thread_ts

            response = self.session.post(
                url,
                files=files,
                data=data_payload,
                timeout=self.config.timeout
            )

        result = response.json()

        if not result.get("ok", False):
            raise SlackAPIError(
                message=result.get("error", "Upload failed"),
                code=result.get("error"),
                response=result
            )

        return result

    def add_reaction(self, name: str, channel: Optional[str] = None,
                      timestamp: Optional[str] = None,
                      file: Optional[str] = None,
                      file_comment: Optional[str] = None) -> bool:
        """Add a reaction emoji to a message or file.

        Args:
            name: Reaction emoji name (without colons)
            channel: Channel ID for message
            timestamp: Message timestamp
            file: File ID to react to
            file_comment: File comment ID to react to

        Returns:
            True if reaction added successfully
        """
        if file:
            payload = {"name": name, "file": file}
            if file_comment:
                payload["file_comment"] = file_comment
        else:
            payload = {"name": name, "channel": channel, "timestamp": timestamp}

        data = self._request("POST", "reactions.add", json=payload)
        return data.get("ok", False)

    def list_channels(self, types: Optional[list[str]] = None,
                       exclude_archived: bool = True) -> list[SlackChannel]:
        """List all channels in workspace.

        Args:
            types: Filter by channel types (public_channel, private_channel, im, mpim)
            exclude_archived: Exclude archived channels

        Returns:
            List of SlackChannel objects
        """
        payload: dict[str, Any] = {"exclude_archived": exclude_archived}

        if types:
            payload["types"] = ",".join(types)

        data = self._request("GET", "conversations.list", params=payload)

        channels = []
        for item in data.get("channels", []):
            channels.append(SlackChannel(
                id=item.get("id", ""),
                name=item.get("name", ""),
                is_channel=item.get("is_channel", False),
                is_group=item.get("is_group", False),
                is_im=item.get("is_im", False),
                is_private=item.get("is_private", False),
                num_members=item.get("num_members", 0),
                topic=item.get("topic", ""),
                purpose=item.get("purpose", "")
            ))

        return channels

    def get_user_info(self, user_id: str) -> dict:
        """Get user profile information.

        Args:
            user_id: Slack user ID

        Returns:
            User profile data
        """
        return self._request("GET", "users.info", params={"user": user_id})

    def open_dm(self, user_id: str) -> str:
        """Open a direct message channel with a user.

        Args:
            user_id: Slack user ID to DM

        Returns:
            Channel ID of the opened DM
        """
        data = self._request("POST", "conversations.open", json={"users": user_id})
        return data.get("channel", {}).get("id", "")

    def schedule_message(self, channel: str, text: str,
                         post_at: datetime, username: Optional[str] = None,
                         icon_emoji: Optional[str] = None) -> dict:
        """Schedule a message for future delivery.

        Args:
            channel: Channel ID to post to
            text: Message text
            post_at: Datetime to post the message
            username: Override bot display name
            icon_emoji: Override bot icon

        Returns:
            Scheduled message response with ID
        """
        import time

        payload: dict[str, Any] = {
            "channel": channel,
            "text": text,
            "post_at": str(int(post_at.timestamp()))
        }

        if username:
            payload["username"] = username

        if icon_emoji:
            payload["icon_emoji"] = icon_emoji

        return self._request("POST", "chat.scheduleMessage", json=payload)

    def get_channel_history(self, channel: str,
                             oldest: Optional[str] = None,
                             latest: Optional[str] = None,
                             limit: int = 200) -> list[SlackMessage]:
        """Get message history for a channel.

        Args:
            channel: Channel ID
            oldest: Oldest message timestamp
            latest: Latest message timestamp
            limit: Number of messages to fetch (max 1000)

        Returns:
            List of SlackMessage objects
        """
        params: dict[str, Any] = {"channel": channel, "limit": min(limit, 1000)}

        if oldest:
            params["oldest"] = oldest

        if latest:
            params["latest"] = latest

        data = self._request("GET", "conversations.history", params=params)

        messages = []
        for item in data.get("messages", []):
            msg = SlackMessage(
                channel=channel,
                ts=item.get("ts", ""),
                text=item.get("text", ""),
                user=item.get("user"),
                thread_ts=item.get("thread_ts")
            )

            if "reactions" in item:
                msg.reactions = [r.get("name") for r in item["reactions"]]

            if "files" in item:
                msg.files = [f.get("id") for f in item["files"]]

            messages.append(msg)

        return messages
