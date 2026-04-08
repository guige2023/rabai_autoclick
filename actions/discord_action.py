"""Discord action module for RabAI AutoClick.

Provides integration with Discord API for server management,
messaging, and webhook automation.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DiscordAction(BaseAction):
    """Discord API integration for messaging and server management.

    Supports sending messages, managing channels, webhook operations,
    and server member management.

    Args:
        config: Discord configuration containing bot_token
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.bot_token = self.config.get("bot_token", "")
        self.api_base = "https://discord.com/api/v10"
        self.headers = {
            "Authorization": f"Bot {self.bot_token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Discord API."""
        url = f"{self.api_base}/{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result if isinstance(result, list) else result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def send_message(
        self,
        channel_id: str,
        content: str,
        embed: Optional[Dict] = None,
        components: Optional[List[Dict]] = None,
    ) -> ActionResult:
        """Send a message to a channel.

        Args:
            channel_id: Discord channel ID
            content: Message content
            embed: Optional embed object
            components: Optional action row components

        Returns:
            ActionResult with sent message
        """
        if not self.bot_token:
            return ActionResult(success=False, error="Missing bot_token")

        data = {"content": content}
        if embed:
            data["embeds"] = [embed]
        if components:
            data["components"] = components

        result = self._make_request("POST", f"channels/{channel_id}/messages", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_channel(self, channel_id: str) -> ActionResult:
        """Get channel information.

        Args:
            channel_id: Discord channel ID

        Returns:
            ActionResult with channel data
        """
        if not self.bot_token:
            return ActionResult(success=False, error="Missing bot_token")

        result = self._make_request("GET", f"channels/{channel_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_channels(self, guild_id: str) -> ActionResult:
        """List all channels in a guild.

        Args:
            guild_id: Discord guild ID

        Returns:
            ActionResult with channels list
        """
        if not self.bot_token:
            return ActionResult(success=False, error="Missing bot_token")

        result = self._make_request("GET", f"guilds/{guild_id}/channels")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"channels": result})

    def create_message(
        self,
        webhook_id: str,
        webhook_token: str,
        content: str,
        username: Optional[str] = None,
        avatar_url: Optional[str] = None,
        embed: Optional[Dict] = None,
    ) -> ActionResult:
        """Send a message via webhook.

        Args:
            webhook_id: Webhook ID
            webhook_token: Webhook token
            content: Message content
            username: Override username
            avatar_url: Override avatar URL
            embed: Optional embed object

        Returns:
            ActionResult with sent message
        """
        url = f"{self.api_base}/webhooks/{webhook_id}/{webhook_token}"
        data = {"content": content}
        if username:
            data["username"] = username
        if avatar_url:
            data["avatar_url"] = avatar_url
        if embed:
            data["embeds"] = [embed]

        req = Request(url, data=json.dumps(data).encode("utf-8"), method="POST")
        req.add_header("Content-Type", "application/json")

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return ActionResult(success=True, data=result)
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(success=False, error=f"HTTP {e.code}: {error_body}")
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def get_guild_member(self, guild_id: str, user_id: str) -> ActionResult:
        """Get a guild member.

        Args:
            guild_id: Discord guild ID
            user_id: User ID

        Returns:
            ActionResult with member data
        """
        if not self.bot_token:
            return ActionResult(success=False, error="Missing bot_token")

        result = self._make_request("GET", f"guilds/{guild_id}/members/{user_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def add_role(
        self, guild_id: str, user_id: str, role_id: str
    ) -> ActionResult:
        """Add a role to a guild member.

        Args:
            guild_id: Discord guild ID
            user_id: User ID
            role_id: Role ID to add

        Returns:
            ActionResult with operation status
        """
        if not self.bot_token:
            return ActionResult(success=False, error="Missing bot_token")

        endpoint = f"guilds/{guild_id}/members/{user_id}/roles/{role_id}"
        result = self._make_request("PUT", endpoint)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"role_added": role_id})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Discord operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "send_message": self.send_message,
            "get_channel": self.get_channel,
            "list_channels": self.list_channels,
            "create_webhook_message": self.create_message,
            "get_member": self.get_guild_member,
            "add_role": self.add_role,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
