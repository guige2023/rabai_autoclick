"""Bot automation action module.

Provides bot automation with command handling, conversation flow,
and plugin architecture.
"""

from __future__ import annotations

import time
import logging
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class CommandScope(Enum):
    """Scope of command applicability."""
    GLOBAL = "global"
    ADMIN = "admin"
    PRIVATE = "private"


@dataclass
class BotCommand:
    """A bot command definition."""
    name: str
    handler: Callable[[Dict[str, Any]], Any]
    description: str = ""
    scope: CommandScope = CommandScope.GLOBAL
    aliases: List[str] = field(default_factory=list)
    cooldown_seconds: float = 0


@dataclass
class BotMessage:
    """A message processed by the bot."""
    content: str
    user_id: str
    channel_id: str
    timestamp: float = field(default_factory=time.time)
    raw: Optional[Dict[str, Any]] = None


class BotAction:
    """Bot automation engine.

    Provides command routing, conversation context, and plugin support.

    Example:
        bot = BotAction(prefix="!")
        bot.command("ping", lambda ctx: "pong")
        bot.command("hello", lambda ctx: f"Hello {ctx['user_id']}!")
        bot.handle_message(message)
    """

    def __init__(
        self,
        prefix: str = "!",
        name: str = "Bot",
    ) -> None:
        """Initialize bot action.

        Args:
            prefix: Command prefix (e.g., '!').
            name: Bot display name.
        """
        self.prefix = prefix
        self.name = name
        self._commands: Dict[str, BotCommand] = {}
        self._middlewares: List[Callable[[BotMessage], Optional[BotMessage]]] = []
        self._cooldowns: Dict[str, float] = {}
        self._context: Dict[str, Any] = {}

    def command(
        self,
        name: str,
        handler: Callable[[Dict[str, Any]], Any],
        description: str = "",
        aliases: Optional[List[str]] = None,
        scope: CommandScope = CommandScope.GLOBAL,
    ) -> "BotAction":
        """Register a bot command.

        Args:
            name: Command name.
            handler: Function to handle the command.
            description: Command description.
            aliases: Alternative command names.
            scope: Command scope.

        Returns:
            Self for chaining.
        """
        cmd = BotCommand(
            name=name,
            handler=handler,
            description=description,
            scope=scope,
            aliases=aliases or [],
        )
        self._commands[name] = cmd
        for alias in cmd.aliases:
            self._commands[alias] = cmd
        return self

    def middleware(
        self,
        middleware: Callable[[BotMessage], Optional[BotMessage]],
    ) -> "BotAction":
        """Add a middleware function.

        Args:
            middleware: Function that processes messages.

        Returns:
            Self for chaining.
        """
        self._middlewares.append(middleware)
        return self

    def handle_message(
        self,
        content: str,
        user_id: str,
        channel_id: str,
        **kwargs,
    ) -> Optional[str]:
        """Handle an incoming message.

        Args:
            content: Message content.
            user_id: User ID who sent the message.
            channel_id: Channel ID where message was sent.
            **kwargs: Additional context.

        Returns:
            Response string or None.
        """
        message = BotMessage(
            content=content,
            user_id=user_id,
            channel_id=channel_id,
            raw=kwargs,
        )

        for mw in self._middlewares:
            result = mw(message)
            if result is None:
                return None
            message = result

        if not content.startswith(self.prefix):
            return None

        parts = content[len(self.prefix):].split()
        if not parts:
            return None

        cmd_name = parts[0].lower()
        args = parts[1:]

        return self._execute_command(cmd_name, message, args)

    def _execute_command(
        self,
        cmd_name: str,
        message: BotMessage,
        args: List[str],
    ) -> Optional[str]:
        """Execute a command."""
        cmd = self._commands.get(cmd_name)
        if not cmd:
            return None

        if cmd.cooldown_seconds > 0:
            key = f"{cmd.name}:{message.user_id}"
            last_used = self._cooldowns.get(key, 0)
            if time.time() - last_used < cmd.cooldown_seconds:
                return None
            self._cooldowns[key] = time.time()

        context = {
            "command": cmd.name,
            "args": args,
            "user_id": message.user_id,
            "channel_id": message.channel_id,
            "raw": message.raw,
            "bot": self,
        }

        try:
            result = cmd.handler(context)
            return str(result) if result is not None else None
        except Exception as e:
            logger.error("Command '%s' failed: %s", cmd.name, e)
            return f"Error: {e}"

    def set_context(self, key: str, value: Any) -> None:
        """Set bot context value."""
        self._context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        """Get bot context value."""
        return self._context.get(key, default)

    def help(self) -> str:
        """Generate help text for all commands."""
        lines = [f"{self.name} Commands:", ""]
        for name, cmd in sorted(self._commands.items()):
            if cmd.scope != CommandScope.GLOBAL:
                continue
            lines.append(f"  {self.prefix}{name} - {cmd.description}")
        return "\n".join(lines)
