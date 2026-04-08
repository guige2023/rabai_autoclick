"""Message Channel Action Module.

Provides message channel pattern for
point-to-point communication.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ChannelType(Enum):
    """Channel type."""
    FIFO = "fifo"
    LIFO = "lifo"
    PRIORITY = "priority"


@dataclass
class Message:
    """Channel message."""
    message_id: str
    payload: Any
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    delivered: bool = False


class MessageChannel:
    """Message channel."""

    def __init__(self, channel_id: str, channel_type: ChannelType = ChannelType.FIFO):
        self.channel_id = channel_id
        self.channel_type = channel_type
        self._messages: List[Message] = []
        self._lock = threading.Lock()

    def send(self, payload: Any, priority: int = 0) -> str:
        """Send message to channel."""
        message_id = f"msg_{int(time.time() * 1000)}"

        message = Message(
            message_id=message_id,
            payload=payload,
            priority=priority
        )

        with self._lock:
            self._messages.append(message)

            if self.channel_type == ChannelType.PRIORITY:
                self._messages.sort(key=lambda m: m.priority, reverse=True)

        return message_id

    def receive(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Receive message from channel."""
        start = time.time()

        while True:
            with self._lock:
                if self._messages:
                    if self.channel_type == ChannelType.LIFO:
                        return self._messages.pop()
                    return self._messages.pop(0)

            if timeout and (time.time() - start) >= timeout:
                return None

            time.sleep(0.01)

    def peek(self) -> Optional[Message]:
        """Peek at next message."""
        with self._lock:
            if self._messages:
                return self._messages[0]
        return None

    def size(self) -> int:
        """Get channel size."""
        with self._lock:
            return len(self._messages)


class ChannelManager:
    """Manages message channels."""

    def __init__(self):
        self._channels: Dict[str, MessageChannel] = {}

    def create_channel(
        self,
        name: str,
        channel_type: ChannelType = ChannelType.FIFO
    ) -> str:
        """Create channel."""
        channel_id = f"ch_{name.lower().replace(' ', '_')}"
        self._channels[channel_id] = MessageChannel(channel_id, channel_type)
        return channel_id

    def get_channel(self, channel_id: str) -> Optional[MessageChannel]:
        """Get channel."""
        return self._channels.get(channel_id)


class MessageChannelAction(BaseAction):
    """Action for message channel operations."""

    def __init__(self):
        super().__init__("message_channel")
        self._manager = ChannelManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute message channel action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "send":
                return self._send(params)
            elif operation == "receive":
                return self._receive(params)
            elif operation == "peek":
                return self._peek(params)
            elif operation == "size":
                return self._size(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create channel."""
        channel_id = self._manager.create_channel(
            name=params.get("name", ""),
            channel_type=ChannelType(params.get("type", "fifo"))
        )
        return ActionResult(success=True, data={"channel_id": channel_id})

    def _send(self, params: Dict) -> ActionResult:
        """Send message."""
        channel = self._manager.get_channel(params.get("channel_id", ""))
        if not channel:
            return ActionResult(success=False, message="Channel not found")

        message_id = channel.send(
            params.get("payload"),
            params.get("priority", 0)
        )
        return ActionResult(success=True, data={"message_id": message_id})

    def _receive(self, params: Dict) -> ActionResult:
        """Receive message."""
        channel = self._manager.get_channel(params.get("channel_id", ""))
        if not channel:
            return ActionResult(success=False, message="Channel not found")

        message = channel.receive(params.get("timeout"))
        if not message:
            return ActionResult(success=False, message="No message available")

        return ActionResult(success=True, data={
            "message_id": message.message_id,
            "payload": message.payload
        })

    def _peek(self, params: Dict) -> ActionResult:
        """Peek message."""
        channel = self._manager.get_channel(params.get("channel_id", ""))
        if not channel:
            return ActionResult(success=False, message="Channel not found")

        message = channel.peek()
        if not message:
            return ActionResult(success=False, message="Channel empty")

        return ActionResult(success=True, data={
            "message_id": message.message_id,
            "payload": message.payload
        })

    def _size(self, params: Dict) -> ActionResult:
        """Get size."""
        channel = self._manager.get_channel(params.get("channel_id", ""))
        if not channel:
            return ActionResult(success=False, message="Channel not found")

        return ActionResult(success=True, data={"size": channel.size()})
