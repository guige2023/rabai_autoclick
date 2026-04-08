"""
Agent-to-agent communication protocol utilities.

Provides message formatting, routing, and response handling
for multi-agent systems.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal
from enum import Enum


class MessageType(Enum):
    """Agent message types."""
    REQUEST = "request"
    RESPONSE = "response"
    HEARTBEAT = "heartbeat"
    BROADCAST = "broadcast"
    ERROR = "error"


@dataclass
class AgentMessage:
    """Agent communication message."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    type: MessageType = MessageType.REQUEST
    sender: str = ""
    recipient: str = ""
    subject: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    correlation_id: str | None = None
    ttl: float = 300.0
    headers: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps({
            "id": self.id,
            "type": self.type.value,
            "sender": self.sender,
            "recipient": self.recipient,
            "subject": self.subject,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "ttl": self.ttl,
            "headers": self.headers,
        })

    @classmethod
    def from_json(cls, data: str | dict) -> "AgentMessage":
        """Deserialize from JSON."""
        if isinstance(data, str):
            data = json.loads(data)
        return cls(
            id=data.get("id", uuid.uuid4().hex[:12]),
            type=MessageType(data.get("type", "request")),
            sender=data.get("sender", ""),
            recipient=data.get("recipient", ""),
            subject=data.get("subject", ""),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", time.time()),
            correlation_id=data.get("correlation_id"),
            ttl=data.get("ttl", 300.0),
            headers=data.get("headers", {}),
        )

    def is_expired(self) -> bool:
        """Check if message TTL has expired."""
        return time.time() - self.timestamp > self.ttl

    def reply(self, payload: dict[str, Any] | None = None) -> "AgentMessage":
        """Create a reply message."""
        return AgentMessage(
            type=MessageType.RESPONSE,
            sender=self.recipient,
            recipient=self.sender,
            correlation_id=self.id,
            payload=payload or {},
        )

    def error_reply(self, error_message: str) -> "AgentMessage":
        """Create an error reply message."""
        return AgentMessage(
            type=MessageType.ERROR,
            sender=self.recipient,
            recipient=self.sender,
            correlation_id=self.id,
            payload={"error": error_message},
        )


class AgentMailbox:
    """In-memory message mailbox for agent communication."""

    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self._inbox: list[AgentMessage] = []
        self._lock = __import__("threading").Lock()

    def send(self, recipient: str, message: AgentMessage) -> None:
        """Send message to recipient's mailbox."""
        message.recipient = recipient
        AgentRegistry.get_instance().deliver(self.agent_id, message)

    def receive(
        self,
        blocking: bool = False,
        timeout: float = 1.0,
    ) -> AgentMessage | None:
        """
        Receive next message from inbox.

        Args:
            blocking: Wait for message if empty
            timeout: Max wait time when blocking

        Returns:
            Next message or None
        """
        start = time.time()
        while True:
            with self._lock:
                self._prune_expired()
                if self._inbox:
                    return self._inbox.pop(0)
            if not blocking:
                return None
            if time.time() - start >= timeout:
                return None
            time.sleep(0.01)

    def deliver(self, message: AgentMessage) -> None:
        """Deliver message to this mailbox."""
        with self._lock:
            if message.recipient == self.agent_id:
                self._inbox.append(message)

    def _prune_expired(self) -> None:
        self._inbox = [m for m in self._inbox if not m.is_expired()]

    @property
    def unread_count(self) -> int:
        """Count of unread messages."""
        with self._lock:
            self._prune_expired()
            return len(self._inbox)


class AgentRegistry:
    """Global agent registry (singleton)."""

    _instance: "AgentRegistry | None" = None

    @classmethod
    def get_instance(cls) -> "AgentRegistry":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._agents: dict[str, AgentMailbox] = {}
        self._lock = __import__("threading").Lock()

    def register(self, agent_id: str) -> AgentMailbox:
        """Register an agent and get its mailbox."""
        with self._lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = AgentMailbox(agent_id)
            return self._agents[agent_id]

    def deregister(self, agent_id: str) -> bool:
        """Deregister an agent."""
        with self._lock:
            if agent_id in self._agents:
                del self._agents[agent_id]
                return True
            return False

    def deliver(self, recipient: str, message: AgentMessage) -> bool:
        """Deliver message to recipient."""
        with self._lock:
            if recipient in self._agents:
                self._agents[recipient].deliver(message)
                return True
        return False

    def list_agents(self) -> list[str]:
        """List all registered agent IDs."""
        with self._lock:
            return list(self._agents.keys())


def create_request(
    sender: str,
    recipient: str,
    subject: str,
    payload: dict[str, Any] | None = None,
) -> AgentMessage:
    """Factory to create a request message."""
    return AgentMessage(
        type=MessageType.REQUEST,
        sender=sender,
        recipient=recipient,
        subject=subject,
        payload=payload or {},
    )


def create_broadcast(
    sender: str,
    subject: str,
    payload: dict[str, Any] | None = None,
) -> AgentMessage:
    """Factory to create a broadcast message."""
    return AgentMessage(
        type=MessageType.BROADCAST,
        sender=sender,
        recipient="*",
        subject=subject,
        payload=payload or {},
    )
