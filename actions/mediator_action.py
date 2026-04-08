"""Mediator action module for RabAI AutoClick.

Provides mediator pattern implementation:
- Mediator: Central coordinator interface
- Colleague: Participant interface
- ConcreteMediator: Specific mediator implementation
- ChatRoom: Example chat room mediator
"""

from typing import Any, Callable, Dict, List, Optional, Set
from abc import ABC, abstractmethod
import uuid
from dataclasses import dataclass, field

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Message:
    """Message between colleagues."""
    sender_id: str
    receiver_id: Optional[str]
    content: Any
    message_type: str = "direct"
    metadata: Dict[str, Any] = field(default_factory=dict)


class Colleague(ABC):
    """Abstract colleague interface."""

    def __init__(self, colleague_id: str):
        self.colleague_id = colleague_id
        self._mediator: Optional["Mediator"] = None

    def set_mediator(self, mediator: "Mediator") -> None:
        """Set the mediator."""
        self._mediator = mediator

    def send(self, message: Message) -> None:
        """Send a message via mediator."""
        if self._mediator:
            self._mediator.route_message(self, message)

    def receive(self, message: Message) -> None:
        """Receive a message."""
        pass

    def get_id(self) -> str:
        """Get colleague ID."""
        return self.colleague_id


class Mediator(ABC):
    """Abstract mediator interface."""

    @abstractmethod
    def register_colleague(self, colleague: Colleague) -> None:
        """Register a colleague."""
        pass

    @abstractmethod
    def unregister_colleague(self, colleague_id: str) -> None:
        """Unregister a colleague."""
        pass

    @abstractmethod
    def route_message(self, sender: Colleague, message: Message) -> None:
        """Route a message."""
        pass

    @abstractmethod
    def broadcast(self, sender: Colleague, content: Any) -> None:
        """Broadcast to all colleagues."""
        pass


class ConcreteColleague(Colleague):
    """Concrete colleague implementation."""

    def __init__(self, colleague_id: str, name: str = ""):
        super().__init__(colleague_id)
        self.name = name or colleague_id
        self._inbox: List[Message] = []

    def receive(self, message: Message) -> None:
        """Receive and store message."""
        self._inbox.append(message)

    def get_inbox(self) -> List[Message]:
        """Get all received messages."""
        return self._inbox.copy()

    def clear_inbox(self) -> int:
        """Clear inbox and return count."""
        count = len(self._inbox)
        self._inbox.clear()
        return count


class ChatRoom(Mediator):
    """Chat room mediator."""

    def __init__(self, room_id: str, room_name: str = ""):
        self.room_id = room_id
        self.room_name = room_name or room_id
        self._colleagues: Dict[str, Colleague] = {}
        self._history: List[Message] = []
        self._channels: Dict[str, Set[str]] = {}

    def register_colleague(self, colleague: Colleague) -> None:
        """Register a colleague."""
        colleague.set_mediator(self)
        self._colleagues[colleague.get_id()] = colleague

    def unregister_colleague(self, colleague_id: str) -> None:
        """Unregister a colleague."""
        if colleague_id in self._colleagues:
            del self._colleagues[colleague_id]

    def route_message(self, sender: Colleague, message: Message) -> None:
        """Route a message to specific colleague."""
        self._history.append(message)

        if message.receiver_id:
            receiver = self._colleagues.get(message.receiver_id)
            if receiver:
                receiver.receive(message)
        else:
            self.broadcast(sender, message.content)

    def broadcast(self, sender: Colleague, content: Any) -> None:
        """Broadcast message to all colleagues."""
        message = Message(
            sender_id=sender.get_id(),
            receiver_id=None,
            content=content,
            message_type="broadcast",
        )
        self._history.append(message)

        for colleague in self._colleagues.values():
            if colleague.get_id() != sender.get_id():
                colleague.receive(message)

    def send_to_channel(self, sender: Colleague, channel: str, content: Any) -> None:
        """Send message to a channel."""
        message = Message(
            sender_id=sender.get_id(),
            receiver_id=None,
            content=content,
            message_type="channel",
            metadata={"channel": channel},
        )
        self._history.append(message)

        channel_members = self._channels.get(channel, set())
        for colleague_id in channel_members:
            colleague = self._colleagues.get(colleague_id)
            if colleague and colleague.get_id() != sender.get_id():
                colleague.receive(message)

    def join_channel(self, colleague_id: str, channel: str) -> bool:
        """Join a channel."""
        if colleague_id not in self._colleagues:
            return False
        if channel not in self._channels:
            self._channels[channel] = set()
        self._channels[channel].add(colleague_id)
        return True

    def leave_channel(self, colleague_id: str, channel: str) -> bool:
        """Leave a channel."""
        if channel in self._channels:
            self._channels[channel].discard(colleague_id)
            return True
        return False

    def get_colleagues(self) -> List[Dict[str, str]]:
        """Get all registered colleagues."""
        return [{"id": c.get_id(), "name": getattr(c, "name", c.get_id())} for c in self._colleagues.values()]

    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get message history."""
        messages = self._history[-limit:]
        return [
            {
                "sender": m.sender_id,
                "receiver": m.receiver_id,
                "content": m.content,
                "type": m.message_type,
                "metadata": m.metadata,
            }
            for m in messages
        ]

    def get_channels(self) -> Dict[str, List[str]]:
        """Get all channels and members."""
        return {ch: list(members) for ch, members in self._channels.items()}


class EventMediator(Mediator):
    """Event-based mediator."""

    def __init__(self):
        self._colleagues: Dict[str, Colleague] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        self._event_history: List[Dict] = []

    def register_colleague(self, colleague: Colleague) -> None:
        """Register a colleague."""
        colleague.set_mediator(self)
        self._colleagues[colleague.get_id()] = colleague

    def unregister_colleague(self, colleague_id: str) -> None:
        """Unregister a colleague."""
        if colleague_id in self._colleagues:
            del self._colleagues[colleague_id]

    def route_message(self, sender: Colleague, message: Message) -> None:
        """Route message as event."""
        self._event_history.append({
            "type": "message",
            "sender": sender.get_id(),
            "content": message.content,
        })

        handlers = self._handlers.get(message.message_type, [])
        for handler in handlers:
            try:
                handler(message)
            except Exception:
                pass

    def broadcast(self, sender: Colleague, content: Any) -> None:
        """Broadcast as event."""
        message = Message(
            sender_id=sender.get_id(),
            receiver_id=None,
            content=content,
            message_type="broadcast",
        )
        self.route_message(sender, message)

    def subscribe(self, event_type: str, handler: Callable[[Message], None]) -> None:
        """Subscribe to event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Callable) -> bool:
        """Unsubscribe from event type."""
        if event_type in self._handlers:
            try:
                self._handlers[event_type].remove(handler)
                return True
            except ValueError:
                pass
        return False

    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get event history."""
        return self._event_history[-limit:]


class MediatorAction(BaseAction):
    """Mediator pattern action."""
    action_type = "mediator"
    display_name = "中介者模式"
    description = "对象间通信中介"

    def __init__(self):
        super().__init__()
        self._mediators: Dict[str, Mediator] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")

            if operation == "create_chatroom":
                return self._create_chatroom(params)
            elif operation == "create_event_mediator":
                return self._create_event_mediator(params)
            elif operation == "register":
                return self._register_colleague(params)
            elif operation == "send":
                return self._send_message(params)
            elif operation == "broadcast":
                return self._broadcast(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "channels":
                return self._manage_channels(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Mediator error: {str(e)}")

    def _create_chatroom(self, params: Dict[str, Any]) -> ActionResult:
        """Create a chat room."""
        room_id = params.get("room_id", str(uuid.uuid4()))
        room_name = params.get("room_name", room_id)

        room = ChatRoom(room_id=room_id, room_name=room_name)
        self._mediators[room_id] = room

        return ActionResult(success=True, message=f"Chat room created: {room_id}", data={"room_id": room_id})

    def _create_event_mediator(self, params: Dict[str, Any]) -> ActionResult:
        """Create an event mediator."""
        mediator_id = params.get("mediator_id", str(uuid.uuid4()))

        mediator = EventMediator()
        self._mediators[mediator_id] = mediator

        return ActionResult(success=True, message=f"Event mediator created: {mediator_id}", data={"mediator_id": mediator_id})

    def _register_colleague(self, params: Dict[str, Any]) -> ActionResult:
        """Register a colleague."""
        mediator_id = params.get("mediator_id")
        colleague_id = params.get("colleague_id", str(uuid.uuid4()))
        name = params.get("name", colleague_id)

        if not mediator_id:
            return ActionResult(success=False, message="mediator_id is required")

        mediator = self._mediators.get(mediator_id)
        if not mediator:
            return ActionResult(success=False, message=f"Mediator not found: {mediator_id}")

        colleague = ConcreteColleague(colleague_id=colleague_id, name=name)
        mediator.register_colleague(colleague)

        return ActionResult(success=True, message=f"Colleague registered: {colleague_id}", data={"colleague_id": colleague_id})

    def _send_message(self, params: Dict[str, Any]) -> ActionResult:
        """Send a message."""
        mediator_id = params.get("mediator_id")
        sender_id = params.get("sender_id")
        receiver_id = params.get("receiver_id")
        content = params.get("content", "")

        if not mediator_id or not sender_id:
            return ActionResult(success=False, message="mediator_id and sender_id are required")

        mediator = self._mediators.get(mediator_id)
        if not mediator:
            return ActionResult(success=False, message=f"Mediator not found: {mediator_id}")

        colleagues = mediator.get_colleagues()
        sender = next((c for c in colleagues if c["id"] == sender_id), None)

        if not sender:
            return ActionResult(success=False, message=f"Sender not found: {sender_id}")

        concrete_colleague = ConcreteColleague(sender_id)
        concrete_colleague.set_mediator(mediator)

        message = Message(sender_id=sender_id, receiver_id=receiver_id, content=content)
        mediator.route_message(concrete_colleague, message)

        return ActionResult(success=True, message="Message sent")

    def _broadcast(self, params: Dict[str, Any]) -> ActionResult:
        """Broadcast a message."""
        mediator_id = params.get("mediator_id")
        sender_id = params.get("sender_id")
        content = params.get("content", "")

        if not mediator_id or not sender_id:
            return ActionResult(success=False, message="mediator_id and sender_id are required")

        mediator = self._mediators.get(mediator_id)
        if not mediator:
            return ActionResult(success=False, message=f"Mediator not found: {mediator_id}")

        sender = ConcreteColleague(sender_id)
        sender.set_mediator(mediator)
        mediator.broadcast(sender, content)

        return ActionResult(success=True, message="Broadcast sent")

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get message history."""
        mediator_id = params.get("mediator_id")
        limit = params.get("limit", 100)

        if not mediator_id:
            return ActionResult(success=False, message="mediator_id is required")

        mediator = self._mediators.get(mediator_id)
        if not mediator:
            return ActionResult(success=False, message=f"Mediator not found: {mediator_id}")

        history = mediator.get_history(limit=limit)

        return ActionResult(success=True, message=f"{len(history)} messages", data={"history": history})

    def _manage_channels(self, params: Dict[str, Any]) -> ActionResult:
        """Manage channels."""
        mediator_id = params.get("mediator_id")
        action = params.get("action")
        channel = params.get("channel")
        colleague_id = params.get("colleague_id")

        if not mediator_id:
            return ActionResult(success=False, message="mediator_id is required")

        mediator = self._mediators.get(mediator_id)
        if not mediator:
            return ActionResult(success=False, message=f"Mediator not found: {mediator_id}")

        if isinstance(mediator, ChatRoom):
            if action == "join" and channel and colleague_id:
                success = mediator.join_channel(colleague_id, channel)
                return ActionResult(success=success, message=f"Joined channel: {channel}" if success else "Join failed")
            elif action == "leave" and channel and colleague_id:
                success = mediator.leave_channel(colleague_id, channel)
                return ActionResult(success=success, message=f"Left channel: {channel}" if success else "Leave failed")
            elif action == "list":
                return ActionResult(success=True, message="Channels listed", data={"channels": mediator.get_channels()})

        return ActionResult(success=False, message="Channel operations not supported for this mediator type")
