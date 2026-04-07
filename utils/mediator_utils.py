"""
Mediator Pattern Implementation

Defines an object that encapsulates how a set of objects interact.
Promotes loose coupling by keeping objects from referring to each other explicitly.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Colleague(ABC):
    """
    Abstract colleague interface.
    Colleagues communicate through the mediator.
    """

    def __init__(self, mediator: Mediator | None = None, name: str = ""):
        self._mediator = mediator
        self._name = name or self.__class__.__name__

    @property
    def name(self) -> str:
        """Get colleague name."""
        return self._name

    @property
    def mediator(self) -> Mediator | None:
        """Get the mediator."""
        return self._mediator

    @mediator.setter
    def mediator(self, value: Mediator) -> None:
        """Set the mediator."""
        self._mediator = value

    @abstractmethod
    def send_message(self, message: str, receiver: str | None = None) -> None:
        """Send a message through the mediator."""
        pass

    @abstractmethod
    def receive_message(self, message: str, sender: str) -> None:
        """Receive a message from another colleague."""
        pass


class Mediator(ABC):
    """
    Abstract mediator interface.
    """

    @abstractmethod
    def register(self, colleague: Colleague) -> None:
        """Register a colleague."""
        pass

    @abstractmethod
    def unregister(self, colleague: Colleague) -> bool:
        """Unregister a colleague."""
        pass

    @abstractmethod
    def send(self, message: str, sender: Colleague, receiver: str | None = None) -> None:
        """
        Send a message from one colleague to another.

        Args:
            message: The message to send.
            sender: The sending colleague.
            receiver: The receiving colleague's name, or None for broadcast.
        """
        pass

    @abstractmethod
    def broadcast(self, message: str, sender: Colleague) -> None:
        """Broadcast a message to all registered colleagues."""
        pass


@dataclass
class Message:
    """A message in the mediator system."""
    id: str = field(default_factory=lambda: f"{time.time():.6f}")
    content: str
    sender: str
    receiver: str | None = None
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class ConcreteMediator(Mediator):
    """
    Concrete mediator implementation.
    """

    def __init__(self):
        self._colleagues: dict[str, Colleague] = {}
        self._message_history: list[Message] = []
        self._metrics: dict[str, int] = {}
        self._on_message_callbacks: list[Callable[[Message], None]] = []

    def register(self, colleague: Colleague) -> None:
        """Register a colleague."""
        self._colleagues[colleague.name] = colleague
        colleague.mediator = self

    def unregister(self, colleague: Colleague) -> bool:
        """Unregister a colleague."""
        if colleague.name in self._colleagues:
            del self._colleagues[colleague.name]
            return True
        return False

    def send(self, message: str, sender: Colleague, receiver: str | None = None) -> None:
        """Send a message to a specific colleague or broadcast."""
        msg = Message(content=message, sender=sender.name, receiver=receiver)
        self._message_history.append(msg)

        self._metrics[sender.name] = self._metrics.get(sender.name, 0) + 1

        if receiver:
            colleague = self._colleagues.get(receiver)
            if colleague:
                colleague.receive_message(message, sender.name)
                self._notify_callbacks(msg)
        else:
            self.broadcast(message, sender)

    def broadcast(self, message: str, sender: Colleague) -> None:
        """Broadcast a message to all colleagues except the sender."""
        msg = Message(content=message, sender=sender.name, receiver=None)
        self._message_history.append(msg)

        for name, colleague in self._colleagues.items():
            if name != sender.name:
                colleague.receive_message(message, sender.name)

        self._notify_callbacks(msg)

    def get_colleague(self, name: str) -> Colleague | None:
        """Get a colleague by name."""
        return self._colleagues.get(name)

    def get_colleague_names(self) -> list[str]:
        """Get all registered colleague names."""
        return list(self._colleagues.keys())

    def get_message_history(
        self,
        sender: str | None = None,
        receiver: str | None = None,
        limit: int = 100,
    ) -> list[Message]:
        """Get message history with optional filtering."""
        history = self._message_history

        if sender:
            history = [m for m in history if m.sender == sender]
        if receiver:
            history = [m for m in history if m.receiver == receiver]

        return history[-limit:]

    @property
    def metrics(self) -> dict[str, int]:
        """Get messaging metrics."""
        return copy.copy(self._metrics)

    def on_message(self, callback: Callable[[Message], None]) -> None:
        """Register a callback for message events."""
        self._on_message_callbacks.append(callback)

    def _notify_callbacks(self, message: Message) -> None:
        for callback in self._on_message_callbacks:
            try:
                callback(message)
            except Exception:
                pass


class ConcreteColleague(Colleague):
    """
    Concrete colleague implementation.
    """

    def __init__(self, name: str = "", mediator: Mediator | None = None):
        super().__init__(mediator=mediator, name=name)
        self._received_messages: list[tuple[str, str]] = []  # (sender, message)

    def send_message(self, message: str, receiver: str | None = None) -> None:
        """Send a message through the mediator."""
        if self._mediator:
            self._mediator.send(message, self, receiver)

    def receive_message(self, message: str, sender: str) -> None:
        """Receive a message from another colleague."""
        self._received_messages.append((sender, message))

    def get_received_messages(self) -> list[tuple[str, str]]:
        """Get all received messages."""
        return copy.copy(self._received_messages)

    def clear_messages(self) -> None:
        """Clear received messages."""
        self._received_messages.clear()


class MediatorBuilder:
    """
    Builder for creating configured mediators.
    """

    def __init__(self):
        self._colleagues: list[type[Colleague]] = []
        self._auto_register: bool = True

    def add_colleague(self, colleague_class: type[Colleague]) -> MediatorBuilder:
        """Add a colleague class to be created."""
        self._colleagues.append(colleague_class)
        return self

    def with_auto_register(self, enabled: bool) -> MediatorBuilder:
        """Set auto-registration behavior."""
        self._auto_register = enabled
        return self

    def build(self) -> tuple[ConcreteMediator, list[Colleague]]:
        """Build the mediator and colleagues."""
        mediator = ConcreteMediator()
        colleagues = []

        for cls in self._colleagues:
            colleague = cls(mediator=mediator)
            colleagues.append(colleague)
            if self._auto_register:
                mediator.register(colleague)

        return mediator, colleagues


@dataclass
class MediatorMetrics:
    """Metrics for mediator operations."""
    total_messages: int = 0
    broadcasts: int = 0
    direct_messages: int = 0
    by_sender: dict[str, int] = field(default_factory=dict)
    by_receiver: dict[str, int] = field(default_factory=dict)


class MeasuredMediator(ConcreteMediator):
    """Mediator with metrics collection."""

    def __init__(self):
        super().__init__()
        self._metrics = MediatorMetrics()

    def send(self, message: str, sender: Colleague, receiver: str | None = None) -> None:
        """Send with metrics."""
        self._metrics.total_messages += 1
        self._metrics.by_sender[sender.name] = self._metrics.by_sender.get(sender.name, 0) + 1

        if receiver:
            self._metrics.direct_messages += 1
            self._metrics.by_receiver[receiver] = self._metrics.by_receiver.get(receiver, 0) + 1
        else:
            self._metrics.broadcasts += 1

        super().send(message, sender, receiver)

    @property
    def metrics(self) -> MediatorMetrics:
        """Get metrics."""
        return self._metrics


class GroupMediator(Mediator):
    """
    Mediator that supports group-based messaging.
    Colleagues can join/leave groups.
    """

    def __init__(self):
        self._colleagues: dict[str, Colleague] = {}
        self._groups: dict[str, set[str]] = {}  # group name -> set of colleague names
        self._colleague_groups: dict[str, set[str]] = {}  # colleague name -> set of group names

    def register(self, colleague: Colleague) -> None:
        self._colleagues[colleague.name] = colleague
        colleague.mediator = self

    def unregister(self, colleague: Colleague) -> bool:
        if colleague.name in self._colleagues:
            del self._colleagues[colleague.name]
            for groups in self._groups.values():
                groups.discard(colleague.name)
            self._colleague_groups.pop(colleague.name, None)
            return True
        return False

    def create_group(self, name: str) -> None:
        """Create a new group."""
        self._groups[name] = set()

    def join_group(self, colleague_name: str, group_name: str) -> bool:
        """Add a colleague to a group."""
        if group_name not in self._groups:
            self.create_group(group_name)

        if colleague_name in self._colleagues:
            self._groups[group_name].add(colleague_name)
            if colleague_name not in self._colleague_groups:
                self._colleague_groups[colleague_name] = set()
            self._colleague_groups[colleague_name].add(group_name)
            return True
        return False

    def leave_group(self, colleague_name: str, group_name: str) -> bool:
        """Remove a colleague from a group."""
        if group_name in self._groups:
            self._groups[group_name].discard(colleague_name)
            if colleague_name in self._colleague_groups:
                self._colleague_groups[colleague_name].discard(group_name)
            return True
        return False

    def send_to_group(self, message: str, sender: Colleague, group_name: str) -> None:
        """Send a message to all members of a group."""
        if group_name in self._groups:
            for colleague_name in self._groups[group_name]:
                if colleague_name != sender.name:
                    colleague = self._colleagues.get(colleague_name)
                    if colleague:
                        colleague.receive_message(message, sender.name)

    def broadcast(self, message: str, sender: Colleague) -> None:
        """Broadcast to all colleagues."""
        for name, colleague in self._colleagues.items():
            if name != sender.name:
                colleague.receive_message(message, sender.name)

    def get_groups(self) -> list[str]:
        """Get all group names."""
        return list(self._groups.keys())

    def get_group_members(self, group_name: str) -> set[str]:
        """Get members of a group."""
        return copy.copy(self._groups.get(group_name, set()))
