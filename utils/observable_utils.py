"""Observable pattern utilities.

Provides observable state management for reactive
automation workflows and UI state tracking.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field


@dataclass
class Change:
    """Represents a state change event."""
    property_name: str
    old_value: Any
    new_value: Any
    source: Any = None


class Observable:
    """Observable object with property change notification.

    Example:
        class State(Observable):
            def __init__(self):
                super().__init__()
                self._value = 0

            @property
            def value(self):
                return self._value

            @value.setter
            def value(self, v):
                self._set_and_notify("value", self._value, v)

        state = State()
        state.on_change("value", lambda c: print(f"changed: {c.new_value}"))
        state.value = 10  # prints "changed: 10"
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, List[Callable[[Change], None]]] = {}
        self._any_handler: List[Callable[[Change], None]] = []
        self._suppressed: Set[str] = set()
        self._suppress_all = False

    def on_change(
        self,
        property_name: str,
        handler: Callable[[Change], None],
    ) -> None:
        """Register handler for property change.

        Args:
            property_name: Property name to watch.
            handler: Callback when property changes.
        """
        if property_name not in self._handlers:
            self._handlers[property_name] = []
        if handler not in self._handlers[property_name]:
            self._handlers[property_name].append(handler)

    def on_any_change(self, handler: Callable[[Change], None]) -> None:
        """Register handler for any property change.

        Args:
            handler: Callback for any change.
        """
        if handler not in self._any_handler:
            self._any_handler.append(handler)

    def off_change(self, property_name: str, handler: Callable[[Change], None]) -> None:
        """Remove change handler.

        Args:
            property_name: Property name.
            handler: Handler to remove.
        """
        if property_name in self._handlers:
            self._handlers[property_name] = [
                h for h in self._handlers[property_name] if h != handler
            ]

    def off_any_change(self, handler: Callable[[Change], None]) -> None:
        """Remove any-change handler.

        Args:
            handler: Handler to remove.
        """
        self._any_handler = [h for h in self._any_handler if h != handler]

    def suppress_notifications(self, *property_names: str) -> None:
        """Temporarily suppress notifications.

        Args:
            *property_names: Properties to suppress. Empty for all.
        """
        if not property_names:
            self._suppress_all = True
        else:
            self._suppressed.update(property_names)

    def resume_notifications(self, *property_names: str) -> None:
        """Resume notifications.

        Args:
            *property_names: Properties to resume. Empty for all.
        """
        if not property_names:
            self._suppress_all = False
            self._suppressed.clear()
        else:
            self._suppressed.difference_update(property_names)

    def notify_change(self, change: Change) -> None:
        """Notify listeners of a change.

        Args:
            change: Change event.
        """
        if self._suppress_all or change.property_name in self._suppressed:
            return

        for handler in self._handlers.get(change.property_name, []):
            handler(change)
        for handler in self._any_handler:
            handler(change)

    def _set_and_notify(
        self,
        property_name: str,
        old_value: Any,
        new_value: Any,
        source: Any = None,
    ) -> None:
        """Helper to set value and notify if changed."""
        if old_value != new_value:
            self.notify_change(Change(property_name, old_value, new_value, source))
