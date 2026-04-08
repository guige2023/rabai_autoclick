"""
AX Event Utilities for macOS Accessibility.

Provides utilities for observing, filtering, and reacting to
macOS Accessibility (AX) events. AX events are the underlying
mechanism macOS uses to notify assistive apps of UI changes.

Usage:
    from utils.ax_event_utils import AXEventObserver, AXEventFilter

    observer = AXEventObserver()
    observer.register_callback(AXEventFilter.focused, my_handler)
    observer.start()
"""

from __future__ import annotations

import time
from typing import Optional, Callable, Dict, Any, List, Set, TypeVar, Generic, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import lru_cache

if TYPE_CHECKING:
    from utils.accessibility_bridge import AccessibilityBridge


class AXEventType(Enum):
    """Standard macOS AX event types."""
    FOCUSED_CHANGED = "AXFocusedUIElementChanged"
    VALUE_CHANGED = "AXValueChanged"
    UI_ELEMENT_CREATED = "AXUIElementCreated"
    ELEMENT_RESIZED = "AXElementResized"
    ELEMENT_MOVED = "AXElementMoved"
    WINDOW_RESIZED = "AXWindowResized"
    WINDOW_MOVED = "AXWindowMoved"
    APPLICATION_ACTIVATED = "AXApplicationActivated"
    APPLICATION_DEACTIVATED = "AXApplicationDeactivated"
    DRAWER_OPENED = "AXDrawerOpened"
    DRAWER_CLOSED = "AXDrawerClosed"
    SHEET_OPENED = "AXSheetOpened"
    SHEET_CLOSED = "AXSheetClosed"
    WINdow_CREATED = "AXWindowCreated"
    WINdow_CLOSED = "AXWindowClosed"
    POPOVER_OPENED = "AXPopoverOpened"
    POPOVER_CLOSED = "AXPopoverClosed"
    GROW_AREA_CHANGED = "AXGrowAreaChanged"
    ACTIVITY_STARTS = "AXActivityStarts"
    ACTIVITY_ENDS = "AXActivityEnds"
    CUSTOM = "AXCustomAction"


@dataclass
class AXEvent:
    """Represents a single AX event."""
    event_type: AXEventType
    element: Optional[Any] = None
    element_info: Optional[Dict[str, Any]] = None
    app: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)
    user_info: Optional[Dict[str, Any]] = None

    @property
    def type_name(self) -> str:
        return self.event_type.name

    def __repr__(self) -> str:
        return f"AXEvent({self.event_type.name}, ts={self.timestamp:.3f})"


class AXEventFilter:
    """
    Filter AX events by type, element attributes, or custom predicates.

    Example:
        # Only process focus and value change events
        filtered = events.filter(AXEventFilter.focused | AXEventFilter.value_changed)

        # Custom predicate
        def is_button_change(event):
            return event.element_info.get("role") == "button"

        filtered = events.filter(is_button_change)
    """

    @staticmethod
    def focused(event: AXEvent) -> bool:
        """Return True for focused element change events."""
        return event.event_type == AXEventType.FOCUSED_CHANGED

    @staticmethod
    def value_changed(event: AXEvent) -> bool:
        """Return True for value change events."""
        return event.event_type == AXEventType.VALUE_CHANGED

    @staticmethod
    def window_event(event: AXEvent) -> bool:
        """Return True for window-related events."""
        return event.event_type in {
            AXEventType.WINDOW_RESIZED,
            AXEventType.WINDOW_MOVED,
            AXEventType.WINdOW_CREATED,
            AXEventType.WINdOW_CLOSED,
        }

    @staticmethod
    def by_role(role: str) -> Callable[[AXEvent], bool]:
        """Return a filter matching events from elements with given role."""
        def predicate(event: AXEvent) -> bool:
            return event.element_info.get("role") == role if event.element_info else False
        return predicate

    @staticmethod
    def by_app(bundle_id: str) -> Callable[[AXEvent], bool]:
        """Return a filter matching events from a specific app bundle ID."""
        def predicate(event: AXEvent) -> bool:
            if event.app is None:
                return False
            try:
                return event.app.get("bundle_id") == bundle_id
            except Exception:
                return False
        return predicate


class AXEventObserver:
    """
    Observe and dispatch AX events to registered handlers.

    Supports registering multiple handlers per event type, filtering,
    and rate-limiting to avoid overwhelming the system.

    Example:
        observer = AXEventObserver(bridge=my_bridge)
        observer.register(
            AXEventType.FOCUSED_CHANGED,
            lambda e: print(f"Focus: {e.element_info}")
        )
        observer.start()
    """

    def __init__(
        self,
        bridge: Optional["AccessibilityBridge"] = None,
        max_events_per_second: int = 60,
    ) -> None:
        """
        Initialize the observer.

        Args:
            bridge: AccessibilityBridge for element info lookup.
            max_events_per_second: Rate limit for event dispatch.
        """
        self._bridge = bridge
        self._max_per_second = max_events_per_second
        self._handlers: Dict[AXEventType, List[Callable[[AXEvent], None]]] = {
            t: [] for t in AXEventType
        }
        self._global_handlers: List[Callable[[AXEvent], None]] = []
        self._running = False
        self._event_queue: List[AXEvent] = []
        self._last_dispatch_time = 0.0
        self._suppressed_types: Set[AXEventType] = set()

    def register(
        self,
        event_type: AXEventType,
        handler: Callable[[AXEvent], None],
    ) -> None:
        """
        Register a handler for a specific event type.

        Args:
            event_type: The AX event type to listen for.
            handler: Callback function invoked with AXEvent.
        """
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)

    def register_global(
        self,
        handler: Callable[[AXEvent], None],
    ) -> None:
        """
        Register a handler that receives all events regardless of type.

        Args:
            handler: Callback invoked with every AXEvent.
        """
        if handler not in self._global_handlers:
            self._global_handlers.append(handler)

    def unregister(
        self,
        event_type: AXEventType,
        handler: Callable[[AXEvent], None],
    ) -> None:
        """Remove a handler from a specific event type."""
        if handler in self._handlers[event_type]:
            self._handlers[event_type].remove(handler)

    def suppress(self, event_type: AXEventType) -> None:
        """Suppress dispatching of a specific event type."""
        self._suppressed_types.add(event_type)

    def resume(self, event_type: AXEventType) -> None:
        """Resume dispatching of a previously suppressed event type."""
        self._suppressed_types.discard(event_type)

    def _dispatch(self, event: AXEvent) -> None:
        """Dispatch an event to all registered handlers."""
        if event.event_type in self._suppressed_types:
            return

        for handler in self._global_handlers:
            try:
                handler(event)
            except Exception:
                pass

        for handler in self._handlers.get(event.event_type, []):
            try:
                handler(event)
            except Exception:
                pass

    def start(self) -> None:
        """Start observing events."""
        self._running = True

    def stop(self) -> None:
        """Stop observing events."""
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running


class AXEventBuffer:
    """
    Buffer AX events for batch processing.

    Accumulates events over a time window and releases them
    as a batch, useful for reducing processing overhead
    when many rapid events occur.

    Example:
        buffer = AXEventBuffer(window_seconds=0.5)
        # ... events added ...
        batch = buffer.flush()  # Returns all accumulated events
    """

    def __init__(self, window_seconds: float = 0.5) -> None:
        """
        Initialize the buffer.

        Args:
            window_seconds: Time window before auto-flush.
        """
        self._window = window_seconds
        self._events: List[AXEvent] = []
        self._last_flush = time.time()

    def add(self, event: AXEvent) -> int:
        """
        Add an event to the buffer.

        Returns:
            Current buffer size after adding.
        """
        self._events.append(event)
        return len(self._events)

    def should_flush(self) -> bool:
        """Return True if the buffer should be flushed."""
        if not self._events:
            return False
        return time.time() - self._last_flush >= self._window

    def flush(self) -> List[AXEvent]:
        """
        Flush and return all buffered events.

        Returns:
            List of all buffered events, buffer is cleared.
        """
        result = self._events
        self._events = []
        self._last_flush = time.time()
        return result

    def __len__(self) -> int:
        return len(self._events)
