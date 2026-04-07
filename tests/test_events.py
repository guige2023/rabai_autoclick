"""Tests for event handling utilities."""

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.events import (
    EventType,
    Event,
    EventHandler,
    EventDispatcher,
    EventBus,
    EventFilter,
    create_event,
)


class DummyHandler(EventHandler):
    """Dummy handler for testing."""

    def __init__(self):
        self.handled = []

    def handle(self, event: Event) -> None:
        self.handled.append(event)


class TestEventType:
    """Tests for EventType."""

    def test_values(self) -> None:
        """Test event type values."""
        assert EventType.WORKFLOW_START.value == "workflow_start"
        assert EventType.ACTION_EXECUTE.value == "action_execute"


class TestEvent:
    """Tests for Event."""

    def test_create(self) -> None:
        """Test creating event."""
        event = Event(type=EventType.CUSTOM, timestamp=datetime.now())
        assert event.type == EventType.CUSTOM

    def test_with_data(self) -> None:
        """Test creating event with data."""
        event = Event(
            type=EventType.ACTION_EXECUTE,
            timestamp=datetime.now(),
            data={"action": "click"},
        )
        assert event.data["action"] == "click"


class TestEventDispatcher:
    """Tests for EventDispatcher."""

    def test_create(self) -> None:
        """Test creating dispatcher."""
        dispatcher = EventDispatcher()
        assert len(dispatcher._handlers) == 0

    def test_subscribe(self) -> None:
        """Test subscribing to events."""
        dispatcher = EventDispatcher()
        handler = DummyHandler()
        dispatcher.subscribe(EventType.CUSTOM, handler)
        assert EventType.CUSTOM in dispatcher._handlers

    def test_unsubscribe(self) -> None:
        """Test unsubscribing from events."""
        dispatcher = EventDispatcher()
        handler = DummyHandler()
        dispatcher.subscribe(EventType.CUSTOM, handler)
        dispatcher.unsubscribe(EventType.CUSTOM, handler)
        assert EventType.CUSTOM not in dispatcher._handlers

    def test_on_off(self) -> None:
        """Test callback registration."""
        dispatcher = EventDispatcher()
        callback_called = []

        def callback(event):
            callback_called.append(event)

        dispatcher.on(EventType.CUSTOM, callback)
        dispatcher.dispatch(Event(type=EventType.CUSTOM, timestamp=datetime.now()))
        assert len(callback_called) == 1

        dispatcher.off(EventType.CUSTOM, callback)
        dispatcher.dispatch(Event(type=EventType.CUSTOM, timestamp=datetime.now()))
        assert len(callback_called) == 1

    def test_emit(self) -> None:
        """Test emitting event."""
        dispatcher = EventDispatcher()
        event = dispatcher.emit(EventType.CUSTOM, {"key": "value"})
        assert event.type == EventType.CUSTOM
        assert event.data["key"] == "value"

    def test_get_history(self) -> None:
        """Test getting event history."""
        dispatcher = EventDispatcher()
        dispatcher.emit(EventType.CUSTOM)
        dispatcher.emit(EventType.ACTION_EXECUTE)
        history = dispatcher.get_history()
        assert len(history) == 2

    def test_clear_history(self) -> None:
        """Test clearing history."""
        dispatcher = EventDispatcher()
        dispatcher.emit(EventType.CUSTOM)
        dispatcher.clear_history()
        assert len(dispatcher.get_history()) == 0


class TestEventBus:
    """Tests for EventBus."""

    def test_singleton(self) -> None:
        """Test singleton behavior."""
        bus1 = EventBus()
        bus2 = EventBus()
        assert bus1 is bus2


class TestEventFilter:
    """Tests for EventFilter."""

    def test_type_filter(self) -> None:
        """Test filtering by type."""
        f = EventFilter().type(EventType.CUSTOM)
        event = Event(type=EventType.CUSTOM, timestamp=datetime.now())
        assert f.matches(event) is True

        event2 = Event(type=EventType.WORKFLOW_START, timestamp=datetime.now())
        assert f.matches(event2) is False

    def test_since_filter(self) -> None:
        """Test filtering by timestamp."""
        now = datetime.now()
        f = EventFilter().since(now)
        event = Event(type=EventType.CUSTOM, timestamp=datetime.now())
        assert f.matches(event) is True

    def test_data_filter(self) -> None:
        """Test filtering by data."""
        f = EventFilter().data(lambda d: d.get("key") == "value")
        event = Event(
            type=EventType.CUSTOM,
            timestamp=datetime.now(),
            data={"key": "value"},
        )
        assert f.matches(event) is True

        event2 = Event(
            type=EventType.CUSTOM,
            timestamp=datetime.now(),
            data={"key": "other"},
        )
        assert f.matches(event2) is False


class TestCreateEvent:
    """Tests for create_event function."""

    def test_create_event(self) -> None:
        """Test creating event."""
        event = create_event(EventType.CUSTOM, key="value")
        assert event.type == EventType.CUSTOM
        assert event.data["key"] == "value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])