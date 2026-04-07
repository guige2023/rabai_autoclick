"""Tests for event bus utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.event_bus import (
    Event,
    EventPriority,
    EventBus,
    EventBusManager,
    Events,
)


class TestEvent:
    """Tests for Event dataclass."""

    def test_basic_event(self) -> None:
        """Test basic event creation."""
        event = Event(type="test", data={"key": "value"})
        assert event.type == "test"
        assert event.data == {"key": "value"}
        assert event.propagation_stopped is False

    def test_stop_propagation(self) -> None:
        """Test stopping event propagation."""
        event = Event(type="test")
        event.stop_propagation()
        assert event.propagation_stopped is True


class TestEventBus:
    """Tests for EventBus."""

    def test_subscribe_decorator(self) -> None:
        """Test subscribe decorator."""
        bus = EventBus()
        received = []

        @bus.subscribe("click")
        def on_click(event):
            received.append(event)

        bus.publish(Event("click", {"x": 100}))
        assert len(received) == 1
        assert received[0].data["x"] == 100

    def test_multiple_subscribers(self) -> None:
        """Test multiple subscribers to same event."""
        bus = EventBus()
        count1 = []
        count2 = []

        @bus.subscribe("click")
        def handler1(event):
            count1.append(event)

        @bus.subscribe("click")
        def handler2(event):
            count2.append(event)

        bus.publish(Event("click"))
        assert len(count1) == 1
        assert len(count2) == 1

    def test_unsubscribe(self) -> None:
        """Test unsubscribing from events."""
        bus = EventBus()
        received = []

        @bus.subscribe("click")
        def handler(event):
            received.append(event)

        bus.publish(Event("click"))
        assert len(received) == 1

        bus.unsubscribe("click", handler)
        bus.publish(Event("click"))
        assert len(received) == 1  # Still 1, not 2

    def test_priority_order(self) -> None:
        """Test subscribers called in priority order."""
        bus = EventBus()
        order = []

        @bus.subscribe("test", priority=EventPriority.LOW)
        def low(event):
            order.append("low")

        @bus.subscribe("test", priority=EventPriority.HIGH)
        def high(event):
            order.append("high")

        @bus.subscribe("test", priority=EventPriority.NORMAL)
        def normal(event):
            order.append("normal")

        bus.publish(Event("test"))
        assert order == ["high", "normal", "low"]

    def test_filter(self) -> None:
        """Test event filtering."""
        bus = EventBus()
        received = []

        def filter_func(event):
            return event.data.get("process", False)

        bus.add_subscription("click", lambda e: received.append(e), filter=filter_func)

        bus.publish(Event("click", {"process": False}))
        bus.publish(Event("click", {"process": True}))

        assert len(received) == 1

    def test_propagation_stopped(self) -> None:
        """Test stopped propagation prevents further handlers."""
        bus = EventBus()
        order = []

        @bus.subscribe("test", priority=EventPriority.HIGH)
        def high(event):
            order.append("high")
            event.stop_propagation()

        @bus.subscribe("test", priority=EventPriority.LOW)
        def low(event):
            order.append("low")

        bus.publish(Event("test"))
        assert order == ["high"]
        assert "low" not in order

    def test_publish_async(self) -> None:
        """Test async publish."""
        bus = EventBus()
        received = []

        @bus.subscribe("click")
        def handler(event):
            received.append(event)

        bus.publish_async(Event("click"))

        time.sleep(0.05)
        assert len(received) == 1

    def test_event_history(self) -> None:
        """Test event history."""
        bus = EventBus()

        bus.publish(Event("click"))
        bus.publish(Event("keypress"))

        history = bus.get_history()
        assert len(history) == 2

        click_history = bus.get_history(event_type="click")
        assert len(click_history) == 1

        limited_history = bus.get_history(limit=1)
        assert len(limited_history) == 1

    def test_clear_history(self) -> None:
        """Test clearing event history."""
        bus = EventBus()

        bus.publish(Event("click"))
        assert len(bus.get_history()) == 1

        bus.clear_history()
        assert len(bus.get_history()) == 0

    def test_get_subscriber_count(self) -> None:
        """Test getting subscriber count."""
        bus = EventBus()

        @bus.subscribe("click")
        def handler1(event):
            pass

        @bus.subscribe("click")
        def handler2(event):
            pass

        assert bus.get_subscriber_count("click") == 2
        assert bus.get_subscriber_count("nonexistent") == 0


class TestEventBusManager:
    """Tests for EventBusManager."""

    def test_get_default_bus(self) -> None:
        """Test getting default bus."""
        manager = EventBusManager()
        bus1 = manager.get_bus()
        bus2 = manager.get_bus("default")
        assert bus1 is bus2

    def test_get_named_bus(self) -> None:
        """Test getting named bus."""
        manager = EventBusManager()
        bus1 = manager.get_bus("workflow")
        bus2 = manager.get_bus("workflow")
        assert bus1 is bus2

    def test_default_bus_property(self) -> None:
        """Test default_bus property."""
        manager = EventBusManager()
        assert manager.default_bus is manager.get_bus()


class TestEvents:
    """Tests for predefined event types."""

    def test_workflow_events(self) -> None:
        """Test workflow event types exist."""
        assert Events.WORKFLOW_STARTED == "workflow.started"
        assert Events.WORKFLOW_STOPPED == "workflow.stopped"
        assert Events.WORKFLOW_PAUSED == "workflow.paused"
        assert Events.WORKFLOW_RESUMED == "workflow.resumed"
        assert Events.WORKFLOW_ERROR == "workflow.error"
        assert Events.WORKFLOW_COMPLETED == "workflow.completed"

    def test_action_events(self) -> None:
        """Test action event types exist."""
        assert Events.ACTION_STARTED == "action.started"
        assert Events.ACTION_COMPLETED == "action.completed"
        assert Events.ACTION_FAILED == "action.failed"

    def test_ui_events(self) -> None:
        """Test UI event types exist."""
        assert Events.UI_CLICK == "ui.click"
        assert Events.UI_KEYPRESS == "ui.keypress"
        assert Events.UI_MOUSE_MOVE == "ui.mouse_move"

    def test_system_events(self) -> None:
        """Test system event types exist."""
        assert Events.SYSTEM_START == "system.start"
        assert Events.SYSTEM_STOP == "system.stop"
        assert Events.SYSTEM_ERROR == "system.error"
        assert Events.CONFIG_CHANGED == "config.changed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])