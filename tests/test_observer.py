"""Tests for observer utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.observer import (
    Observer,
    Observable,
    PropertyObserver,
    PropertyChange,
    EventEmitter,
)


class MockObserver(Observer):
    """Mock observer for testing."""

    def __init__(self) -> None:
        self.updates = []

    def update(self, event: dict) -> None:
        """Record update."""
        self.updates.append(event)


class TestObservable:
    """Tests for Observable."""

    def test_attach_detach(self) -> None:
        """Test attaching and detaching observers."""
        subject = Observable()
        observer = MockObserver()

        subject.attach(observer)
        assert observer in subject._observers

        subject.detach(observer)
        assert observer not in subject._observers

    def test_notify(self) -> None:
        """Test notifying observers."""
        subject = Observable()
        observer = MockObserver()

        subject.attach(observer)
        subject.notify({"message": "test"})

        assert len(observer.updates) == 1
        assert observer.updates[0]["message"] == "test"

    def test_event_handlers(self) -> None:
        """Test event handler registration."""
        subject = Observable()
        received = []

        def handler(event):
            received.append(event)

        subject.on("click", handler)
        subject.emit("click", {"x": 100})

        assert len(received) == 1

    def test_off(self) -> None:
        """Test removing event handlers."""
        subject = Observable()
        received = []

        def handler(event):
            received.append(event)

        subject.on("click", handler)
        subject.off("click", handler)
        subject.emit("click", {})

        assert len(received) == 0


class TestPropertyObserver:
    """Tests for PropertyObserver."""

    def test_property_change_notification(self) -> None:
        """Test property change notification."""
        class Person(PropertyObserver):
            def __init__(self):
                super().__init__()
                self._name = ""

            @property
            def name(self):
                return self._name

            @name.setter
            def name(self, value):
                old = self._name
                self._name = value
                self.notify_property_change("name", old, value)

        person = Person()
        changes = []

        person.observe_property("name", lambda c: changes.append(c))

        person.name = "Alice"
        assert len(changes) == 1
        assert changes[0].old_value == ""
        assert changes[0].new_value == "Alice"


class TestEventEmitter:
    """Tests for EventEmitter."""

    def test_on_off(self) -> None:
        """Test registering and unregistering handlers."""
        emitter = EventEmitter()
        received = []

        def handler(data):
            received.append(data)

        emitter.on("data", handler)
        emitter.emit("data", "test")

        assert received[0] == "test"

        emitter.off("data", handler)
        emitter.emit("data", "test2")
        assert len(received) == 1

    def test_once(self) -> None:
        """Test one-time handler."""
        emitter = EventEmitter()
        received = []

        def handler(data):
            received.append(data)

        emitter.once("single", handler)
        emitter.emit("single", "first")
        emitter.emit("single", "second")

        assert received == ["first"]

    def test_listener_count(self) -> None:
        """Test listener count."""
        emitter = EventEmitter()

        def handler():
            pass

        emitter.on("event", handler)
        assert emitter.listener_count("event") == 1

        emitter.off("event", handler)
        assert emitter.listener_count("event") == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])