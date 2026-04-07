"""Tests for notification utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.notification import (
    NotificationLevel,
    Notification,
    NotificationCenter,
    SystemNotification,
    NotificationQueue,
    NotificationFilter,
)


class TestNotificationLevel:
    """Tests for NotificationLevel."""

    def test_values(self) -> None:
        """Test level values."""
        assert NotificationLevel.DEBUG.value == "debug"
        assert NotificationLevel.INFO.value == "info"
        assert NotificationLevel.ERROR.value == "error"


class TestNotification:
    """Tests for Notification."""

    def test_create(self) -> None:
        """Test creating notification."""
        n = Notification(
            title="Test",
            message="Test message",
        )
        assert n.title == "Test"
        assert n.message == "Test message"
        assert n.level == NotificationLevel.INFO


class TestNotificationCenter:
    """Tests for NotificationCenter."""

    def test_create(self) -> None:
        """Test creating center."""
        center = NotificationCenter()
        assert len(center._handlers) == 0

    def test_add_handler(self) -> None:
        """Test adding handler."""
        center = NotificationCenter()
        center.add_handler(NotificationLevel.INFO, lambda n: None)
        assert NotificationLevel.INFO in center._handlers

    def test_notify(self) -> None:
        """Test sending notification."""
        center = NotificationCenter()
        received = []

        def handler(n):
            received.append(n)

        center.add_handler(NotificationLevel.INFO, handler)
        n = Notification(title="Test", message="Message")
        center.notify(n)

        assert len(received) == 1
        assert received[0].title == "Test"

    def test_get_history(self) -> None:
        """Test getting history."""
        center = NotificationCenter()
        center.notify(Notification(title="1", message=""))
        center.notify(Notification(title="2", message=""))
        history = center.get_history(limit=1)
        assert len(history) == 1
        assert history[0].title == "2"

    def test_clear_history(self) -> None:
        """Test clearing history."""
        center = NotificationCenter()
        center.notify(Notification(title="Test", message=""))
        center.clear_history()
        assert len(center.get_history()) == 0


class TestSystemNotification:
    """Tests for SystemNotification."""

    def test_show_returns_bool(self) -> None:
        """Test show returns boolean."""
        result = SystemNotification.show("Test", "Message")
        assert isinstance(result, bool)

    def test_show_toast_returns_bool(self) -> None:
        """Test show_toast returns boolean."""
        result = SystemNotification.show_toast("Test", "Message")
        assert isinstance(result, bool)


class TestNotificationQueue:
    """Tests for NotificationQueue."""

    def test_create(self) -> None:
        """Test creating queue."""
        queue = NotificationQueue(batch_interval=1.0)
        assert queue._batch_interval == 1.0
        assert queue.size == 0

    def test_enqueue(self) -> None:
        """Test adding to queue."""
        queue = NotificationQueue()
        queue.enqueue(Notification(title="Test", message=""))
        assert queue.size == 1

    def test_start_stop(self) -> None:
        """Test starting and stopping queue."""
        queue = NotificationQueue()
        center = NotificationCenter()
        queue.start(center)
        assert queue._running is True
        queue.stop()
        assert queue._running is False


class TestNotificationFilter:
    """Tests for NotificationFilter."""

    def test_create(self) -> None:
        """Test creating filter."""
        f = NotificationFilter()
        assert f._level_filter is None

    def test_level_filter(self) -> None:
        """Test level filter."""
        f = NotificationFilter().level(NotificationLevel.WARNING)
        assert f._level_filter == NotificationLevel.WARNING

    def test_keyword_filter(self) -> None:
        """Test keyword filter."""
        f = NotificationFilter().keyword("test")
        assert f._keyword_filter == "test"

    def test_matches_level(self) -> None:
        """Test matching by level."""
        f = NotificationFilter().level(NotificationLevel.WARNING)
        n = Notification(title="Test", message="", level=NotificationLevel.ERROR)
        assert f.matches(n) is True

        n_low = Notification(title="Test", message="", level=NotificationLevel.INFO)
        assert f.matches(n_low) is False

    def test_matches_keyword(self) -> None:
        """Test matching by keyword."""
        f = NotificationFilter().keyword("hello")
        n = Notification(title="Hello World", message="")
        assert f.matches(n) is True

        n2 = Notification(title="Goodbye", message="")
        assert f.matches(n2) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])