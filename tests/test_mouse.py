"""Tests for mouse utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.mouse import (
    MouseButton,
    MouseEventType,
    MouseEvent,
    Point,
    Rectangle,
    ClickPattern,
    MouseRecorder,
    MouseSimulator,
    MousePositionTracker,
)


class TestMouseButton:
    """Tests for MouseButton."""

    def test_values(self) -> None:
        """Test button values."""
        assert MouseButton.LEFT.value == 1
        assert MouseButton.RIGHT.value == 2
        assert MouseButton.MIDDLE.value == 4


class TestMouseEventType:
    """Tests for MouseEventType."""

    def test_values(self) -> None:
        """Test event type values."""
        assert MouseEventType.MOVE.value == "move"
        assert MouseEventType.CLICK.value == "click"


class TestMouseEvent:
    """Tests for MouseEvent."""

    def test_create(self) -> None:
        """Test creating mouse event."""
        event = MouseEvent(
            event_type=MouseEventType.CLICK,
            x=100,
            y=200,
        )
        assert event.x == 100
        assert event.y == 200
        assert event.button == MouseButton.LEFT


class TestPoint:
    """Tests for Point."""

    def test_create(self) -> None:
        """Test creating point."""
        p = Point(10, 20)
        assert p.x == 10
        assert p.y == 20

    def test_distance_to(self) -> None:
        """Test distance calculation."""
        p1 = Point(0, 0)
        p2 = Point(3, 4)
        assert p1.distance_to(p2) == 5.0

    def test_add(self) -> None:
        """Test adding points."""
        p1 = Point(1, 2)
        p2 = Point(3, 4)
        result = p1 + p2
        assert result.x == 4
        assert result.y == 6

    def test_subtract(self) -> None:
        """Test subtracting points."""
        p1 = Point(5, 7)
        p2 = Point(2, 3)
        result = p1 - p2
        assert result.x == 3
        assert result.y == 4


class TestRectangle:
    """Tests for Rectangle."""

    def test_create(self) -> None:
        """Test creating rectangle."""
        r = Rectangle(10, 20, 100, 50)
        assert r.x == 10
        assert r.y == 20
        assert r.width == 100
        assert r.height == 50

    def test_left_right(self) -> None:
        """Test left and right edges."""
        r = Rectangle(10, 20, 100, 50)
        assert r.left == 10
        assert r.right == 110

    def test_top_bottom(self) -> None:
        """Test top and bottom edges."""
        r = Rectangle(10, 20, 100, 50)
        assert r.top == 20
        assert r.bottom == 70

    def test_contains(self) -> None:
        """Test point containment."""
        r = Rectangle(0, 0, 100, 100)
        assert r.contains(50, 50) is True
        assert r.contains(150, 150) is False

    def test_center(self) -> None:
        """Test center calculation."""
        r = Rectangle(0, 0, 100, 100)
        center = r.center()
        assert center.x == 50
        assert center.y == 50


class TestClickPattern:
    """Tests for ClickPattern."""

    def test_add_click(self) -> None:
        """Test adding click."""
        pattern = ClickPattern()
        pattern.add_click(100, 200)
        assert len(pattern.clicks) == 1

    def test_add_double_click(self) -> None:
        """Test adding double click."""
        pattern = ClickPattern()
        pattern.add_double_click(100, 200)
        assert len(pattern.clicks) == 1

    def test_add_drag(self) -> None:
        """Test adding drag."""
        pattern = ClickPattern()
        pattern.add_drag(0, 0, 100, 100)
        assert len(pattern.clicks) == 1

    def test_clear(self) -> None:
        """Test clearing pattern."""
        pattern = ClickPattern()
        pattern.add_click(100, 200)
        pattern.clear()
        assert len(pattern.clicks) == 0


class TestMouseRecorder:
    """Tests for MouseRecorder."""

    def test_start_stop(self) -> None:
        """Test recording state."""
        recorder = MouseRecorder()
        recorder.start_recording()
        assert recorder.is_recording is True
        recorder.stop_recording()
        assert recorder.is_recording is False

    def test_record_event(self) -> None:
        """Test recording events."""
        recorder = MouseRecorder()
        recorder.start_recording()
        event = MouseEvent(MouseEventType.CLICK, 100, 200)
        recorder.record_event(event)
        assert len(recorder.events) == 1

    def test_clear(self) -> None:
        """Test clearing events."""
        recorder = MouseRecorder()
        recorder.start_recording()
        event = MouseEvent(MouseEventType.CLICK, 100, 200)
        recorder.record_event(event)
        recorder.clear()
        assert len(recorder.events) == 0


class TestMouseSimulator:
    """Tests for MouseSimulator."""

    def test_click_exists(self) -> None:
        """Test click method exists."""
        assert hasattr(MouseSimulator, "click")

    def test_double_click_exists(self) -> None:
        """Test double_click method exists."""
        assert hasattr(MouseSimulator, "double_click")

    def test_right_click_exists(self) -> None:
        """Test right_click method exists."""
        assert hasattr(MouseSimulator, "right_click")

    def test_move_exists(self) -> None:
        """Test move method exists."""
        assert hasattr(MouseSimulator, "move")

    def test_drag_exists(self) -> None:
        """Test drag method exists."""
        assert hasattr(MouseSimulator, "drag")


class TestMousePositionTracker:
    """Tests for MousePositionTracker."""

    def test_create(self) -> None:
        """Test creating tracker."""
        tracker = MousePositionTracker()
        assert tracker._interval == 0.1
        assert tracker._running is False

    def test_start_stop(self) -> None:
        """Test starting and stopping."""
        tracker = MousePositionTracker()
        tracker.start()
        assert tracker._running is True
        tracker.stop()
        assert tracker._running is False

    def test_record_position(self) -> None:
        """Test recording position."""
        tracker = MousePositionTracker()
        tracker.start()
        tracker.record_position(100, 200, 1.0)
        positions = tracker.get_positions()
        assert len(positions) == 1
        assert positions[0] == (100, 200, 1.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])