"""Tests for window management utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.window import WindowInfo, WindowFinder, WindowOperations, WindowMonitor


class TestWindowInfo:
    """Tests for WindowInfo."""

    def test_create(self) -> None:
        """Test creating window info."""
        info = WindowInfo(
            hwnd=1234,
            title="Test Window",
            process_name="test.exe",
            process_id=5678,
            rect=(0, 0, 800, 600),
        )
        assert info.hwnd == 1234
        assert info.title == "Test Window"
        assert info.visible is True


class TestWindowFinder:
    """Tests for WindowFinder."""

    def test_add_filter(self) -> None:
        """Test adding filter."""
        finder = WindowFinder()
        finder.add_filter(lambda w: w.hwnd > 0)
        assert len(finder._filters) == 1

    def test_with_title(self) -> None:
        """Test filtering by title."""
        finder = WindowFinder().with_title("test")
        assert len(finder._filters) == 1

    def test_with_process(self) -> None:
        """Test filtering by process."""
        finder = WindowFinder().with_process("notepad")
        assert len(finder._filters) == 1

    def test_visible_only(self) -> None:
        """Test visible only filter."""
        finder = WindowFinder().visible_only()
        assert len(finder._filters) == 1

    def test_enabled_only(self) -> None:
        """Test enabled only filter."""
        finder = WindowFinder().enabled_only()
        assert len(finder._filters) == 1

    def test_find_all(self) -> None:
        """Test finding all windows."""
        finder = WindowFinder()
        windows = finder.find_all()
        assert isinstance(windows, list)

    def test_find_first(self) -> None:
        """Test finding first window."""
        finder = WindowFinder()
        window = finder.find_first()
        assert window is None or isinstance(window, WindowInfo)


class TestWindowOperations:
    """Tests for WindowOperations."""

    def test_operations_exist(self) -> None:
        """Test that operations exist."""
        assert hasattr(WindowOperations, "bring_to_front")
        assert hasattr(WindowOperations, "minimize")
        assert hasattr(WindowOperations, "maximize")
        assert hasattr(WindowOperations, "restore")
        assert hasattr(WindowOperations, "close")
        assert hasattr(WindowOperations, "set_position")


class TestWindowMonitor:
    """Tests for WindowMonitor."""

    def test_create(self) -> None:
        """Test creating monitor."""
        monitor = WindowMonitor()
        assert monitor.is_running is False

    def test_on_create(self) -> None:
        """Test registering create callback."""
        monitor = WindowMonitor()
        monitor.on_create(lambda w: None)
        assert "create" in monitor._callbacks

    def test_on_destroy(self) -> None:
        """Test registering destroy callback."""
        monitor = WindowMonitor()
        monitor.on_destroy(lambda h: None)
        assert "destroy" in monitor._callbacks

    def test_on_focus(self) -> None:
        """Test registering focus callback."""
        monitor = WindowMonitor()
        monitor.on_focus(lambda w: None)
        assert "focus" in monitor._callbacks

    def test_start_stop(self) -> None:
        """Test starting and stopping monitor."""
        monitor = WindowMonitor()
        monitor.start()
        assert monitor.is_running is True
        monitor.stop()
        assert monitor.is_running is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])