"""Tests for clipboard utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.clipboard import (
    ClipboardFormat,
    Clipboard,
    ClipboardHistory,
    ClipboardMonitor,
    ClipboardFormatter,
)


class TestClipboardFormat:
    """Tests for ClipboardFormat."""

    def test_values(self) -> None:
        """Test format values."""
        assert ClipboardFormat.TEXT == "text"
        assert ClipboardFormat.HTML == "html"
        assert ClipboardFormat.IMAGE == "image"
        assert ClipboardFormat.FILES == "files"


class TestClipboard:
    """Tests for Clipboard."""

    def test_get_text(self) -> None:
        """Test getting text."""
        result = Clipboard.get_text()
        assert result is None or isinstance(result, str)

    def test_set_text(self) -> None:
        """Test setting text."""
        result = Clipboard.set_text("test")
        assert isinstance(result, bool)

    def test_clear(self) -> None:
        """Test clearing clipboard."""
        result = Clipboard.clear()
        assert isinstance(result, bool)


class TestClipboardHistory:
    """Tests for ClipboardHistory."""

    def test_create(self) -> None:
        """Test creating history."""
        history = ClipboardHistory(max_size=10)
        assert history.size == 0

    def test_add(self) -> None:
        """Test adding to history."""
        history = ClipboardHistory()
        history.add("test")
        assert history.size == 1
        assert history.get(0) == "test"

    def test_add_duplicate(self) -> None:
        """Test adding duplicate moves to front."""
        history = ClipboardHistory()
        history.add("test1")
        history.add("test2")
        history.add("test1")
        assert history.get(0) == "test1"
        assert history.get(1) == "test2"

    def test_get_out_of_bounds(self) -> None:
        """Test getting out of bounds index."""
        history = ClipboardHistory()
        assert history.get(10) is None

    def test_search(self) -> None:
        """Test searching history."""
        history = ClipboardHistory()
        history.add("hello world")
        history.add("goodbye world")
        results = history.search("hello")
        assert len(results) == 1
        assert "hello world" in results[0]

    def test_clear(self) -> None:
        """Test clearing history."""
        history = ClipboardHistory()
        history.add("test")
        history.clear()
        assert history.size == 0


class TestClipboardMonitor:
    """Tests for ClipboardMonitor."""

    def test_create(self) -> None:
        """Test creating monitor."""
        monitor = ClipboardMonitor()
        assert monitor._interval == 0.5
        assert monitor.is_running is False

    def test_on_change(self) -> None:
        """Test registering callback."""
        monitor = ClipboardMonitor()
        monitor.on_change(lambda x: None)
        assert len(monitor._callbacks) == 1

    def test_start_stop(self) -> None:
        """Test starting and stopping."""
        monitor = ClipboardMonitor()
        monitor.start()
        assert monitor.is_running is True
        monitor.stop()
        assert monitor.is_running is False


class TestClipboardFormatter:
    """Tests for ClipboardFormatter."""

    def test_to_plain_text(self) -> None:
        """Test HTML to plain text."""
        html = "<p>Hello</p>"
        result = ClipboardFormatter.to_plain_text(html)
        assert "Hello" in result

    def test_to_html(self) -> None:
        """Test plain text to HTML."""
        text = "Hello"
        result = ClipboardFormatter.to_html(text)
        assert "Hello" in result
        assert "<html>" in result

    def test_strip_formatting(self) -> None:
        """Test stripping formatting."""
        text = "  Hello  \x00World\x01"
        result = ClipboardFormatter.strip_formatting(text)
        assert "Hello" in result
        assert "World" in result
        assert "\x00" not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])