"""Tests for hotkey utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestHotkeyManager:
    """Tests for HotkeyManager."""

    def test_parse_hotkey(self) -> None:
        """Test parsing hotkey strings."""
        from utils.hotkey import HotkeyManager
        result = HotkeyManager.parse_hotkey("ctrl+shift+a")
        assert result == "ctrl+shift+a"

    def test_format_hotkey(self) -> None:
        """Test formatting hotkey for display."""
        from utils.hotkey import HotkeyManager
        result = HotkeyManager.format_hotkey("ctrl+shift+a")
        assert "CTRL" in result
        assert "SHIFT" in result
        assert "A" in result

    def test_default_hotkeys(self) -> None:
        """Test default hotkeys exist."""
        from utils.hotkey import HotkeyManager
        defaults = HotkeyManager.DEFAULT_HOTKEYS
        assert "start" in defaults
        assert "stop" in defaults
        assert "pause" in defaults


if __name__ == "__main__":
    pytest.main([__file__, "-v"])