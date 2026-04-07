"""Tests for keyboard utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.keyboard import (
    KeyCode,
    KeySequence,
    Hotkey,
    HotkeyManager,
    KeyMapper,
)


class TestKeyCode:
    """Tests for KeyCode."""

    def test_special_keys(self) -> None:
        """Test special key codes exist."""
        assert KeyCode.ENTER == 0x0D
        assert KeyCode.ESCAPE == 0x1B
        assert KeyCode.SPACE == 0x20

    def test_arrow_keys(self) -> None:
        """Test arrow key codes."""
        assert KeyCode.LEFT == 0x25
        assert KeyCode.UP == 0x26
        assert KeyCode.RIGHT == 0x27
        assert KeyCode.DOWN == 0x28

    def test_function_keys(self) -> None:
        """Test function key codes."""
        assert KeyCode.F1 == 0x70
        assert KeyCode.F12 == 0x7B


class TestKeySequence:
    """Tests for KeySequence."""

    def test_press(self) -> None:
        """Test adding key press."""
        seq = KeySequence()
        seq.press(0x41)
        assert ("press", 0x41) in seq.events

    def test_release(self) -> None:
        """Test adding key release."""
        seq = KeySequence()
        seq.release(0x41)
        assert ("release", 0x41) in seq.events

    def test_type(self) -> None:
        """Test key type (press and release)."""
        seq = KeySequence()
        seq.type(0x41)
        events = seq.events
        assert ("press", 0x41) in events
        assert ("release", 0x41) in events

    def test_text(self) -> None:
        """Test text typing."""
        seq = KeySequence()
        seq.text("AB")
        events = seq.events
        assert ("press", 0x41) in events
        assert ("release", 0x41) in events

    def test_hold_release(self) -> None:
        """Test hold and release all."""
        seq = KeySequence()
        seq.hold(0x11, 0x41).release_all(0x11, 0x41)
        events = seq.events
        assert ("press", 0x11) in events
        assert ("release", 0x41) in events

    def test_clear(self) -> None:
        """Test clearing sequence."""
        seq = KeySequence()
        seq.press(0x41)
        seq.clear()
        assert len(seq.events) == 0


class TestHotkey:
    """Tests for Hotkey."""

    def test_create(self) -> None:
        """Test creating hotkey."""
        hk = Hotkey({0x11}, 0x43)  # Ctrl+C
        assert 0x11 in hk.modifiers
        assert hk.key_code == 0x43

    def test_equality(self) -> None:
        """Test hotkey equality."""
        hk1 = Hotkey({0x11}, 0x43)
        hk2 = Hotkey({0x11}, 0x43)
        hk3 = Hotkey({0x10}, 0x43)
        assert hk1 == hk2
        assert hk1 != hk3

    def test_hash(self) -> None:
        """Test hotkey hashing."""
        hk = Hotkey({0x11}, 0x43)
        assert hash(hk) == hash(hk)


class TestHotkeyManager:
    """Tests for HotkeyManager."""

    def test_create(self) -> None:
        """Test creating manager."""
        manager = HotkeyManager()
        assert len(manager._handlers) == 0

    def test_register(self) -> None:
        """Test registering hotkey."""
        manager = HotkeyManager()
        hk = Hotkey({0x11}, 0x43)
        manager.register(hk, lambda: None)
        assert hk in manager._handlers

    def test_unregister(self) -> None:
        """Test unregistering hotkey."""
        manager = HotkeyManager()
        hk = Hotkey({0x11}, 0x43)
        manager.register(hk, lambda: None)
        manager.unregister(hk)
        assert hk not in manager._handlers

    def test_trigger(self) -> None:
        """Test triggering hotkey."""
        called = []

        def handler():
            called.append(1)

        manager = HotkeyManager()
        hk = Hotkey({0x11}, 0x43)
        manager.register(hk, handler)
        manager.trigger(hk)
        assert called == [1]


class TestKeyMapper:
    """Tests for KeyMapper."""

    def test_create(self) -> None:
        """Test creating mapper."""
        mapper = KeyMapper()
        assert len(mapper._mappings) == 0

    def test_map(self) -> None:
        """Test mapping sequence."""
        mapper = KeyMapper()
        mapper.map("ctrl+c", lambda: None)
        assert "ctrl+c" in mapper._mappings

    def test_unmap(self) -> None:
        """Test unmapping sequence."""
        mapper = KeyMapper()
        mapper.map("ctrl+c", lambda: None)
        result = mapper.unmap("ctrl+c")
        assert result is True
        assert "ctrl+c" not in mapper._mappings

    def test_execute(self) -> None:
        """Test executing mapped sequence."""
        called = []
        mapper = KeyMapper()
        mapper.map("ctrl+c", lambda: called.append(1))
        result = mapper.execute("ctrl+c")
        assert result is True
        assert called == [1]

    def test_execute_unknown(self) -> None:
        """Test executing unknown sequence."""
        mapper = KeyMapper()
        result = mapper.execute("ctrl+x")
        assert result is False

    def test_get_action(self) -> None:
        """Test getting action."""
        mapper = KeyMapper()
        action = lambda: None
        mapper.map("ctrl+c", action)
        assert mapper.get_action("ctrl+c") is action


if __name__ == "__main__":
    pytest.main([__file__, "-v"])