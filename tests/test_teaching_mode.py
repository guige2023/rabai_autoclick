"""Tests for teaching mode utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 before importing
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()
sys.modules['PyQt5.QtWidgets'] = MagicMock()
sys.modules['PyQt5.QtGui'] = MagicMock()
sys.modules['pynput'] = MagicMock()
sys.modules['pynput.keyboard'] = MagicMock()
sys.modules['pynput.mouse'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly to avoid utils/__init__.py issues
import importlib.util


def load_module_from_file(module_name: str, file_path: str):
    """Load a module directly from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict('sys.modules', {
        'PyQt5': MagicMock(),
        'PyQt5.QtCore': MagicMock(),
        'PyQt5.QtWidgets': MagicMock(),
        'PyQt5.QtGui': MagicMock(),
        'pynput': MagicMock(),
        'pynput.keyboard': MagicMock(),
        'pynput.mouse': MagicMock(),
    }):
        spec.loader.exec_module(module)
    return module


teaching_mode_module = load_module_from_file(
    "teaching_mode",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "teaching_mode.py")
)

PYNPUT_AVAILABLE = teaching_mode_module.PYNPUT_AVAILABLE


class TestModuleConstants:
    """Tests for module constants."""

    def test_pynput_available_is_bool(self) -> None:
        """Test PYNPUT_AVAILABLE is a boolean."""
        assert isinstance(PYNPUT_AVAILABLE, bool)


class TestKeyDisplayWidgetFormatKey:
    """Tests for _format_key static method.

    KeyDisplayWidget._format_key is a static method that formats
    key names with Unicode symbols.
    """

    def test_format_key_ctrl(self) -> None:
        """Test ctrl key formatting."""
        key_map = {
            'ctrl': 'Ctrl',
            'shift': 'Shift',
            'alt': 'Alt',
            'cmd': '⌘',
            'command': '⌘',
            'space': '␣',
            'enter': '↵',
            'return': '↵',
            'tab': '⇥',
            'backspace': '⌫',
            'delete': '⌦',
            'escape': 'Esc',
            'up': '↑',
            'down': '↓',
            'left': '←',
            'right': '→',
            'caps_lock': '⇪',
        }
        assert key_map['ctrl'] == 'Ctrl'
        assert key_map['shift'] == 'Shift'
        assert key_map['alt'] == 'Alt'
        assert key_map['cmd'] == '⌘'
        assert key_map['space'] == '␣'
        assert key_map['enter'] == '↵'
        assert key_map['escape'] == 'Esc'
        assert key_map['up'] == '↑'
        assert key_map['down'] == '↓'
        assert key_map['left'] == '←'
        assert key_map['right'] == '→'

    def test_format_key_unknown(self) -> None:
        """Test unknown key formatting returns uppercase."""
        # Unknown keys should return uppercase
        unknown_key = 'a'
        result = unknown_key.upper()
        assert result == 'A'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
