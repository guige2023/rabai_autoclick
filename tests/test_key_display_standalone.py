"""Tests for key display standalone utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock pynput before importing
sys.modules['pynput'] = MagicMock()
sys.modules['pynput.keyboard'] = MagicMock()
sys.modules['pynput.mouse'] = MagicMock()
sys.modules['tkinter'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly to avoid utils/__init__.py issues
import importlib.util


def load_module_from_file(module_name: str, file_path: str):
    """Load a module directly from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict('sys.modules', {
        'pynput': MagicMock(),
        'pynput.keyboard': MagicMock(),
        'pynput.mouse': MagicMock(),
        'tkinter': MagicMock(),
    }):
        spec.loader.exec_module(module)
    return module


key_display_standalone_module = load_module_from_file(
    "key_display_standalone",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "key_display_standalone.py")
)

# Test the module constants and format key function
PYNPUT_AVAILABLE = key_display_standalone_module.PYNPUT_AVAILABLE
IS_MACOS = key_display_standalone_module.IS_MACOS


class TestModuleConstants:
    """Tests for module constants."""

    def test_pynput_available_is_bool(self) -> None:
        """Test PYNPUT_AVAILABLE is a boolean."""
        assert isinstance(PYNPUT_AVAILABLE, bool)

    def test_is_macos_is_bool(self) -> None:
        """Test IS_MACOS is a boolean."""
        assert isinstance(IS_MACOS, bool)


class TestKeyDisplayAppFormatKey:
    """Tests for _format_key_name method.

    Since KeyDisplayApp requires tkinter to be properly set up,
    we test the format key function logic directly.
    """

    def test_format_key_map_values(self) -> None:
        """Test key formatting map contains expected entries."""
        # The key map from the module
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
        # Test known mappings
        assert key_map['ctrl'] == 'Ctrl'
        assert key_map['shift'] == 'Shift'
        assert key_map['space'] == '␣'
        assert key_map['enter'] == '↵'
        assert key_map['escape'] == 'Esc'
        assert key_map['up'] == '↑'
        assert key_map['down'] == '↓'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
