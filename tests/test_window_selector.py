"""Tests for window selector utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 and platform-specific libraries before importing
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()
sys.modules['PyQt5.QtWidgets'] = MagicMock()
sys.modules['PyQt5.QtGui'] = MagicMock()
sys.modules['pygetwindow'] = MagicMock()

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
        'pygetwindow': MagicMock(),
    }):
        spec.loader.exec_module(module)
    return module


window_selector_module = load_module_from_file(
    "window_selector",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "window_selector.py")
)

WindowInfo = window_selector_module.WindowInfo
IS_MACOS = window_selector_module.IS_MACOS
GW_AVAILABLE = window_selector_module.GW_AVAILABLE


class TestWindowInfo:
    """Tests for WindowInfo class."""

    def test_create_minimal(self) -> None:
        """Test creating WindowInfo with minimal args."""
        info = WindowInfo(title="Test Window")
        assert info.title == "Test Window"
        assert info.hwnd is None
        assert info.left == 0
        assert info.top == 0
        assert info.width == 0
        assert info.height == 0

    def test_create_full(self) -> None:
        """Test creating WindowInfo with all args."""
        info = WindowInfo(
            title="Test Window",
            hwnd=1234,
            left=100,
            top=200,
            width=800,
            height=600
        )
        assert info.title == "Test Window"
        assert info.hwnd == 1234
        assert info.left == 100
        assert info.top == 200
        assert info.width == 800
        assert info.height == 600

    def test_region(self) -> None:
        """Test region property."""
        info = WindowInfo(title="Test", left=100, top=200, width=800, height=600)
        assert info.region == (100, 200, 800, 600)

    def test_center(self) -> None:
        """Test center property."""
        info = WindowInfo(title="Test", left=100, top=200, width=800, height=600)
        # Center is left + width//2, top + height//2
        assert info.center == (100 + 400, 200 + 300)  # (500, 500)


class TestModuleConstants:
    """Tests for module constants."""

    def test_is_macos_is_bool(self) -> None:
        """Test IS_MACOS is a boolean."""
        assert isinstance(IS_MACOS, bool)

    def test_gw_available_is_bool(self) -> None:
        """Test GW_AVAILABLE is a boolean."""
        assert isinstance(GW_AVAILABLE, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
