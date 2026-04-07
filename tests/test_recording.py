"""Tests for recording utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 before any imports to avoid utils/__init__.py issues
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import directly from the module file to avoid utils/__init__.py issues
import importlib.util
spec = importlib.util.spec_from_file_location(
    "recording",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "recording.py")
)
recording_module = importlib.util.module_from_spec(spec)

with patch.dict('sys.modules', {'PyQt5': MagicMock(), 'PyQt5.QtCore': MagicMock()}):
    spec.loader.exec_module(recording_module)

RecordedAction = recording_module.RecordedAction
check_pynput_permission = recording_module.check_pynput_permission
PYNPUT_AVAILABLE = recording_module.PYNPUT_AVAILABLE
PYAUTOGUI_AVAILABLE = recording_module.PYAUTOGUI_AVAILABLE


class TestRecordedAction:
    """Tests for RecordedAction."""

    def test_create(self) -> None:
        """Test creating RecordedAction."""
        action = RecordedAction(
            action_type="click",
            timestamp=1.5,
            params={"x": 100, "y": 200}
        )
        assert action.action_type == "click"
        assert action.timestamp == 1.5
        assert action.params == {"x": 100, "y": 200}

    def test_to_dict(self) -> None:
        """Test converting to dictionary."""
        action = RecordedAction(
            action_type="click",
            timestamp=1.5,
            params={"x": 100, "y": 200}
        )
        d = action.to_dict()
        assert isinstance(d, dict)
        assert d["action_type"] == "click"
        assert d["timestamp"] == 1.5
        assert d["params"] == {"x": 100, "y": 200}

    def test_from_dict(self) -> None:
        """Test creating from dictionary."""
        data = {
            "action_type": "scroll",
            "timestamp": 2.5,
            "params": {"dx": 0, "dy": -1}
        }
        action = RecordedAction.from_dict(data)
        assert action.action_type == "scroll"
        assert action.timestamp == 2.5
        assert action.params == {"dx": 0, "dy": -1}

    def test_roundtrip(self) -> None:
        """Test to_dict and from_dict roundtrip."""
        original = RecordedAction(
            action_type="key_press",
            timestamp=3.0,
            params={"key": "a", "modifiers": ["shift"]}
        )
        d = original.to_dict()
        restored = RecordedAction.from_dict(d)
        assert restored.action_type == original.action_type
        assert restored.timestamp == original.timestamp
        assert restored.params == original.params


class TestCheckPynputPermission:
    """Tests for check_pynput_permission function."""

    def test_returns_bool(self) -> None:
        """Test check_pynput_permission returns a boolean."""
        result = check_pynput_permission()
        assert isinstance(result, bool)


class TestModuleConstants:
    """Tests for module constants."""

    def test_pynput_available_is_bool(self) -> None:
        """Test PYNPUT_AVAILABLE is a boolean."""
        assert isinstance(PYNPUT_AVAILABLE, bool)

    def test_pyautogui_available_is_bool(self) -> None:
        """Test PYAUTOGUI_AVAILABLE is a boolean."""
        assert isinstance(PYAUTOGUI_AVAILABLE, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
