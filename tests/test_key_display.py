"""Tests for key display utilities."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Mock PyQt5 before any imports
sys.modules['PyQt5'] = MagicMock()
sys.modules['PyQt5.QtCore'] = MagicMock()
sys.modules['PyQt5.QtWidgets'] = MagicMock()
sys.modules['PyQt5.QtGui'] = MagicMock()

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
    }):
        spec.loader.exec_module(module)
    return module


key_display_module = load_module_from_file(
    "key_display",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "key_display.py")
)

KeyDisplayWindow = key_display_module.KeyDisplayWindow
key_display_window = key_display_module.key_display_window


class TestKeyDisplayWindow:
    """Tests for KeyDisplayWindow."""

    def test_init(self) -> None:
        """Test initialization."""
        window = KeyDisplayWindow()
        assert window._process is None
        assert window._enabled is False
        assert window._script_path.endswith('key_display_standalone.py')

    def test_is_enabled_initially_false(self) -> None:
        """Test is_enabled returns False initially."""
        window = KeyDisplayWindow()
        assert window.is_enabled() is False

    def test_is_enabled_with_terminated_process(self) -> None:
        """Test is_enabled returns False when process terminated."""
        window = KeyDisplayWindow()
        mock_process = MagicMock()
        mock_process.poll.return_value = 1  # Process has exited
        window._process = mock_process
        window._enabled = True
        assert window.is_enabled() is False
        assert window._process is None

    def test_is_enabled_with_running_process(self) -> None:
        """Test is_enabled returns True when process running."""
        window = KeyDisplayWindow()
        mock_process = MagicMock()
        mock_process.poll.return_value = None  # Process still running
        window._process = mock_process
        window._enabled = True
        assert window.is_enabled() is True

    def test_disable(self) -> None:
        """Test disable terminates process."""
        window = KeyDisplayWindow()
        mock_process = MagicMock()
        window._process = mock_process
        window._enabled = True
        window.disable()
        mock_process.terminate.assert_called_once()
        assert window._process is None
        assert window._enabled is False

    def test_disable_when_not_running(self) -> None:
        """Test disable when no process running."""
        window = KeyDisplayWindow()
        window.disable()
        assert window._process is None
        assert window._enabled is False

    def test_toggle_from_disabled(self) -> None:
        """Test toggle from disabled state."""
        window = KeyDisplayWindow()
        with patch.object(window, 'enable', return_value=True) as mock_enable:
            window._enabled = False
            result = window.toggle()
            mock_enable.assert_called_once()
            assert result is True

    def test_toggle_from_enabled(self) -> None:
        """Test toggle from enabled state."""
        window = KeyDisplayWindow()
        window._enabled = True
        with patch.object(window, 'disable') as mock_disable:
            result = window.toggle()
            mock_disable.assert_called_once()
            assert result is False

    def test_enable_already_enabled(self) -> None:
        """Test enable when already enabled and running."""
        window = KeyDisplayWindow()
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        window._process = mock_process
        window._enabled = True
        with patch('subprocess.Popen') as mock_popen:
            result = window.enable()
            mock_popen.assert_not_called()
            assert result is True

    def test_enable_starts_new_process(self) -> None:
        """Test enable starts new process when none running."""
        window = KeyDisplayWindow()
        mock_popen = MagicMock()
        with patch.object(window, '_script_path', '/fake/path/key_display_standalone.py'):
            with patch('subprocess.Popen', return_value=mock_popen) as mock_call:
                result = window.enable()
                mock_call.assert_called_once()
                assert result is True
                assert window._enabled is True

    def test_enable_failure(self) -> None:
        """Test enable handles failure gracefully."""
        window = KeyDisplayWindow()
        with patch('subprocess.Popen', side_effect=Exception("Test error")):
            result = window.enable()
            assert result is False
            assert window._enabled is False


class TestGlobalInstance:
    """Tests for global singleton instance."""

    def test_global_instance_exists(self) -> None:
        """Test global instance exists."""
        assert key_display_window is not None
        assert isinstance(key_display_window, KeyDisplayWindow)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
