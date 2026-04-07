"""Tests for file watcher utilities."""

import os
import sys
import tempfile
import threading
import time
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


file_watcher_module = load_module_from_file(
    "file_watcher",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "utils", "file_watcher.py")
)

FileEventType = file_watcher_module.FileEventType
FileEvent = file_watcher_module.FileEvent
FileWatcher = file_watcher_module.FileWatcher
ConfigWatcher = file_watcher_module.ConfigWatcher
DirectoryWatcher = file_watcher_module.DirectoryWatcher


class TestFileEventType:
    """Tests for FileEventType enum."""

    def test_all_types_exist(self) -> None:
        """Test all event types exist."""
        assert FileEventType.CREATED is not None
        assert FileEventType.MODIFIED is not None
        assert FileEventType.DELETED is not None
        assert FileEventType.RENAMED is not None


class TestFileEvent:
    """Tests for FileEvent dataclass."""

    def test_create(self) -> None:
        """Test creating FileEvent."""
        event = FileEvent(
            event_type=FileEventType.MODIFIED,
            path="/path/to/file.txt"
        )
        assert event.event_type == FileEventType.MODIFIED
        assert event.path == "/path/to/file.txt"
        assert event.timestamp > 0

    def test_create_with_old_path(self) -> None:
        """Test creating FileEvent with old_path."""
        event = FileEvent(
            event_type=FileEventType.RENAMED,
            path="/new/path.txt",
            old_path="/old/path.txt"
        )
        assert event.old_path == "/old/path.txt"


class TestFileWatcher:
    """Tests for FileWatcher class."""

    def test_init(self) -> None:
        """Test initialization."""
        watcher = FileWatcher(poll_interval=0.5)
        assert watcher.poll_interval == 0.5
        assert watcher.is_running is False

    def test_watch_and_unwatch(self) -> None:
        """Test watch and unwatch."""
        watcher = FileWatcher()
        callback = MagicMock()
        watcher.watch("/path/to/file.txt", callback)
        assert "/path/to/file.txt" in watcher._watchers
        watcher.unwatch("/path/to/file.txt")
        assert "/path/to/file.txt" not in watcher._watchers

    def test_start_stop(self) -> None:
        """Test start and stop."""
        watcher = FileWatcher(poll_interval=0.01)
        watcher.start()
        assert watcher.is_running is True
        watcher.stop()
        assert watcher.is_running is False

    def test_start_not_running_twice(self) -> None:
        """Test starting when not running does nothing."""
        watcher = FileWatcher()
        watcher.start()
        watcher.start()  # Should not start twice
        assert watcher.is_running is True
        watcher.stop()

    def test_stop_not_running(self) -> None:
        """Test stopping when not running does nothing."""
        watcher = FileWatcher()
        watcher.stop()  # Should not raise
        assert watcher.is_running is False

    def test_watch_sets_initial_state(self) -> None:
        """Test watch sets initial state for existing file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_path = f.name

        try:
            watcher = FileWatcher()
            callback = MagicMock()
            watcher.watch(temp_path, callback)
            assert temp_path in watcher._file_states
            state = watcher._file_states[temp_path]
            assert state["exists"] is True
        finally:
            os.unlink(temp_path)

        watcher.unwatch(temp_path)

    def test_should_include_default_excludes(self) -> None:
        """Test default exclude patterns."""
        watcher = FileWatcher()
        # Default excludes .git, __pycache__, *.pyc
        assert watcher._should_include("file.txt") is True
        assert watcher._should_include("file.pyc") is False
        assert watcher._should_include(".git/config") is False
        assert watcher._should_include("__pycache__/file.py") is False

    def test_should_include_with_patterns(self) -> None:
        """Test include patterns."""
        watcher = FileWatcher(include_patterns=["*.json", "*.yaml"])
        assert watcher._should_include("config.json") is True
        assert watcher._should_include("config.yaml") is True
        assert watcher._should_include("config.txt") is False


class TestConfigWatcher:
    """Tests for ConfigWatcher class."""

    def test_init(self) -> None:
        """Test initialization."""
        watcher = ConfigWatcher(poll_interval=1.0, max_retries=5)
        assert watcher.poll_interval == 1.0
        assert watcher.max_retries == 5

    def test_watch(self) -> None:
        """Test watching a config."""
        watcher = ConfigWatcher()
        config = {"key": "value"}
        watcher.watch("/path/to/config.json", config)
        assert "/path/to/config.json" in watcher._configs
        assert watcher._configs["/path/to/config.json"] == config

    def test_on_reload(self) -> None:
        """Test registering reload callback."""
        watcher = ConfigWatcher()
        callback = MagicMock()
        watcher.on_reload(callback)
        assert callback in watcher._reload_callbacks

    def test_reload_manual(self) -> None:
        """Test manual reload."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            import json
            json.dump({"key": "value1"}, f)
            temp_path = f.name

        try:
            watcher = ConfigWatcher()
            watcher.watch(temp_path, {"key": "original"})
            result = watcher.reload(temp_path)
            assert result is not None
            assert result["key"] == "value1"
        finally:
            os.unlink(temp_path)


class TestDirectoryWatcher:
    """Tests for DirectoryWatcher class."""

    def test_init(self) -> None:
        """Test initialization."""
        watcher = DirectoryWatcher("/path/to/dir", recursive=True, poll_interval=0.5)
        assert watcher.path == "/path/to/dir"
        assert watcher.recursive is True
        assert watcher.poll_interval == 0.5

    def test_watch_callback(self) -> None:
        """Test registering callback."""
        watcher = DirectoryWatcher("/path/to/dir")
        callback = MagicMock()
        watcher.watch(callback)
        assert callback in watcher._callbacks


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
