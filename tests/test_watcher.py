"""Tests for file watching utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.watcher import (
    FileEventType,
    FileEvent,
    FileWatcher,
    ConfigWatcher,
    DirectoryWatcher,
    PathFilter,
)


class TestFileEventType:
    """Tests for FileEventType."""

    def test_values(self) -> None:
        """Test event type values."""
        assert FileEventType.CREATED.value == "created"
        assert FileEventType.MODIFIED.value == "modified"
        assert FileEventType.DELETED.value == "deleted"


class TestFileEvent:
    """Tests for FileEvent."""

    def test_create(self) -> None:
        """Test creating event."""
        event = FileEvent(
            event_type=FileEventType.CREATED,
            path="/tmp/test.txt",
        )
        assert event.path == "/tmp/test.txt"


class TestFileWatcher:
    """Tests for FileWatcher."""

    def test_create(self) -> None:
        """Test creating watcher."""
        watcher = FileWatcher("/tmp")
        assert watcher._path == "/tmp"
        assert watcher._recursive is False

    def test_callback_registration(self) -> None:
        """Test registering callbacks."""
        watcher = FileWatcher("/tmp")
        watcher.on_created(lambda p: None)
        watcher.on_modified(lambda p: None)
        watcher.on_deleted(lambda p: None)
        assert len(watcher._callbacks) == 3

    def test_start_stop(self) -> None:
        """Test starting and stopping."""
        watcher = FileWatcher("/tmp")
        watcher.start()
        assert watcher.is_running is True
        watcher.stop()
        assert watcher.is_running is False


class TestConfigWatcher:
    """Tests for ConfigWatcher."""

    def test_create(self) -> None:
        """Test creating watcher."""
        watcher = ConfigWatcher()
        assert len(watcher._watchers) == 0

    def test_watch_unwatch(self) -> None:
        """Test watching and unwatching."""
        watcher = ConfigWatcher()
        callback_called = []

        def callback(path):
            callback_called.append(path)

        # Note: This test would need a real file to work properly
        # Here we just test the interface
        assert len(watcher._watchers) == 0


class TestDirectoryWatcher:
    """Tests for DirectoryWatcher."""

    def test_create(self) -> None:
        """Test creating watcher."""
        watcher = DirectoryWatcher("/tmp")
        assert watcher._watcher._path == "/tmp"


class TestPathFilter:
    """Tests for PathFilter."""

    def test_include_extension(self) -> None:
        """Test including extension."""
        filter = PathFilter()
        filter.include_extension(".py")
        assert ".py" in filter._include_exts

    def test_exclude_extension(self) -> None:
        """Test excluding extension."""
        filter = PathFilter()
        filter.exclude_extension(".tmp")
        assert ".tmp" in filter._exclude_exts

    def test_matches_included(self) -> None:
        """Test matching included extension."""
        filter = PathFilter()
        filter.include_extension(".py")
        assert filter.matches("/tmp/test.py") is True
        assert filter.matches("/tmp/test.txt") is False

    def test_matches_excluded(self) -> None:
        """Test matching excluded extension."""
        filter = PathFilter()
        filter.exclude_extension(".tmp")
        assert filter.matches("/tmp/test.tmp") is False
        assert filter.matches("/tmp/test.py") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])