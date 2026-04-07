"""Tests for application logger utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.app_logger import (
    LogEntry,
    AppLogger,
)


class TestLogEntry:
    """Tests for LogEntry."""

    def test_create(self) -> None:
        """Test creating LogEntry."""
        entry = LogEntry(level="INFO", message="test message", module="test_module")
        assert entry.level == "INFO"
        assert entry.message == "test message"
        assert entry.module == "test_module"
        assert entry.timestamp is not None

    def test_to_dict(self) -> None:
        """Test converting to dictionary."""
        entry = LogEntry(level="INFO", message="test", module="mod")
        d = entry.to_dict()
        assert isinstance(d, dict)
        assert d["level"] == "INFO"
        assert d["message"] == "test"
        assert d["module"] == "mod"
        assert "timestamp" in d


class TestAppLogger:
    """Tests for AppLogger."""

    def test_singleton(self) -> None:
        """Test AppLogger is singleton."""
        logger1 = AppLogger()
        logger2 = AppLogger()
        assert logger1 is logger2

    def test_debug(self) -> None:
        """Test debug logging."""
        logger = AppLogger()
        # Should not raise
        logger.debug("debug message")

    def test_info(self) -> None:
        """Test info logging."""
        logger = AppLogger()
        logger.info("info message")

    def test_warning(self) -> None:
        """Test warning logging."""
        logger = AppLogger()
        logger.warning("warning message")

    def test_error(self) -> None:
        """Test error logging."""
        logger = AppLogger()
        logger.error("error message")

    def test_critical(self) -> None:
        """Test critical logging."""
        logger = AppLogger()
        logger.critical("critical message")

    def test_success(self) -> None:
        """Test success logging."""
        logger = AppLogger()
        logger.success("success message")

    def test_add_listener(self) -> None:
        """Test adding listener."""
        logger = AppLogger()
        received = []

        def listener(entry):
            received.append(entry)

        logger.add_listener(listener)
        logger.info("test")
        assert len(received) >= 1

    def test_remove_listener(self) -> None:
        """Test removing listener."""
        logger = AppLogger()

        def listener(entry):
            pass

        logger.add_listener(listener)
        logger.remove_listener(listener)
        # Listener should no longer receive events

    def test_get_entries(self) -> None:
        """Test getting log entries."""
        logger = AppLogger()
        logger.info("test message")
        entries = logger.get_entries(count=10)
        assert isinstance(entries, list)

    def test_get_entries_by_level(self) -> None:
        """Test getting entries by level."""
        logger = AppLogger()
        logger.info("info message")
        entries = logger.get_entries_by_level("INFO")
        assert isinstance(entries, list)
        for e in entries:
            assert e.level == "INFO"

    def test_clear(self) -> None:
        """Test clearing entries."""
        logger = AppLogger()
        logger.info("test")
        logger.clear()
        entries = logger.get_entries()
        assert len(entries) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])