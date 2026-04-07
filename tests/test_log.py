"""Tests for structured logging utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.log import (
    LogLevel,
    LogRecord,
    StructuredFormatter,
    JSONFormatter,
    ConsoleHandler,
    FileHandler,
    Logger,
    LogManager,
    get_logger,
    add_handler,
    set_level_all,
)


class TestLogLevel:
    """Tests for LogLevel."""

    def test_values(self) -> None:
        """Test log level values."""
        assert LogLevel.DEBUG.value == 10
        assert LogLevel.INFO.value == 20
        assert LogLevel.ERROR.value == 40


class TestLogRecord:
    """Tests for LogRecord."""

    def test_create(self) -> None:
        """Test creating record."""
        import time
        record = LogRecord(
            timestamp=time.time(),
            level=LogLevel.INFO,
            message="test",
            logger="test",
        )
        assert record.message == "test"
        assert record.level == LogLevel.INFO


class TestStructuredFormatter:
    """Tests for StructuredFormatter."""

    def test_format(self) -> None:
        """Test formatting."""
        import time
        formatter = StructuredFormatter()
        record = LogRecord(
            timestamp=time.time(),
            level=LogLevel.INFO,
            message="test",
            logger="test",
        )
        formatted = formatter.format(record)
        assert "INFO" in formatted
        assert "test" in formatted


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    def test_format(self) -> None:
        """Test JSON formatting."""
        import time
        import json
        formatter = JSONFormatter()
        record = LogRecord(
            timestamp=time.time(),
            level=LogLevel.INFO,
            message="test",
            logger="test",
        )
        formatted = formatter.format(record)
        data = json.loads(formatted)
        assert data["message"] == "test"


class TestConsoleHandler:
    """Tests for ConsoleHandler."""

    def test_create(self) -> None:
        """Test creating handler."""
        handler = ConsoleHandler()
        assert handler is not None


class TestFileHandler:
    """Tests for FileHandler."""

    def test_create(self) -> None:
        """Test creating handler."""
        handler = FileHandler("/tmp/test.log")
        assert handler._path == "/tmp/test.log"


class TestLogger:
    """Tests for Logger."""

    def test_create(self) -> None:
        """Test creating logger."""
        logger = Logger("test")
        assert logger._name == "test"

    def test_add_handler(self) -> None:
        """Test adding handler."""
        logger = Logger("test")
        handler = ConsoleHandler()
        logger.add_handler(handler)
        assert len(logger._handlers) == 1

    def test_set_level(self) -> None:
        """Test setting level."""
        logger = Logger("test")
        logger.set_level(LogLevel.ERROR)
        assert logger._level == LogLevel.ERROR

    def test_debug_info(self) -> None:
        """Test debug and info logging."""
        logger = Logger("test")
        # Should not raise
        logger.debug("debug message")
        logger.info("info message")


class TestLogManager:
    """Tests for LogManager."""

    def test_get_logger(self) -> None:
        """Test getting logger."""
        manager = LogManager()
        logger = manager.get_logger("test")
        assert logger._name == "test"

    def test_same_logger(self) -> None:
        """Test getting same logger."""
        manager = LogManager()
        logger1 = manager.get_logger("test")
        logger2 = manager.get_logger("test")
        assert logger1 is logger2


class TestModuleFunctions:
    """Tests for module-level functions."""

    def test_get_logger(self) -> None:
        """Test get_logger function."""
        logger = get_logger("test")
        assert logger is not None

    def test_add_handler(self) -> None:
        """Test add_handler function."""
        handler = ConsoleHandler()
        add_handler(handler)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])