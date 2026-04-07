"""Tests for logging utilities."""

import logging
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logging_utils import (
    LogFormatter,
    setup_logging,
    get_logger,
    LogCapture,
    add_file_handler,
    set_level,
    LoggerAdapter,
    get_adapter,
    rotate_log_file,
)


class TestLogFormatter:
    """Tests for LogFormatter."""

    def test_format_record(self) -> None:
        """Test formatting log record."""
        formatter = LogFormatter("%(levelname)s - %(message)s")
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "INFO" in result
        assert "test message" in result


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_setup_default(self) -> None:
        """Test setup with defaults."""
        logger = setup_logging()
        assert logger.name == "rabai"
        assert logger.level == logging.INFO

    def test_setup_with_level(self) -> None:
        """Test setup with custom level."""
        logger = setup_logging(level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_setup_with_file(self) -> None:
        """Test setup with log file."""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_path = f.name
        try:
            logger = setup_logging(log_file=os.path.basename(log_path), log_dir=os.path.dirname(log_path))
            assert len(logger.handlers) >= 2  # Console + file
        finally:
            if os.path.exists(log_path):
                os.unlink(log_path)


class TestGetLogger:
    """Tests for get_logger."""

    def test_get_logger(self) -> None:
        """Test getting logger."""
        logger = get_logger("test")
        assert logger.name == "rabai.test"

    def test_get_logger_hierarchy(self) -> None:
        """Test logger hierarchy."""
        parent = get_logger("module")
        child = get_logger("module.sub")
        assert parent.name == "rabai.module"
        assert child.name == "rabai.module.sub"


class TestLogCapture:
    """Tests for LogCapture."""

    def test_capture_messages(self) -> None:
        """Test capturing log messages."""
        logger = logging.getLogger("rabai.test_capture")
        with LogCapture(logger_name="rabai.test_capture") as capture:
            logger.info("test message")
        assert any("test message" in msg for msg in capture.messages)

    def test_context_manager(self) -> None:
        """Test context manager behavior."""
        with LogCapture(logger_name="rabai.test_context") as capture:
            pass
        # Handler should be removed after context


class TestAddFileHandler:
    """Tests for add_file_handler."""

    def test_add_file_handler(self) -> None:
        """Test adding file handler."""
        logger = logging.getLogger("rabai.test_handler")
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_path = f.name
        try:
            handler = add_file_handler(logger, log_path)
            assert handler is not None
            logger.removeHandler(handler)
        finally:
            if os.path.exists(log_path):
                os.unlink(log_path)


class TestSetLevel:
    """Tests for set_level."""

    def test_set_level_debug(self) -> None:
        """Test setting level to DEBUG."""
        logger = logging.getLogger("rabai.test_level")
        set_level(logger, "DEBUG")
        assert logger.level == logging.DEBUG

    def test_set_level_invalid(self) -> None:
        """Test setting invalid level."""
        logger = logging.getLogger("rabai.test_level_invalid")
        set_level(logger, "INVALID")
        assert logger.level == logging.INFO  # Default


class TestLoggerAdapter:
    """Tests for LoggerAdapter."""

    def test_process(self) -> None:
        """Test processing log message."""
        logger = logging.getLogger("rabai.test_adapter")
        adapter = LoggerAdapter(logger, {"context_key": "context_value"})
        msg, kwargs = adapter.process("test message", {})
        assert msg == "test message"


class TestGetAdapter:
    """Tests for get_adapter."""

    def test_get_adapter(self) -> None:
        """Test getting logger adapter."""
        logger = logging.getLogger("rabai.test_get_adapter")
        adapter = get_adapter(logger, {"key": "value"})
        assert isinstance(adapter, LoggerAdapter)


class TestRotateLogFile:
    """Tests for rotate_log_file."""

    def test_rotate_nonexistent(self) -> None:
        """Test rotating nonexistent file."""
        rotate_log_file("/nonexistent/logfile.log")
        # Should not raise

    def test_rotate_existing(self) -> None:
        """Test rotating existing file."""
        with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as f:
            log_path = f.name
            f.write(b"existing log content\n")
        try:
            rotate_log_file(log_path, keep=3)
            # Original file should be renamed to .1
            assert os.path.exists(log_path + ".1")
        finally:
            for ext in ["", ".1", ".2", ".3"]:
                path = log_path + ext if ext else log_path
                if os.path.exists(path):
                    os.unlink(path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])