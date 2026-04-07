"""Tests for logging utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import Logger, logger


class TestLogger:
    """Tests for Logger."""

    def test_singleton(self) -> None:
        """Test singleton behavior."""
        log1 = Logger()
        log2 = Logger()
        assert log1 is log2

    def test_logger_instance(self) -> None:
        """Test global logger instance."""
        assert logger is not None

    def test_debug_method(self) -> None:
        """Test debug method exists."""
        assert hasattr(logger, "debug")

    def test_info_method(self) -> None:
        """Test info method exists."""
        assert hasattr(logger, "info")

    def test_warning_method(self) -> None:
        """Test warning method exists."""
        assert hasattr(logger, "warning")

    def test_error_method(self) -> None:
        """Test error method exists."""
        assert hasattr(logger, "error")

    def test_critical_method(self) -> None:
        """Test critical method exists."""
        assert hasattr(logger, "critical")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])