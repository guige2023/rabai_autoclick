"""Logging utilities for RabAI AutoClick.

Provides:
- Logging setup
- Log formatting
- Log handlers
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


class LogFormatter(logging.Formatter):
    """Custom log formatter with color support."""

    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record: logging.LogRecord) -> str:
        """Format log record."""
        if sys.stdout.isatty():
            color = self.COLORS.get(record.levelname, '')
            record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None,
    format_string: Optional[str] = None,
) -> logging.Logger:
    """Set up application logging.

    Args:
        level: Logging level.
        log_file: Optional log file name.
        log_dir: Optional log directory.
        format_string: Optional format string.

    Returns:
        Configured logger.
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logger = logging.getLogger("rabai")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = LogFormatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    if log_file:
        if log_dir:
            log_path = Path(log_dir)
            log_path.mkdir(parents=True, exist_ok=True)
            log_file_path = log_path / log_file
        else:
            log_file_path = Path(log_file)

        file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get logger for module.

    Args:
        name: Logger name.

    Returns:
        Logger instance.
    """
    return logging.getLogger(f"rabai.{name}")


class LogCapture:
    """Capture log messages for testing.

    Usage:
        with LogCapture() as capture:
            logger.info("test message")
        assert "test message" in capture.messages
    """

    def __init__(self, logger_name: str = "rabai", level: int = logging.INFO) -> None:
        """Initialize log capture.

        Args:
            logger_name: Name of logger to capture.
            level: Minimum level to capture.
        """
        self.logger_name = logger_name
        self.level = level
        self.handler: Optional[logging.Handler] = None
        self.messages: list = []

    def __enter__(self) -> 'LogCapture':
        """Start capturing."""
        logger = logging.getLogger(self.logger_name)
        self.handler = CapturingHandler(self.messages)
        self.handler.setLevel(self.level)
        logger.addHandler(self.handler)
        return self

    def __exit__(self, *args: Any) -> None:
        """Stop capturing."""
        if self.handler:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)


class CapturingHandler(logging.Handler):
    """Handler that captures log records."""

    def __init__(self, messages: list) -> None:
        """Initialize capturing handler.

        Args:
            messages: List to capture messages.
        """
        super().__init__()
        self.messages = messages

    def emit(self, record: logging.LogRecord) -> None:
        """Emit record."""
        self.messages.append(self.format(record))


def add_file_handler(
    logger: logging.Logger,
    path: str,
    level: int = logging.DEBUG,
) -> logging.FileHandler:
    """Add file handler to logger.

    Args:
        logger: Logger to add handler to.
        path: Log file path.
        level: Logging level.

    Returns:
        Created handler.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return handler


def set_level(logger: logging.Logger, level: str) -> None:
    """Set logger level.

    Args:
        logger: Logger to modify.
        level: Level name.
    """
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))


class LoggerAdapter(logging.LoggerAdapter):
    """Custom logger adapter with context."""

    def process(self, msg: str, kwargs: dict) -> tuple:
        """Process log message with extra context."""
        if 'extra' in kwargs:
            for key, value in kwargs['extra'].items():
                self.extra[key] = value
        return msg, kwargs


def get_adapter(
    logger: logging.Logger,
    context: dict,
) -> LoggerAdapter:
    """Get logger adapter with context.

    Args:
        logger: Base logger.
        context: Context dict.

    Returns:
        LoggerAdapter instance.
    """
    return LoggerAdapter(logger, context)


def rotate_log_file(path: str, keep: int = 5) -> None:
    """Rotate log file.

    Args:
        path: Path to log file.
        keep: Number of old logs to keep.
    """
    path = Path(path)
    if not path.exists():
        return

    # Rename existing logs
    for i in range(keep - 1, 0, -1):
        old_path = path.with_suffix(f".{i}")
        new_path = path.with_suffix(f".{i + 1}")

        if old_path.exists():
            old_path.rename(new_path)

    # Rename current log
    path.rename(path.with_suffix(".1"))