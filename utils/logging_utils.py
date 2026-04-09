"""
Logging Utilities for UI Automation.

This module provides structured logging utilities for automation workflows,
with support for multiple output formats and log levels.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class LogFormat(Enum):
    """Log output formats."""
    TEXT = auto()
    JSON = auto()
    STRUCTURED = auto()


@dataclass
class LogEntry:
    """
    A structured log entry.
    
    Attributes:
        timestamp: Log timestamp
        level: Log level
        message: Log message
        logger_name: Name of the logger
        module: Module name
        function: Function name
        line: Line number
        metadata: Additional log metadata
    """
    timestamp: float
    level: LogLevel
    message: str
    logger_name: str = ""
    module: str = ""
    function: str = ""
    line: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "level": self.level.name,
            "message": self.message,
            "logger": self.logger_name,
            "module": self.module,
            "function": self.function,
            "line": self.line,
            **self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)


class AutomationLogger:
    """
    Structured logger for automation workflows.
    
    Example:
        logger = AutomationLogger(name="automation")
        logger.info("Starting workflow", step=1, total_steps=10)
    """
    
    def __init__(
        self,
        name: str = "automation",
        level: LogLevel = LogLevel.INFO,
        format_type: LogFormat = LogFormat.STRUCTURED,
        output_file: Optional[Path] = None
    ):
        self.name = name
        self.level = level
        self.format_type = format_type
        self.output_file = output_file
        self._handlers: list[logging.Handler] = []
        self._setup_handlers()
    
    def _setup_handlers(self) -> None:
        """Setup log handlers."""
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.level.value)
        
        formatter = self._get_formatter()
        console_handler.setFormatter(formatter)
        
        self._handlers.append(console_handler)
        
        # File handler if specified
        if self.output_file:
            file_handler = logging.FileHandler(self.output_file)
            file_handler.setLevel(self.level.value)
            file_handler.setFormatter(formatter)
            self._handlers.append(file_handler)
        
        # Setup root logger
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(self.level.value)
        self._logger.propagate = False
        
        for handler in self._handlers:
            if handler not in self._logger.handlers:
                self._logger.addHandler(handler)
    
    def _get_formatter(self) -> logging.Formatter:
        """Get the appropriate formatter."""
        if self.format_type == LogFormat.JSON:
            return logging.Formatter('%(message)s')
        elif self.format_type == LogFormat.STRUCTURED:
            return logging.Formatter(
                '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        else:
            return logging.Formatter(
                '[%(asctime)s] %(levelname)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
    
    def _log(
        self,
        level: LogLevel,
        message: str,
        **metadata
    ) -> None:
        """Internal log method."""
        if level.value < self.level.value:
            return
        
        extra = {"_automation": True}
        
        if metadata:
            if self.format_type == LogFormat.JSON:
                message = json.dumps({"message": message, **metadata})
            else:
                extra_items = " ".join(f"{k}={v}" for k, v in metadata.items())
                message = f"{message} | {extra_items}"
        
        self._logger.log(level.value, message, extra=extra)
    
    def debug(self, message: str, **metadata) -> None:
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, **metadata)
    
    def info(self, message: str, **metadata) -> None:
        """Log info message."""
        self._log(LogLevel.INFO, message, **metadata)
    
    def warning(self, message: str, **metadata) -> None:
        """Log warning message."""
        self._log(LogLevel.WARNING, message, **metadata)
    
    def error(self, message: str, **metadata) -> None:
        """Log error message."""
        self._log(LogLevel.ERROR, message, **metadata)
    
    def critical(self, message: str, **metadata) -> None:
        """Log critical message."""
        self._log(LogLevel.CRITICAL, message, **metadata)
    
    def exception(self, message: str, **metadata) -> None:
        """Log exception with traceback."""
        self.error(message, exc_info=True, **metadata)


class LogCollector:
    """
    Collects and aggregates logs from multiple sources.
    
    Example:
        collector = LogCollector()
        collector.add_logger(automation_logger)
        logs = collector.get_logs(level=LogLevel.ERROR)
    """
    
    def __init__(self):
        self._loggers: list[AutomationLogger] = []
        self._entries: list[LogEntry] = []
        self._max_entries = 10000
    
    def add_logger(self, logger: AutomationLogger) -> None:
        """Add a logger to collect from."""
        self._loggers.append(logger)
    
    def get_logs(
        self,
        level: Optional[LogLevel] = None,
        limit: int = 1000
    ) -> list[LogEntry]:
        """Get collected log entries."""
        entries = self._entries
        
        if level:
            entries = [e for e in entries if e.level.value >= level.value]
        
        return entries[-limit:]
    
    def clear(self) -> None:
        """Clear collected entries."""
        self._entries.clear()
    
    @property
    def count(self) -> int:
        """Get total entry count."""
        return len(self._entries)


class LogRotation:
    """
    Manages log file rotation.
    
    Example:
        rotator = LogRotation("logs/app.log", max_bytes=10*1024*1024, backup_count=5)
        rotator.rotate_if_needed()
    """
    
    def __init__(
        self,
        filename: str,
        max_bytes: int = 10 * 1024 * 1024,  # 10 MB
        backup_count: int = 5
    ):
        self.filename = Path(filename)
        self.max_bytes = max_bytes
        self.backup_count = backup_count
    
    def should_rotate(self) -> bool:
        """Check if rotation is needed."""
        if not self.filename.exists():
            return False
        
        return self.filename.stat().st_size >= self.max_bytes
    
    def rotate(self) -> Optional[str]:
        """
        Perform log rotation.
        
        Returns:
            Name of the archived log file if rotated, None otherwise
        """
        if not self.should_rotate():
            return None
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        archive_name = f"{self.filename}.{timestamp}"
        
        # Find next available backup number
        backup_num = 1
        while self.filename.with_suffix(f".log.{backup_num}").exists():
            backup_num += 1
        
        # Rotate existing backups
        for i in range(backup_num - 1, 0, -1):
            src = self.filename.with_suffix(f".log.{i}")
            dst = self.filename.with_suffix(f".log.{i + 1}")
            if src.exists():
                src.rename(dst)
        
        # Rename current log
        self.filename.rename(self.filename.with_suffix(".log.1"))
        
        return str(archive_name)
    
    def cleanup_old_backups(self) -> int:
        """
        Remove old backup files beyond backup_count.
        
        Returns:
            Number of files removed
        """
        removed = 0
        
        for i in range(1, self.backup_count + 10):
            backup_path = self.filename.with_suffix(f".log.{i}")
            if backup_path.exists():
                backup_path.unlink()
                removed += 1
        
        return removed
