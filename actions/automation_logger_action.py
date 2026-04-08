"""
Automation Logger Action Module.

Structured logging for automation workflows with log levels,
 categorization, rotation, and multiple output destinations.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging
import json


class LogLevel(Enum):
    """Log severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(Enum):
    """Log output format."""
    TEXT = "text"
    JSON = "json"
    CSV = "csv"


@dataclass
class LogEntry:
    """A single log entry."""
    timestamp: float
    level: LogLevel
    message: str
    category: str = "general"
    metadata: dict[str, Any] = field(default_factory=dict)
    workflow_id: Optional[str] = None
    task_id: Optional[str] = None


class AutomationLoggerAction:
    """
    Structured logging for automation workflows.

    Provides categorized, structured logging with multiple
    output formats and log rotation support.

    Example:
        logger = AutomationLoggerAction(output_dir="/var/log/automation")
        logger.log("INFO", "Task started", category="workflow", workflow_id="wf_001")
        logger.log("ERROR", "Task failed", metadata={"error": "timeout"})
    """

    def __init__(
        self,
        output_dir: Optional[str] = None,
        format: LogFormat = LogFormat.TEXT,
        max_file_size_mb: int = 100,
        backup_count: int = 5,
        log_level: LogLevel = LogLevel.INFO,
    ) -> None:
        self.output_dir = output_dir
        self.format = format
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.backup_count = backup_count
        self.log_level = log_level
        self._entries: list[LogEntry] = []
        self._file_handles: dict[str, Any] = {}

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

    def log(
        self,
        level: str,
        message: str,
        category: str = "general",
        workflow_id: Optional[str] = None,
        task_id: Optional[str] = None,
        **metadata: Any,
    ) -> LogEntry:
        """Log a message at the specified level."""
        log_level = LogLevel.INFO
        for ll in LogLevel:
            if ll.value == level.upper():
                log_level = ll
                break

        if log_level.value < self.log_level.value:
            return LogEntry(
                timestamp=time.time(),
                level=log_level,
                message=message,
            )

        entry = LogEntry(
            timestamp=time.time(),
            level=log_level,
            message=message,
            category=category,
            workflow_id=workflow_id,
            task_id=task_id,
            metadata=metadata,
        )

        self._entries.append(entry)
        self._write_entry(entry)

        return entry

    def debug(
        self,
        message: str,
        category: str = "general",
        **metadata: Any,
    ) -> LogEntry:
        """Log a debug message."""
        return self.log("DEBUG", message, category, **metadata)

    def info(
        self,
        message: str,
        category: str = "general",
        **metadata: Any,
    ) -> LogEntry:
        """Log an info message."""
        return self.log("INFO", message, category, **metadata)

    def warning(
        self,
        message: str,
        category: str = "general",
        **metadata: Any,
    ) -> LogEntry:
        """Log a warning message."""
        return self.log("WARNING", message, category, **metadata)

    def error(
        self,
        message: str,
        category: str = "general",
        **metadata: Any,
    ) -> LogEntry:
        """Log an error message."""
        return self.log("ERROR", message, category, **metadata)

    def critical(
        self,
        message: str,
        category: str = "general",
        **metadata: Any,
    ) -> LogEntry:
        """Log a critical message."""
        return self.log("CRITICAL", message, category, **metadata)

    def _write_entry(self, entry: LogEntry) -> None:
        """Write a log entry to file."""
        if not self.output_dir:
            return

        filename = f"{entry.category}.log"
        filepath = os.path.join(self.output_dir, filename)

        try:
            if self.format == LogFormat.JSON:
                line = json.dumps({
                    "timestamp": entry.timestamp,
                    "level": entry.level.value,
                    "message": entry.message,
                    "category": entry.category,
                    "workflow_id": entry.workflow_id,
                    "task_id": entry.task_id,
                    **entry.metadata,
                }) + "\n"
            else:
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(entry.timestamp))
                meta_str = json.dumps(entry.metadata) if entry.metadata else ""
                line = f"{ts} [{entry.level.value}] {entry.category}: {entry.message} {meta_str}\n"

            with open(filepath, "a") as f:
                f.write(line)

        except Exception as e:
            pass

    def get_recent(
        self,
        count: int = 100,
        level: Optional[LogLevel] = None,
        category: Optional[str] = None,
    ) -> list[LogEntry]:
        """Get recent log entries."""
        entries = self._entries[-count:]

        if level:
            entries = [e for e in entries if e.level == level]

        if category:
            entries = [e for e in entries if e.category == category]

        return entries

    def get_by_workflow(
        self,
        workflow_id: str,
    ) -> list[LogEntry]:
        """Get all log entries for a workflow."""
        return [e for e in self._entries if e.workflow_id == workflow_id]

    def export(
        self,
        filepath: str,
        format: Optional[LogFormat] = None,
    ) -> None:
        """Export log entries to a file."""
        fmt = format or self.format

        with open(filepath, "w") as f:
            if fmt == LogFormat.JSON:
                json_entries = [
                    {
                        "timestamp": e.timestamp,
                        "level": e.level.value,
                        "message": e.message,
                        "category": e.category,
                        **e.metadata,
                    }
                    for e in self._entries
                ]
                json.dump(json_entries, f, indent=2)

            elif fmt == LogFormat.CSV:
                import csv
                writer = csv.DictWriter(f, fieldnames=["timestamp", "level", "category", "message"])
                writer.writeheader()
                for e in self._entries:
                    writer.writerow({
                        "timestamp": e.timestamp,
                        "level": e.level.value,
                        "category": e.category,
                        "message": e.message,
                    })

            else:
                for e in self._entries:
                    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(e.timestamp))
                    f.write(f"{ts} [{e.level.value}] {e.category}: {e.message}\n")

    def clear(self) -> None:
        """Clear in-memory log entries."""
        self._entries.clear()
