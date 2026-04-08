"""
Automation logger utilities for recording automation workflow events.

Provides structured logging with screenshots, action recording,
and searchable log storage for debugging automation sequences.
"""

from __future__ import annotations

import json
import time
import os
import threading
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class LogLevel(Enum):
    """Log severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(Enum):
    """Log output formats."""
    JSON = "json"
    TEXT = "text"
    HTML = "html"


@dataclass
class LogEntry:
    """Single log entry."""
    timestamp: float
    level: LogLevel
    message: str
    category: str
    screenshot_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    action_type: Optional[str] = None
    duration: Optional[float] = None


class AutomationLogger:
    """Structured logger for automation workflows."""
    
    def __init__(self, name: str = "automation",
                 log_dir: str = "/tmp/automation_logs",
                 format: LogFormat = LogFormat.JSON):
        """
        Initialize automation logger.
        
        Args:
            name: Logger name (used as log file prefix).
            log_dir: Directory for log files.
            format: Output format.
        """
        self.name = name
        self.log_dir = Path(log_dir)
        self.format = format
        self._entries: List[LogEntry] = []
        self._lock = threading.Lock()
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._ensure_log_dir()
    
    def _ensure_log_dir(self) -> None:
        """Create log directory if needed."""
        self.log_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_log_path(self) -> Path:
        """Get current log file path."""
        return self.log_dir / f"{self.name}_{self._session_id}.{self.format.value}"
    
    def log(self, level: LogLevel, message: str,
            category: str = "general",
            screenshot_path: Optional[str] = None,
            metadata: Optional[Dict[str, Any]] = None,
            action_type: Optional[str] = None,
            duration: Optional[float] = None) -> None:
        """
        Log an entry.
        
        Args:
            level: Log level.
            message: Log message.
            category: Log category.
            screenshot_path: Optional screenshot path.
            metadata: Optional metadata dict.
            action_type: Optional action type tag.
            duration: Optional duration in seconds.
        """
        entry = LogEntry(
            timestamp=time.time(),
            level=level,
            message=message,
            category=category,
            screenshot_path=screenshot_path,
            metadata=metadata or {},
            action_type=action_type,
            duration=duration
        )
        
        with self._lock:
            self._entries.append(entry)
            self._write_entry(entry)
    
    def _write_entry(self, entry: LogEntry) -> None:
        """Write entry to file."""
        try:
            path = self._get_log_path()
            
            if self.format == LogFormat.JSON:
                with open(path, 'a') as f:
                    f.write(json.dumps(self._entry_to_dict(entry)) + '\n')
            else:
                with open(path, 'a') as f:
                    f.write(self._format_text(entry) + '\n')
        except Exception:
            pass
    
    def _entry_to_dict(self, entry: LogEntry) -> Dict[str, Any]:
        """Convert entry to dict."""
        return {
            'timestamp': entry.timestamp,
            'datetime': datetime.fromtimestamp(entry.timestamp).isoformat(),
            'level': entry.level.value,
            'message': entry.message,
            'category': entry.category,
            'screenshot_path': entry.screenshot_path,
            'metadata': entry.metadata,
            'action_type': entry.action_type,
            'duration': entry.duration
        }
    
    def _format_text(self, entry: LogEntry) -> str:
        """Format entry as text line."""
        dt = datetime.fromtimestamp(entry.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        parts = [f"[{dt}]", f"[{entry.level.value}]", f"[{entry.category}]", entry.message]
        
        if entry.action_type:
            parts.append(f"[action={entry.action_type}]")
        if entry.duration:
            parts.append(f"[{entry.duration:.3f}s]")
        if entry.screenshot_path:
            parts.append(f"[screenshot={entry.screenshot_path}]")
        
        return " ".join(parts)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.log(LogLevel.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.log(LogLevel.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.log(LogLevel.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self.log(LogLevel.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message."""
        self.log(LogLevel.CRITICAL, message, **kwargs)
    
    def log_action(self, action_type: str, details: Dict[str, Any],
                   screenshot_path: Optional[str] = None,
                   duration: Optional[float] = None) -> None:
        """
        Log an automation action.
        
        Args:
            action_type: Type of action (click, type, etc.).
            details: Action details dict.
            screenshot_path: Optional screenshot.
            duration: Action duration.
        """
        self.log(
            LogLevel.INFO,
            f"Action: {action_type}",
            category="action",
            action_type=action_type,
            screenshot_path=screenshot_path,
            metadata=details,
            duration=duration
        )
    
    def log_screenshot(self, message: str, screenshot_path: str,
                       metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Log with screenshot.
        
        Args:
            message: Log message.
            screenshot_path: Path to screenshot.
            metadata: Optional metadata.
        """
        self.log(
            LogLevel.INFO,
            message,
            category="screenshot",
            screenshot_path=screenshot_path,
            metadata=metadata or {}
        )
    
    def get_entries(self, level: Optional[LogLevel] = None,
                    category: Optional[str] = None,
                    limit: int = 100) -> List[LogEntry]:
        """
        Get log entries with optional filtering.
        
        Args:
            level: Optional level filter.
            category: Optional category filter.
            limit: Max entries to return.
            
        Returns:
            List of matching LogEntry.
        """
        with self._lock:
            entries = self._entries.copy()
        
        if level:
            entries = [e for e in entries if e.level == level]
        if category:
            entries = [e for e in entries if e.category == category]
        
        return entries[-limit:]
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get log summary statistics.
        
        Returns:
            Dict with counts by level and category.
        """
        with self._lock:
            entries = self._entries.copy()
        
        level_counts = {}
        category_counts = {}
        
        for entry in entries:
            level_counts[entry.level.value] = level_counts.get(entry.level.value, 0) + 1
            category_counts[entry.category] = category_counts.get(entry.category, 0) + 1
        
        return {
            'session_id': self._session_id,
            'total_entries': len(entries),
            'level_counts': level_counts,
            'category_counts': category_counts,
            'start_time': entries[0].timestamp if entries else None,
            'end_time': entries[-1].timestamp if entries else None
        }
    
    def export_html(self, output_path: Optional[str] = None) -> str:
        """
        Export log as HTML report.
        
        Args:
            output_path: Optional output path.
            
        Returns:
            Path to exported HTML.
        """
        with self._lock:
            entries = self._entries.copy()
        
        html = self._generate_html(entries)
        
        if output_path is None:
            output_path = str(self.log_dir / f"{self.name}_{self._session_id}.html")
        
        with open(output_path, 'w') as f:
            f.write(html)
        
        return output_path
    
    def _generate_html(self, entries: List[LogEntry]) -> str:
        """Generate HTML report."""
        rows = []
        for entry in entries:
            level_class = entry.level.value.lower()
            dt = datetime.fromtimestamp(entry.timestamp).strftime("%H:%M:%S.%f")[:-3]
            
            row = f'''
            <tr class="{level_class}">
                <td>{dt}</td>
                <td>{entry.level.value}</td>
                <td>{entry.category}</td>
                <td>{entry.message}</td>
                <td>{entry.action_type or ''}</td>
                <td>{f"{entry.duration:.3f}s" if entry.duration else ''}</td>
                <td>{'<img src="file://{0}" width="100"/>'.format(entry.screenshot_path) if entry.screenshot_path else ''}</td>
            </tr>
            '''
            rows.append(row)
        
        return f'''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Automation Log - {self.name}</title>
            <style>
                body {{ font-family: monospace; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #333; color: white; }}
                tr.debug {{ background-color: #f8f8f8; }}
                tr.info {{ background-color: #e8f4f8; }}
                tr.warning {{ background-color: #fff3cd; }}
                tr.error {{ background-color: #f8d7da; }}
                tr.critical {{ background-color: #f5c2c7; }}
                img {{ max-width: 150px; }}
            </style>
        </head>
        <body>
            <h1>Automation Log: {self.name}</h1>
            <p>Session: {self._session_id}</p>
            <table>
                <tr>
                    <th>Time</th>
                    <th>Level</th>
                    <th>Category</th>
                    <th>Message</th>
                    <th>Action</th>
                    <th>Duration</th>
                    <th>Screenshot</th>
                </tr>
                {''.join(rows)}
            </table>
        </body>
        </html>
        '''
