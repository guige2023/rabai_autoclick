"""Audit logging utilities for RabAI AutoClick.

Logs all workflow executions with timestamps, user info, duration,
and success/failure status to a JSON file with support for JSON Lines export,
log rotation, compression, archival, metrics, correlation IDs, and sanitization.
"""

import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import sys
import threading
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from functools import wraps


class LogLevel(IntEnum):
    """Log levels compatible with Python logging."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class AuditLogger:
    """Production-ready audit logger with rotation, compression, and querying.
    
    Thread-safe audit logger that stores execution records in JSON format
    with support for JSON Lines export, log rotation, compression, archival,
    structured querying, metrics, correlation IDs, and sensitive data sanitization.
    """
    
    # Sensitive field patterns for sanitization
    SENSITIVE_PATTERNS = [
        (re.compile(r'(password|passwd|pwd)[=:]\s*["\']?[^"\',\s]+', re.I), r'\1=<REDACTED>'),
        (re.compile(r'(secret|token|api_key|apikey|auth_token)[=:]\s*["\']?[^"\',\s]+', re.I), r'\1=<REDACTED>'),
        (re.compile(r'(bearer|basic)\s+[a-zA-Z0-9\-_.]+', re.I), r'\1 <REDACTED>'),
        (re.compile(r'(access_token|refresh_token)[=:]\s*["\']?[^"\',\s]+', re.I), r'\1=<REDACTED>'),
    ]
    
    # Fields that should be completely masked
    MASKED_FIELDS = {'password', 'passwd', 'pwd', 'secret', 'token', 'api_key', 
                     'apikey', 'auth_token', 'access_token', 'refresh_token', 'bearer'}
    
    def __init__(
        self,
        log_file: Optional[str] = None,
        max_entries: int = 10000,
        max_file_size: int = 10 * 1024 * 1024,  # 10 MB
        max_file_age_days: int = 30,
        archive_dir: Optional[str] = None,
        log_level: LogLevel = LogLevel.INFO,
        enable_syslog: bool = False,
        syslog_host: Optional[str] = None,
        syslog_port: int = 514,
        syslog_facility: int = getattr(socket, 'LOG_USER', 1),  # 1 = LOG_USER fallback
        sanitize: bool = True,
        json_lines: bool = True,
        rotation_lock_timeout: float = 5.0,
    ) -> None:
        """Initialize the audit logger.
        
        Args:
            log_file: Path to the audit log file. Defaults to
                     logs/audit.json in the project root.
            max_entries: Maximum number of entries to keep in JSON file (FIFO overflow).
            max_file_size: Maximum file size before rotation (bytes). Default 10 MB.
            max_file_age_days: Maximum age of log files before archival (days).
            archive_dir: Directory for archived/compressed logs. Defaults to
                        logs/archive in the project root.
            log_level: Minimum log level to record.
            enable_syslog: Enable remote syslog logging.
            syslog_host: Syslog server hostname/IP.
            syslog_port: Syslog server port.
            syslog_facility: Syslog facility (socket.LOG_*).
            sanitize: Enable sensitive data sanitization.
            json_lines: Enable JSON Lines format for new entries.
            rotation_lock_timeout: Timeout for rotation lock acquisition.
        """
        if log_file is None:
            project_root = os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
            log_file = os.path.join(project_root, "logs", "audit.json")
        
        if archive_dir is None:
            archive_dir = os.path.join(os.path.dirname(log_file), "archive")
        
        self._log_file = log_file
        self._json_lines_file = log_file + ".jsonl"
        self._max_entries = max_entries
        self._max_file_size = max_file_size
        self._max_file_age_days = max_file_age_days
        self._archive_dir = archive_dir
        self._log_level = LogLevel(log_level)
        self._enable_syslog = enable_syslog
        self._syslog_host = syslog_host
        self._syslog_port = syslog_port
        self._syslog_facility = syslog_facility
        self._sanitize = sanitize
        self._json_lines = json_lines
        self._rotation_lock_timeout = rotation_lock_timeout
        
        self._lock = threading.RLock()
        self._syslog_lock = threading.Lock()
        self._correlation_stack: Dict[int, List[str]] = defaultdict(list)
        
        # Ensure directories exist
        os.makedirs(os.path.dirname(self._log_file), exist_ok=True)
        os.makedirs(self._archive_dir, exist_ok=True)
        
        # Initialize JSON file if it doesn't exist
        if not os.path.exists(self._log_file):
            self._write_entries([])
        
        # Setup syslog handler
        self._syslog_handler: Optional[logging.Handler] = None
        if self._enable_syslog and self._syslog_host:
            self._setup_syslog()
        
        # Track rotation state
        self._last_rotation_check = time.time()
        self._rotation_check_interval = 3600  # Check rotation every hour
    
    def _setup_syslog(self) -> None:
        """Setup syslog handler for remote logging."""
        try:
            self._syslog_handler = logging.handlers.SysLogHandler(
                address=(self._syslog_host, self._syslog_port),
                facility=self._syslog_facility
            )
            formatter = logging.Formatter(
                'audit_logger: %(message)s',
                encoding='utf-8'
            )
            self._syslog_handler.setFormatter(formatter)
        except Exception:
            # Fall back to local syslog if remote fails
            try:
                self._syslog_handler = logging.handlers.SysLogHandler(
                    facility=self._syslog_facility
                )
            except Exception:
                self._syslog_handler = None
    
    def _sanitize_data(self, data: Any) -> Any:
        """Sanitize sensitive data in logs.
        
        Args:
            data: Data to sanitize.
            
        Returns:
            Sanitized data with sensitive values redacted.
        """
        if not self._sanitize:
            return data
        
        if isinstance(data, dict):
            return {k: self._sanitize_value(k, v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._sanitize_data(item) for item in data]
        elif isinstance(data, str):
            return self._sanitize_string(data)
        return data
    
    def _sanitize_value(self, key: str, value: Any) -> Any:
        """Sanitize a single key-value pair.
        
        Args:
            key: Dictionary key.
            value: Value to potentially sanitize.
            
        Returns:
            Original value or redacted string for sensitive fields.
        """
        key_lower = key.lower()
        if key_lower in self.MASKED_FIELDS:
            return "<REDACTED>"
        if isinstance(value, str):
            return self._sanitize_string(value)
        return self._sanitize_data(value)
    
    def _sanitize_string(self, text: str) -> str:
        """Apply regex-based sanitization to a string.
        
        Args:
            text: String to sanitize.
            
        Returns:
            Sanitized string.
        """
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            text = pattern.sub(replacement, text)
        return text
    
    def _should_rotate(self) -> bool:
        """Check if log rotation is needed.
        
        Returns:
            True if rotation should occur.
        """
        if os.path.exists(self._log_file):
            stat = os.stat(self._log_file)
            if stat.st_size >= self._max_file_size:
                return True
        
        # Also check JSON Lines file
        if os.path.exists(self._json_lines_file):
            stat = os.stat(self._json_lines_file)
            if stat.st_size >= self._max_file_size:
                return True
        
        return False
    
    def _rotate_logs(self) -> None:
        """Rotate log files: compress old logs and start new ones."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        
        # Rotate main JSON file
        if os.path.exists(self._log_file) and os.path.getsize(self._log_file) > 0:
            rotated_name = f"{self._log_file}.{timestamp}"
            self._compress_and_archive(self._log_file, rotated_name)
        
        # Rotate JSON Lines file
        if os.path.exists(self._json_lines_file) and os.path.getsize(self._json_lines_file) > 0:
            rotated_name = f"{self._json_lines_file}.{timestamp}"
            self._compress_and_archive(self._json_lines_file, rotated_name)
        
        # Start fresh files
        self._write_entries([])
        with open(self._json_lines_file, "w", encoding="utf-8") as f:
            pass
    
    def _compress_and_archive(self, source_file: str, archive_name: str) -> None:
        """Compress a log file and move to archive.
        
        Args:
            source_file: Path to file to compress.
            archive_name: Name for the archived file (without .gz extension).
        """
        try:
            with open(source_file, 'rb') as f_in:
                with gzip.open(archive_name + ".gz", 'wb', compresslevel=6) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Remove original
            os.remove(source_file)
        except Exception:
            # If compression fails, just rename without compression
            if os.path.exists(source_file):
                try:
                    shutil.move(source_file, archive_name)
                except Exception:
                    pass
    
    def _archive_old_logs(self) -> None:
        """Archive and compress logs older than max_file_age_days."""
        if not os.path.exists(self._archive_dir):
            return
        
        cutoff_time = time.time() - (self._max_file_age_days * 86400)
        
        for filename in os.listdir(self._archive_dir):
            filepath = os.path.join(self._archive_dir, filename)
            if os.path.isfile(filepath):
                try:
                    if os.path.getmtime(filepath) < cutoff_time:
                        # Already compressed (ends with .gz) or old file
                        if not filename.endswith('.gz'):
                            compressed = filepath + ".gz"
                            with open(filepath, 'rb') as f_in:
                                with gzip.open(compressed, 'wb', compresslevel=6) as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                            os.remove(filepath)
                        # Files older than 2x max_age are deleted
                        if os.path.getmtime(filepath if filename.endswith('.gz') else compressed) < cutoff_time - (self._max_file_age_days * 86400):
                            os.remove(filepath if filename.endswith('.gz') else compressed)
                except Exception:
                    pass
    
    def _check_rotation_needed(self) -> None:
        """Periodically check if rotation is needed (throttled)."""
        now = time.time()
        if now - self._last_rotation_check < self._rotation_check_interval:
            return
        
        self._last_rotation_check = now
        
        # Check size-based rotation
        if self._should_rotate():
            with self._lock:
                self._rotate_logs()
        
        # Check age-based archival
        self._archive_old_logs()
    
    def _read_entries(self) -> List[Dict[str, Any]]:
        """Read all entries from the audit log file (JSON array format).
        
        Returns:
            List of audit log entries.
        """
        try:
            with open(self._log_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
        except (json.JSONDecodeError, FileNotFoundError):
            return []
    
    def _read_jsonl_entries(self) -> List[Dict[str, Any]]:
        """Read entries from the JSON Lines file.
        
        Returns:
            List of audit log entries.
        """
        entries = []
        if not os.path.exists(self._json_lines_file):
            return entries
        
        try:
            with open(self._json_lines_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        except (FileNotFoundError, IOError):
            pass
        
        return entries
    
    def _write_entries(self, entries: List[Dict[str, Any]]) -> None:
        """Write entries to the audit log file (JSON array format).
        
        Args:
            entries: List of audit log entries to write.
        """
        with open(self._log_file, "w", encoding="utf-8") as f:
            json.dump(entries, f, indent=2, default=str)
    
    def _append_jsonl(self, entry: Dict[str, Any]) -> None:
        """Append a single entry to the JSON Lines file.
        
        Args:
            entry: Audit log entry to append.
        """
        with open(self._json_lines_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, default=str) + "\n")
    
    def push_correlation_id(self, correlation_id: Optional[str] = None) -> str:
        """Push a correlation ID onto the stack for the current thread.
        
        Args:
            correlation_id: Optional ID to use. If None, generates a new one.
            
        Returns:
            The correlation ID that was pushed.
        """
        thread_id = threading.get_ident()
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        self._correlation_stack[thread_id].append(correlation_id)
        return correlation_id
    
    def pop_correlation_id(self) -> Optional[str]:
        """Pop the most recent correlation ID from the stack.
        
        Returns:
            The popped correlation ID or None if stack was empty.
        """
        thread_id = threading.get_ident()
        if self._correlation_stack[thread_id]:
            return self._correlation_stack[thread_id].pop()
        return None
    
    def get_correlation_id(self) -> Optional[str]:
        """Get the current correlation ID without removing it.
        
        Returns:
            The current correlation ID or None.
        """
        thread_id = threading.get_ident()
        stack = self._correlation_stack[thread_id]
        return stack[-1] if stack else None
    
    def log_execution(
        self,
        workflow_name: str,
        user: Optional[str] = None,
        duration: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        action_type: Optional[str] = None,
        log_level: LogLevel = LogLevel.INFO,
        correlation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Log a workflow execution.
        
        Args:
            workflow_name: Name of the workflow executed.
            user: User who executed the workflow.
            duration: Execution duration in seconds.
            success: Whether execution succeeded.
            error: Error message if execution failed.
            metadata: Additional metadata to log.
            action_type: Type of action (for structured querying).
            log_level: Log level for this entry.
            correlation_id: Correlation ID for tracking across runs.
            
        Returns:
            The created audit entry.
        """
        # Check rotation periodically
        self._check_rotation_needed()
        
        # Check log level
        if LogLevel(log_level) < self._log_level:
            return {}
        
        # Get or create correlation ID
        if correlation_id is None:
            correlation_id = self.get_correlation_id()
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        # Sanitize metadata
        sanitized_metadata = self._sanitize_data(metadata) if metadata else {}
        sanitized_error = self._sanitize_string(error) if error else None
        
        entry = {
            "timestamp": datetime.now().isoformat(),
            "timestamp_unix": time.time(),
            "workflow_name": workflow_name,
            "user": user or "unknown",
            "duration_seconds": duration,
            "success": success,
            "error": sanitized_error,
            "metadata": sanitized_metadata,
            "action_type": action_type or "workflow_execution",
            "log_level": LogLevel(log_level).name,
            "correlation_id": correlation_id,
            "version": "2.0",
        }
        
        with self._lock:
            # Write to JSON array (for backward compatibility)
            entries = self._read_entries()
            entries.append(entry)
            
            # Enforce max entries (FIFO)
            if len(entries) > self._max_entries:
                entries = entries[-self._max_entries:]
            
            self._write_entries(entries)
            
            # Write to JSON Lines format
            if self._json_lines:
                self._append_jsonl(entry)
        
        # Send to syslog if enabled
        if self._enable_syslog and self._syslog_handler:
            self._send_syslog(entry)
        
        return entry
    
    def _send_syslog(self, entry: Dict[str, Any]) -> None:
        """Send entry to syslog server.
        
        Args:
            entry: Audit log entry to send.
        """
        with self._syslog_lock:
            try:
                if self._syslog_handler:
                    level_name = entry.get("log_level", "INFO")
                    level = getattr(logging, level_name, logging.INFO)
                    
                    # Create a minimal log record
                    record = logging.LogRecord(
                        name="audit_logger",
                        level=level,
                        pathname="",
                        lineno=0,
                        msg=json.dumps(entry, default=str),
                        args=(),
                        exc_info=None
                    )
                    self._syslog_handler.emit(record)
            except Exception:
                pass
    
    def log_action(
        self,
        action_type: str,
        action_name: str,
        user: Optional[str] = None,
        duration: Optional[float] = None,
        success: bool = True,
        error: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        log_level: LogLevel = LogLevel.INFO,
        correlation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Log an individual action within a workflow.
        
        Args:
            action_type: Type of action (e.g., 'click', 'type', 'navigate').
            action_name: Name of the action.
            user: User who performed the action.
            duration: Action duration in seconds.
            success: Whether action succeeded.
            error: Error message if failed.
            params: Action parameters.
            log_level: Log level for this entry.
            correlation_id: Correlation ID for tracking.
            
        Returns:
            The created audit entry.
        """
        metadata = {
            "action_name": action_name,
            "params": self._sanitize_data(params) if params else {},
        }
        
        return self.log_execution(
            workflow_name=f"{action_type}:{action_name}",
            user=user,
            duration=duration,
            success=success,
            error=error,
            metadata=metadata,
            action_type=action_type,
            log_level=log_level,
            correlation_id=correlation_id,
        )
    
    def query_logs(
        self,
        workflow_name: Optional[str] = None,
        user: Optional[str] = None,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        success: Optional[bool] = None,
        action_type: Optional[str] = None,
        correlation_id: Optional[str] = None,
        log_level: Optional[Union[str, LogLevel]] = None,
        limit: int = 100,
        include_archived: bool = False,
    ) -> List[Dict[str, Any]]:
        """Query audit logs with filters.
        
        Args:
            workflow_name: Filter by workflow name (partial match).
            user: Filter by user (partial match).
            start_time: Filter by start timestamp (ISO format or datetime).
            end_time: Filter by end timestamp (ISO format or datetime).
            success: Filter by success status.
            action_type: Filter by action type.
            correlation_id: Filter by correlation ID.
            log_level: Filter by minimum log level.
            limit: Maximum number of entries to return.
            include_archived: Include archived/compressed logs in search.
            
        Returns:
            List of matching audit entries.
        """
        # Normalize time filters
        start_ts = self._normalize_timestamp(start_time) if start_time else None
        end_ts = self._normalize_timestamp(end_time) if end_time else None
        
        # Normalize log level
        min_level = LogLevel.INFO
        if log_level:
            if isinstance(log_level, str):
                min_level = LogLevel[log_level.upper()]
            else:
                min_level = LogLevel(log_level)
        
        results = []
        
        # Query current JSON file
        entries = self._read_entries()
        results.extend(self._filter_entries(
            entries, workflow_name, user, start_ts, end_ts,
            success, action_type, correlation_id, min_level, limit - len(results)
        ))
        
        # Query JSON Lines file if different from JSON file
        if self._json_lines and os.path.exists(self._json_lines_file):
            jsonl_entries = self._read_jsonl_entries()
            results.extend(self._filter_entries(
                jsonl_entries, workflow_name, user, start_ts, end_ts,
                success, action_type, correlation_id, min_level, limit - len(results)
            ))
        
        # Optionally query archived logs
        if include_archived and os.path.exists(self._archive_dir):
            archived_results = self._search_archived_logs(
                workflow_name, user, start_ts, end_ts,
                success, action_type, correlation_id, min_level, limit - len(results)
            )
            results.extend(archived_results)
        
        # Sort by timestamp descending and limit
        results.sort(key=lambda x: x.get('timestamp_unix', 0), reverse=True)
        return results[:limit]
    
    def _normalize_timestamp(self, ts: Union[str, datetime]) -> float:
        """Convert timestamp to Unix float.
        
        Args:
            ts: Timestamp as ISO string or datetime.
            
        Returns:
            Unix timestamp float.
        """
        if isinstance(ts, datetime):
            return ts.timestamp()
        try:
            return datetime.fromisoformat(ts.replace('Z', '+00:00')).timestamp()
        except (ValueError, AttributeError):
            return 0.0
    
    def _filter_entries(
        self,
        entries: List[Dict[str, Any]],
        workflow_name: Optional[str],
        user: Optional[str],
        start_ts: Optional[float],
        end_ts: Optional[float],
        success: Optional[bool],
        action_type: Optional[str],
        correlation_id: Optional[str],
        min_level: LogLevel,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Filter entries based on criteria.
        
        Args:
            entries: Entries to filter.
            workflow_name: Workflow name filter.
            user: User filter.
            start_ts: Start timestamp (Unix).
            end_ts: End timestamp (Unix).
            success: Success filter.
            action_type: Action type filter.
            correlation_id: Correlation ID filter.
            min_level: Minimum log level.
            limit: Max results.
            
        Returns:
            Filtered entries.
        """
        results = []
        
        for entry in reversed(entries):
            # Filter by workflow name
            if workflow_name and workflow_name.lower() not in entry.get(
                "workflow_name", ""
            ).lower():
                continue
            
            # Filter by user
            if user and user.lower() not in entry.get("user", "").lower():
                continue
            
            # Filter by time range
            ts = entry.get("timestamp_unix", 0)
            if start_ts and ts < start_ts:
                continue
            if end_ts and ts > end_ts:
                continue
            
            # Filter by success
            if success is not None and entry.get("success") != success:
                continue
            
            # Filter by action type
            if action_type and entry.get("action_type") != action_type:
                continue
            
            # Filter by correlation ID
            if correlation_id and entry.get("correlation_id") != correlation_id:
                continue
            
            # Filter by log level
            level_name = entry.get("log_level", "INFO")
            try:
                level = LogLevel[level_name]
                if level < min_level:
                    continue
            except KeyError:
                pass
            
            results.append(entry)
            
            if len(results) >= limit:
                break
        
        return results
    
    def _search_archived_logs(
        self,
        workflow_name: Optional[str],
        user: Optional[str],
        start_ts: Optional[float],
        end_ts: Optional[float],
        success: Optional[bool],
        action_type: Optional[str],
        correlation_id: Optional[str],
        min_level: LogLevel,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Search through archived/compressed log files.
        
        Args:
            Same as _filter_entries.
            
        Returns:
            Matching entries from archives.
        """
        results = []
        
        for filename in os.listdir(self._archive_dir):
            if len(results) >= limit:
                break
                
            filepath = os.path.join(self._archive_dir, filename)
            if not os.path.isfile(filepath):
                continue
            
            try:
                # Open gzipped or plain file
                if filename.endswith('.gz'):
                    opener = lambda f: gzip.open(f, 'rb')
                    reader = lambda f: (json.loads(line.decode('utf-8')) for line in f if line.strip())
                else:
                    opener = lambda f: open(f, 'r', encoding='utf-8')
                    reader = lambda f: (json.loads(line) for line in f if line.strip())
                
                with opener(filepath) as f:
                    for entry in reader(f):
                        if self._matches_filter(entry, workflow_name, user, start_ts, 
                                               end_ts, success, action_type, 
                                               correlation_id, min_level):
                            results.append(entry)
                            if len(results) >= limit:
                                break
                                
            except Exception:
                continue
        
        return results
    
    def _matches_filter(
        self,
        entry: Dict[str, Any],
        workflow_name: Optional[str],
        user: Optional[str],
        start_ts: Optional[float],
        end_ts: Optional[float],
        success: Optional[bool],
        action_type: Optional[str],
        correlation_id: Optional[str],
        min_level: LogLevel,
    ) -> bool:
        """Check if entry matches all filters."""
        if workflow_name and workflow_name.lower() not in entry.get("workflow_name", "").lower():
            return False
        if user and user.lower() not in entry.get("user", "").lower():
            return False
        
        ts = entry.get("timestamp_unix", 0)
        if start_ts and ts < start_ts:
            return False
        if end_ts and ts > end_ts:
            return False
        
        if success is not None and entry.get("success") != success:
            return False
        if action_type and entry.get("action_type") != action_type:
            return False
        if correlation_id and entry.get("correlation_id") != correlation_id:
            return False
        
        level_name = entry.get("log_level", "INFO")
        try:
            if LogLevel[level_name] < min_level:
                return False
        except KeyError:
            pass
        
        return True
    
    def get_statistics(
        self,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        action_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get audit log statistics.
        
        Args:
            start_time: Filter by start timestamp.
            end_time: Filter by end timestamp.
            action_type: Filter by action type.
            
        Returns:
            Dictionary with execution statistics.
        """
        entries = self._read_entries()
        
        # Also include JSON Lines entries
        if self._json_lines and os.path.exists(self._json_lines_file):
            jsonl_entries = self._read_jsonl_entries()
            # Deduplicate by timestamp + workflow_name
            seen = set()
            for e in jsonl_entries:
                key = (e.get('timestamp'), e.get('workflow_name'))
                if key not in seen:
                    seen.add(key)
                    entries.append(e)
        
        # Normalize time filters
        start_ts = self._normalize_timestamp(start_time) if start_time else None
        end_ts = self._normalize_timestamp(end_time) if end_time else None
        
        # Apply filters
        filtered = []
        for entry in entries:
            ts = entry.get("timestamp_unix", 0)
            if start_ts and ts < start_ts:
                continue
            if end_ts and ts > end_ts:
                continue
            if action_type and entry.get("action_type") != action_type:
                continue
            filtered.append(entry)
        
        if not filtered:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "success_rate": 0.0,
                "average_duration": 0.0,
                "total_duration": 0.0,
                "action_counts": {},
                "error_counts": {},
                "unique_workflows": 0,
                "unique_users": 0,
                "unique_correlation_ids": 0,
            }
        
        successful = sum(1 for e in filtered if e.get("success", False))
        failed = len(filtered) - successful
        
        durations = [
            e.get("duration_seconds", 0)
            for e in filtered
            if e.get("duration_seconds") is not None
        ]
        total_duration = sum(durations)
        avg_duration = total_duration / len(durations) if durations else 0.0
        
        # Count by action type
        action_counts = defaultdict(int)
        for e in filtered:
            at = e.get("action_type", "unknown")
            action_counts[at] += 1
        
        # Count errors
        error_counts = defaultdict(int)
        for e in filtered:
            if not e.get("success", True):
                err = e.get("error", "unknown_error")
                error_counts[err] += 1
        
        return {
            "total_executions": len(filtered),
            "successful_executions": successful,
            "failed_executions": failed,
            "success_rate": successful / len(filtered) if filtered else 0.0,
            "average_duration": avg_duration,
            "total_duration": total_duration,
            "min_duration": min(durations) if durations else 0.0,
            "max_duration": max(durations) if durations else 0.0,
            "action_counts": dict(action_counts),
            "error_counts": dict(error_counts),
            "unique_workflows": len(set(e.get("workflow_name") for e in filtered)),
            "unique_users": len(set(e.get("user") for e in filtered)),
            "unique_correlation_ids": len(set(e.get("correlation_id") for e in filtered if e.get("correlation_id"))),
        }
    
    def get_metrics(
        self,
        time_window_hours: int = 24,
        group_by: str = "hour",
    ) -> Dict[str, Any]:
        """Get time-series metrics for dashboards.
        
        Args:
            time_window_hours: Time window to analyze.
            group_by: Grouping granularity ('hour', 'day', 'action_type').
            
        Returns:
            Metrics suitable for dashboard consumption.
        """
        cutoff = time.time() - (time_window_hours * 3600)
        entries = self._read_entries()
        
        # Filter to time window
        recent = [e for e in entries if e.get("timestamp_unix", 0) >= cutoff]
        
        if not recent:
            return {
                "time_window_hours": time_window_hours,
                "group_by": group_by,
                "series": [],
                "summary": {
                    "total": 0,
                    "success_rate": 0.0,
                    "avg_duration": 0.0,
                }
            }
        
        # Group entries
        groups = defaultdict(list)
        for entry in recent:
            if group_by == "hour":
                dt = datetime.fromtimestamp(entry.get("timestamp_unix", 0))
                key = dt.strftime("%Y-%m-%d %H:00")
            elif group_by == "day":
                dt = datetime.fromtimestamp(entry.get("timestamp_unix", 0))
                key = dt.strftime("%Y-%m-%d")
            elif group_by == "action_type":
                key = entry.get("action_type", "unknown")
            else:
                key = "all"
            
            groups[key].append(entry)
        
        # Build series
        series = []
        for key in sorted(groups.keys()):
            group_entries = groups[key]
            durations = [e.get("duration_seconds", 0) for e in group_entries if e.get("duration_seconds")]
            successful = sum(1 for e in group_entries if e.get("success", False))
            
            series.append({
                "timestamp": key,
                "count": len(group_entries),
                "success_count": successful,
                "failure_count": len(group_entries) - successful,
                "success_rate": successful / len(group_entries) if group_entries else 0.0,
                "avg_duration": sum(durations) / len(durations) if durations else 0.0,
            })
        
        # Overall summary
        all_durations = [e.get("duration_seconds", 0) for e in recent if e.get("duration_seconds")]
        total_success = sum(1 for e in recent if e.get("success", False))
        
        return {
            "time_window_hours": time_window_hours,
            "group_by": group_by,
            "series": series,
            "summary": {
                "total": len(recent),
                "success_rate": total_success / len(recent) if recent else 0.0,
                "avg_duration": sum(all_durations) / len(all_durations) if all_durations else 0.0,
            }
        }
    
    def export_jsonl(
        self,
        output_file: Optional[str] = None,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        compress: bool = False,
    ) -> str:
        """Export logs in JSON Lines format.
        
        Args:
            output_file: Output file path. Defaults to logs/export_TIMESTAMP.jsonl.
            start_time: Filter by start timestamp.
            end_time: Filter by end timestamp.
            compress: Compress output with gzip.
            
        Returns:
            Path to exported file.
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(
                os.path.dirname(self._log_file),
                f"export_{timestamp}.jsonl"
            )
        
        entries = self.query_logs(
            start_time=start_time,
            end_time=end_time,
            limit=self._max_entries,
        )
        
        if compress:
            with gzip.open(output_file + ".gz", "wt", encoding="utf-8") as f:
                for entry in entries:
                    f.write(json.dumps(entry, default=str) + "\n")
            return output_file + ".gz"
        else:
            with open(output_file, "w", encoding="utf-8") as f:
                for entry in entries:
                    f.write(json.dumps(entry, default=str) + "\n")
            return output_file
    
    def force_rotation(self) -> None:
        """Manually trigger log rotation."""
        with self._lock:
            self._rotate_logs()
    
    def flush(self) -> None:
        """Ensure all pending writes are complete."""
        # In this implementation, writes are immediate, but this
        # method exists for API compatibility
        pass
    
    def close(self) -> None:
        """Close the logger and release resources."""
        if self._syslog_handler:
            try:
                self._syslog_handler.close()
            except Exception:
                pass
            self._syslog_handler = None
    
    def __enter__(self) -> "AuditLogger":
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def with_correlation_id(logger: AuditLogger, correlation_id: Optional[str] = None):
    """Decorator to automatically manage correlation IDs for a function.
    
    Args:
        logger: AuditLogger instance.
        correlation_id: Optional correlation ID to use.
        
    Returns:
        Decorated function that manages correlation ID lifecycle.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            cid = logger.push_correlation_id(correlation_id)
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                logger.pop_correlation_id()
        return wrapper
    return decorator
