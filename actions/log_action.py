"""
Logging configuration and log management actions.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta


def setup_logger(
    name: str,
    level: str = 'INFO',
    log_file: Optional[str] = None,
    format_string: Optional[str] = None,
    date_format: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with specified configuration.

    Args:
        name: Logger name.
        level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        log_file: Optional file path for logging.
        format_string: Custom format string.
        date_format: Custom date format.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    if logger.handlers:
        logger.handlers.clear()

    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    if date_format is None:
        date_format = '%Y-%m-%d %H:%M:%S'

    formatter = logging.Formatter(format_string, datefmt=date_format)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def configure_root_logger(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> None:
    """
    Configure the root logger.

    Args:
        level: Logging level.
        log_file: Optional file path.
        format_string: Custom format string.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string or '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(format_string or '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logging.getLogger().addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger by name.

    Args:
        name: Logger name.

    Returns:
        Logger instance.
    """
    return logging.getLogger(name)


def set_log_level(logger_name: str, level: str) -> None:
    """
    Set the log level for a specific logger.

    Args:
        logger_name: Name of the logger.
        level: New log level.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level.upper()))


def parse_log_file(
    log_file: str,
    level_filter: Optional[str] = None,
    pattern: Optional[str] = None,
    max_lines: int = 1000
) -> List[Dict[str, Any]]:
    """
    Parse a log file and extract entries.

    Args:
        log_file: Path to log file.
        level_filter: Filter by log level (ERROR, WARNING, etc).
        pattern: Optional regex pattern to match.
        max_lines: Maximum lines to read.

    Returns:
        List of log entry dictionaries.
    """
    if not Path(log_file).exists():
        return []

    import re

    entries = []
    level_pattern = level_filter.upper() if level_filter else None

    log_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
        r' - (?P<name>[\w\.]+) - (?P<level>\w+) - (?P<message>.+)'
    )

    with open(log_file, 'r') as f:
        for i, line in enumerate(f):
            if i >= max_lines:
                break

            match = log_pattern.match(line)
            if match:
                entry = match.groupdict()

                if level_pattern and entry['level'] != level_pattern:
                    continue

                if pattern and not re.search(pattern, entry['message']):
                    continue

                entries.append(entry)

    return entries


def filter_log_entries(
    entries: List[Dict[str, Any]],
    levels: Optional[List[str]] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    contains: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Filter log entries by various criteria.

    Args:
        entries: List of log entries.
        levels: Filter by log levels.
        start_time: Filter entries after this time.
        end_time: Filter entries before this time.
        contains: Filter by message content.

    Returns:
        Filtered log entries.
    """
    filtered = entries

    if levels:
        levels_upper = [l.upper() for l in levels]
        filtered = [e for e in filtered if e.get('level', '').upper() in levels_upper]

    if start_time:
        filtered = [
            e for e in filtered
            if datetime.strptime(e.get('timestamp', ''), '%Y-%m-%d %H:%M:%S') >= start_time
        ]

    if end_time:
        filtered = [
            e for e in filtered
            if datetime.strptime(e.get('timestamp', ''), '%Y-%m-%d %H:%M:%S') <= end_time
        ]

    if contains:
        filtered = [e for e in filtered if contains.lower() in e.get('message', '').lower()]

    return filtered


def aggregate_errors(log_file: str, time_window: int = 300) -> Dict[str, Any]:
    """
    Aggregate similar errors from a log file.

    Args:
        log_file: Path to log file.
        time_window: Time window in seconds for grouping.

    Returns:
        Dictionary of error summaries.
    """
    import re
    from collections import defaultdict

    error_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
        r' - .* - (?P<level>ERROR|CRITICAL) - (?P<message>.+)'
    )

    errors: Dict[str, List] = defaultdict(list)

    if not Path(log_file).exists():
        return {'errors': []}

    with open(log_file, 'r') as f:
        for line in f:
            match = error_pattern.match(line)
            if match:
                msg = match.group('message')
                key = msg[:100]
                errors[key].append(match.group('timestamp'))

    results = []
    for msg, timestamps in errors.items():
        results.append({
            'message_prefix': msg,
            'count': len(timestamps),
            'first_occurrence': timestamps[0],
            'last_occurrence': timestamps[-1],
        })

    results.sort(key=lambda x: x['count'], reverse=True)

    return {'errors': results}


def rotate_log_file(
    log_file: str,
    backup_count: int = 5,
    max_bytes: int = 10 * 1024 * 1024
) -> Dict[str, Any]:
    """
    Rotate a log file (like logrotate).

    Args:
        log_file: Path to log file.
        backup_count: Number of backup files to keep.
        max_bytes: Max size before rotation.

    Returns:
        Rotation result.
    """
    from shutil import move

    log_path = Path(log_file)

    if not log_path.exists():
        return {'success': False, 'error': 'Log file does not exist'}

    if log_path.stat().st_size < max_bytes:
        return {'success': True, 'rotated': False}

    for i in range(backup_count - 1, 0, -1):
        src = Path(f'{log_file}.{i}')
        dst = Path(f'{log_file}.{i + 1}')

        if src.exists():
            move(str(src), str(dst))

    move(log_file, f'{log_file}.1')

    log_path.touch()

    return {'success': True, 'rotated': True, 'backup': f'{log_file}.1'}


def tail_log_file(
    log_file: str,
    lines: int = 100,
    follow: bool = False
) -> List[str]:
    """
    Read the last N lines from a log file.

    Args:
        log_file: Path to log file.
        lines: Number of lines to read.
        follow: If True, keep reading new lines.

    Returns:
        List of log lines.
    """
    if not Path(log_file).exists():
        return []

    with open(log_file, 'r') as f:
        f.seek(0, 2)
        file_size = f.tell()

        if file_size > 0:
            f.seek(max(0, file_size - 10000))
            last_lines = f.readlines()

        return last_lines[-lines:] if last_lines else []


def get_log_summary(log_file: str) -> Dict[str, Any]:
    """
    Get a summary of a log file.

    Args:
        log_file: Path to log file.

    Returns:
        Summary dictionary.
    """
    if not Path(log_file).exists():
        return {'error': 'Log file not found'}

    stats = Path(log_file).stat()

    entries = parse_log_file(log_file, max_lines=10000)

    levels = {}
    for entry in entries:
        level = entry.get('level', 'UNKNOWN')
        levels[level] = levels.get(level, 0) + 1

    return {
        'file': log_file,
        'size_bytes': stats.st_size,
        'size_mb': round(stats.st_size / (1024 * 1024), 2),
        'last_modified': datetime.fromtimestamp(stats.st_mtime).isoformat(),
        'entries_analyzed': len(entries),
        'level_counts': levels,
        'total_errors': levels.get('ERROR', 0) + levels.get('CRITICAL', 0),
    }


def create_log_handler(
    handler_type: str,
    **kwargs
) -> logging.Handler:
    """
    Create a logging handler of the specified type.

    Args:
        handler_type: Type of handler ('stream', 'file', 'rotating', 'timed').
        **kwargs: Handler-specific arguments.

    Returns:
        Logging handler instance.
    """
    handler_type = handler_type.lower()

    if handler_type == 'stream':
        return logging.StreamHandler(sys.stdout)

    elif handler_type == 'file':
        filename = kwargs.get('filename', 'app.log')
        return logging.FileHandler(filename)

    elif handler_type == 'rotating':
        from logging.handlers import RotatingFileHandler
        filename = kwargs.get('filename', 'app.log')
        max_bytes = kwargs.get('max_bytes', 10 * 1024 * 1024)
        backup_count = kwargs.get('backup_count', 5)
        return RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)

    elif handler_type == 'timed':
        from logging.handlers import TimedRotatingFileHandler
        filename = kwargs.get('filename', 'app.log')
        when = kwargs.get('when', 'midnight')
        interval = kwargs.get('interval', 1)
        backup_count = kwargs.get('backup_count', 7)
        return TimedRotatingFileHandler(
            filename,
            when=when,
            interval=interval,
            backupCount=backup_count
        )

    else:
        raise ValueError(f"Unknown handler type: {handler_type}")


class LogCapture:
    """Context manager to capture log output."""

    def __init__(self, logger_name: str = '', level: str = 'DEBUG'):
        """
        Initialize log capture.

        Args:
            logger_name: Logger to capture (empty for root).
            level: Minimum level to capture.
        """
        self.logger_name = logger_name
        self.level = getattr(logging, level.upper())
        self.handler: Optional[logging.Handler] = None
        self.captured: List[str] = []

    def __enter__(self):
        """Start capturing."""
        self.handler = CapturedHandler(self.captured)
        self.handler.setLevel(self.level)

        logger = logging.getLogger(self.logger_name)
        logger.addHandler(self.handler)
        logger.setLevel(self.level)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop capturing."""
        if self.handler:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)


class CapturedHandler(logging.Handler):
    """Handler that stores records in a list."""

    def __init__(self, captured_list: List[str]):
        """
        Initialize.

        Args:
            captured_list: List to store captured messages.
        """
        super().__init__()
        self.captured = captured_list

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a record."""
        self.captured.append(self.format(record))
