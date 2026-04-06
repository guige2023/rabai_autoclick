"""Application logger for RabAI AutoClick.

Provides a singleton logger with in-memory log entries, file persistence,
and listener callbacks for log events.
"""

import logging
import os
import sys
import json
import threading
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Callable, Deque, Dict, List, Optional


class LogEntry:
    """Represents a single log entry."""
    
    def __init__(
        self, 
        level: str, 
        message: str, 
        module: str = ""
    ) -> None:
        """Initialize a log entry.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, etc.).
            message: Log message content.
            module: Optional module name that generated the log.
        """
        self.timestamp: datetime = datetime.now()
        self.level: str = level
        self.message: str = message
        self.module: str = module
    
    def to_dict(self) -> Dict[str, str]:
        """Convert log entry to dictionary.
        
        Returns:
            Dictionary with timestamp, level, message, and module keys.
        """
        return {
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            'level': self.level,
            'message': self.message,
            'module': self.module
        }


class AppLogger:
    """Singleton application logger with multiple output targets.
    
    Outputs to:
    - In-memory deque (last 500 entries)
    - Daily rotating log files
    - Console (INFO and above)
    - Registered listener callbacks
    """
    
    _instance: Optional['AppLogger'] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls) -> 'AppLogger':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialized = True
        
        # In-memory log entries (max 500)
        self._entries: Deque[LogEntry] = deque(maxlen=500)
        
        # Listener callbacks
        self._listeners: List[Callable[[LogEntry], None]] = []
        
        # Log directory
        self._log_dir: str = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'logs'
        )
        os.makedirs(self._log_dir, exist_ok=True)
        
        # Standard logger
        self._logger: logging.Logger = logging.getLogger('RabAI_AutoClick')
        self._logger.setLevel(logging.DEBUG)
        
        self._setup_file_handler()
        self._setup_console_handler()
    
    def _setup_file_handler(self) -> None:
        """Set up file handler for DEBUG+ level logging."""
        log_file = os.path.join(
            self._log_dir, 
            f'app_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
    
    def _setup_console_handler(self) -> None:
        """Set up console handler for INFO+ level logging."""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self._logger.addHandler(console_handler)
    
    def _add_entry(
        self, 
        level: str, 
        message: str, 
        module: str = ""
    ) -> None:
        """Add a log entry and notify listeners.
        
        Args:
            level: Log level string.
            message: Log message.
            module: Optional module name.
        """
        entry = LogEntry(level, message, module)
        self._entries.append(entry)
        
        for listener in self._listeners:
            try:
                listener(entry)
            except Exception:
                pass
    
    def add_listener(
        self, 
        callback: Callable[[LogEntry], None]
    ) -> None:
        """Register a listener callback for log entries.
        
        Args:
            callback: Function to call with each LogEntry.
        """
        self._listeners.append(callback)
    
    def remove_listener(
        self, 
        callback: Callable[[LogEntry], None]
    ) -> None:
        """Unregister a listener callback.
        
        Args:
            callback: Previously registered callback to remove.
        """
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def debug(self, message: str, module: str = "") -> None:
        """Log a debug message."""
        self._logger.debug(message)
        self._add_entry('DEBUG', message, module)
    
    def info(self, message: str, module: str = "") -> None:
        """Log an info message."""
        self._logger.info(message)
        self._add_entry('INFO', message, module)
    
    def warning(self, message: str, module: str = "") -> None:
        """Log a warning message."""
        self._logger.warning(message)
        self._add_entry('WARNING', message, module)
    
    def error(self, message: str, module: str = "") -> None:
        """Log an error message."""
        self._logger.error(message)
        self._add_entry('ERROR', message, module)
    
    def critical(self, message: str, module: str = "") -> None:
        """Log a critical message."""
        self._logger.critical(message)
        self._add_entry('CRITICAL', message, module)
    
    def success(self, message: str, module: str = "") -> None:
        """Log a success message."""
        self._logger.info(f"✓ {message}")
        self._add_entry('SUCCESS', message, module)
    
    def get_entries(self, count: int = 50) -> List[LogEntry]:
        """Get recent log entries.
        
        Args:
            count: Maximum number of entries to return.
            
        Returns:
            List of recent LogEntry objects.
        """
        return list(self._entries)[-count:]
    
    def get_entries_by_level(self, level: str) -> List[LogEntry]:
        """Get log entries filtered by level.
        
        Args:
            level: Log level to filter by.
            
        Returns:
            List of matching LogEntry objects.
        """
        return [e for e in self._entries if e.level == level]
    
    def clear(self) -> None:
        """Clear all in-memory log entries."""
        self._entries.clear()
    
    def export_to_file(self, filepath: str) -> bool:
        """Export log entries to a JSON file.
        
        Args:
            filepath: Path to write the JSON file.
            
        Returns:
            True if exported successfully, False otherwise.
        """
        try:
            entries = [e.to_dict() for e in self._entries]
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False


# Global singleton instance
app_logger: AppLogger = AppLogger()
