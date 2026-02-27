import logging
import os
import sys
from datetime import datetime
from typing import Optional, List, Callable
from pathlib import Path
import json
import threading
from collections import deque


class LogEntry:
    def __init__(self, level: str, message: str, module: str = ""):
        self.timestamp = datetime.now()
        self.level = level
        self.message = message
        self.module = module
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            'level': self.level,
            'message': self.message,
            'module': self.module
        }


class AppLogger:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        
        self._entries: deque = deque(maxlen=500)
        self._listeners: List[Callable] = []
        self._log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        os.makedirs(self._log_dir, exist_ok=True)
        
        self._logger = logging.getLogger('RabAI_AutoClick')
        self._logger.setLevel(logging.DEBUG)
        
        self._setup_file_handler()
        self._setup_console_handler()
    
    def _setup_file_handler(self):
        log_file = os.path.join(self._log_dir, f'app_{datetime.now().strftime("%Y%m%d")}.log')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
    
    def _setup_console_handler(self):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self._logger.addHandler(console_handler)
    
    def _add_entry(self, level: str, message: str, module: str = ""):
        entry = LogEntry(level, message, module)
        self._entries.append(entry)
        for listener in self._listeners:
            try:
                listener(entry)
            except Exception:
                pass
    
    def add_listener(self, callback: Callable) -> None:
        self._listeners.append(callback)
    
    def remove_listener(self, callback: Callable) -> None:
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def debug(self, message: str, module: str = ""):
        self._logger.debug(message)
        self._add_entry('DEBUG', message, module)
    
    def info(self, message: str, module: str = ""):
        self._logger.info(message)
        self._add_entry('INFO', message, module)
    
    def warning(self, message: str, module: str = ""):
        self._logger.warning(message)
        self._add_entry('WARNING', message, module)
    
    def error(self, message: str, module: str = ""):
        self._logger.error(message)
        self._add_entry('ERROR', message, module)
    
    def critical(self, message: str, module: str = ""):
        self._logger.critical(message)
        self._add_entry('CRITICAL', message, module)
    
    def success(self, message: str, module: str = ""):
        self._logger.info(f"✓ {message}")
        self._add_entry('SUCCESS', message, module)
    
    def get_entries(self, count: int = 50) -> List[LogEntry]:
        return list(self._entries)[-count:]
    
    def get_entries_by_level(self, level: str) -> List[LogEntry]:
        return [e for e in self._entries if e.level == level]
    
    def clear(self):
        self._entries.clear()
    
    def export_to_file(self, filepath: str) -> bool:
        try:
            entries = [e.to_dict() for e in self._entries]
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False


app_logger = AppLogger()
