"""Logging utilities for RabAI AutoClick.

Provides a singleton Logger class with console and file handlers.
"""

import logging
import os
from datetime import datetime
from typing import Optional


class Logger:
    """Singleton logger for RabAI AutoClick.
    
    Outputs to both console (INFO+) and daily rotating log files (DEBUG+).
    """
    
    _instance: Optional['Logger'] = None
    
    def __new__(cls) -> 'Logger':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialized = True
        self._logger = logging.getLogger('RabAI_AutoClick')
        self._logger.setLevel(logging.DEBUG)
        
        self._setup_handlers()
    
    def _setup_handlers(self) -> None:
        """Set up console and file handlers."""
        # Console handler (INFO and above)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self._logger.addHandler(console_handler)
        
        # File handler (DEBUG and above)
        log_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'logs'
        )
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(
            log_dir, 
            f'autoclick_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        )
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
    
    def debug(self, message: str) -> None:
        """Log a debug message."""
        self._logger.debug(message)
    
    def info(self, message: str) -> None:
        """Log an info message."""
        self._logger.info(message)
    
    def warning(self, message: str) -> None:
        """Log a warning message."""
        self._logger.warning(message)
    
    def error(self, message: str) -> None:
        """Log an error message."""
        self._logger.error(message)
    
    def critical(self, message: str) -> None:
        """Log a critical message."""
        self._logger.critical(message)


# Global logger instance
logger: Logger = Logger()
