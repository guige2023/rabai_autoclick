"""Logging configuration action module for RabAI AutoClick.

Provides logging operations:
- LoggingSetupAction: Configure logging
- LoggingLogAction: Write log entry
- LoggingRotateAction: Rotate log files
- LoggingSearchAction: Search log entries
"""

from __future__ import annotations

import logging
import sys
import os
import time
from typing import Any, Dict, List, Optional
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class LoggingSetupAction(BaseAction):
    """Configure logging."""
    action_type = "logging_setup"
    display_name = "日志配置"
    description = "配置日志系统"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute logging setup."""
        level = params.get('level', 'INFO')  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        log_file = params.get('log_file', None)
        max_bytes = params.get('max_bytes', 10485760)  # 10MB
        backup_count = params.get('backup_count', 5)
        format_str = params.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        output_var = params.get('output_var', 'logging_configured')

        try:
            resolved_level = context.resolve_value(level) if context else level
            resolved_file = context.resolve_value(log_file) if context else log_file
            resolved_format = context.resolve_value(format_str) if context else format_str

            numeric_level = getattr(logging, resolved_level.upper(), logging.INFO)

            handlers = []
            formatter = logging.Formatter(resolved_format)

            if resolved_file:
                _os.makedirs(_os.path.dirname(resolved_file) or '.', exist_ok=True)
                if max_bytes > 0:
                    file_handler = RotatingFileHandler(
                        resolved_file,
                        maxBytes=int(max_bytes),
                        backupCount=int(backup_count)
                    )
                else:
                    file_handler = logging.FileHandler(resolved_file)
                file_handler.setFormatter(formatter)
                handlers.append(file_handler)

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            handlers.append(console_handler)

            logging.basicConfig(
                level=numeric_level,
                handlers=handlers,
                force=True
            )

            result = {
                'configured': True,
                'level': resolved_level,
                'log_file': resolved_file,
                'handlers': len(handlers),
            }
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Logging configured: {resolved_level}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Logging setup error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'level': 'INFO', 'log_file': None, 'max_bytes': 10485760,
            'backup_count': 5, 'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'output_var': 'logging_configured'
        }


class LoggingLogAction(BaseAction):
    """Write log entry."""
    action_type = "logging_log"
    display_name = "写入日志"
    description = "写入日志条目"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute logging."""
        message = params.get('message', '')
        level = params.get('level', 'INFO')
        logger_name = params.get('logger_name', '')
        output_var = params.get('output_var', 'log_result')

        if not message:
            return ActionResult(success=False, message="message is required")

        try:
            resolved_message = context.resolve_value(message) if context else message
            resolved_level = context.resolve_value(level) if context else level

            if logger_name:
                logger = logging.getLogger(logger_name)
            else:
                logger = logging

            log_func = getattr(logger, resolved_level.lower(), logger.info)
            log_func(resolved_message)

            result = {'logged': True, 'level': resolved_level, 'message': resolved_message}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Logged: {resolved_message[:50]}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Logging error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'level': 'INFO', 'logger_name': '', 'output_var': 'log_result'}


class LoggingRotateAction(BaseAction):
    """Rotate log files."""
    action_type = "logging_rotate"
    display_name = "日志轮转"
    description = "轮转日志文件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute log rotation."""
        log_file = params.get('log_file', '')
        when = params.get('when', 'midnight')  # midnight, S (seconds), M (minutes), H (hours), D (days)
        interval = params.get('interval', 1)
        backup_count = params.get('backup_count', 7)
        output_var = params.get('output_var', 'rotate_result')

        if not log_file:
            return ActionResult(success=False, message="log_file is required")

        try:
            resolved_file = context.resolve_value(log_file) if context else log_file
            resolved_when = context.resolve_value(when) if context else when
            resolved_interval = context.resolve_value(interval) if context else interval
            resolved_backup = context.resolve_value(backup_count) if context else backup_count

            _os.makedirs(_os.path.dirname(resolved_file) or '.', exist_ok=True)

            handler = TimedRotatingFileHandler(
                resolved_file,
                when=resolved_when,
                interval=int(resolved_interval),
                backupCount=int(resolved_backup)
            )

            # Force rotation
            handler.emit(logging.LogRecord(
                name='rotation',
                level=logging.INFO,
                pathname='',
                lineno=0,
                msg='Log rotation triggered',
                args=(),
                exc_info=None
            ))

            result = {
                'rotated': True,
                'log_file': resolved_file,
                'backup_count': resolved_backup,
            }
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Log rotated: {resolved_file}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Log rotate error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['log_file']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'when': 'midnight', 'interval': 1, 'backup_count': 7, 'output_var': 'rotate_result'}


class LoggingSearchAction(BaseAction):
    """Search log entries."""
    action_type = "logging_search"
    display_name = "日志搜索"
    description = "搜索日志条目"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute log search."""
        log_file = params.get('log_file', '')
        pattern = params.get('pattern', '')
        level = params.get('level', None)  # filter by level
        since = params.get('since', None)  # ISO timestamp
        limit = params.get('limit', 100)
        output_var = params.get('output_var', 'log_matches')

        if not log_file:
            return ActionResult(success=False, message="log_file is required")

        try:
            import re

            resolved_file = context.resolve_value(log_file) if context else log_file
            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_limit = context.resolve_value(limit) if context else limit

            if resolved_pattern:
                regex = re.compile(resolved_pattern)

            matches = []
            count = 0
            with open(resolved_file, 'r') as f:
                for line in f:
                    count += 1
                    if resolved_pattern and not regex.search(line):
                        continue
                    if level and level.upper() not in line:
                        continue
                    matches.append(line.strip())
                    if len(matches) >= resolved_limit:
                        break

            result = {'matches': matches, 'count': len(matches), 'lines_searched': count}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Found {len(matches)} matches", data=result)
        except FileNotFoundError:
            return ActionResult(success=False, message=f"Log file not found: {resolved_file}")
        except Exception as e:
            return ActionResult(success=False, message=f"Log search error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['log_file']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pattern': '', 'level': None, 'since': None, 'limit': 100, 'output_var': 'log_matches'}
