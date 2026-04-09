"""Automation Logger Action Module.

Provides structured logging capabilities for automation workflows.
"""

import time
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class AutomationLoggerAction(BaseAction):
    """Structured logger for automation workflows.
    
    Provides context-aware logging with configurable outputs.
    """
    action_type = "automation_logger"
    display_name = "自动化日志"
    description = "为工作流提供结构化日志"
    
    def __init__(self):
        super().__init__()
        self._logs: deque = deque(maxlen=10000)
        self._min_level = LogLevel.INFO
        self._handlers: List[Callable] = []
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logging operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, level, message, metadata.
        
        Returns:
            ActionResult with logging result.
        """
        action = params.get('action', 'log')
        
        if action == 'log':
            return self._log_message(params)
        elif action == 'get_logs':
            return self._get_logs(params)
        elif action == 'clear':
            return self._clear_logs(params)
        elif action == 'set_level':
            return self._set_level(params)
        elif action == 'add_handler':
            return self._add_handler(params)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _log_message(self, params: Dict) -> ActionResult:
        """Log a message."""
        level_str = params.get('level', 'INFO')
        message = params.get('message', '')
        metadata = params.get('metadata', {})
        
        try:
            level = LogLevel[level_str.upper()]
        except KeyError:
            level = LogLevel.INFO
        
        if level.value < self._min_level.value:
            return ActionResult(
                success=True,
                data={'logged': False, 'reason': 'below_min_level'},
                error=None
            )
        
        log_entry = {
            'timestamp': time.time(),
            'level': level.name,
            'message': message,
            'metadata': metadata,
            'formatted': f"[{level.name}] {message}"
        }
        
        self._logs.append(log_entry)
        
        # Call handlers
        for handler in self._handlers:
            try:
                handler(log_entry)
            except Exception:
                pass
        
        return ActionResult(
            success=True,
            data={'logged': True, 'entry': log_entry},
            error=None
        )
    
    def _get_logs(self, params: Dict) -> ActionResult:
        """Get logs with filtering."""
        level = params.get('level', None)
        since = params.get('since', 0)
        limit = params.get('limit', 100)
        
        filtered = [
            log for log in self._logs
            if log['timestamp'] >= since
            and (level is None or log['level'] == level.upper())
        ]
        
        return ActionResult(
            success=True,
            data={
                'logs': list(filtered)[-limit:],
                'count': len(filtered)
            },
            error=None
        )
    
    def _clear_logs(self, params: Dict) -> ActionResult:
        """Clear all logs."""
        cleared = len(self._logs)
        self._logs.clear()
        
        return ActionResult(
            success=True,
            data={'cleared_count': cleared},
            error=None
        )
    
    def _set_level(self, params: Dict) -> ActionResult:
        """Set minimum log level."""
        level_str = params.get('level', 'INFO')
        try:
            self._min_level = LogLevel[level_str.upper()]
        except KeyError:
            return ActionResult(
                success=False,
                data=None,
                error=f"Invalid level: {level_str}"
            )
        
        return ActionResult(
            success=True,
            data={'min_level': self._min_level.name},
            error=None
        )
    
    def _add_handler(self, params: Dict) -> ActionResult:
        """Add a log handler."""
        handler_type = params.get('handler_type', 'file')
        config = params.get('config', {})
        
        if handler_type == 'file':
            handler = self._create_file_handler(config)
        elif handler_type == 'console':
            handler = self._create_console_handler(config)
        elif handler_type == 'webhook':
            handler = self._create_webhook_handler(config)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown handler type: {handler_type}"
            )
        
        self._handlers.append(handler)
        
        return ActionResult(
            success=True,
            data={'handler_type': handler_type},
            error=None
        )
    
    def _create_file_handler(self, config: Dict) -> Callable:
        """Create file handler."""
        filepath = config.get('path', '/tmp/automation.log')
        
        def handler(log_entry: Dict):
            with open(filepath, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        
        return handler
    
    def _create_console_handler(self, config: Dict) -> Callable:
        """Create console handler."""
        def handler(log_entry: Dict):
            print(log_entry['formatted'])
        return handler
    
    def _create_webhook_handler(self, config: Dict) -> Callable:
        """Create webhook handler."""
        webhook_url = config.get('url', '')
        
        def handler(log_entry: Dict):
            import urllib.request
            try:
                data = json.dumps(log_entry).encode()
                req = urllib.request.Request(
                    webhook_url,
                    data=data,
                    headers={'Content-Type': 'application/json'}
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception:
                pass
        
        return handler


class LogAggregatorAction(BaseAction):
    """Aggregate logs from multiple sources.
    
    Collects and combines logs from different workflow components.
    """
    action_type = "log_aggregator"
    display_name = "日志聚合"
    description = "从多个来源收集和组合日志"
    
    def __init__(self):
        super().__init__()
        self._sources: Dict[str, List[Dict]] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute log aggregation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, source, logs.
        
        Returns:
            ActionResult with aggregated logs.
        """
        action = params.get('action', 'add')
        source = params.get('source', 'default')
        
        if action == 'add':
            return self._add_logs(source, params)
        elif action == 'get_all':
            return self._get_all_logs(params)
        elif action == 'query':
            return self._query_logs(params)
        elif action == 'clear':
            return self._clear_source(source)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _add_logs(self, source: str, params: Dict) -> ActionResult:
        """Add logs from a source."""
        logs = params.get('logs', [])
        
        if source not in self._sources:
            self._sources[source] = []
        
        self._sources[source].extend(logs)
        
        return ActionResult(
            success=True,
            data={
                'source': source,
                'added_count': len(logs),
                'total_count': len(self._sources[source])
            },
            error=None
        )
    
    def _get_all_logs(self, params: Dict) -> ActionResult:
        """Get all aggregated logs."""
        level = params.get('level', None)
        since = params.get('since', 0)
        limit = params.get('limit', 1000)
        
        all_logs = []
        for source, logs in self._sources.items():
            for log in logs:
                log['source'] = source
                all_logs.append(log)
        
        # Filter
        filtered = [
            log for log in all_logs
            if log['timestamp'] >= since
            and (level is None or log.get('level') == level.upper())
        ]
        
        # Sort by timestamp
        filtered.sort(key=lambda x: x['timestamp'])
        
        return ActionResult(
            success=True,
            data={
                'logs': filtered[-limit:],
                'count': len(filtered),
                'sources': list(self._sources.keys())
            },
            error=None
        )
    
    def _query_logs(self, params: Dict) -> ActionResult:
        """Query logs with filters."""
        query = params.get('query', {})
        sources = params.get('sources', [])
        
        all_logs = []
        for source, logs in self._sources.items():
            if sources and source not in sources:
                continue
            for log in logs:
                log['source'] = source
                all_logs.append(log)
        
        # Apply query filters
        if 'message_contains' in query:
            term = query['message_contains'].lower()
            all_logs = [
                log for log in all_logs
                if term in log.get('message', '').lower()
            ]
        
        return ActionResult(
            success=True,
            data={
                'logs': all_logs,
                'count': len(all_logs)
            },
            error=None
        )
    
    def _clear_source(self, source: str) -> ActionResult:
        """Clear logs for a source."""
        count = len(self._sources.get(source, []))
        if source in self._sources:
            del self._sources[source]
        
        return ActionResult(
            success=True,
            data={'source': source, 'cleared_count': count},
            error=None
        )


def register_actions():
    """Register all Automation Logger actions."""
    return [
        AutomationLoggerAction,
        LogAggregatorAction,
    ]
