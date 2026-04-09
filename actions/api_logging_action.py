"""API Logging Action Module.

Provides structured API logging with log levels, formatting,
rotation, aggregation, and log-based analytics.
"""

import time
import threading
import sys
import os
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass
class LogEntry:
    """Individual log entry."""
    timestamp: float
    level: LogLevel
    message: str
    service: str
    trace_id: Optional[str]
    user_id: Optional[str]
    metadata: Dict[str, Any]


class ApiLoggingAction(BaseAction):
    """API Structured Logger.

    Structured logging with multiple output formats,
    log levels, rotation, and analytics.
    """
    action_type = "api_logging"
    display_name = "API日志系统"
    description = "结构化API日志系统，支持分级和聚合"

    _logs: List[LogEntry] = []
    _log_aggregates: Dict[str, Dict[str, int]] = {}
    _lock = threading.RLock()
    _max_logs: int = 10000

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute logging operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'log', 'query', 'aggregate', 'rotate',
                               'export', 'get_stats', 'clear'
                - level: str - DEBUG, INFO, WARNING, ERROR, CRITICAL
                - message: str - log message
                - service: str (optional) - service name
                - trace_id: str (optional) - trace identifier
                - user_id: str (optional) - user identifier
                - metadata: dict (optional) - additional metadata
                - time_range: str (optional) - '1h', '24h', '7d'
                - filters: dict (optional) - query filters

        Returns:
            ActionResult with log operation result.
        """
        start_time = time.time()
        operation = params.get('operation', 'log')

        try:
            with self._lock:
                if operation == 'log':
                    return self._log_message(params, start_time)
                elif operation == 'query':
                    return self._query_logs(params, start_time)
                elif operation == 'aggregate':
                    return self._aggregate_logs(params, start_time)
                elif operation == 'rotate':
                    return self._rotate_logs(params, start_time)
                elif operation == 'export':
                    return self._export_logs(params, start_time)
                elif operation == 'get_stats':
                    return self._get_stats(params, start_time)
                elif operation == 'clear':
                    return self._clear_logs(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Logging error: {str(e)}",
                duration=time.time() - start_time
            )

    def _log_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Log a message."""
        level_str = params.get('level', 'INFO')
        message = params.get('message', '')
        service = params.get('service', 'unknown')
        trace_id = params.get('trace_id')
        user_id = params.get('user_id')
        metadata = params.get('metadata', {})

        try:
            level = LogLevel[level_str.upper()]
        except KeyError:
            level = LogLevel.INFO

        entry = LogEntry(
            timestamp=time.time(),
            level=level,
            message=message,
            service=service,
            trace_id=trace_id,
            user_id=user_id,
            metadata=metadata
        )

        self._logs.append(entry)

        if len(self._logs) > self._max_logs:
            self._logs = self._logs[-self._max_logs // 2:]

        if service not in self._log_aggregates:
            self._log_aggregates[service] = {'DEBUG': 0, 'INFO': 0, 'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0}
        self._log_aggregates[service][level.name] += 1

        return ActionResult(
            success=True,
            message=f"Logged: [{level.name}] {message[:50]}",
            data={'timestamp': entry.timestamp, 'level': level.name},
            duration=time.time() - start_time
        )

    def _query_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Query logs with filters."""
        time_range = params.get('time_range', '1h')
        filters = params.get('filters', {})
        limit = params.get('limit', 100)

        if time_range == '1h':
            cutoff = time.time() - 3600
        elif time_range == '24h':
            cutoff = time.time() - 86400
        elif time_range == '7d':
            cutoff = time.time() - 604800
        else:
            cutoff = time.time() - 3600

        results = []
        for entry in reversed(self._logs):
            if entry.timestamp < cutoff:
                continue

            if filters:
                match = True
                if 'service' in filters and entry.service != filters['service']:
                    match = False
                if 'level' in filters and entry.level.name != filters['level']:
                    match = False
                if 'trace_id' in filters and entry.trace_id != filters['trace_id']:
                    match = False
                if 'user_id' in filters and entry.user_id != filters['user_id']:
                    match = False
                if 'message_contains' in filters and filters['message_contains'] not in entry.message:
                    match = False
                if match:
                    results.append(entry)
            else:
                results.append(entry)

            if len(results) >= limit:
                break

        return ActionResult(
            success=True,
            message=f"Found {len(results)} log entries",
            data={
                'count': len(results),
                'logs': [
                    {'timestamp': e.timestamp, 'level': e.level.name, 'message': e.message,
                     'service': e.service, 'trace_id': e.trace_id, 'user_id': e.user_id, 'metadata': e.metadata}
                    for e in results
                ]
            },
            duration=time.time() - start_time
        )

    def _aggregate_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Aggregate logs by service, level, time bucket."""
        group_by = params.get('group_by', 'service')
        time_range = params.get('time_range', '1h')

        if time_range == '1h':
            cutoff = time.time() - 3600
        elif time_range == '24h':
            cutoff = time.time() - 86400
        elif time_range == '7d':
            cutoff = time.time() - 604800
        else:
            cutoff = time.time() - 3600

        aggregates: Dict[str, Dict[str, Any]] = {}

        for entry in self._logs:
            if entry.timestamp < cutoff:
                continue

            if group_by == 'service':
                key = entry.service
            elif group_by == 'level':
                key = entry.level.name
            elif group_by == 'minute':
                key = time.strftime('%Y-%m-%d %H:%M', time.localtime(entry.timestamp))
            elif group_by == 'hour':
                key = time.strftime('%Y-%m-%d %H:00', time.localtime(entry.timestamp))
            else:
                key = entry.service

            if key not in aggregates:
                aggregates[key] = {'count': 0, 'levels': {}, 'errors': []}

            aggregates[key]['count'] += 1
            if entry.level.name not in aggregates[key]['levels']:
                aggregates[key]['levels'][entry.level.name] = 0
            aggregates[key]['levels'][entry.level.name] += 1

            if entry.level.value >= LogLevel.ERROR.value:
                aggregates[key]['errors'].append({'timestamp': entry.timestamp, 'message': entry.message})

        return ActionResult(
            success=True,
            message=f"Aggregated by {group_by}",
            data={'aggregates': aggregates, 'group_by': group_by},
            duration=time.time() - start_time
        )

    def _rotate_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Rotate/compact log storage."""
        max_age_hours = params.get('max_age_hours', 24)
        max_count = params.get('max_count', 5000)
        cutoff = time.time() - (max_age_hours * 3600)

        original_count = len(self._logs)
        self._logs = [e for e in self._logs if e.timestamp >= cutoff]

        if len(self._logs) > max_count:
            self._logs = self._logs[-max_count:]

        rotated = original_count - len(self._logs)

        return ActionResult(
            success=True,
            message=f"Rotated {rotated} log entries",
            data={'rotated_count': rotated, 'remaining_count': len(self._logs)},
            duration=time.time() - start_time
        )

    def _export_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Export logs in specified format."""
        export_format = params.get('format', 'json')
        time_range = params.get('time_range', '1h')
        limit = params.get('limit', 1000)

        if time_range == '1h':
            cutoff = time.time() - 3600
        elif time_range == '24h':
            cutoff = time.time() - 86400
        elif time_range == '7d':
            cutoff = time.time() - 604800
        else:
            cutoff = time.time() - 3600

        logs_to_export = [e for e in self._logs if e.timestamp >= cutoff][-limit:]

        if export_format == 'json':
            content = json.dumps([
                {'timestamp': e.timestamp, 'level': e.level.name, 'message': e.message,
                 'service': e.service, 'trace_id': e.trace_id, 'metadata': e.metadata}
                for e in logs_to_export
            ], indent=2)
        elif export_format == 'csv':
            lines = ['timestamp,level,service,message,trace_id']
            for e in logs_to_export:
                lines.append(f'{e.timestamp},{e.level.name},{e.service},{e.message},{e.trace_id or ""}')
            content = '\n'.join(lines)
        else:
            content = '\n'.join(f'[{e.timestamp}] {e.level.name} {e.service}: {e.message}' for e in logs_to_export)

        return ActionResult(
            success=True,
            message=f"Exported {len(logs_to_export)} logs as {export_format}",
            data={'format': export_format, 'count': len(logs_to_export), 'content_preview': content[:500]},
            duration=time.time() - start_time
        )

    def _get_stats(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get log statistics."""
        total = len(self._logs)
        by_level = {'DEBUG': 0, 'INFO': 0, 'WARNING': 0, 'ERROR': 0, 'CRITICAL': 0}
        by_service: Dict[str, int] = {}

        for entry in self._logs:
            by_level[entry.level.name] += 1
            by_service[entry.service] = by_service.get(entry.service, 0) + 1

        return ActionResult(
            success=True,
            message="Log statistics",
            data={
                'total_logs': total,
                'by_level': by_level,
                'by_service': by_service,
                'services': list(by_service.keys()),
            },
            duration=time.time() - start_time
        )

    def _clear_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clear all logs."""
        cleared = len(self._logs)
        self._logs.clear()
        self._log_aggregates.clear()

        return ActionResult(
            success=True,
            message=f"Cleared {cleared} log entries",
            data={'cleared_count': cleared},
            duration=time.time() - start_time
        )
