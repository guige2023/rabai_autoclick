"""
Log aggregation and analysis module for distributed systems.

Collects, parses, and analyzes logs from multiple sources with
search, filtering, and alerting capabilities.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Iterator, Optional


class LogLevel(Enum):
    """Standard log levels."""
    TRACE = 10
    DEBUG = 20
    INFO = 30
    WARNING = 40
    ERROR = 50
    CRITICAL = 60
    FATAL = 70


class LogFormat(Enum):
    """Supported log formats."""
    JSON = "json"
    PLAINTEXT = "plaintext"
    SYSLOG = "syslog"
    APACHE = "apache"
    NGINX = "nginx"
    CUSTOM = "custom"


@dataclass
class LogEntry:
    """A single log entry."""
    id: str
    timestamp: float
    level: LogLevel
    message: str
    source: str
    fields: dict = field(default_factory=dict)
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    service: Optional[str] = None
    host: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "level": self.level.name,
            "message": self.message,
            "source": self.source,
            "fields": self.fields,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "service": self.service,
            "host": self.host,
        }


@dataclass
class LogQuery:
    """Query parameters for log search."""
    query: str = ""
    level: Optional[LogLevel] = None
    levels: Optional[list[LogLevel]] = None
    sources: Optional[list[str]] = None
    services: Optional[list[str]] = None
    time_from: Optional[float] = None
    time_to: Optional[float] = None
    trace_id: Optional[str] = None
    limit: int = 100
    offset: int = 0


@dataclass
class LogAlert:
    """Alert configuration for log patterns."""
    name: str
    query: str
    condition: str
    threshold: int
    window_seconds: int
    severity: str = "warning"
    enabled: bool = True
    recipients: list[str] = field(default_factory=list)


@dataclass
class LogStats:
    """Log statistics."""
    total_count: int
    by_level: dict
    by_source: dict
    by_service: dict
    time_range: tuple[float, float]


class LogParser:
    """Parser for various log formats."""

    @staticmethod
    def parse_json(line: str) -> Optional[dict]:
        """Parse JSON-formatted log line."""
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def parse_plaintext(line: str, pattern: Optional[str] = None) -> dict:
        """Parse plaintext log line with optional regex pattern."""
        if pattern:
            match = re.match(pattern, line)
            if match:
                return match.groupdict()

        default_pattern = r"(?P<timestamp>\S+ \S+ \d+ \d+:\d+:\d+)"
        default_pattern += r" (?P<host>\S+) (?P<service>\S+)"
        default_pattern += r"\[(?P<pid>\d+)\]: (?P<message>.*)"

        match = re.match(default_pattern, line)
        if match:
            return match.groupdict()

        return {"raw": line}

    @staticmethod
    def parse_syslog(line: str) -> dict:
        """Parse syslog-formatted line."""
        pattern = r"<(?P<priority>\d+)>"
        pattern += r"(?P<timestamp>\w+ \d+ \d+:\d+:\d+)"
        pattern += r" (?P<host>\S+) (?P<service>\S+)"
        pattern += r"\[(?P<pid>\d+)\]: (?P<message>.*)"

        match = re.match(pattern, line)
        if match:
            data = match.groupdict()
            priority = int(data["priority"])
            data["level"] = LogLevel(
                (priority % 8) + 30 if (priority % 8) < 7 else 50
            )
            return data

        return {"raw": line}

    @staticmethod
    def parse_apache(line: str) -> dict:
        """Parse Apache access log line."""
        pattern = r'(?P<host>\S+) \S+ \S+'
        pattern += r' \[(?P<timestamp>[^\]]+)\]'
        pattern += r' "(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)"'
        pattern += r' (?P<status>\d+) (?P<size>\S+)'

        match = re.match(pattern, line)
        if match:
            return match.groupdict()
        return {"raw": line}

    @staticmethod
    def parse_nginx(line: str) -> dict:
        """Parse Nginx access log line."""
        return LogParser.parse_apache(line)


class LogAggregator:
    """
    Log aggregation and analysis service.

    Collects logs from multiple sources, provides search and filtering,
    generates statistics, and supports alerting.
    """

    def __init__(self, retention_days: int = 30):
        self.retention_days = retention_days
        self._logs: list[LogEntry] = []
        self._logs_by_source: dict[str, list[LogEntry]] = defaultdict(list)
        self._logs_by_service: dict[str, list[LogEntry]] = defaultdict(list)
        self._alerts: dict[str, LogAlert] = {}
        self._alert_states: dict[str, int] = defaultdict(int)
        self._indexed = False

    def ingest(
        self,
        line: str,
        source: str,
        format: LogFormat = LogFormat.PLAINTEXT,
        timestamp: Optional[float] = None,
        default_level: LogLevel = LogLevel.INFO,
    ) -> Optional[LogEntry]:
        """Ingest a log line from a source."""
        timestamp = timestamp or time.time()

        parsed = None
        level = default_level
        message = line
        fields = {}

        if format == LogFormat.JSON:
            parsed = LogParser.parse_json(line)
            if parsed:
                message = parsed.get("message", parsed.get("msg", line))
                level_str = parsed.get("level", parsed.get("severity", "INFO")).upper()
                try:
                    level = LogLevel[level_str]
                except KeyError:
                    level = default_level
                fields = {k: v for k, v in parsed.items()
                         if k not in ("message", "msg", "level", "severity", "timestamp")}

        elif format == LogFormat.SYSLOG:
            parsed = LogParser.parse_syslog(line)
            message = parsed.get("message", line)
            level = parsed.get("level", default_level)
            fields = {k: v for k, v in parsed.items()
                     if k not in ("message", "level", "timestamp")}

        elif format == LogFormat.APACHE or format == LogFormat.NGINX:
            parsed = LogParser.parse_apache(line) if format == LogFormat.APACHE else LogParser.parse_nginx(line)
            message = line
            fields = parsed
            status = int(parsed.get("status", 200))
            if status >= 500:
                level = LogLevel.ERROR
            elif status >= 400:
                level = LogLevel.WARNING

        else:
            fields = {"raw": line}

        entry = LogEntry(
            id=str(uuid.uuid4()),
            timestamp=timestamp,
            level=level,
            message=message,
            source=source,
            fields=fields,
            trace_id=fields.get("trace_id"),
            span_id=fields.get("span_id"),
            service=fields.get("service"),
            host=fields.get("host"),
        )

        self._logs.append(entry)
        self._logs_by_source[source].append(entry)
        if entry.service:
            self._logs_by_service[entry.service].append(entry)

        self._indexed = False
        return entry

    def ingest_batch(
        self,
        lines: list[str],
        source: str,
        format: LogFormat = LogFormat.PLAINTEXT,
        default_level: LogLevel = LogLevel.INFO,
    ) -> int:
        """Ingest multiple log lines."""
        count = 0
        for line in lines:
            if self.ingest(line, source, format, default_level=default_level):
                count += 1
        return count

    def query(self, q: LogQuery) -> list[LogEntry]:
        """Query logs with filters."""
        results = self._logs

        if q.time_from:
            results = [l for l in results if l.timestamp >= q.time_from]
        if q.time_to:
            results = [l for l in results if l.timestamp <= q.time_to]

        if q.level:
            results = [l for l in results if l.level == q.level]
        elif q.levels:
            results = [l for l in results if l.level in q.levels]

        if q.sources:
            results = [l for l in results if l.source in q.sources]
        if q.services:
            results = [l for l in results if l.service in q.services]

        if q.trace_id:
            results = [l for l in results if l.trace_id == q.trace_id]

        if q.query:
            query_lower = q.query.lower()
            results = [l for l in results if query_lower in l.message.lower()]

        return results[q.offset:q.offset + q.limit]

    def search(
        self,
        query: str,
        level: Optional[LogLevel] = None,
        time_range_seconds: Optional[int] = None,
        limit: int = 100,
    ) -> list[LogEntry]:
        """Quick search across logs."""
        cutoff = time.time() - time_range_seconds if time_range_seconds else None
        q = LogQuery(
            query=query,
            level=level,
            time_from=cutoff,
            limit=limit,
        )
        return self.query(q)

    def get_stats(self, time_range_seconds: Optional[int] = None) -> LogStats:
        """Get log statistics."""
        cutoff = time.time() - time_range_seconds if time_range_seconds else None
        logs = self._logs
        if cutoff:
            logs = [l for l in logs if l.timestamp >= cutoff]

        if not logs:
            return LogStats(
                total_count=0,
                by_level={},
                by_source={},
                by_service={},
                time_range=(time.time(), time.time()),
            )

        by_level = defaultdict(int)
        by_source = defaultdict(int)
        by_service = defaultdict(int)

        for log in logs:
            by_level[log.level.name] += 1
            by_source[log.source] += 1
            if log.service:
                by_service[log.service] += 1

        return LogStats(
            total_count=len(logs),
            by_level=dict(by_level),
            by_source=dict(by_source),
            by_service=dict(by_service),
            time_range=(logs[0].timestamp, logs[-1].timestamp),
        )

    def create_alert(
        self,
        name: str,
        query: str,
        condition: str,
        threshold: int,
        window_seconds: int,
        severity: str = "warning",
        recipients: Optional[list[str]] = None,
    ) -> LogAlert:
        """Create a log-based alert."""
        alert = LogAlert(
            name=name,
            query=query,
            condition=condition,
            threshold=threshold,
            window_seconds=window_seconds,
            severity=severity,
            recipients=recipients or [],
        )
        self._alerts[name] = alert
        return alert

    def evaluate_alerts(self) -> list[tuple[LogAlert, int]]:
        """Evaluate all alerts against current logs."""
        triggered = []
        now = time.time()

        for name, alert in self._alerts.items():
            if not alert.enabled:
                continue

            cutoff = now - alert.window_seconds
            matches = self.search(
                alert.query,
                time_range_seconds=alert.window_seconds,
                limit=alert.threshold + 1,
            )

            count = len(matches)

            should_trigger = False
            if alert.condition == "greater_than" and count > alert.threshold:
                should_trigger = True
            elif alert.condition == "equals" and count == alert.threshold:
                should_trigger = True

            if should_trigger:
                triggered.append((alert, count))

        return triggered

    def cleanup_old_logs(self) -> int:
        """Remove logs older than retention period."""
        cutoff = time.time() - (self.retention_days * 86400)
        old_logs = [l for l in self._logs if l.timestamp < cutoff]
        self._logs = [l for l in self._logs if l.timestamp >= cutoff]

        for source in self._logs_by_source:
            self._logs_by_source[source] = [
                l for l in self._logs_by_source[source] if l.timestamp >= cutoff
            ]

        for service in self._logs_by_service:
            self._logs_by_service[service] = [
                l for l in self._logs_by_service[service] if l.timestamp >= cutoff
            ]

        return len(old_logs)
