"""
Observability and Telemetry Utilities.

Provides utilities for distributed tracing, metrics collection,
log aggregation, and service mesh observability.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class TraceStatus(Enum):
    """Status of a trace span."""
    OK = "ok"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class SpanContext:
    """Context for a trace span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    sampled: bool = True
    baggage: dict[str, str] = field(default_factory=dict)


@dataclass
class Span:
    """A trace span."""
    name: str
    span_type: str
    context: SpanContext
    start_time: datetime
    end_time: Optional[datetime] = None
    status: TraceStatus = TraceStatus.OK
    service_name: str = ""
    operation_name: str = ""
    duration_ms: float = 0.0
    tags: dict[str, Any] = field(default_factory=dict)
    logs: list[dict[str, Any]] = field(default_factory=list)
    references: list[dict[str, str]] = field(default_factory=list)

    def finish(self, status: TraceStatus = TraceStatus.OK) -> None:
        """Finish the span."""
        self.end_time = datetime.now()
        self.status = status
        self.duration_ms = (self.end_time - self.start_time).total_seconds() * 1000


@dataclass
class MetricPoint:
    """A single metric data point."""
    metric_name: str
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"


@dataclass
class LogEntry:
    """A log entry."""
    log_id: str
    timestamp: datetime
    level: str
    service: str
    message: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    attributes: dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None


@dataclass
class Alert:
    """An alert notification."""
    alert_id: str
    name: str
    severity: str
    state: str
    fired_at: datetime
    resolved_at: Optional[datetime] = None
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    query: str = ""


class DistributedTracer:
    """Distributed tracing implementation."""

    def __init__(
        self,
        service_name: str,
        db_path: Optional[Path] = None,
        sample_rate: float = 1.0,
    ) -> None:
        self.service_name = service_name
        self.db_path = db_path or Path("traces.db")
        self.sample_rate = sample_rate
        self._active_spans: dict[str, Span] = {}
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the traces database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS traces (
                trace_id TEXT PRIMARY KEY,
                trace_json TEXT NOT NULL,
                started_at TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS spans (
                span_id TEXT PRIMARY KEY,
                trace_id TEXT NOT NULL,
                span_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_spans_trace
            ON spans(trace_id)
        """)
        conn.commit()
        conn.close()

    def start_span(
        self,
        name: str,
        span_type: str = "operation",
        parent_context: Optional[SpanContext] = None,
        sampled: bool = True,
    ) -> Span:
        """Start a new trace span."""
        if sampled and self.sample_rate < 1.0:
            sampled = (hashlib.md5(str(time.time()).encode()).hexdigest()[0] <= hex(int(self.sample_rate * 16))[2:])

        trace_id = parent_context.trace_id if parent_context else str(uuid.uuid4())
        span_id = str(uuid.uuid4())

        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_context.span_id if parent_context else None,
            sampled=sampled,
        )

        span = Span(
            name=name,
            span_type=span_type,
            context=context,
            start_time=datetime.now(),
            service_name=self.service_name,
            operation_name=name,
        )

        if sampled:
            with self._lock:
                self._active_spans[span_id] = span

        return span

    def finish_span(
        self,
        span: Span,
        status: TraceStatus = TraceStatus.OK,
    ) -> None:
        """Finish a span."""
        span.finish(status)

        with self._lock:
            if span.context.span_id in self._active_spans:
                del self._active_spans[span.context.span_id]

        self._save_span(span)

    def add_span_log(
        self,
        span: Span,
        message: str,
        attributes: Optional[dict[str, Any]] = None,
    ) -> None:
        """Add a log event to a span."""
        span.logs.append({
            "timestamp": datetime.now().isoformat(),
            "message": message,
            "attributes": attributes or {},
        })

    def add_span_tag(
        self,
        span: Span,
        key: str,
        value: Any,
    ) -> None:
        """Add a tag to a span."""
        span.tags[key] = value

    def inject_context(
        self,
        context: SpanContext,
        format: str = "http_headers",
    ) -> dict[str, str]:
        """Inject context for propagation."""
        if format == "http_headers":
            return {
                "X-Trace-Id": context.trace_id,
                "X-Span-Id": context.span_id,
                "X-Sampled": "1" if context.sampled else "0",
            }
        return {}

    def extract_context(
        self,
        carrier: dict[str, str],
        format: str = "http_headers",
    ) -> Optional[SpanContext]:
        """Extract context from propagation headers."""
        if format == "http_headers":
            trace_id = carrier.get("X-Trace-Id")
            span_id = carrier.get("X-Span-Id")

            if trace_id and span_id:
                return SpanContext(
                    trace_id=trace_id,
                    span_id=str(uuid.uuid4()),
                    parent_span_id=span_id,
                    sampled=carrier.get("X-Sampled") == "1",
                )

        return None

    def _save_span(self, span: Span) -> None:
        """Save a span to the database."""
        if not span.context.sampled:
            return

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.execute("""
            INSERT OR IGNORE INTO traces (trace_id, trace_json, started_at)
            VALUES (?, ?, ?)
        """, (
            span.context.trace_id,
            json.dumps({"service_name": self.service_name}),
            span.start_time.isoformat(),
        ))

        cursor.execute("""
            INSERT OR REPLACE INTO spans (span_id, trace_id, span_json, started_at)
            VALUES (?, ?, ?, ?)
        """, (
            span.context.span_id,
            span.context.trace_id,
            json.dumps({
                "name": span.name,
                "span_type": span.span_type,
                "parent_span_id": span.context.parent_span_id,
                "status": span.status.value,
                "service_name": span.service_name,
                "operation_name": span.operation_name,
                "duration_ms": span.duration_ms,
                "tags": span.tags,
                "logs": span.logs,
                "end_time": span.end_time.isoformat() if span.end_time else None,
            }),
            span.start_time.isoformat(),
        ))

        conn.commit()
        conn.close()


class MetricsCollector:
    """Metrics collection and aggregation."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("metrics.db")
        self._metrics: dict[str, list[MetricPoint]] = {}
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the metrics database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                labels_json TEXT,
                metric_type TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_metrics_name_time
            ON metrics(metric_name, timestamp DESC)
        """)
        conn.commit()
        conn.close()

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] = self._counters.get(key, 0) + value

            self._record_metric(name, self._counters[key], labels, "counter")

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric."""
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value

            self._record_metric(name, value, labels, "gauge")

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Observe a value for histogram metric."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._histograms:
                self._histograms[key] = []
            self._histograms[key].append(value)

            self._record_metric(name, value, labels, "histogram")

    def get_counter(self, name: str, labels: Optional[dict[str, str]] = None) -> float:
        """Get current counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Optional[dict[str, str]] = None) -> float:
        """Get current gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key, 0.0)

    def get_histogram_stats(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> dict[str, float]:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        values = self._histograms.get(key, [])

        if not values:
            return {"count": 0, "sum": 0, "min": 0, "max": 0, "avg": 0}

        sorted_values = sorted(values)
        count = len(sorted_values)

        return {
            "count": count,
            "sum": sum(sorted_values),
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "avg": sum(sorted_values) / count,
            "p50": sorted_values[count // 2],
            "p90": sorted_values[int(count * 0.9)],
            "p99": sorted_values[int(count * 0.99)],
        }

    def query_metrics(
        self,
        name: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        labels: Optional[dict[str, str]] = None,
        limit: int = 1000,
    ) -> list[MetricPoint]:
        """Query historical metric data."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = "SELECT * FROM metrics WHERE metric_name = ?"
        params: list[Any] = [name]

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [
            MetricPoint(
                metric_name=row["metric_name"],
                value=row["value"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                labels=json.loads(row["labels_json"]) if row["labels_json"] else {},
                metric_type=row["metric_type"] or "gauge",
            )
            for row in rows
        ]

    def _record_metric(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]],
        metric_type: str,
    ) -> None:
        """Record a metric point."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metrics (metric_name, value, timestamp, labels_json, metric_type)
            VALUES (?, ?, ?, ?, ?)
        """, (
            name,
            value,
            datetime.now().isoformat(),
            json.dumps(labels) if labels else None,
            metric_type,
        ))
        conn.commit()
        conn.close()

    def _make_key(self, name: str, labels: Optional[dict[str, str]]) -> str:
        """Create a unique key for a metric with labels."""
        if not labels:
            return name
        return f"{name}:{json.dumps(labels, sort_keys=True)}"


class LogAggregator:
    """Log aggregation and querying."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("logs.db")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the logs database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                log_id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                service TEXT NOT NULL,
                message TEXT NOT NULL,
                trace_id TEXT,
                span_id TEXT,
                attributes_json TEXT,
                stack_trace TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_logs_service_time
            ON logs(service, timestamp DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_logs_trace
            ON logs(trace_id)
        """)
        conn.commit()
        conn.close()

    def log(
        self,
        level: str,
        service: str,
        message: str,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
        attributes: Optional[dict[str, Any]] = None,
        stack_trace: Optional[str] = None,
    ) -> LogEntry:
        """Log an entry."""
        entry = LogEntry(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            level=level,
            service=service,
            message=message,
            trace_id=trace_id,
            span_id=span_id,
            attributes=attributes or {},
            stack_trace=stack_trace,
        )

        self._save_log(entry)
        return entry

    def query_logs(
        self,
        service: Optional[str] = None,
        level: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        trace_id: Optional[str] = None,
        search_text: Optional[str] = None,
        limit: int = 100,
    ) -> list[LogEntry]:
        """Query logs with filters."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        query = "SELECT * FROM logs WHERE 1=1"
        params: list[Any] = []

        if service:
            query += " AND service = ?"
            params.append(service)

        if level:
            query += " AND level = ?"
            params.append(level)

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        if trace_id:
            query += " AND trace_id = ?"
            params.append(trace_id)

        if search_text:
            query += " AND message LIKE ?"
            params.append(f"%{search_text}%")

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_log(row) for row in rows]

    def _save_log(self, entry: LogEntry) -> None:
        """Save a log entry to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO logs (
                log_id, timestamp, level, service, message,
                trace_id, span_id, attributes_json, stack_trace
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            entry.log_id,
            entry.timestamp.isoformat(),
            entry.level,
            entry.service,
            entry.message,
            entry.trace_id,
            entry.span_id,
            json.dumps(entry.attributes),
            entry.stack_trace,
        ))
        conn.commit()
        conn.close()

    def _row_to_log(self, row: sqlite3.Row) -> LogEntry:
        """Convert a database row to a LogEntry."""
        return LogEntry(
            log_id=row["log_id"],
            timestamp=datetime.fromisoformat(row["timestamp"]),
            level=row["level"],
            service=row["service"],
            message=row["message"],
            trace_id=row["trace_id"],
            span_id=row["span_id"],
            attributes=json.loads(row["attributes_json"]) if row["attributes_json"] else {},
            stack_trace=row["stack_trace"],
        )


class AlertManager:
    """Alerting and notification management."""

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self.db_path = db_path or Path("alerts.db")
        self._alert_handlers: list[Callable[[Alert], None]] = []
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the alerts database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                severity TEXT NOT NULL,
                state TEXT NOT NULL,
                fired_at TEXT NOT NULL,
                resolved_at TEXT,
                labels_json TEXT,
                annotations_json TEXT,
                query TEXT
            )
        """)
        conn.commit()
        conn.close()

    def fire_alert(
        self,
        name: str,
        severity: str,
        labels: Optional[dict[str, str]] = None,
        annotations: Optional[dict[str, str]] = None,
        query: str = "",
    ) -> Alert:
        """Fire a new alert."""
        alert = Alert(
            alert_id=f"alert_{int(time.time())}_{hashlib.md5(name.encode()).hexdigest()[:8]}",
            name=name,
            severity=severity,
            state="firing",
            fired_at=datetime.now(),
            labels=labels or {},
            annotations=annotations or {},
            query=query,
        )

        self._save_alert(alert)

        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception:
                pass

        return alert

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve a firing alert."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE alerts SET state = ?, resolved_at = ?
            WHERE alert_id = ?
        """, ("resolved", datetime.now().isoformat(), alert_id))
        conn.commit()
        conn.close()

        return cursor.rowcount > 0

    def get_active_alerts(
        self,
        severity: Optional[str] = None,
    ) -> list[Alert]:
        """Get all firing alerts."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if severity:
            cursor.execute("""
                SELECT * FROM alerts WHERE state = 'firing' AND severity = ?
                ORDER BY fired_at DESC
            """, (severity,))
        else:
            cursor.execute("""
                SELECT * FROM alerts WHERE state = 'firing'
                ORDER BY fired_at DESC
            """)

        rows = cursor.fetchall()
        conn.close()

        return [self._row_to_alert(row) for row in rows]

    def register_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register an alert notification handler."""
        self._alert_handlers.append(handler)

    def _save_alert(self, alert: Alert) -> None:
        """Save an alert to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO alerts (
                alert_id, name, severity, state, fired_at,
                resolved_at, labels_json, annotations_json, query
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            alert.alert_id,
            alert.name,
            alert.severity,
            alert.state,
            alert.fired_at.isoformat(),
            alert.resolved_at.isoformat() if alert.resolved_at else None,
            json.dumps(alert.labels),
            json.dumps(alert.annotations),
            alert.query,
        ))
        conn.commit()
        conn.close()

    def _row_to_alert(self, row: sqlite3.Row) -> Alert:
        """Convert a database row to an Alert."""
        return Alert(
            alert_id=row["alert_id"],
            name=row["name"],
            severity=row["severity"],
            state=row["state"],
            fired_at=datetime.fromisoformat(row["fired_at"]),
            resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
            labels=json.loads(row["labels_json"]) if row["labels_json"] else {},
            annotations=json.loads(row["annotations_json"]) if row["annotations_json"] else {},
            query=row["query"] or "",
        )
