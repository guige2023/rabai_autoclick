"""
Real-time Workflow Monitoring and Alerting System v22

A comprehensive monitoring solution providing:
- Real-time metrics tracking for workflow executions
- Resource monitoring (CPU, memory, disk, network)
- Configurable alert rules with thresholds
- Multi-channel alert delivery (Email, SMS, Slack, webhooks)
- Intelligent alert grouping to reduce noise
- Automatic alert escalation for unacknowledged alerts
- On-call rotation schedule management
- Incident creation and tracking
- SLA compliance monitoring
- HTTP health check endpoints
"""

import json
import time
import threading
import smtplib
import logging
import hashlib
import re
import sqlite3
import psutil
import socket
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import copy

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class MetricType(Enum):
    """Types of metrics tracked in real-time."""
    EXECUTION_COUNT = "execution_count"
    EXECUTION_SUCCESS = "execution_success"
    EXECUTION_FAILURE = "execution_failure"
    EXECUTION_DURATION = "execution_duration"
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    DISK_USAGE = "disk_usage"
    NETWORK_IO = "network_io"
    GAUGE = "gauge"
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    CUSTOM = "custom"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertState(Enum):
    """Alert state lifecycle."""
    PENDING = "pending"
    FIRING = "firing"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    ESCALATED = "escalated"


class IncidentStatus(Enum):
    """Incident status values."""
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    INVESTIGATING = "investigating"
    MITIGATED = "mitigated"
    RESOLVED = "resolved"
    CLOSED = "closed"


class OnCallScheduleType(Enum):
    """On-call schedule types."""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    ESCALATION = "escalation"


class MonitoringStatus(Enum):
    """General monitoring status values."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"
    STOPPED = "stopped"


class HealthCheckStatus(Enum):
    """Health check status values."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class SLAStatus(Enum):
    """SLA compliance status."""
    MET = "met"
    AT_RISK = "at_risk"
    BREACHED = "breached"


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    metric_type: MetricType
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class MetricWindow:
    """Time window for metric aggregation."""
    metric_type: MetricType
    window_size_seconds: int
    points: List[MetricPoint] = field(default_factory=list)
    aggregation_fn: str = "avg"  # avg, sum, min, max, count


@dataclass
class ResourceMetrics:
    """Current resource usage snapshot."""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    disk_percent: float
    disk_free_gb: float
    network_sent_mb: float
    network_recv_mb: float
    thread_count: int
    process_count: int


@dataclass
class AlertRule:
    """Defines an alert condition and threshold."""
    id: str
    name: str
    metric_type: MetricType
    condition: str  # gt, lt, eq, gte, lte, between
    threshold: float
    threshold_high: Optional[float] = None  # For 'between' conditions
    severity: AlertSeverity = AlertSeverity.WARNING
    enabled: bool = True
    description: str = ""
    cooldown_seconds: int = 300
    last_triggered: Optional[float] = None


@dataclass
class Alert:
    """Represents a triggered alert."""
    id: str
    rule_id: str
    rule_name: str
    severity: AlertSeverity
    state: AlertState
    message: str
    metric_value: float
    threshold: float
    timestamp: float
    acknowledged_at: Optional[float] = None
    acknowledged_by: Optional[str] = None
    resolved_at: Optional[float] = None
    escalated_at: Optional[float] = None
    labels: Dict[str, str] = field(default_factory=dict)
    group_key: Optional[str] = None
    incident_id: Optional[str] = None


@dataclass
class AlertGroup:
    """Groups similar alerts to reduce noise."""
    group_key: str
    alert_count: int
    first_alert_time: float
    last_alert_time: float
    representative_alert: Alert
    alerts: List[Alert] = field(default_factory=list)
    suppressed: bool = False


@dataclass
class MonitoringAlert:
    """Represents a monitoring alert."""
    alert_id: str
    metric_name: str
    status: MonitoringStatus
    message: str
    current_value: float
    threshold: float
    timestamp: float
    resolved: bool = False


@dataclass
class MetricValue:
    """A metric value with metadata."""
    name: str
    value: float
    timestamp: float
    metric_type: MetricType
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = ""


@dataclass
class EscalationPolicy:
    """Defines escalation levels and timing."""
    id: str
    name: str
    levels: List[Dict[str, Any]] = field(default_factory=list)
    # Each level: {"delay_minutes": int, "recipients": List[str], "channel": str}


@dataclass
class OnCallRecipient:
    """Person on-call."""
    user_id: str
    name: str
    email: str
    phone: Optional[str] = None
    slack_id: Optional[str] = None


@dataclass
class OnCallSchedule:
    """On-call schedule entry."""
    id: str
    schedule_type: OnCallScheduleType
    recipient: OnCallRecipient
    start_time: float
    end_time: float
    timezone: str = "UTC"


@dataclass
class Incident:
    """Represents an incident."""
    id: str
    title: str
    description: str
    status: IncidentStatus
    severity: AlertSeverity
    created_at: float
    acknowledged_at: Optional[float] = None
    mitigated_at: Optional[float] = None
    resolved_at: Optional[float] = None
    created_by: str = "system"
    assignees: List[str] = field(default_factory=list)
    alerts: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    sla_deadline: Optional[float] = None
    notes: List[str] = field(default_factory=list)


@dataclass
class SLADefinition:
    """SLA configuration for incident response."""
    id: str
    name: str
    response_time_minutes: int
    resolution_time_minutes: int
    severity: AlertSeverity
    enabled: bool = True


@dataclass
class SLACompliance:
    """SLA compliance record."""
    sla_id: str
    sla_name: str
    incident_id: str
    response_time_minutes: float
    resolution_time_minutes: float
    response_met: bool
    resolution_met: bool
    created_at: float
    response_at: Optional[float] = None
    resolved_at: Optional[float] = None


@dataclass
class HealthCheck:
    """HTTP health check configuration."""
    name: str
    url: str
    method: str = "GET"
    expected_status: int = 200
    timeout_seconds: int = 10
    interval_seconds: int = 60
    enabled: bool = True


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    name: str
    status: HealthCheckStatus
    response_time_ms: float
    timestamp: float
    status_code: Optional[int] = None
    error_message: Optional[str] = None


# =============================================================================
# WorkflowMonitoring Class
# =============================================================================

class WorkflowMonitoring:
    """
    Real-time monitoring and alerting system for workflows.
    
    Features:
    - Real-time metrics tracking
    - Resource monitoring (CPU, memory, disk, network)
    - Configurable alert rules
    - Multi-channel alert delivery
    - Alert grouping and suppression
    - Automatic escalation
    - On-call rotation management
    - Incident management
    - SLA tracking
    - HTTP health endpoints
    """
    
    def __init__(
        self,
        db_path: str = "~/.rabai/monitoring.db",
        config: Optional[Dict[str, Any]] = None
    ):
        self.db_path = os.path.expanduser(db_path)
        self.config = config or {}
        
        # Metrics storage
        self._metrics: Dict[MetricType, deque] = defaultdict(
            lambda: deque(maxlen=10000)
        )
        self._resource_history: deque = deque(maxlen=1000)
        
        # Alerting
        self._alert_rules: Dict[str, AlertRule] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_groups: Dict[str, AlertGroup] = {}
        self._escalation_policies: Dict[str, EscalationPolicy] = {}
        
        # On-call
        self._oncall_schedules: List[OnCallSchedule] = []
        self._oncall_recipients: Dict[str, OnCallRecipient] = {}
        
        # Incidents
        self._incidents: Dict[str, Incident] = {}
        
        # SLAs
        self._sla_definitions: Dict[str, SLADefinition] = {}
        self._sla_compliance: Dict[str, List[SLACompliance]] = defaultdict(list)
        
        # Health checks
        self._health_checks: Dict[str, HealthCheck] = {}
        self._health_check_results: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=100)
        )
        
        # Alert channels configuration
        self._channel_configs: Dict[str, Dict[str, Any]] = {
            "email": {
                "enabled": False,
                "smtp_host": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "",
                "password": "",
                "from_address": "",
                "use_tls": True
            },
            "slack": {
                "enabled": False,
                "webhook_url": "",
                "channel": "#alerts"
            },
            "webhook": {
                "enabled": False,
                "url": "",
                "headers": {}
            },
            "sms": {
                "enabled": False,
                "provider": "twilio",
                "account_sid": "",
                "auth_token": "",
                "from_number": "",
                "to_numbers": []
            }
        }
        
        # Threading
        self._lock = threading.RLock()
        self._monitoring_thread: Optional[threading.Thread] = None
        self._running = False
        
        # Metrics callbacks
        self._metric_callbacks: List[Callable[[MetricPoint], None]] = []
        
        # Initialize database
        self._init_database()
        
        # Load default configurations
        self._load_default_config()
    
    def _init_database(self):
        """Initialize the monitoring database."""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_type TEXT NOT NULL,
                value REAL NOT NULL,
                labels TEXT,
                timestamp REAL NOT NULL
            )
        """)
        
        # Alert rules table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_rules (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                metric_type TEXT NOT NULL,
                condition TEXT NOT NULL,
                threshold REAL NOT NULL,
                threshold_high REAL,
                severity TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                description TEXT,
                cooldown_seconds INTEGER NOT NULL,
                last_triggered REAL
            )
        """)
        
        # Alerts table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                rule_id TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                severity TEXT NOT NULL,
                state TEXT NOT NULL,
                message TEXT NOT NULL,
                metric_value REAL NOT NULL,
                threshold REAL NOT NULL,
                timestamp REAL NOT NULL,
                acknowledged_at REAL,
                acknowledged_by TEXT,
                resolved_at REAL,
                escalated_at REAL,
                labels TEXT,
                group_key TEXT,
                incident_id TEXT
            )
        """)
        
        # Incidents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL,
                severity TEXT NOT NULL,
                created_at REAL NOT NULL,
                acknowledged_at REAL,
                mitigated_at REAL,
                resolved_at REAL,
                created_by TEXT,
                assignees TEXT,
                alerts TEXT,
                labels TEXT,
                sla_deadline REAL,
                notes TEXT
            )
        """)
        
        # SLA definitions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sla_definitions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                response_time_minutes INTEGER NOT NULL,
                resolution_time_minutes INTEGER NOT NULL,
                severity TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )
        """)
        
        # On-call schedules table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS oncall_schedules (
                id TEXT PRIMARY KEY,
                schedule_type TEXT NOT NULL,
                user_id TEXT NOT NULL,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT,
                slack_id TEXT,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                timezone TEXT NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _load_default_config(self):
        """Load default alert rules and configurations."""
        # Default CPU alert rule
        self.add_alert_rule(AlertRule(
            id="cpu_high",
            name="High CPU Usage",
            metric_type=MetricType.CPU_USAGE,
            condition="gt",
            threshold=80.0,
            severity=AlertSeverity.WARNING,
            description="CPU usage exceeded 80%"
        ))
        
        # Default memory alert rule
        self.add_alert_rule(AlertRule(
            id="memory_high",
            name="High Memory Usage",
            metric_type=MetricType.MEMORY_USAGE,
            condition="gt",
            threshold=85.0,
            severity=AlertSeverity.ERROR,
            description="Memory usage exceeded 85%"
        ))
        
        # Default disk alert rule
        self.add_alert_rule(AlertRule(
            id="disk_high",
            name="High Disk Usage",
            metric_type=MetricType.DISK_USAGE,
            condition="gt",
            threshold=90.0,
            severity=AlertSeverity.CRITICAL,
            description="Disk usage exceeded 90%"
        ))
        
        # Default SLA definitions
        self.add_sla_definition(SLADefinition(
            id="sla_critical",
            name="Critical Incident SLA",
            response_time_minutes=15,
            resolution_time_minutes=60,
            severity=AlertSeverity.CRITICAL
        ))
        
        self.add_sla_definition(SLADefinition(
            id="sla_error",
            name="Error Incident SLA",
            response_time_minutes=30,
            resolution_time_minutes=240,
            severity=AlertSeverity.ERROR
        ))
        
        self.add_sla_definition(SLADefinition(
            id="sla_warning",
            name="Warning Incident SLA",
            response_time_minutes=120,
            resolution_time_minutes=480,
            severity=AlertSeverity.WARNING
        ))
    
    # -------------------------------------------------------------------------
    # Real-time Metrics
    # -------------------------------------------------------------------------
    
    def record_metric(
        self,
        metric_type: MetricType,
        value: float,
        labels: Optional[Dict[str, str]] = None,
        timestamp: Optional[float] = None
    ) -> MetricPoint:
        """Record a metric data point."""
        point = MetricPoint(
            timestamp=timestamp or time.time(),
            metric_type=metric_type,
            value=value,
            labels=labels or {}
        )
        
        with self._lock:
            self._metrics[metric_type].append(point)
        
        # Persist to database
        self._persist_metric(point)
        
        # Notify callbacks
        for callback in self._metric_callbacks:
            try:
                callback(point)
            except Exception as e:
                logger.error(f"Metric callback error: {e}")
        
        # Check alert rules
        self._evaluate_alert_rules(point)
        
        return point
    
    def get_metrics(
        self,
        metric_type: MetricType,
        since: Optional[float] = None,
        until: Optional[float] = None,
        labels_filter: Optional[Dict[str, str]] = None
    ) -> List[MetricPoint]:
        """Get metrics of a specific type within time range."""
        with self._lock:
            points = list(self._metrics.get(metric_type, []))
        
        # Filter by time
        if since:
            points = [p for p in points if p.timestamp >= since]
        if until:
            points = [p for p in points if p.timestamp <= until]
        
        # Filter by labels
        if labels_filter:
            points = [
                p for p in points
                if all(p.labels.get(k) == v for k, v in labels_filter.items())
            ]
        
        return points
    
    def get_aggregated_metrics(
        self,
        metric_type: MetricType,
        window_seconds: int,
        aggregation: str = "avg",
        since: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """Get aggregated metrics over time windows."""
        points = self.get_metrics(metric_type, since=since)
        
        if not points:
            return []
        
        # Group by time window
        windows: Dict[int, List[float]] = defaultdict(list)
        for point in points:
            window_key = int(point.timestamp // window_seconds)
            windows[window_key].append(point.value)
        
        # Aggregate
        result = []
        for window_key in sorted(windows.keys()):
            values = windows[window_key]
            timestamp = window_key * window_seconds
            
            if aggregation == "avg":
                value = statistics.mean(values)
            elif aggregation == "sum":
                value = sum(values)
            elif aggregation == "min":
                value = min(values)
            elif aggregation == "max":
                value = max(values)
            elif aggregation == "count":
                value = len(values)
            else:
                value = statistics.mean(values)
            
            result.append({
                "timestamp": timestamp,
                "value": value,
                "count": len(values)
            })
        
        return result
    
    def register_metric_callback(
        self,
        callback: Callable[[MetricPoint], None]
    ):
        """Register a callback for metric updates."""
        self._metric_callbacks.append(callback)
    
    def _persist_metric(self, point: MetricPoint):
        """Persist metric to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO metrics (metric_type, value, labels, timestamp)
                VALUES (?, ?, ?, ?)
            """, (
                point.metric_type.value,
                point.value,
                json.dumps(point.labels),
                point.timestamp
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist metric: {e}")
    
    # -------------------------------------------------------------------------
    # Resource Monitoring
    # -------------------------------------------------------------------------
    
    def get_current_resources(self) -> ResourceMetrics:
        """Get current system resource usage."""
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            net_io = psutil.net_io_counters()
            process = psutil.Process()
            
            return ResourceMetrics(
                timestamp=time.time(),
                cpu_percent=cpu,
                memory_percent=memory.percent,
                memory_mb=memory.used / (1024 * 1024),
                disk_percent=disk.percent,
                disk_free_gb=disk.free / (1024 * 1024 * 1024),
                network_sent_mb=net_io.bytes_sent / (1024 * 1024),
                network_recv_mb=net_io.bytes_recv / (1024 * 1024),
                thread_count=threading.active_count(),
                process_count=len(process.children(recursive=True)) + 1
            )
        except Exception as e:
            logger.error(f"Failed to get resource metrics: {e}")
            return ResourceMetrics(
                timestamp=time.time(),
                cpu_percent=0,
                memory_percent=0,
                memory_mb=0,
                disk_percent=0,
                disk_free_gb=0,
                network_sent_mb=0,
                network_recv_mb=0,
                thread_count=0,
                process_count=0
            )
    
    def record_resource_metrics(self):
        """Record current resource metrics."""
        resources = self.get_current_resources()
        self._resource_history.append(resources)
        
        # Record individual metrics
        self.record_metric(MetricType.CPU_USAGE, resources.cpu_percent)
        self.record_metric(MetricType.MEMORY_USAGE, resources.memory_percent)
        self.record_metric(MetricType.DISK_USAGE, resources.disk_percent)
        self.record_metric(
            MetricType.NETWORK_IO,
            resources.network_sent_mb + resources.network_recv_mb
        )
        
        return resources
    
    def get_resource_history(
        self,
        since: Optional[float] = None,
        until: Optional[float] = None
    ) -> List[ResourceMetrics]:
        """Get historical resource metrics."""
        history = list(self._resource_history)
        
        if since:
            history = [r for r in history if r.timestamp >= since]
        if until:
            history = [r for r in history if r.timestamp <= until]
        
        return history
    
    def start_resource_monitoring(self, interval_seconds: int = 10):
        """Start background resource monitoring."""
        if self._running:
            return
        
        self._running = True
        
        def monitor():
            while self._running:
                try:
                    self.record_resource_metrics()
                except Exception as e:
                    logger.error(f"Resource monitoring error: {e}")
                time.sleep(interval_seconds)
        
        self._monitoring_thread = threading.Thread(target=monitor, daemon=True)
        self._monitoring_thread.start()
        logger.info("Resource monitoring started")
    
    def stop_resource_monitoring(self):
        """Stop background resource monitoring."""
        self._running = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        logger.info("Resource monitoring stopped")
    
    # -------------------------------------------------------------------------
    # Alert Rules
    # -------------------------------------------------------------------------
    
    def add_alert_rule(self, rule: AlertRule) -> AlertRule:
        """Add or update an alert rule."""
        with self._lock:
            self._alert_rules[rule.id] = rule
        self._persist_alert_rule(rule)
        return rule
    
    def get_alert_rule(self, rule_id: str) -> Optional[AlertRule]:
        """Get an alert rule by ID."""
        return self._alert_rules.get(rule_id)
    
    def get_all_alert_rules(self) -> List[AlertRule]:
        """Get all alert rules."""
        return list(self._alert_rules.values())
    
    def delete_alert_rule(self, rule_id: str) -> bool:
        """Delete an alert rule."""
        with self._lock:
            if rule_id in self._alert_rules:
                del self._alert_rules[rule_id]
                self._delete_alert_rule_db(rule_id)
                return True
        return False
    
    def enable_alert_rule(self, rule_id: str, enabled: bool = True):
        """Enable or disable an alert rule."""
        rule = self._alert_rules.get(rule_id)
        if rule:
            rule.enabled = enabled
            self._persist_alert_rule(rule)
    
    def _persist_alert_rule(self, rule: AlertRule):
        """Persist alert rule to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO alert_rules 
                (id, name, metric_type, condition, threshold, threshold_high,
                 severity, enabled, description, cooldown_seconds, last_triggered)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule.id, rule.name, rule.metric_type.value, rule.condition,
                rule.threshold, rule.threshold_high, rule.severity.value,
                rule.enabled, rule.description, rule.cooldown_seconds,
                rule.last_triggered
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist alert rule: {e}")
    
    def _delete_alert_rule_db(self, rule_id: str):
        """Delete alert rule from database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to delete alert rule: {e}")
    
    def _evaluate_alert_rules(self, point: MetricPoint):
        """Evaluate all applicable alert rules for a metric point."""
        with self._lock:
            applicable_rules = [
                r for r in self._alert_rules.values()
                if r.enabled and r.metric_type == point.metric_type
            ]
        
        for rule in applicable_rules:
            if self._should_trigger_alert(rule, point):
                self._trigger_alert(rule, point)
    
    def _should_trigger_alert(self, rule: AlertRule, point: MetricPoint) -> bool:
        """Check if an alert should be triggered."""
        # Check cooldown
        if rule.last_triggered:
            if time.time() - rule.last_triggered < rule.cooldown_seconds:
                return False
        
        # Check condition
        value = point.value
        
        if rule.condition == "gt":
            return value > rule.threshold
        elif rule.condition == "lt":
            return value < rule.threshold
        elif rule.condition == "gte":
            return value >= rule.threshold
        elif rule.condition == "lte":
            return value <= rule.threshold
        elif rule.condition == "eq":
            return abs(value - rule.threshold) < 0.001
        elif rule.condition == "between":
            if rule.threshold_high is None:
                return False
            return rule.threshold <= value <= rule.threshold_high
        
        return False
    
    def _trigger_alert(self, rule: AlertRule, point: MetricPoint):
        """Trigger an alert."""
        rule.last_triggered = time.time()
        
        # Generate group key
        group_key = self._generate_alert_group_key(rule, point)
        
        alert = Alert(
            id=f"alert_{int(time.time() * 1000)}",
            rule_id=rule.id,
            rule_name=rule.name,
            severity=rule.severity,
            state=AlertState.FIRING,
            message=f"{rule.name}: {point.value:.2f} (threshold: {rule.threshold})",
            metric_value=point.value,
            threshold=rule.threshold,
            timestamp=point.timestamp,
            labels=point.labels,
            group_key=group_key
        )
        
        # Check if this alert should be grouped
        if group_key in self._alert_groups:
            self._add_to_alert_group(group_key, alert)
            return
        
        with self._lock:
            self._active_alerts[alert.id] = alert
        
        # Group the alert
        self._group_alert(alert)
        
        # Persist and send notifications
        self._persist_alert(alert)
        self._send_alert_notifications(alert)
        
        logger.info(f"Alert triggered: {alert.rule_name} ({alert.severity.value})")
    
    def _generate_alert_group_key(self, rule: AlertRule, point: MetricPoint) -> str:
        """Generate a key for grouping similar alerts."""
        labels_str = json.dumps(point.labels, sort_keys=True)
        key_base = f"{rule.id}:{labels_str}"
        return hashlib.md5(key_base.encode()).hexdigest()[:12]
    
    def _group_alert(self, alert: Alert):
        """Group an alert with similar alerts."""
        group_key = alert.group_key
        if not group_key:
            return
        
        if group_key in self._alert_groups:
            return
        
        group = AlertGroup(
            group_key=group_key,
            alert_count=1,
            first_alert_time=alert.timestamp,
            last_alert_time=alert.timestamp,
            representative_alert=alert,
            alerts=[alert]
        )
        
        self._alert_groups[group_key] = group
    
    def _add_to_alert_group(self, group_key: str, alert: Alert):
        """Add an alert to an existing group."""
        group = self._alert_groups.get(group_key)
        if not group:
            return
        
        group.alert_count += 1
        group.last_alert_time = alert.timestamp
        group.alerts.append(alert)
        
        # Keep the most severe alert as representative
        if alert.severity.value > group.representative_alert.severity.value:
            group.representative_alert = alert
        
        # Suppress if too many alerts in short time
        time_window = group.last_alert_time - group.first_alert_time
        if group.alert_count > 10 and time_window < 300:
            group.suppressed = True
            logger.info(f"Alert group {group_key} suppressed due to high volume")
    
    # -------------------------------------------------------------------------
    # Alert Management
    # -------------------------------------------------------------------------
    
    def get_active_alerts(
        self,
        state: Optional[AlertState] = None,
        severity: Optional[AlertSeverity] = None
    ) -> List[Alert]:
        """Get active alerts, optionally filtered."""
        alerts = list(self._active_alerts.values())
        
        if state:
            alerts = [a for a in alerts if a.state == state]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        
        return alerts
    
    def acknowledge_alert(
        self,
        alert_id: str,
        user_id: str
    ) -> Optional[Alert]:
        """Acknowledge an alert."""
        alert = self._active_alerts.get(alert_id)
        if not alert:
            return None
        
        alert.state = AlertState.ACKNOWLEDGED
        alert.acknowledged_at = time.time()
        alert.acknowledged_by = user_id
        
        self._persist_alert(alert)
        
        # Create incident if needed
        if alert.severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
            self._create_incident_from_alert(alert)
        
        return alert
    
    def resolve_alert(self, alert_id: str) -> Optional[Alert]:
        """Resolve an alert."""
        alert = self._active_alerts.get(alert_id)
        if not alert:
            return None
        
        alert.state = AlertState.RESOLVED
        alert.resolved_at = time.time()
        
        self._persist_alert(alert)
        
        # Update incident if linked
        if alert.incident_id:
            self._check_incident_resolution(alert.incident_id)
        
        return alert
    
    def get_alert_groups(
        self,
        suppressed: Optional[bool] = None
    ) -> List[AlertGroup]:
        """Get alert groups."""
        groups = list(self._alert_groups.values())
        
        if suppressed is not None:
            groups = [g for g in groups if g.suppressed == suppressed]
        
        return groups
    
    def _persist_alert(self, alert: Alert):
        """Persist alert to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO alerts 
                (id, rule_id, rule_name, severity, state, message, metric_value,
                 threshold, timestamp, acknowledged_at, acknowledged_by, resolved_at,
                 escalated_at, labels, group_key, incident_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert.id, alert.rule_id, alert.rule_name, alert.severity.value,
                alert.state.value, alert.message, alert.metric_value,
                alert.threshold, alert.timestamp, alert.acknowledged_at,
                alert.acknowledged_by, alert.resolved_at, alert.escalated_at,
                json.dumps(alert.labels), alert.group_key, alert.incident_id
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist alert: {e}")
    
    # -------------------------------------------------------------------------
    # Alert Channels
    # -------------------------------------------------------------------------
    
    def configure_channel(
        self,
        channel: str,
        config: Dict[str, Any]
    ):
        """Configure an alert channel."""
        if channel in self._channel_configs:
            self._channel_configs[channel].update(config)
            self._channel_configs[channel]["enabled"] = True
    
    def disable_channel(self, channel: str):
        """Disable an alert channel."""
        if channel in self._channel_configs:
            self._channel_configs[channel]["enabled"] = False
    
    def _send_alert_notifications(self, alert: Alert):
        """Send alert notifications to all enabled channels."""
        # Skip if alert is grouped and suppressed
        if alert.group_key:
            group = self._alert_groups.get(alert.group_key)
            if group and group.suppressed:
                return
        
        channels = [
            ("email", self._send_email_alert),
            ("slack", self._send_slack_alert),
            ("webhook", self._send_webhook_alert),
            ("sms", self._send_sms_alert)
        ]
        
        for channel_name, sender in channels:
            config = self._channel_configs.get(channel_name, {})
            if config.get("enabled"):
                try:
                    sender(alert)
                except Exception as e:
                    logger.error(f"Failed to send {channel_name} alert: {e}")
    
    def _send_email_alert(self, alert: Alert):
        """Send alert via email."""
        config = self._channel_configs["email"]
        
        if not config.get("username") or not config.get("from_address"):
            return
        
        msg = MIMEMultipart()
        msg['From'] = config["from_address"]
        msg['To'] = config.get("to_addresses", config["username"])
        msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.rule_name}"
        
        body = f"""
Alert: {alert.rule_name}
Severity: {alert.severity.value.upper()}
Time: {datetime.fromtimestamp(alert.timestamp).isoformat()}
Message: {alert.message}

This is an automated alert from RAbAI AutoClick Monitoring System.
        """
        msg.attach(MIMEText(body, 'plain'))
        
        try:
            server = smtplib.SMTP(config["smtp_host"], config["smtp_port"])
            if config.get("use_tls"):
                server.starttls()
            server.login(config["username"], config["password"])
            server.send_message(msg)
            server.quit()
            logger.info(f"Email alert sent for {alert.id}")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
    
    def _send_slack_alert(self, alert: Alert):
        """Send alert via Slack webhook."""
        config = self._channel_configs["slack"]
        
        if not config.get("webhook_url"):
            return
        
        severity_emoji = {
            AlertSeverity.INFO: ":information_source:",
            AlertSeverity.WARNING: ":warning:",
            AlertSeverity.ERROR: ":rotating_light:",
            AlertSeverity.CRITICAL: ":fire:"
        }
        
        payload = {
            "channel": config.get("channel", "#alerts"),
            "username": "RABAI Alert Bot",
            "icon_emoji": severity_emoji.get(alert.severity, ":bell:"),
            "attachments": [{
                "title": alert.rule_name,
                "color": {
                    AlertSeverity.INFO: "#00ff00",
                    AlertSeverity.WARNING: "#ffff00",
                    AlertSeverity.ERROR: "#ff0000",
                    AlertSeverity.CRITICAL: "#ff0000"
                }.get(alert.severity, "#cccccc"),
                "fields": [
                    {"title": "Severity", "value": alert.severity.value.upper(), "short": True},
                    {"title": "Status", "value": alert.state.value.upper(), "short": True},
                    {"title": "Value", "value": f"{alert.metric_value:.2f}", "short": True},
                    {"title": "Threshold", "value": f"{alert.threshold:.2f}", "short": True}
                ],
                "text": alert.message,
                "footer": "RABAI Monitoring",
                "ts": alert.timestamp
            }]
        }
        
        try:
            req = Request(
                config["webhook_url"],
                data=json.dumps(payload).encode('utf-8'),
                headers={"Content-Type": "application/json"}
            )
            urlopen(req, timeout=10)
            logger.info(f"Slack alert sent for {alert.id}")
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
    
    def _send_webhook_alert(self, alert: Alert):
        """Send alert via webhook."""
        config = self._channel_configs["webhook"]
        
        if not config.get("url"):
            return
        
        payload = {
            "alert_id": alert.id,
            "rule_id": alert.rule_id,
            "rule_name": alert.rule_name,
            "severity": alert.severity.value,
            "state": alert.state.value,
            "message": alert.message,
            "metric_value": alert.metric_value,
            "threshold": alert.threshold,
            "timestamp": alert.timestamp,
            "labels": alert.labels
        }
        
        try:
            headers = {"Content-Type": "application/json"}
            headers.update(config.get("headers", {}))
            
            req = Request(
                config["url"],
                data=json.dumps(payload).encode('utf-8'),
                headers=headers
            )
            urlopen(req, timeout=10)
            logger.info(f"Webhook alert sent for {alert.id}")
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
    
    def _send_sms_alert(self, alert: Alert):
        """Send alert via SMS (Twilio)."""
        config = self._channel_configs["sms"]
        
        if not config.get("enabled") or not config.get("to_numbers"):
            return
        
        # This would integrate with Twilio or similar
        # For now, log the SMS that would be sent
        message = f"[{alert.severity.value.upper()}] {alert.rule_name}: {alert.message}"
        
        logger.info(f"SMS alert would be sent: {message}")
        logger.info(f"Would send to: {config['to_numbers']}")
    
    # -------------------------------------------------------------------------
    # Alert Escalation
    # -------------------------------------------------------------------------
    
    def add_escalation_policy(self, policy: EscalationPolicy):
        """Add an escalation policy."""
        self._escalation_policies[policy.id] = policy
    
    def get_escalation_policy(self, policy_id: str) -> Optional[EscalationPolicy]:
        """Get an escalation policy."""
        return self._escalation_policies.get(policy_id)
    
    def escalate_alert(
        self,
        alert_id: str,
        escalation_level: int,
        reason: str = ""
    ) -> Optional[Alert]:
        """Escalate an alert to the next level."""
        alert = self._active_alerts.get(alert_id)
        if not alert:
            return None
        
        alert.state = AlertState.ESCALATED
        alert.escalated_at = time.time()
        
        self._persist_alert(alert)
        
        # Notify on-call if available
        oncall = self.get_current_oncall()
        if oncall:
            self._notify_oncall(alert, escalation_level)
        
        logger.info(f"Alert {alert_id} escalated to level {escalation_level}")
        return alert
    
    def check_escalations(self):
        """Check for alerts that need escalation."""
        current_time = time.time()
        
        for alert in self._active_alerts.values():
            if alert.state not in [AlertState.FIRING, AlertState.ESCALATED]:
                continue
            
            # Check if unacknowledged for too long
            unacknowledged_time = current_time - alert.timestamp
            
            if unacknowledged_time > 300:  # 5 minutes
                if alert.state != AlertState.ESCALATED:
                    self.escalate_alert(alert.id, 1, "Unacknowledged for 5 minutes")
            
            if unacknowledged_time > 900:  # 15 minutes
                self.escalate_alert(alert.id, 2, "Unacknowledged for 15 minutes")
    
    def start_escalation_checker(self, interval_seconds: int = 60):
        """Start background escalation checker."""
        def checker():
            while self._running:
                try:
                    self.check_escalations()
                except Exception as e:
                    logger.error(f"Escalation checker error: {e}")
                time.sleep(interval_seconds)
        
        thread = threading.Thread(target=checker, daemon=True)
        thread.start()
    
    # -------------------------------------------------------------------------
    # On-Call Rotation
    # -------------------------------------------------------------------------
    
    def add_oncall_recipient(self, recipient: OnCallRecipient):
        """Add an on-call recipient."""
        self._oncall_recipients[recipient.user_id] = recipient
    
    def add_oncall_schedule(self, schedule: OnCallSchedule):
        """Add an on-call schedule entry."""
        self._oncall_schedules.append(schedule)
        self._persist_oncall_schedule(schedule)
    
    def get_current_oncall(self) -> Optional[OnCallRecipient]:
        """Get the current on-call person."""
        current_time = time.time()
        
        for schedule in self._oncall_schedules:
            if schedule.start_time <= current_time <= schedule.end_time:
                return schedule.recipient
        
        return None
    
    def get_oncall_schedule(
        self,
        since: Optional[float] = None,
        until: Optional[float] = None
    ) -> List[OnCallSchedule]:
        """Get on-call schedules within time range."""
        schedules = list(self._oncall_schedules)
        
        if since:
            schedules = [s for s in schedules if s.end_time >= since]
        if until:
            schedules = [s for s in schedules if s.start_time <= until]
        
        return sorted(schedules, key=lambda s: s.start_time)
    
    def get_next_oncall(self, count: int = 3) -> List[OnCallRecipient]:
        """Get the next N on-call people in rotation."""
        schedules = self.get_oncall_schedule(since=time.time())
        recipients = []
        seen = set()
        
        for schedule in schedules:
            if schedule.recipient.user_id not in seen:
                recipients.append(schedule.recipient)
                seen.add(schedule.recipient.user_id)
                if len(recipients) >= count:
                    break
        
        return recipients
    
    def _persist_oncall_schedule(self, schedule: OnCallSchedule):
        """Persist on-call schedule to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO oncall_schedules 
                (id, schedule_type, user_id, name, email, phone, slack_id,
                 start_time, end_time, timezone)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                schedule.id, schedule.schedule_type.value,
                schedule.recipient.user_id, schedule.recipient.name,
                schedule.recipient.email, schedule.recipient.phone,
                schedule.recipient.slack_id, schedule.start_time,
                schedule.end_time, schedule.timezone
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist on-call schedule: {e}")
    
    def _notify_oncall(self, alert: Alert, level: int):
        """Notify on-call person about an alert."""
        oncall = self.get_current_oncall()
        if not oncall:
            logger.warning(f"No on-call person to notify for alert {alert.id}")
            return
        
        message = f"Escalated Alert (Level {level}): {alert.rule_name}"
        
        # Send via configured channels
        if oncall.email:
            self._send_oncall_email(oncall, alert, level)
        
        if oncall.slack_id:
            self._send_oncall_slack(oncall, alert, level)
    
    def _send_oncall_email(self, recipient: OnCallRecipient, alert: Alert, level: int):
        """Send notification to on-call via email."""
        logger.info(f"Would notify {recipient.email} about alert {alert.id}")
    
    def _send_oncall_slack(self, recipient: OnCallRecipient, alert: Alert, level: int):
        """Send notification to on-call via Slack."""
        logger.info(f"Would notify {recipient.slack_id} about alert {alert.id}")
    
    # -------------------------------------------------------------------------
    # Incident Management
    # -------------------------------------------------------------------------
    
    def create_incident(
        self,
        title: str,
        description: str,
        severity: AlertSeverity,
        labels: Optional[Dict[str, str]] = None
    ) -> Incident:
        """Create a new incident."""
        incident = Incident(
            id=f"inc_{int(time.time() * 1000)}",
            title=title,
            description=description,
            status=IncidentStatus.OPEN,
            severity=severity,
            created_at=time.time(),
            labels=labels or {}
        )
        
        # Set SLA deadline
        sla = self._get_sla_for_severity(severity)
        if sla:
            incident.sla_deadline = incident.created_at + (sla.response_time_minutes * 60)
        
        self._incidents[incident.id] = incident
        self._persist_incident(incident)
        
        logger.info(f"Incident created: {incident.id} - {title}")
        return incident
    
    def _create_incident_from_alert(self, alert: Alert):
        """Create an incident from an alert."""
        incident = self.create_incident(
            title=f"Alert: {alert.rule_name}",
            description=alert.message,
            severity=alert.severity,
            labels=alert.labels
        )
        
        alert.incident_id = incident.id
        incident.alerts.append(alert.id)
        
        self._persist_alert(alert)
        self._persist_incident(incident)
    
    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Get an incident by ID."""
        return self._incidents.get(incident_id)
    
    def get_all_incidents(
        self,
        status: Optional[IncidentStatus] = None,
        since: Optional[float] = None
    ) -> List[Incident]:
        """Get all incidents, optionally filtered."""
        incidents = list(self._incidents.values())
        
        if status:
            incidents = [i for i in incidents if i.status == status]
        if since:
            incidents = [i for i in incidents if i.created_at >= since]
        
        return sorted(incidents, key=lambda i: i.created_at, reverse=True)
    
    def update_incident_status(
        self,
        incident_id: str,
        status: IncidentStatus
    ) -> Optional[Incident]:
        """Update incident status."""
        incident = self._incidents.get(incident_id)
        if not incident:
            return None
        
        incident.status = status
        
        if status == IncidentStatus.ACKNOWLEDGED:
            incident.acknowledged_at = time.time()
        elif status == IncidentStatus.MITIGATED:
            incident.mitigated_at = time.time()
        elif status == IncidentStatus.RESOLVED:
            incident.resolved_at = time.time()
        
        self._persist_incident(incident)
        
        # Check SLA compliance
        self._track_sla_compliance(incident)
        
        return incident
    
    def add_incident_note(self, incident_id: str, note: str) -> Optional[Incident]:
        """Add a note to an incident."""
        incident = self._incidents.get(incident_id)
        if not incident:
            return None
        
        incident.notes.append(note)
        self._persist_incident(incident)
        
        return incident
    
    def assign_incident(
        self,
        incident_id: str,
        user_id: str
    ) -> Optional[Incident]:
        """Assign an incident to a user."""
        incident = self._incidents.get(incident_id)
        if not incident:
            return None
        
        if user_id not in incident.assignees:
            incident.assignees.append(user_id)
        
        self._persist_incident(incident)
        
        return incident
    
    def link_alert_to_incident(
        self,
        alert_id: str,
        incident_id: str
    ) -> bool:
        """Link an alert to an incident."""
        alert = self._active_alerts.get(alert_id)
        incident = self._incidents.get(incident_id)
        
        if not alert or not incident:
            return False
        
        alert.incident_id = incident_id
        if alert_id not in incident.alerts:
            incident.alerts.append(alert_id)
        
        self._persist_alert(alert)
        self._persist_incident(incident)
        
        return True
    
    def _check_incident_resolution(self, incident_id: str):
        """Check if all linked alerts are resolved."""
        incident = self._incidents.get(incident_id)
        if not incident:
            return
        
        all_resolved = True
        for alert_id in incident.alerts:
            alert = self._active_alerts.get(alert_id)
            if alert and alert.state != AlertState.RESOLVED:
                all_resolved = False
                break
        
        if all_resolved and incident.alerts:
            self.update_incident_status(incident_id, IncidentStatus.RESOLVED)
    
    def _persist_incident(self, incident: Incident):
        """Persist incident to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO incidents 
                (id, title, description, status, severity, created_at,
                 acknowledged_at, mitigated_at, resolved_at, created_by,
                 assignees, alerts, labels, sla_deadline, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                incident.id, incident.title, incident.description,
                incident.status.value, incident.severity.value,
                incident.created_at, incident.acknowledged_at,
                incident.mitigated_at, incident.resolved_at,
                incident.created_by, json.dumps(incident.assignees),
                json.dumps(incident.alerts), json.dumps(incident.labels),
                incident.sla_deadline, json.dumps(incident.notes)
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist incident: {e}")
    
    # -------------------------------------------------------------------------
    # SLA Tracking
    # -------------------------------------------------------------------------
    
    def add_sla_definition(self, sla: SLADefinition):
        """Add an SLA definition."""
        self._sla_definitions[sla.id] = sla
        self._persist_sla_definition(sla)
    
    def get_sla_definition(self, sla_id: str) -> Optional[SLADefinition]:
        """Get an SLA definition."""
        return self._sla_definitions.get(sla_id)
    
    def get_all_sla_definitions(self) -> List[SLADefinition]:
        """Get all SLA definitions."""
        return list(self._sla_definitions.values())
    
    def _get_sla_for_severity(self, severity: AlertSeverity) -> Optional[SLADefinition]:
        """Get SLA definition for a severity level."""
        for sla in self._sla_definitions.values():
            if sla.severity == severity and sla.enabled:
                return sla
        return None
    
    def _track_sla_compliance(self, incident: Incident):
        """Track SLA compliance for an incident."""
        sla = self._get_sla_for_severity(incident.severity)
        if not sla:
            return
        
        response_time = None
        resolution_time = None
        
        if incident.acknowledged_at:
            response_time = (incident.acknowledged_at - incident.created_at) / 60
        
        if incident.resolved_at:
            resolution_time = (incident.resolved_at - incident.created_at) / 60
        
        compliance = SLACompliance(
            sla_id=sla.id,
            sla_name=sla.name,
            incident_id=incident.id,
            response_time_minutes=response_time or 0,
            resolution_time_minutes=resolution_time or 0,
            response_met=response_time <= sla.response_time_minutes if response_time else False,
            resolution_met=resolution_time <= sla.resolution_time_minutes if resolution_time else False,
            response_at=incident.acknowledged_at,
            resolved_at=incident.resolved_at,
            created_at=incident.created_at
        )
        
        self._sla_compliance[sla.id].append(compliance)
    
    def get_sla_compliance(
        self,
        sla_id: Optional[str] = None,
        since: Optional[float] = None
    ) -> List[SLACompliance]:
        """Get SLA compliance records."""
        if sla_id:
            records = self._sla_compliance.get(sla_id, [])
        else:
            records = []
            for records_list in self._sla_compliance.values():
                records.extend(records_list)
        
        if since:
            records = [r for r in records if r.created_at >= since]
        
        return records
    
    def get_sla_compliance_rate(self, sla_id: str) -> Dict[str, float]:
        """Get SLA compliance rate."""
        records = self._sla_compliance.get(sla_id, [])
        
        if not records:
            return {"response_rate": 0.0, "resolution_rate": 0.0, "total": 0}
        
        response_met = sum(1 for r in records if r.response_met)
        resolution_met = sum(1 for r in records if r.resolution_met)
        total = len(records)
        
        return {
            "response_rate": (response_met / total) * 100 if total > 0 else 0.0,
            "resolution_rate": (resolution_met / total) * 100 if total > 0 else 0.0,
            "total": total
        }
    
    def get_sla_status_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of SLA status for all definitions."""
        summary = {}
        
        for sla_id in self._sla_definitions:
            compliance = self.get_sla_compliance_rate(sla_id)
            sla = self._sla_definitions[sla_id]
            
            summary[sla_id] = {
                "name": sla.name,
                "severity": sla.severity.value,
                **compliance
            }
        
        return summary
    
    def check_sla_deadlines(self) -> List[Incident]:
        """Check for incidents approaching SLA deadline."""
        current_time = time.time()
        at_risk = []
        
        for incident in self._incidents.values():
            if incident.status in [IncidentStatus.RESOLVED, IncidentStatus.CLOSED]:
                continue
            
            if incident.sla_deadline:
                time_remaining = incident.sla_deadline - current_time
                
                # Alert if less than 20% time remaining
                sla = self._get_sla_for_severity(incident.severity)
                if sla:
                    total_time = sla.response_time_minutes * 60
                    if time_remaining < total_time * 0.2 and time_remaining > 0:
                        at_risk.append(incident)
        
        return at_risk
    
    def _persist_sla_definition(self, sla: SLADefinition):
        """Persist SLA definition to database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO sla_definitions 
                (id, name, response_time_minutes, resolution_time_minutes,
                 severity, enabled)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                sla.id, sla.name, sla.response_time_minutes,
                sla.resolution_time_minutes, sla.severity.value, sla.enabled
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to persist SLA definition: {e}")
    
    # -------------------------------------------------------------------------
    # Health Checks
    # -------------------------------------------------------------------------
    
    def add_health_check(self, health_check: HealthCheck):
        """Add a health check."""
        self._health_checks[health_check.name] = health_check
    
    def get_health_check(self, name: str) -> Optional[HealthCheck]:
        """Get a health check by name."""
        return self._health_checks.get(name)
    
    def get_all_health_checks(self) -> List[HealthCheck]:
        """Get all health checks."""
        return list(self._health_checks.values())
    
    def run_health_check(self, name: str) -> HealthCheckResult:
        """Run a specific health check."""
        health_check = self._health_checks.get(name)
        if not health_check:
            return HealthCheckResult(
                name=name,
                status=HealthCheckStatus.UNHEALTHY,
                response_time_ms=0,
                error_message="Health check not found"
            )
        
        return self._execute_health_check(health_check)
    
    def run_all_health_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all health checks."""
        results = {}
        
        for name, health_check in self._health_checks.items():
            if health_check.enabled:
                result = self._execute_health_check(health_check)
                results[name] = result
                self._health_check_results[name].append(result)
        
        return results
    
    def _execute_health_check(self, health_check: HealthCheck) -> HealthCheckResult:
        """Execute a health check."""
        start_time = time.time()
        
        try:
            req = Request(health_check.url)
            req.get_method = lambda: health_check.method
            
            response = urlopen(req, timeout=health_check.timeout_seconds)
            response_time = (time.time() - start_time) * 1000
            
            status_code = response.getcode()
            is_healthy = status_code == health_check.expected_status
            
            return HealthCheckResult(
                name=health_check.name,
                status=HealthCheckStatus.HEALTHY if is_healthy else HealthCheckStatus.DEGRADED,
                response_time_ms=response_time,
                status_code=status_code
            )
        
        except HTTPError as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=health_check.name,
                status=HealthCheckStatus.UNHEALTHY,
                response_time_ms=response_time,
                status_code=e.code,
                error_message=str(e)
            )
        
        except URLError as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=health_check.name,
                status=HealthCheckStatus.UNHEALTHY,
                response_time_ms=response_time,
                error_message=str(e.reason)
            )
        
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            return HealthCheckResult(
                name=health_check.name,
                status=HealthCheckStatus.UNHEALTHY,
                response_time_ms=response_time,
                error_message=str(e)
            )
    
    def get_health_check_history(
        self,
        name: str,
        limit: int = 100
    ) -> List[HealthCheckResult]:
        """Get health check history."""
        results = list(self._health_check_results.get(name, []))
        return results[-limit:]
    
    def get_overall_health_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        results = self.run_all_health_checks()
        
        if not results:
            return {
                "status": HealthCheckStatus.HEALTHY.value,
                "checks": {},
                "timestamp": time.time()
            }
        
        healthy = sum(1 for r in results.values() if r.status == HealthCheckStatus.HEALTHY)
        degraded = sum(1 for r in results.values() if r.status == HealthCheckStatus.DEGRADED)
        unhealthy = sum(1 for r in results.values() if r.status == HealthCheckStatus.UNHEALTHY)
        total = len(results)
        
        # Overall status
        if unhealthy > 0:
            overall = HealthCheckStatus.UNHEALTHY
        elif degraded > 0:
            overall = HealthCheckStatus.DEGRADED
        else:
            overall = HealthCheckStatus.HEALTHY
        
        return {
            "status": overall.value,
            "summary": {
                "healthy": healthy,
                "degraded": degraded,
                "unhealthy": unhealthy,
                "total": total
            },
            "checks": {
                name: {
                    "status": result.status.value,
                    "response_time_ms": result.response_time_ms,
                    "status_code": result.status_code,
                    "error": result.error_message
                }
                for name, result in results.items()
            },
            "timestamp": time.time()
        }
    
    def get_health_check_metrics(self) -> Dict[str, Any]:
        """Get health check metrics for monitoring."""
        all_results = {}
        
        for name, results in self._health_check_results.items():
            if not results:
                continue
            
            values = [r.response_time_ms for r in results]
            
            all_results[name] = {
                "avg_response_time": statistics.mean(values),
                "min_response_time": min(values),
                "max_response_time": max(values),
                "success_rate": sum(1 for r in results if r.status == HealthCheckStatus.HEALTHY) / len(results) * 100,
                "sample_count": len(results)
            }
        
        return all_results
    
    def start_health_check_runner(self, interval_seconds: int = 60):
        """Start background health check runner."""
        def runner():
            while self._running:
                try:
                    self.run_all_health_checks()
                except Exception as e:
                    logger.error(f"Health check runner error: {e}")
                time.sleep(interval_seconds)
        
        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
    
    # -------------------------------------------------------------------------
    # HTTP Health Endpoints
    # -------------------------------------------------------------------------
    
    def get_health_endpoint(self) -> Dict[str, Any]:
        """Get health endpoint data for HTTP response."""
        health = self.get_overall_health_status()
        
        return {
            "status": health["status"],
            "timestamp": health["timestamp"],
            "version": "22.0.0",
            "uptime": time.time() - getattr(self, "_start_time", time.time()),
            "checks": health["checks"],
            "summary": health.get("summary", {})
        }
    
    def get_ready_endpoint(self) -> Dict[str, Any]:
        """Get readiness endpoint data."""
        # Check if system is ready to serve traffic
        active_alerts = self.get_active_alerts(state=AlertState.FIRING)
        critical_alerts = [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
        
        is_ready = len(critical_alerts) == 0
        
        return {
            "ready": is_ready,
            "timestamp": time.time(),
            "blocking_issues": [
                {"alert_id": a.id, "rule_name": a.rule_name}
                for a in critical_alerts[:5]
            ]
        }
    
    def get_live_endpoint(self) -> Dict[str, Any]:
        """Get liveness endpoint data."""
        return {
            "alive": True,
            "timestamp": time.time()
        }
    
    # -------------------------------------------------------------------------
    # Dashboard Data
    # -------------------------------------------------------------------------
    
    def get_dashboard_summary(self) -> Dict[str, Any]:
        """Get summary data for monitoring dashboard."""
        current_time = time.time()
        one_hour_ago = current_time - 3600
        
        # Alert stats
        active_alerts = self.get_active_alerts()
        firing_alerts = self.get_active_alerts(state=AlertState.FIRING)
        
        # Incident stats
        open_incidents = self.get_all_incidents(status=IncidentStatus.OPEN)
        investigating_incidents = self.get_all_incidents(status=IncidentStatus.INVESTIGATING)
        
        # SLA status
        sla_summary = self.get_sla_status_summary()
        
        # Health
        health = self.get_overall_health_status()
        
        # Resource summary
        resources = self.get_current_resources()
        
        return {
            "alerts": {
                "total_active": len(active_alerts),
                "firing": len(firing_alerts),
                "by_severity": {
                    "critical": len([a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]),
                    "error": len([a for a in active_alerts if a.severity == AlertSeverity.ERROR]),
                    "warning": len([a for a in active_alerts if a.severity == AlertSeverity.WARNING]),
                    "info": len([a for a in active_alerts if a.severity == AlertSeverity.INFO])
                }
            },
            "incidents": {
                "open": len(open_incidents),
                "investigating": len(investigating_incidents),
                "total": len(self._incidents)
            },
            "sla": sla_summary,
            "health": health,
            "resources": {
                "cpu_percent": resources.cpu_percent,
                "memory_percent": resources.memory_percent,
                "disk_percent": resources.disk_percent
            },
            "oncall": {
                "current": self.get_current_oncall()
            },
            "timestamp": current_time
        }
    
    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    
    def start(self):
        """Start the monitoring system."""
        self._start_time = time.time()
        self.start_resource_monitoring(interval_seconds=10)
        self.start_escalation_checker(interval_seconds=60)
        self.start_health_check_runner(interval_seconds=60)
        logger.info("Workflow monitoring system started")
    
    def stop(self):
        """Stop the monitoring system."""
        self.stop_resource_monitoring()
        self._running = False
        logger.info("Workflow monitoring system stopped")
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()


# Required import for the file
import os
