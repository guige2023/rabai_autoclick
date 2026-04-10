"""
Workflow Analytics and Reporting System v22
Comprehensive analytics, time series analysis, anomaly detection, dashboards, and alerting
"""
import json
import time
import threading
import os
import statistics
import math
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import copy


class MetricType(Enum):
    """Metric types for tracking"""
    SUCCESS_RATE = "success_rate"
    DURATION = "duration"
    FREQUENCY = "frequency"
    FAILURE_COUNT = "failure_count"
    ACTION_COUNT = "action_count"


class TimeGranularity(Enum):
    """Time granularity for time series analysis"""
    MINUTE = "minute"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class TrendDirection(Enum):
    """Trend direction"""
    INCREASING = "increasing"
    DECREASING = "decreasing"
    STABLE = "stable"


@dataclass
class ExecutionRecord:
    """Record of a single workflow execution"""
    execution_id: str
    workflow_id: str
    workflow_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    duration_seconds: float = 0.0
    actions_executed: List[str] = field(default_factory=list)
    actions_failed: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ActionMetrics:
    """Metrics for a specific action"""
    action_name: str
    total_executions: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    last_executed: Optional[datetime] = None
    success_rate: float = 0.0

    def update(self, success: bool, duration: float):
        self.total_executions += 1
        self.total_duration += duration
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1
        self.avg_duration = self.total_duration / self.total_executions if self.total_executions > 0 else 0.0
        self.success_rate = self.success_count / self.total_executions if self.total_executions > 0 else 0.0
        self.last_executed = datetime.now()


@dataclass
class WorkflowMetrics:
    """Metrics for a specific workflow"""
    workflow_id: str
    workflow_name: str
    total_executions: int = 0
    success_count: int = 0
    failure_count: int = 0
    total_duration: float = 0.0
    avg_duration: float = 0.0
    min_duration: float = float('inf')
    max_duration: float = 0.0
    last_executed: Optional[datetime] = None
    success_rate: float = 0.0
    action_breakdown: Dict[str, int] = field(default_factory=dict)
    anomaly_score: float = 0.0
    trend: TrendDirection = TrendDirection.STABLE
    alert_active: bool = False

    def update(self, record: ExecutionRecord):
        self.total_executions += 1
        self.total_duration += record.duration_seconds
        self.avg_duration = self.total_duration / self.total_executions
        self.min_duration = min(self.min_duration, record.duration_seconds)
        self.max_duration = max(self.max_duration, record.duration_seconds)
        if record.success:
            self.success_count += 1
        else:
            self.failure_count += 1
        self.success_rate = self.success_count / self.total_executions
        self.last_executed = record.end_time or datetime.now()
        for action in record.actions_executed:
            self.action_breakdown[action] = self.action_breakdown.get(action, 0) + 1


@dataclass
class TimeSeriesPoint:
    """Single point in time series data"""
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    """Alert for anomalous behavior"""
    alert_id: str
    workflow_id: str
    workflow_name: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    metric_name: str
    metric_value: float
    threshold: float
    acknowledged: bool = False
    resolved: bool = False


@dataclass
class ScheduledReport:
    """Scheduled report configuration"""
    report_id: str
    name: str
    report_type: str  # 'weekly', 'monthly'
    enabled: bool = True
    last_generated: Optional[datetime] = None
    next_generation: Optional[datetime] = None
    recipients: List[str] = field(default_factory=list)
    include_sections: List[str] = field(default_factory=list)


class WorkflowAnalytics:
    """
    Comprehensive workflow analytics and reporting system.
    
    Features:
    - Execution metrics tracking (success rate, duration, frequency)
    - Time series analysis (daily, weekly, monthly)
    - Action-level analytics
    - User behavior analytics
    - Custom SVG dashboards
    - Anomaly detection and alerting
    - Trend detection
    - JSON export for BI tools
    - Scheduled reports
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.analytics_file = os.path.join(data_dir, "workflow_analytics.json")
        self.alerts_file = os.path.join(data_dir, "analytics_alerts.json")
        self.reports_file = os.path.join(data_dir, "scheduled_reports.json")
        
        os.makedirs(data_dir, exist_ok=True)
        
        # Core data structures
        self.executions: Dict[str, ExecutionRecord] = {}
        self.workflow_metrics: Dict[str, WorkflowMetrics] = {}
        self.action_metrics: Dict[str, ActionMetrics] = {}
        self.time_series: Dict[str, Dict[str, List[TimeSeriesPoint]]] = defaultdict(lambda: defaultdict(list))
        
        # User behavior tracking
        self.user_activity: Dict[str, List[str]] = defaultdict(list)  # user_id -> execution_ids
        self.peak_hours: Dict[int, int] = defaultdict(int)  # hour -> execution count
        
        # Alerting
        self.alerts: List[Alert] = []
        self.alert_callbacks: List[Callable[[Alert], None]] = []
        
        # Scheduled reports
        self.scheduled_reports: List[ScheduledReport] = []
        self._report_lock = threading.Lock()
        
        # Anomaly detection settings
        self.anomaly_config = {
            'success_rate_threshold': 0.2,  # Alert if success rate drops by 20%
            'duration_threshold_std': 3.0,  # Alert if duration > mean + 3*std
            'min_samples_for_anomaly': 10,   # Min executions before anomaly detection
        }
        
        # Rolling windows for trend analysis
        self._trend_window_size = 30  # Number of executions to consider for trends
        self._execution_order: deque = deque(maxlen=1000)  # Track execution order
        
        # Load existing data
        self._load_data()
        
        # Start background tasks
        self._background_thread: Optional[threading.Thread] = None
        self._running = False

    def record_execution(self, record: ExecutionRecord) -> None:
        """Record a workflow execution"""
        self.executions[record.execution_id] = record
        self._execution_order.append(record.execution_id)
        
        # Update workflow metrics
        if record.workflow_id not in self.workflow_metrics:
            self.workflow_metrics[record.workflow_id] = WorkflowMetrics(
                workflow_id=record.workflow_id,
                workflow_name=record.workflow_name
            )
        self.workflow_metrics[record.workflow_id].update(record)
        
        # Update action metrics
        for action in record.actions_executed:
            if action not in self.action_metrics:
                self.action_metrics[action] = ActionMetrics(action_name=action)
            self.action_metrics[action].update(success=True, duration=0.0)
        
        for action in record.actions_failed:
            if action not in self.action_metrics:
                self.action_metrics[action] = ActionMetrics(action_name=action)
            self.action_metrics[action].update(success=False, duration=0.0)
        
        # Update user activity
        if record.user_id:
            self.user_activity[record.user_id].append(record.execution_id)
        
        # Update peak hours
        hour = record.start_time.hour
        self.peak_hours[hour] += 1
        
        # Update time series
        self._update_time_series(record)
        
        # Check for anomalies
        self._check_anomalies(record.workflow_id)
        
        # Save data periodically
        self._save_data()

    def _update_time_series(self, record: ExecutionRecord) -> None:
        """Update time series data for the workflow"""
        wf_id = record.workflow_id
        
        # Daily time series
        date_key = record.start_time.strftime('%Y-%m-%d')
        self.time_series[wf_id]['daily'].append(TimeSeriesPoint(
            timestamp=record.start_time,
            value=1.0 if record.success else 0.0,
            metadata={'duration': record.duration_seconds, 'success': record.success}
        ))
        
        # Keep only last 365 days of daily data
        cutoff = datetime.now() - timedelta(days=365)
        self.time_series[wf_id]['daily'] = [
            p for p in self.time_series[wf_id]['daily'] if p.timestamp > cutoff
        ]

    def _check_anomalies(self, workflow_id: str) -> None:
        """Check for anomalous behavior in workflow"""
        metrics = self.workflow_metrics.get(workflow_id)
        if not metrics or metrics.total_executions < self.anomaly_config['min_samples_for_anomaly']:
            return
        
        # Calculate anomaly score
        anomaly_score = self._calculate_anomaly_score(workflow_id)
        metrics.anomaly_score = anomaly_score
        
        # Check for success rate drop
        recent_executions = list(self.executions.values())
        recent_executions = [
            e for e in recent_executions 
            if e.workflow_id == workflow_id
        ][-self._trend_window_size:]
        
        if len(recent_executions) >= 5:
            recent_success_rate = sum(1 for e in recent_executions if e.success) / len(recent_executions)
            overall_success_rate = metrics.success_rate
            
            # Alert if recent success rate is significantly lower
            if recent_success_rate < overall_success_rate - self.anomaly_config['success_rate_threshold']:
                self._create_alert(
                    workflow_id=workflow_id,
                    workflow_name=metrics.workflow_name,
                    severity=AlertSeverity.WARNING,
                    message=f"Success rate dropped from {overall_success_rate:.1%} to {recent_success_rate:.1%}",
                    metric_name='success_rate',
                    metric_value=recent_success_rate,
                    threshold=overall_success_rate - self.anomaly_config['success_rate_threshold']
                )
                metrics.alert_active = True
        
        # Check for duration anomaly
        durations = [e.duration_seconds for e in recent_executions]
        if len(durations) >= 5:
            mean_duration = statistics.mean(durations)
            std_duration = statistics.stdev(durations) if len(durations) > 1 else 0
            last_duration = durations[-1]
            
            if std_duration > 0:
                z_score = (last_duration - mean_duration) / std_duration
                if abs(z_score) > self.anomaly_config['duration_threshold_std']:
                    self._create_alert(
                        workflow_id=workflow_id,
                        workflow_name=metrics.workflow_name,
                        severity=AlertSeverity.CRITICAL if z_score > 0 else AlertSeverity.INFO,
                        message=f"Duration anomaly detected: {last_duration:.2f}s (z-score: {z_score:.2f})",
                        metric_name='duration',
                        metric_value=last_duration,
                        threshold=mean_duration + self.anomaly_config['duration_threshold_std'] * std_duration
                    )

    def _calculate_anomaly_score(self, workflow_id: str) -> float:
        """Calculate anomaly score for a workflow (0-100)"""
        metrics = self.workflow_metrics.get(workflow_id)
        if not metrics or metrics.total_executions < self.anomaly_config['min_samples_for_anomaly']:
            return 0.0
        
        recent_executions = [
            e for e in self.executions.values() 
            if e.workflow_id == workflow_id
        ][-self._trend_window_size:]
        
        if len(recent_executions) < 5:
            return 0.0
        
        # Success rate deviation
        recent_success_rate = sum(1 for e in recent_executions if e.success) / len(recent_executions)
        success_deviation = abs(recent_success_rate - metrics.success_rate)
        
        # Duration deviation
        durations = [e.duration_seconds for e in recent_executions]
        mean_duration = statistics.mean(durations)
        std_duration = statistics.stdev(durations) if len(durations) > 1 else 0
        
        duration_score = 0.0
        if std_duration > 0 and metrics.avg_duration > 0:
            duration_ratio = mean_duration / metrics.avg_duration
            duration_score = min(abs(duration_ratio - 1.0) * 50, 50)
        
        # Combine scores
        success_score = min(success_deviation * 100, 50)
        total_score = success_score + duration_score
        
        return min(total_score, 100.0)

    def _create_alert(self, workflow_id: str, workflow_name: str, severity: AlertSeverity,
                     message: str, metric_name: str, metric_value: float, threshold: float) -> Alert:
        """Create and store an alert"""
        alert = Alert(
            alert_id=f"alert_{int(time.time() * 1000)}",
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            severity=severity,
            message=message,
            timestamp=datetime.now(),
            metric_name=metric_name,
            metric_value=metric_value,
            threshold=threshold
        )
        self.alerts.append(alert)
        self._save_alerts()
        
        # Notify callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception:
                pass
        
        return alert

    def register_alert_callback(self, callback: Callable[[Alert], None]) -> None:
        """Register a callback for alert notifications"""
        self.alert_callbacks.append(callback)

    def detect_trends(self, workflow_id: str, metric: MetricType = MetricType.SUCCESS_RATE,
                     window_size: Optional[int] = None) -> Tuple[TrendDirection, float]:
        """
        Detect trend for a workflow metric.
        
        Returns: (TrendDirection, slope)
        """
        window_size = window_size or self._trend_window_size
        
        executions = [
            e for e in self.executions.values() 
            if e.workflow_id == workflow_id
        ]
        executions.sort(key=lambda x: x.start_time)
        executions = executions[-window_size:]
        
        if len(executions) < 3:
            return TrendDirection.STABLE, 0.0
        
        if metric == MetricType.SUCCESS_RATE:
            values = [1.0 if e.success else 0.0 for e in executions]
        elif metric == MetricType.DURATION:
            values = [e.duration_seconds for e in executions]
        elif metric == MetricType.FREQUENCY:
            # Already sorted by time, compute intervals
            values = []
            for i in range(1, len(executions)):
                interval = (executions[i].start_time - executions[i-1].start_time).total_seconds()
                values.append(interval)
            if not values:
                return TrendDirection.STABLE, 0.0
        else:
            return TrendDirection.STABLE, 0.0
        
        # Simple linear regression to find slope
        n = len(values)
        x_vals = list(range(n))
        x_mean = sum(x_vals) / n
        y_mean = sum(values) / n
        
        numerator = sum((x_vals[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return TrendDirection.STABLE, 0.0
        
        slope = numerator / denominator
        
        # Normalize slope to determine direction
        y_range = max(values) - min(values) if max(values) != min(values) else 1
        normalized_slope = slope / y_range if y_range > 0 else 0
        
        if normalized_slope > 0.05:
            direction = TrendDirection.INCREASING
        elif normalized_slope < -0.05:
            direction = TrendDirection.DECREASING
        else:
            direction = TrendDirection.STABLE
        
        # Update workflow metrics trend
        if workflow_id in self.workflow_metrics:
            self.workflow_metrics[workflow_id].trend = direction
        
        return direction, slope

    def get_time_series(self, workflow_id: str, granularity: TimeGranularity = TimeGranularity.DAILY,
                        start_date: Optional[datetime] = None,
                        end_date: Optional[datetime] = None) -> List[TimeSeriesPoint]:
        """Get time series data for a workflow"""
        gran_key = granularity.value
        if workflow_id not in self.time_series or gran_key not in self.time_series[workflow_id]:
            return []
        
        points = self.time_series[workflow_id][gran_key]
        
        if start_date:
            points = [p for p in points if p.timestamp >= start_date]
        if end_date:
            points = [p for p in points if p.timestamp <= end_date]
        
        return points

    def get_execution_metrics(self, workflow_id: Optional[str] = None) -> Dict[str, Any]:
        """Get execution metrics for a workflow or all workflows"""
        if workflow_id:
            metrics = self.workflow_metrics.get(workflow_id)
            if not metrics:
                return {}
            return {
                'workflow_id': metrics.workflow_id,
                'workflow_name': metrics.workflow_name,
                'total_executions': metrics.total_executions,
                'success_count': metrics.success_count,
                'failure_count': metrics.failure_count,
                'success_rate': metrics.success_rate,
                'avg_duration': metrics.avg_duration,
                'min_duration': metrics.min_duration if metrics.min_duration != float('inf') else 0,
                'max_duration': metrics.max_duration,
                'anomaly_score': metrics.anomaly_score,
                'trend': metrics.trend.value,
                'action_breakdown': metrics.action_breakdown
            }
        else:
            return {
                wf_id: {
                    'workflow_name': m.workflow_name,
                    'total_executions': m.total_executions,
                    'success_rate': m.success_rate,
                    'avg_duration': m.avg_duration,
                    'anomaly_score': m.anomaly_score
                }
                for wf_id, m in self.workflow_metrics.items()
            }

    def get_action_analytics(self) -> Dict[str, ActionMetrics]:
        """Get analytics for all actions"""
        # Sort by total executions
        sorted_actions = sorted(
            self.action_metrics.items(),
            key=lambda x: x[1].total_executions,
            reverse=True
        )
        return dict(sorted_actions)

    def get_most_used_actions(self, limit: int = 10) -> List[Tuple[str, ActionMetrics]]:
        """Get most frequently used actions"""
        sorted_actions = sorted(
            self.action_metrics.items(),
            key=lambda x: x[1].total_executions,
            reverse=True
        )
        return sorted_actions[:limit]

    def get_most_failure_prone_actions(self, limit: int = 10) -> List[Tuple[str, ActionMetrics]]:
        """Get actions with highest failure rates"""
        actions_with_failures = [
            (name, metrics) for name, metrics in self.action_metrics.items()
            if metrics.failure_count > 0
        ]
        sorted_actions = sorted(
            actions_with_failures,
            key=lambda x: x[1].failure_count / x[1].total_executions if x[1].total_executions > 0 else 0,
            reverse=True
        )
        return sorted_actions[:limit]

    def get_user_behavior_analytics(self) -> Dict[str, Any]:
        """Get user behavior analytics"""
        # Most active users
        user_activity_sorted = sorted(
            self.user_activity.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )
        
        # Peak hours
        peak_hours_sorted = sorted(self.peak_hours.items(), key=lambda x: x[1], reverse=True)
        
        # Most active workflows
        workflow_executions = defaultdict(int)
        for record in self.executions.values():
            workflow_executions[record.workflow_name] += 1
        
        most_active_workflows = sorted(
            workflow_executions.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            'most_active_users': [
                {'user_id': uid, 'execution_count': len(exids)}
                for uid, exids in user_activity_sorted[:10]
            ],
            'peak_hours': [
                {'hour': hour, 'execution_count': count}
                for hour, count in peak_hours_sorted
            ],
            'most_active_workflows': [
                {'workflow_name': name, 'execution_count': count}
                for name, count in most_active_workflows[:10]
            ]
        }

    def get_peak_usage_times(self) -> List[Dict[str, Any]]:
        """Get peak usage times by hour of day"""
        return [
            {'hour': hour, 'count': count, 'label': f"{hour:02d}:00"}
            for hour, count in sorted(self.peak_hours.items())
        ]

    def generate_html_dashboard(self, workflow_id: Optional[str] = None) -> str:
        """Generate an HTML dashboard with SVG charts"""
        metrics = self.get_execution_metrics(workflow_id)
        
        if workflow_id and not metrics:
            return "<html><body><h1>No data available for this workflow</h1></body></html>"
        
        workflows_data = metrics if not workflow_id else {workflow_id: metrics}
        
        # Generate SVG charts
        success_rate_chart = self._generate_success_rate_svg(workflows_data)
        duration_chart = self._generate_duration_svg(workflows_data)
        action_chart = self._generate_action_breakdown_svg()
        trend_chart = self._generate_trend_svg(workflow_id) if workflow_id else ""
        
        # Get recent alerts
        recent_alerts = [
            a for a in self.alerts 
            if not a.acknowledged and not a.resolved
        ][-5:]
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Workflow Analytics Dashboard</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .dashboard {{ max-width: 1200px; margin: 0 auto; }}
        h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .metric-value {{ font-size: 32px; font-weight: bold; color: #4CAF50; }}
        .metric-label {{ color: #777; font-size: 14px; margin-top: 5px; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin: 20px 0; }}
        .alert {{ padding: 10px; margin: 5px 0; border-radius: 4px; }}
        .alert-warning {{ background: #fff3cd; border-left: 4px solid #ffc107; }}
        .alert-critical {{ background: #f8d7da; border-left: 4px solid #dc3545; }}
        .alert-info {{ background: #d1ecf1; border-left: 4px solid #17a2b8; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #4CAF50; color: white; }}
    </style>
</head>
<body>
    <div class="dashboard">
        <h1>📊 Workflow Analytics Dashboard</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>📈 Execution Metrics</h2>
        <div class="metrics-grid">
"""
        
        for wf_id, m in workflows_data.items():
            html += f"""
            <div class="metric-card">
                <div class="metric-value">{m.get('total_executions', 0)}</div>
                <div class="metric-label">Total Executions - {m.get('workflow_name', wf_id)}</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{m.get('success_rate', 0):.1%}</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{m.get('avg_duration', 0):.2f}s</div>
                <div class="metric-label">Avg Duration</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{m.get('anomaly_score', 0):.1f}</div>
                <div class="metric-label">Anomaly Score (0-100)</div>
            </div>
"""
        
        html += f"""
        </div>
        
        <h2>📊 Success Rate Overview</h2>
        <div class="chart-container">
            {success_rate_chart}
        </div>
        
        <h2>⏱️ Duration Analysis</h2>
        <div class="chart-container">
            {duration_chart}
        </div>
        
        <h2>🔧 Action Breakdown</h2>
        <div class="chart-container">
            {action_chart}
        </div>
"""
        
        if trend_chart:
            html += f"""
        <h2>📉 Trend Analysis</h2>
        <div class="chart-container">
            {trend_chart}
        </div>
"""
        
        # Alerts section
        html += """
        <h2>🚨 Active Alerts</h2>
        <div class="chart-container">
"""
        if recent_alerts:
            for alert in recent_alerts:
                severity_class = f"alert-{alert.severity.value}"
                html += f"""
            <div class="alert {severity_class}">
                <strong>[{alert.severity.value.upper()}]</strong> {alert.message}<br>
                <small>Workflow: {alert.workflow_name} | Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</small>
            </div>
"""
        else:
            html += "<p>No active alerts.</p>"
        
        html += """
        </div>
        
        <h2>🔝 Top Actions</h2>
        <div class="chart-container">
            <table>
                <tr>
                    <th>Action</th>
                    <th>Executions</th>
                    <th>Success Rate</th>
                    <th>Avg Duration</th>
                </tr>
"""
        
        for action_name, action_metrics in self.get_most_used_actions(5):
            html += f"""
                <tr>
                    <td>{action_name}</td>
                    <td>{action_metrics.total_executions}</td>
                    <td>{action_metrics.success_rate:.1%}</td>
                    <td>{action_metrics.avg_duration:.2f}s</td>
                </tr>
"""
        
        html += """
            </table>
        </div>
        
        <h2>👥 User Behavior</h2>
        <div class="chart-container">
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Value</th>
                </tr>
"""
        
        behavior = self.get_user_behavior_analytics()
        if behavior['most_active_workflows']:
            top_wf = behavior['most_active_workflows'][0]
            html += f"""
                <tr>
                    <td>Most Active Workflow</td>
                    <td>{top_wf['workflow_name']} ({top_wf['execution_count']} runs)</td>
                </tr>
"""
        
        if behavior['peak_hours']:
            peak = behavior['peak_hours'][0]
            html += f"""
                <tr>
                    <td>Peak Hour</td>
                    <td>{peak['hour']:02d}:00 ({peak['execution_count']} executions)</td>
                </tr>
"""
        
        html += """
            </table>
        </div>
    </div>
</body>
</html>
"""
        return html

    def _generate_success_rate_svg(self, workflows_data: Dict) -> str:
        """Generate SVG bar chart for success rates"""
        if not workflows_data:
            return "<p>No data available</p>"
        
        data = []
        for wf_id, m in workflows_data.items():
            success_rate = m.get('success_rate', 0) * 100
            name = m.get('workflow_name', wf_id)[:20]
            data.append((name, success_rate))
        
        max_val = 100
        bar_height = 30
        chart_height = len(data) * (bar_height + 10) + 40
        chart_width = 500
        
        svg = f'<svg width="100%" height="{chart_height}" viewBox="0 0 {chart_width} {chart_height}">'
        svg += f'''
        <rect width="{chart_width}" height="{chart_height}" fill="#fff"/>
        <text x="10" y="20" font-family="Arial" font-size="12" fill="#333">Success Rate (%)</text>
        <line x1="60" y1="30" x2="60" y2="{chart_height - 10}" stroke="#ccc"/>
        <line x1="60" y1="{chart_height - 10}" x2="{chart_width - 10}" y2="{chart_height - 10}" stroke="#ccc"/>
'''
        
        for i, (name, rate) in enumerate(data):
            y = 50 + i * (bar_height + 10)
            bar_width = (rate / max_val) * (chart_width - 80)
            
            color = '#4CAF50' if rate >= 80 else '#FFC107' if rate >= 50 else '#dc3545'
            
            svg += f'<text x="5" y="{y + 20}" font-family="Arial" font-size="11" fill="#333">{name}</text>'
            svg += f'<rect x="65" y="{y}" width="{bar_width}" height="{bar_height}" fill="{color}" rx="3"/>'
            svg += f'<text x="{70 + bar_width}" y="{y + 20}" font-family="Arial" font-size="11" fill="#333">{rate:.1f}%</text>'
        
        svg += '</svg>'
        return svg

    def _generate_duration_svg(self, workflows_data: Dict) -> str:
        """Generate SVG bar chart for durations"""
        if not workflows_data:
            return "<p>No data available</p>"
        
        data = []
        for wf_id, m in workflows_data.items():
            avg_dur = m.get('avg_duration', 0)
            name = m.get('workflow_name', wf_id)[:20]
            data.append((name, avg_dur))
        
        max_val = max(d[1] for d in data) if data else 1
        bar_height = 30
        chart_height = len(data) * (bar_height + 10) + 40
        chart_width = 500
        
        svg = f'<svg width="100%" height="{chart_height}" viewBox="0 0 {chart_width} {chart_height}">'
        svg += f'''
        <rect width="{chart_width}" height="{chart_height}" fill="#fff"/>
        <text x="10" y="20" font-family="Arial" font-size="12" fill="#333">Avg Duration (s)</text>
        <line x1="60" y1="30" x2="60" y2="{chart_height - 10}" stroke="#ccc"/>
        <line x1="60" y1="{chart_height - 10}" x2="{chart_width - 10}" y2="{chart_height - 10}" stroke="#ccc"/>
'''
        
        for i, (name, dur) in enumerate(data):
            y = 50 + i * (bar_height + 10)
            bar_width = (dur / max_val) * (chart_width - 80) if max_val > 0 else 0
            
            svg += f'<text x="5" y="{y + 20}" font-family="Arial" font-size="11" fill="#333">{name}</text>'
            svg += f'<rect x="65" y="{y}" width="{bar_width}" height="{bar_height}" fill="#2196F3" rx="3"/>'
            svg += f'<text x="{70 + bar_width}" y="{y + 20}" font-family="Arial" font-size="11" fill="#333">{dur:.2f}s</text>'
        
        svg += '</svg>'
        return svg

    def _generate_action_breakdown_svg(self) -> str:
        """Generate SVG pie chart for action breakdown"""
        top_actions = self.get_most_used_actions(6)
        
        if not top_actions:
            return "<p>No action data available</p>"
        
        total = sum(a.total_executions for _, a in top_actions)
        if total == 0:
            return "<p>No action data available</p>"
        
        colors = ['#4CAF50', '#2196F3', '#FFC107', '#dc3545', '#9C27B0', '#FF5722']
        cx, cy, r = 150, 120, 100
        
        svg = f'<svg width="100%" height="280" viewBox="0 0 350 280">'
        svg += '<rect width="350" height="280" fill="#fff"/>'
        
        start_angle = -90
        for i, (action_name, metrics) in enumerate(top_actions):
            pct = metrics.total_executions / total
            angle = pct * 360
            end_angle = start_angle + angle
            
            x1 = cx + r * math.cos(math.radians(start_angle))
            y1 = cy + r * math.sin(math.radians(start_angle))
            x2 = cx + r * math.cos(math.radians(end_angle))
            y2 = cy + r * math.sin(math.radians(end_angle))
            
            large_arc = 1 if angle > 180 else 0
            
            path = f'M {cx} {cy} L {x1} {y1} A {r} {r} 0 {large_arc} 1 {x2} {y2} Z'
            
            svg += f'<path d="{path}" fill="{colors[i % len(colors)]}" stroke="#fff" stroke-width="2"/>'
            
            mid_angle = math.radians(start_angle + angle / 2)
            label_x = cx + (r + 30) * math.cos(mid_angle)
            label_y = cy + (r + 30) * math.sin(mid_angle)
            
            short_name = action_name[:15] + '...' if len(action_name) > 15 else action_name
            svg += f'<text x="{label_x}" y="{label_y}" font-family="Arial" font-size="10" text-anchor="middle" fill="#333">{short_name}</text>'
            svg += f'<text x="{label_x}" y="{label_y + 12}" font-family="Arial" font-size="9" text-anchor="middle" fill="#666">{pct*100:.1f}%</text>'
            
            start_angle = end_angle
        
        svg += '</svg>'
        return svg

    def _generate_trend_svg(self, workflow_id: str) -> str:
        """Generate SVG line chart for trends"""
        points = self.get_time_series(workflow_id, TimeGranularity.DAILY)
        
        if len(points) < 2:
            return "<p>Insufficient data for trend analysis</p>"
        
        values = [p.value for p in points]
        timestamps = [p.timestamp for p in points]
        
        min_val, max_val = min(values), max(values)
        val_range = max_val - min_val if max_val != min_val else 1
        
        width, height = 600, 200
        padding = 40
        
        svg = f'<svg width="100%" height="{height + 40}" viewBox="0 0 {width} {height + 40}">'
        svg += f'<rect width="{width}" height="{height + 40}" fill="#fff"/>'
        
        # Grid lines
        for i in range(5):
            y = padding + i * (height - 2 * padding) / 4
            svg += f'<line x1="{padding}" y1="{y}" x2="{width - padding}" y2="{y}" stroke="#eee" stroke-dasharray="4"/>'
        
        # Line chart
        path_data = ""
        for i, (ts, val) in enumerate(zip(timestamps, values)):
            x = padding + (i / (len(values) - 1)) * (width - 2 * padding)
            y = padding + (1 - (val - min_val) / val_range) * (height - 2 * padding)
            
            if i == 0:
                path_data = f'M {x} {y}'
            else:
                path_data += f' L {x} {y}'
        
        svg += f'<path d="{path_data}" fill="none" stroke="#4CAF50" stroke-width="2"/>'
        
        # Data points
        for i, (ts, val) in enumerate(zip(timestamps, values)):
            x = padding + (i / (len(values) - 1)) * (width - 2 * padding)
            y = padding + (1 - (val - min_val) / val_range) * (height - 2 * padding)
            svg += f'<circle cx="{x}" cy="{y}" r="3" fill="#4CAF50"/>'
        
        # X-axis labels
        if len(timestamps) > 0:
            svg += f'<text x="{padding}" y="{height + 25}" font-family="Arial" font-size="10" fill="#666">{timestamps[0].strftime("%m/%d")}</text>'
            svg += f'<text x="{width - padding}" y="{height + 25}" font-family="Arial" font-size="10" fill="#666" text-anchor="end">{timestamps[-1].strftime("%m/%d")}</text>'
        
        svg += '</svg>'
        return svg

    def export_analytics_json(self, workflow_id: Optional[str] = None, 
                              include_time_series: bool = True,
                              include_alerts: bool = True) -> str:
        """Export analytics data as JSON for external BI tools"""
        export_data = {
            'export_timestamp': datetime.now().isoformat(),
            'version': '22.0.0',
            'workflow_metrics': {},
            'action_analytics': {},
            'user_behavior': {},
            'alerts': [],
            'time_series': {}
        }
        
        # Workflow metrics
        if workflow_id:
            metrics = self.get_execution_metrics(workflow_id)
            if metrics:
                export_data['workflow_metrics'][workflow_id] = metrics
        else:
            export_data['workflow_metrics'] = self.get_execution_metrics()
        
        # Action analytics
        for action_name, action_metrics in self.get_action_analytics().items():
            export_data['action_analytics'][action_name] = {
                'total_executions': action_metrics.total_executions,
                'success_count': action_metrics.success_count,
                'failure_count': action_metrics.failure_count,
                'success_rate': action_metrics.success_rate,
                'avg_duration': action_metrics.avg_duration,
                'last_executed': action_metrics.last_executed.isoformat() if action_metrics.last_executed else None
            }
        
        # User behavior
        export_data['user_behavior'] = self.get_user_behavior_analytics()
        
        # Alerts
        if include_alerts:
            for alert in self.alerts:
                export_data['alerts'].append({
                    'alert_id': alert.alert_id,
                    'workflow_id': alert.workflow_id,
                    'workflow_name': alert.workflow_name,
                    'severity': alert.severity.value,
                    'message': alert.message,
                    'timestamp': alert.timestamp.isoformat(),
                    'metric_name': alert.metric_name,
                    'metric_value': alert.metric_value,
                    'threshold': alert.threshold,
                    'acknowledged': alert.acknowledged,
                    'resolved': alert.resolved
                })
        
        # Time series
        if include_time_series:
            for wf_id, gran_dict in self.time_series.items():
                if workflow_id and wf_id != workflow_id:
                    continue
                export_data['time_series'][wf_id] = {}
                for gran, points in gran_dict.items():
                    export_data['time_series'][wf_id][gran] = [
                        {
                            'timestamp': p.timestamp.isoformat(),
                            'value': p.value,
                            'metadata': p.metadata
                        }
                        for p in points[-100:]  # Last 100 points per granularity
                    ]
        
        return json.dumps(export_data, indent=2, ensure_ascii=False)

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                self._save_alerts()
                return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.resolved = True
                alert.acknowledged = True
                self._save_alerts()
                return True
        return False

    def create_scheduled_report(self, name: str, report_type: str,
                               recipients: List[str],
                               include_sections: List[str]) -> ScheduledReport:
        """Create a scheduled report configuration"""
        report = ScheduledReport(
            report_id=f"report_{int(time.time() * 1000)}",
            name=name,
            report_type=report_type,
            recipients=recipients,
            include_sections=include_sections
        )
        
        # Calculate next generation time
        report.next_generation = self._calculate_next_report_time(report_type)
        
        self.scheduled_reports.append(report)
        self._save_reports()
        
        return report

    def _calculate_next_report_time(self, report_type: str) -> datetime:
        """Calculate next report generation time"""
        now = datetime.now()
        
        if report_type == 'weekly':
            # Next Monday
            days_until_monday = (7 - now.weekday()) % 7
            if days_until_monday == 0:
                days_until_monday = 7
            return now + timedelta(days=days_until_monday, hours=9)
        elif report_type == 'monthly':
            # First day of next month at 9am
            if now.month == 12:
                return datetime(now.year + 1, 1, 1, 9, 0, 0)
            else:
                return datetime(now.year, now.month + 1, 1, 9, 0, 0)
        else:
            return now + timedelta(days=1)

    def generate_report(self, report: ScheduledReport) -> Dict[str, Any]:
        """Generate a report based on scheduled configuration"""
        report_data = {
            'report_id': report.report_id,
            'name': report.name,
            'generated_at': datetime.now().isoformat(),
            'period': self._get_report_period(report.report_type),
            'sections': {}
        }
        
        for section in report.include_sections:
            if section == 'execution_metrics':
                report_data['sections']['execution_metrics'] = self.get_execution_metrics()
            elif section == 'action_analytics':
                report_data['sections']['action_analytics'] = {
                    name: {
                        'total_executions': m.total_executions,
                        'success_rate': m.success_rate,
                        'avg_duration': m.avg_duration
                    }
                    for name, m in self.get_action_analytics().items()
                }
            elif section == 'user_behavior':
                report_data['sections']['user_behavior'] = self.get_user_behavior_analytics()
            elif section == 'alerts':
                report_data['sections']['alerts'] = [
                    {'message': a.message, 'severity': a.severity.value, 'timestamp': a.timestamp.isoformat()}
                    for a in self.alerts[-30:]  # Last 30 alerts
                ]
            elif section == 'trends':
                trends = {}
                for wf_id in self.workflow_metrics:
                    direction, slope = self.detect_trends(wf_id)
                    trends[wf_id] = {
                        'direction': direction.value,
                        'slope': slope
                    }
                report_data['sections']['trends'] = trends
            elif section == 'anomaly_scores':
                report_data['sections']['anomaly_scores'] = {
                    wf_id: m.anomaly_score
                    for wf_id, m in self.workflow_metrics.items()
                }
        
        # Update report status
        report.last_generated = datetime.now()
        report.next_generation = self._calculate_next_report_time(report.report_type)
        self._save_reports()
        
        return report_data

    def _get_report_period(self, report_type: str) -> Dict[str, str]:
        """Get the time period covered by a report"""
        now = datetime.now()
        
        if report_type == 'weekly':
            start = now - timedelta(days=7)
        elif report_type == 'monthly':
            start = now - timedelta(days=30)
        else:
            start = now - timedelta(days=1)
        
        return {
            'start': start.isoformat(),
            'end': now.isoformat(),
            'type': report_type
        }

    def start_background_tasks(self) -> None:
        """Start background tasks for scheduled reports and maintenance"""
        if self._running:
            return
        
        self._running = True
        self._background_thread = threading.Thread(target=self._background_loop, daemon=True)
        self._background_thread.start()

    def stop_background_tasks(self) -> None:
        """Stop background tasks"""
        self._running = False
        if self._background_thread:
            self._background_thread.join(timeout=5)

    def _background_loop(self) -> None:
        """Background loop for scheduled tasks"""
        while self._running:
            try:
                self._process_scheduled_reports()
                self._cleanup_old_data()
            except Exception:
                pass
            
            time.sleep(60)  # Check every minute

    def _process_scheduled_reports(self) -> None:
        """Process any due scheduled reports"""
        now = datetime.now()
        
        for report in self.scheduled_reports:
            if report.enabled and report.next_generation and now >= report.next_generation:
                with self._report_lock:
                    self.generate_report(report)

    def _cleanup_old_data(self) -> None:
        """Clean up old execution records to prevent memory bloat"""
        cutoff = datetime.now() - timedelta(days=90)
        
        old_ids = [
            eid for eid, rec in self.executions.items()
            if rec.start_time < cutoff
        ]
        
        for eid in old_ids:
            del self.executions[eid]

    def _save_data(self) -> None:
        """Save analytics data to disk"""
        try:
            data = {
                'workflow_metrics': {
                    wf_id: {
                        'workflow_id': m.workflow_id,
                        'workflow_name': m.workflow_name,
                        'total_executions': m.total_executions,
                        'success_count': m.success_count,
                        'failure_count': m.failure_count,
                        'total_duration': m.total_duration,
                        'avg_duration': m.avg_duration,
                        'min_duration': m.min_duration,
                        'max_duration': m.max_duration,
                        'success_rate': m.success_rate,
                        'action_breakdown': m.action_breakdown,
                        'anomaly_score': m.anomaly_score,
                        'trend': m.trend.value
                    }
                    for wf_id, m in self.workflow_metrics.items()
                },
                'action_metrics': {
                    name: {
                        'action_name': m.action_name,
                        'total_executions': m.total_executions,
                        'success_count': m.success_count,
                        'failure_count': m.failure_count,
                        'total_duration': m.total_duration,
                        'avg_duration': m.avg_duration,
                        'success_rate': m.success_rate,
                        'last_executed': m.last_executed.isoformat() if m.last_executed else None
                    }
                    for name, m in self.action_metrics.items()
                },
                'user_activity': dict(self.user_activity),
                'peak_hours': dict(self.peak_hours),
                'last_updated': datetime.now().isoformat()
            }
            
            with open(self.analytics_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _load_data(self) -> None:
        """Load analytics data from disk"""
        if not os.path.exists(self.analytics_file):
            return
        
        try:
            with open(self.analytics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Restore workflow metrics
            for wf_id, m_data in data.get('workflow_metrics', {}).items():
                metrics = WorkflowMetrics(
                    workflow_id=m_data['workflow_id'],
                    workflow_name=m_data['workflow_name'],
                    total_executions=m_data.get('total_executions', 0),
                    success_count=m_data.get('success_count', 0),
                    failure_count=m_data.get('failure_count', 0),
                    total_duration=m_data.get('total_duration', 0.0),
                    avg_duration=m_data.get('avg_duration', 0.0),
                    min_duration=m_data.get('min_duration', float('inf')),
                    max_duration=m_data.get('max_duration', 0.0),
                    success_rate=m_data.get('success_rate', 0.0),
                    action_breakdown=m_data.get('action_breakdown', {}),
                    anomaly_score=m_data.get('anomaly_score', 0.0),
                    trend=TrendDirection(m_data.get('trend', 'stable'))
                )
                self.workflow_metrics[wf_id] = metrics
            
            # Restore action metrics
            for name, m_data in data.get('action_metrics', {}).items():
                metrics = ActionMetrics(
                    action_name=m_data['action_name'],
                    total_executions=m_data.get('total_executions', 0),
                    success_count=m_data.get('success_count', 0),
                    failure_count=m_data.get('failure_count', 0),
                    total_duration=m_data.get('total_duration', 0.0),
                    avg_duration=m_data.get('avg_duration', 0.0),
                    success_rate=m_data.get('success_rate', 0.0),
                    last_executed=datetime.fromisoformat(m_data['last_executed']) if m_data.get('last_executed') else None
                )
                self.action_metrics[name] = metrics
            
            # Restore user activity
            self.user_activity = defaultdict(list, data.get('user_activity', {}))
            self.peak_hours = defaultdict(int, data.get('peak_hours', {}))
            
        except Exception:
            pass

    def _save_alerts(self) -> None:
        """Save alerts to disk"""
        try:
            alerts_data = [
                {
                    'alert_id': a.alert_id,
                    'workflow_id': a.workflow_id,
                    'workflow_name': a.workflow_name,
                    'severity': a.severity.value,
                    'message': a.message,
                    'timestamp': a.timestamp.isoformat(),
                    'metric_name': a.metric_name,
                    'metric_value': a.metric_value,
                    'threshold': a.threshold,
                    'acknowledged': a.acknowledged,
                    'resolved': a.resolved
                }
                for a in self.alerts[-1000:]  # Keep last 1000 alerts
            ]
            
            with open(self.alerts_file, 'w', encoding='utf-8') as f:
                json.dump(alerts_data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def _save_reports(self) -> None:
        """Save scheduled reports to disk"""
        try:
            reports_data = [
                {
                    'report_id': r.report_id,
                    'name': r.name,
                    'report_type': r.report_type,
                    'enabled': r.enabled,
                    'last_generated': r.last_generated.isoformat() if r.last_generated else None,
                    'next_generation': r.next_generation.isoformat() if r.next_generation else None,
                    'recipients': r.recipients,
                    'include_sections': r.include_sections
                }
                for r in self.scheduled_reports
            ]
            
            with open(self.reports_file, 'w', encoding='utf-8') as f:
                json.dump(reports_data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all analytics"""
        return {
            'total_workflows': len(self.workflow_metrics),
            'total_executions': sum(m.total_executions for m in self.workflow_metrics.values()),
            'overall_success_rate': (
                sum(m.success_count for m in self.workflow_metrics.values()) /
                sum(m.total_executions for m in self.workflow_metrics.values())
                if sum(m.total_executions for m in self.workflow_metrics.values()) > 0 else 0
            ),
            'total_actions': len(self.action_metrics),
            'total_alerts': len([a for a in self.alerts if not a.resolved]),
            'active_alerts': len([a for a in self.alerts if not a.acknowledged and not a.resolved]),
            'scheduled_reports': len([r for r in self.scheduled_reports if r.enabled])
        }


def create_workflow_analytics(data_dir: str = "data") -> WorkflowAnalytics:
    """Factory function to create WorkflowAnalytics instance"""
    return WorkflowAnalytics(data_dir=data_dir)
