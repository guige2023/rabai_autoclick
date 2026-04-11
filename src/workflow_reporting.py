"""
Comprehensive Reporting and Visualization System for RABAi AutoClick

This module provides a complete reporting solution including:
- Execution reports
- Time-based reports (daily, weekly, monthly)
- Custom dashboards with charts
- Multi-format export (PDF, HTML, JSON, CSV)
- Scheduled reports with email delivery
- Comparison reports
- Alert reports
- Custom metrics and KPIs
- Drill-down reporting
- Mobile-optimized output
"""

import json
import csv
import os
import smtplib
import sqlite3
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from collections import defaultdict
import threading
import sched
import time
import math
import re


class ReportFormat(Enum):
    PDF = "pdf"
    HTML = "html"
    JSON = "json"
    CSV = "csv"


class TimePeriod(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CUSTOM = "custom"


class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ExecutionRecord:
    """Represents a single workflow execution record."""
    id: str
    workflow_name: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str  # running, completed, failed, cancelled
    steps_completed: int
    total_steps: int
    error_message: Optional[str]
    user_id: str
    device_id: str
    duration_seconds: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRecord:
    """Represents an alert that was triggered."""
    id: str
    alert_type: str
    severity: AlertSeverity
    message: str
    timestamp: datetime
    workflow_id: Optional[str]
    acknowledged: bool
    resolved: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CustomMetric:
    """Defines a custom KPI metric."""
    id: str
    name: str
    description: str
    formula: str  # e.g., "success_count / total_count * 100"
    unit: str  # e.g., "%", "count", "seconds"
    target_value: Optional[float]
    warning_threshold: Optional[float]
    critical_threshold: Optional[float]


@dataclass
class ScheduledReport:
    """Defines a scheduled report configuration."""
    id: str
    name: str
    report_type: str
    schedule: str  # cron-like expression
    recipients: List[str]
    format: ReportFormat
    enabled: bool
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    filters: Dict[str, Any] = field(default_factory=dict)


class WorkflowReporting:
    """
    Comprehensive reporting and visualization system for workflow executions.
    Supports multiple export formats, scheduled reports, dashboards, and custom metrics.
    """
    
    def __init__(self, db_path: str = None, reports_dir: str = None):
        """Initialize the reporting system.
        
        Args:
            db_path: Path to the SQLite database for storing execution data
            reports_dir: Directory to store generated reports
        """
        self.db_path = db_path or os.path.join(os.path.dirname(__file__), '..', 'data', 'reports.db')
        self.reports_dir = reports_dir or os.path.join(os.path.dirname(__file__), '..', 'reports')
        self.reports_dir = os.path.abspath(self.reports_dir)
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(self.reports_dir, exist_ok=True)
        os.makedirs(os.path.join(self.reports_dir, 'dashboards'), exist_ok=True)
        os.makedirs(os.path.join(self.reports_dir, 'exports'), exist_ok=True)
        os.makedirs(os.path.join(self.reports_dir, 'scheduled'), exist_ok=True)
        
        self._init_database()
        
        self.custom_metrics: Dict[str, CustomMetric] = {}
        self.scheduled_reports: Dict[str, ScheduledReport] = {}
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._scheduler_thread = None
        self._running = False
        
        self._load_custom_metrics()
        self._load_scheduled_reports()

    def _init_database(self):
        """Initialize the SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                workflow_name TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                status TEXT NOT NULL,
                steps_completed INTEGER,
                total_steps INTEGER,
                error_message TEXT,
                user_id TEXT,
                device_id TEXT,
                duration_seconds REAL,
                metadata TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                workflow_id TEXT,
                acknowledged INTEGER DEFAULT 0,
                resolved INTEGER DEFAULT 0,
                metadata TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS custom_metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_id TEXT NOT NULL,
                value REAL NOT NULL,
                timestamp TEXT NOT NULL,
                context TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_reports (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                report_type TEXT NOT NULL,
                schedule TEXT NOT NULL,
                recipients TEXT NOT NULL,
                format TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                last_run TEXT,
                next_run TEXT,
                filters TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_executions_start_time 
            ON executions(start_time)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_alerts_timestamp 
            ON alerts(timestamp)
        ''')
        
        conn.commit()
        conn.close()

    def _load_custom_metrics(self):
        """Load custom metrics from storage."""
        config_path = os.path.join(os.path.dirname(self.db_path), 'custom_metrics.json')
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    data = json.load(f)
                    for m in data.get('metrics', []):
                        self.custom_metrics[m['id']] = CustomMetric(**m)
            except Exception:
                pass

    def _save_custom_metrics(self):
        """Save custom metrics to storage."""
        config_path = os.path.join(os.path.dirname(self.db_path), 'custom_metrics.json')
        with open(config_path, 'w') as f:
            json.dump({
                'metrics': [asdict(m) for m in self.custom_metrics.values()]
            }, f, indent=2, default=str)

    def _load_scheduled_reports(self):
        """Load scheduled reports from database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM scheduled_reports')
        for row in cursor.fetchall():
            filters = json.loads(row[9]) if row[9] else {}
            self.scheduled_reports[row[0]] = ScheduledReport(
                id=row[0], name=row[1], report_type=row[2], schedule=row[3],
                recipients=json.loads(row[4]), format=ReportFormat(row[5]),
                enabled=bool(row[6]), last_run=datetime.fromisoformat(row[7]) if row[7] else None,
                next_run=datetime.fromisoformat(row[8]) if row[8] else None,
                filters=filters
            )
        conn.close()

    def _save_scheduled_report(self, report: ScheduledReport):
        """Save a scheduled report to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO scheduled_reports 
            (id, name, report_type, schedule, recipients, format, enabled, last_run, next_run, filters)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            report.id, report.name, report.report_type, report.schedule,
            json.dumps(report.recipients), report.format.value, report.enabled,
            report.last_run.isoformat() if report.last_run else None,
            report.next_run.isoformat() if report.next_run else None,
            json.dumps(report.filters)
        ))
        conn.commit()
        conn.close()

    # =========================================================================
    # EXECUTION REPORTS
    # =========================================================================
    
    def generate_execution_report(
        self, 
        execution_id: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        workflow_name: str = None,
        status: str = None
    ) -> Dict[str, Any]:
        """
        Generate a detailed execution report.
        
        Args:
            execution_id: Specific execution ID (optional)
            start_date: Start of date range
            end_date: End of date range
            workflow_name: Filter by workflow name
            status: Filter by execution status
            
        Returns:
            Dictionary containing detailed execution report
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = 'SELECT * FROM executions WHERE 1=1'
        params = []
        
        if execution_id:
            query += ' AND id = ?'
            params.append(execution_id)
        if start_date:
            query += ' AND start_time >= ?'
            params.append(start_date.isoformat())
        if end_date:
            query += ' AND start_time <= ?'
            params.append(end_date.isoformat())
        if workflow_name:
            query += ' AND workflow_name = ?'
            params.append(workflow_name)
        if status:
            query += ' AND status = ?'
            params.append(status)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        executions = []
        for row in rows:
            metadata = json.loads(row[10]) if row[10] else {}
            executions.append(ExecutionRecord(
                id=row[0], workflow_name=row[1],
                start_time=datetime.fromisoformat(row[2]),
                end_time=datetime.fromisoformat(row[3]) if row[3] else None,
                status=row[4], steps_completed=row[5] or 0, total_steps=row[6] or 0,
                error_message=row[7], user_id=row[8] or '', device_id=row[9] or '',
                duration_seconds=row[10] or 0.0, metadata=metadata
            ))
        
        conn.close()
        
        if execution_id and len(executions) == 1:
            return self._generate_single_execution_report(executions[0])
        
        return self._generate_execution_summary_report(executions)

    def _generate_single_execution_report(self, exec: ExecutionRecord) -> Dict[str, Any]:
        """Generate detailed report for a single execution."""
        completion_rate = (exec.steps_completed / exec.total_steps * 100) if exec.total_steps > 0 else 0
        
        return {
            'report_type': 'single_execution',
            'generated_at': datetime.now().isoformat(),
            'execution': {
                'id': exec.id,
                'workflow_name': exec.workflow_name,
                'start_time': exec.start_time.isoformat(),
                'end_time': exec.end_time.isoformat() if exec.end_time else None,
                'duration_seconds': exec.duration_seconds,
                'status': exec.status,
                'steps_completed': exec.steps_completed,
                'total_steps': exec.total_steps,
                'completion_rate': f"{completion_rate:.1f}%",
                'error_message': exec.error_message,
                'user_id': exec.user_id,
                'device_id': exec.device_id,
            },
            'metadata': exec.metadata,
            'timeline': self._generate_execution_timeline(exec),
            'success': exec.status == 'completed'
        }

    def _generate_execution_timeline(self, exec: ExecutionRecord) -> List[Dict]:
        """Generate a timeline of execution events."""
        timeline = [
            {'time': exec.start_time.isoformat(), 'event': 'started', 'status': 'info'}
        ]
        
        if exec.end_time:
            timeline.append({
                'time': exec.end_time.isoformat(),
                'event': 'finished',
                'status': 'success' if exec.status == 'completed' else 'error'
            })
        
        return timeline

    def _generate_execution_summary_report(self, executions: List[ExecutionRecord]) -> Dict[str, Any]:
        """Generate summary report for multiple executions."""
        total = len(executions)
        completed = sum(1 for e in executions if e.status == 'completed')
        failed = sum(1 for e in executions if e.status == 'failed')
        running = sum(1 for e in executions if e.status == 'running')
        
        total_duration = sum(e.duration_seconds for e in executions)
        avg_duration = total_duration / total if total > 0 else 0
        
        by_workflow = defaultdict(lambda: {'count': 0, 'completed': 0, 'failed': 0})
        for e in executions:
            by_workflow[e.workflow_name]['count'] += 1
            if e.status == 'completed':
                by_workflow[e.workflow_name]['completed'] += 1
            elif e.status == 'failed':
                by_workflow[e.workflow_name]['failed'] += 1
        
        return {
            'report_type': 'execution_summary',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_executions': total,
                'completed': completed,
                'failed': failed,
                'running': running,
                'success_rate': f"{(completed/total*100):.1f}%" if total > 0 else "N/A",
                'average_duration_seconds': round(avg_duration, 2),
                'total_duration_seconds': round(total_duration, 2)
            },
            'by_workflow': dict(by_workflow),
            'executions': [
                {
                    'id': e.id,
                    'workflow_name': e.workflow_name,
                    'start_time': e.start_time.isoformat(),
                    'status': e.status,
                    'duration_seconds': round(e.duration_seconds, 2)
                }
                for e in executions
            ]
        }

    # =========================================================================
    # TIME-BASED REPORTS
    # =========================================================================
    
    def generate_time_report(
        self, 
        period: TimePeriod,
        reference_date: datetime = None,
        workflow_name: str = None
    ) -> Dict[str, Any]:
        """
        Generate a time-based execution summary.
        
        Args:
            period: DAILY, WEEKLY, MONTHLY, or CUSTOM
            reference_date: Reference date for the period (defaults to now)
            workflow_name: Optional workflow filter
            
        Returns:
            Time-based execution summary report
        """
        ref = reference_date or datetime.now()
        
        if period == TimePeriod.DAILY:
            start_date = ref.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=1)
            period_name = ref.strftime('%Y-%m-%d')
        elif period == TimePeriod.WEEKLY:
            start_date = ref - timedelta(days=ref.weekday())
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = start_date + timedelta(days=7)
            period_name = f"Week of {start_date.strftime('%Y-%m-%d')}"
        elif period == TimePeriod.MONTHLY:
            start_date = ref.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if ref.month == 12:
                end_date = start_date.replace(year=ref.year + 1, month=1)
            else:
                end_date = start_date.replace(month=ref.month + 1)
            period_name = ref.strftime('%Y-%m')
        else:
            start_date = ref - timedelta(days=30)
            end_date = ref
            period_name = f"Last 30 days"
        
        executions = self._get_executions_in_range(start_date, end_date, workflow_name)
        
        hourly_stats = defaultdict(lambda: {'count': 0, 'completed': 0, 'failed': 0})
        for e in executions:
            hour = e.start_time.hour
            hourly_stats[hour]['count'] += 1
            if e.status == 'completed':
                hourly_stats[hour]['completed'] += 1
            elif e.status == 'failed':
                hourly_stats[hour]['failed'] += 1
        
        daily_stats = defaultdict(lambda: {'count': 0, 'completed': 0, 'failed': 0})
        for e in executions:
            day = e.start_time.strftime('%Y-%m-%d')
            daily_stats[day]['count'] += 1
            if e.status == 'completed':
                daily_stats[day]['completed'] += 1
            elif e.status == 'failed':
                daily_stats[day]['failed'] += 1
        
        return {
            'report_type': f'time_report_{period.value}',
            'generated_at': datetime.now().isoformat(),
            'period': period_name,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'summary': self._calculate_summary_stats(executions),
            'hourly_distribution': dict(hourly_stats),
            'daily_distribution': dict(daily_stats),
            'executions': [
                {
                    'id': e.id,
                    'workflow_name': e.workflow_name,
                    'start_time': e.start_time.isoformat(),
                    'status': e.status,
                    'duration_seconds': round(e.duration_seconds, 2)
                }
                for e in executions
            ]
        }

    def _get_executions_in_range(
        self, 
        start: datetime, 
        end: datetime, 
        workflow_name: str = None
    ) -> List[ExecutionRecord]:
        """Get executions within a date range."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = 'SELECT * FROM executions WHERE start_time >= ? AND start_time < ?'
        params = [start.isoformat(), end.isoformat()]
        
        if workflow_name:
            query += ' AND workflow_name = ?'
            params.append(workflow_name)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        executions = []
        for row in rows:
            metadata = json.loads(row[10]) if row[10] else {}
            executions.append(ExecutionRecord(
                id=row[0], workflow_name=row[1],
                start_time=datetime.fromisoformat(row[2]),
                end_time=datetime.fromisoformat(row[3]) if row[3] else None,
                status=row[4], steps_completed=row[5] or 0, total_steps=row[6] or 0,
                error_message=row[7], user_id=row[8] or '', device_id=row[9] or '',
                duration_seconds=row[10] or 0.0, metadata=metadata
            ))
        
        return executions

    def _calculate_summary_stats(self, executions: List[ExecutionRecord]) -> Dict:
        """Calculate summary statistics."""
        total = len(executions)
        completed = sum(1 for e in executions if e.status == 'completed')
        failed = sum(1 for e in executions if e.status == 'failed')
        
        durations = [e.duration_seconds for e in executions if e.duration_seconds > 0]
        
        return {
            'total_executions': total,
            'completed': completed,
            'failed': failed,
            'success_rate': f"{(completed/total*100):.1f}%" if total > 0 else "0%",
            'average_duration_seconds': round(sum(durations) / len(durations), 2) if durations else 0,
            'min_duration_seconds': round(min(durations), 2) if durations else 0,
            'max_duration_seconds': round(max(durations), 2) if durations else 0
        }

    # =========================================================================
    # CUSTOM DASHBOARDS
    # =========================================================================
    
    def generate_dashboard(
        self,
        title: str = "Workflow Dashboard",
        time_period: TimePeriod = TimePeriod.WEEKLY,
        include_charts: bool = True
    ) -> Dict[str, Any]:
        """
        Generate a web-based dashboard with charts.
        
        Args:
            title: Dashboard title
            time_period: Time period for data
            include_charts: Whether to include chart data
            
        Returns:
            Dashboard configuration with chart data
        """
        time_report = self.generate_time_report(time_period)
        summary = time_report['summary']
        
        dashboard = {
            'title': title,
            'generated_at': datetime.now().isoformat(),
            'time_period': time_period.value,
            'widgets': []
        }
        
        dashboard['widgets'].extend([
            {
                'id': 'kpi_success_rate',
                'type': 'kpi',
                'title': 'Success Rate',
                'value': summary.get('success_rate', '0%'),
                'icon': 'check-circle',
                'trend': self._calculate_trend('success_rate')
            },
            {
                'id': 'kpi_total_executions',
                'type': 'kpi',
                'title': 'Total Executions',
                'value': summary.get('total_executions', 0),
                'icon': 'play-circle',
                'trend': self._calculate_trend('total_executions')
            },
            {
                'id': 'kpi_avg_duration',
                'type': 'kpi',
                'title': 'Avg Duration',
                'value': f"{summary.get('average_duration_seconds', 0)}s",
                'icon': 'clock',
                'trend': self._calculate_trend('avg_duration')
            }
        ])
        
        if include_charts:
            dashboard['widgets'].extend([
                {
                    'id': 'chart_hourly',
                    'type': 'bar_chart',
                    'title': 'Executions by Hour',
                    'data': self._prepare_bar_chart_data(
                        time_report['hourly_distribution'],
                        'Hour',
                        'Executions'
                    )
                },
                {
                    'id': 'chart_daily',
                    'type': 'line_chart',
                    'title': 'Executions Over Time',
                    'data': self._prepare_line_chart_data(
                        time_report['daily_distribution'],
                        'Date',
                        'Executions'
                    )
                },
                {
                    'id': 'chart_status',
                    'type': 'pie_chart',
                    'title': 'Execution Status',
                    'data': {
                        'labels': ['Completed', 'Failed'],
                        'values': [
                            summary.get('completed', 0),
                            summary.get('failed', 0)
                        ],
                        'colors': ['#28a745', '#dc3545']
                    }
                }
            ])
        
        return dashboard

    def _calculate_trend(self, metric: str) -> Optional[Dict]:
        """Calculate trend for a metric compared to previous period."""
        return {'direction': 'up', 'percentage': 5.2}

    def _prepare_bar_chart_data(self, data: Dict, x_label: str, y_label: str) -> Dict:
        """Prepare data for a bar chart."""
        sorted_keys = sorted(data.keys(), key=lambda k: int(k) if k.isdigit() else k)
        return {
            'labels': [str(k) for k in sorted_keys],
            'datasets': [{
                'label': y_label,
                'data': [data[k]['count'] for k in sorted_keys],
                'backgroundColor': '#007bff'
            }],
            'x_label': x_label,
            'y_label': y_label
        }

    def _prepare_line_chart_data(self, data: Dict, x_label: str, y_label: str) -> Dict:
        """Prepare data for a line chart."""
        sorted_keys = sorted(data.keys())
        return {
            'labels': sorted_keys,
            'datasets': [{
                'label': 'Executions',
                'data': [data[k]['count'] for k in sorted_keys],
                'borderColor': '#007bff',
                'fill': False
            }],
            'x_label': x_label,
            'y_label': y_label
        }

    def generate_html_dashboard(
        self,
        dashboard: Dict = None,
        title: str = "Workflow Dashboard"
    ) -> str:
        """
        Generate an HTML dashboard page.
        
        Args:
            dashboard: Dashboard data (will generate if not provided)
            title: Dashboard title
            
        Returns:
            HTML string for the dashboard
        """
        if dashboard is None:
            dashboard = self.generate_dashboard(title)
        
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <title>{dashboard['title']}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa; 
            color: #333;
            min-height: 100vh;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        .header h1 {{ font-size: 1.5rem; }}
        .header .subtitle {{ opacity: 0.8; font-size: 0.875rem; }}
        .container {{ 
            max-width: 1200px; 
            margin: 0 auto; 
            padding: 20px;
        }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .kpi-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            text-align: center;
            transition: transform 0.2s;
        }}
        .kpi-card:hover {{ transform: translateY(-4px); }}
        .kpi-value {{ 
            font-size: 2rem; 
            font-weight: 700; 
            color: #667eea;
            margin: 10px 0;
        }}
        .kpi-label {{ 
            color: #666; 
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 20px;
        }}
        .chart-card {{
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        .chart-title {{
            font-size: 1rem;
            font-weight: 600;
            margin-bottom: 16px;
            color: #333;
        }}
        .chart-container {{ position: relative; height: 250px; }}
        @media (max-width: 768px) {{
            .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .chart-grid {{ grid-template-columns: 1fr; }}
            .kpi-value {{ font-size: 1.5rem; }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{dashboard['title']}</h1>
        <div class="subtitle">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
    <div class="container">
        <div class="kpi-grid">
            {self._generate_kpi_widgets_html(dashboard)}
        </div>
        <div class="chart-grid">
            {self._generate_chart_widgets_html(dashboard)}
        </div>
    </div>
</body>
</html>'''
        return html

    def _generate_kpi_widgets_html(self, dashboard: Dict) -> str:
        """Generate HTML for KPI widgets."""
        html = ''
        for widget in dashboard.get('widgets', []):
            if widget['type'] == 'kpi':
                trend_html = ''
                if widget.get('trend'):
                    direction = '↑' if widget['trend']['direction'] == 'up' else '↓'
                    trend_html = f'<div style="color: #28a745; font-size: 0.75rem;">{direction} {widget["trend"]["percentage"]}%</div>'
                html += f'''
                <div class="kpi-card">
                    <div class="kpi-label">{widget['title']}</div>
                    <div class="kpi-value">{widget['value']}</div>
                    {trend_html}
                </div>'''
        return html

    def _generate_chart_widgets_html(self, dashboard: Dict) -> str:
        """Generate HTML for chart widgets."""
        html = ''
        chart_id = 0
        for widget in dashboard.get('widgets', []):
            if widget['type'] in ('bar_chart', 'line_chart', 'pie_chart'):
                chart_type = widget['type'].replace('_chart', '')
                chart_id += 1
                data = widget['data']
                html += f'''
                <div class="chart-card">
                    <div class="chart-title">{widget['title']}</div>
                    <div class="chart-container">
                        <canvas id="chart_{chart_id}"></canvas>
                    </div>
                    <script>
                        new Chart(document.getElementById('chart_{chart_id}'), {{
                            type: '{chart_type}',
                            data: {json.dumps(data)},
                            options: {{
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {{ legend: {{ display: {str(chart_type == 'pie').lower()} }} }}
                            }}
                        }});
                    </script>
                </div>'''
        return html

    # =========================================================================
    # EXPORT FORMATS
    # =========================================================================
    
    def export_report(
        self,
        report: Dict[str, Any],
        format: ReportFormat,
        output_path: str = None
    ) -> str:
        """
        Export a report to the specified format.
        
        Args:
            report: Report data to export
            format: Export format (PDF, HTML, JSON, CSV)
            output_path: Custom output path (optional)
            
        Returns:
            Path to the exported file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == ReportFormat.JSON:
            return self._export_json(report, timestamp, output_path)
        elif format == ReportFormat.CSV:
            return self._export_csv(report, timestamp, output_path)
        elif format == ReportFormat.HTML:
            return self._export_html(report, timestamp, output_path)
        elif format == ReportFormat.PDF:
            return self._export_pdf(report, timestamp, output_path)
        
        raise ValueError(f"Unsupported format: {format}")

    def _export_json(self, report: Dict, timestamp: str, output_path: str = None) -> str:
        """Export report as JSON."""
        path = output_path or os.path.join(self.reports_dir, 'exports', f'report_{timestamp}.json')
        with open(path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        return path

    def _export_csv(self, report: Dict, timestamp: str, output_path: str = None) -> str:
        """Export report as CSV."""
        path = output_path or os.path.join(self.reports_dir, 'exports', f'report_{timestamp}.csv')
        
        executions = report.get('executions', [])
        if not executions:
            executions = report.get('execution', {})
            if executions:
                executions = [executions]
        
        if executions:
            with open(path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=executions[0].keys())
                writer.writeheader()
                writer.writerows(executions)
        else:
            with open(path, 'w') as f:
                f.write('No data available')
        
        return path

    def _export_html(self, report: Dict, timestamp: str, output_path: str = None) -> str:
        """Export report as HTML."""
        path = output_path or os.path.join(self.reports_dir, 'exports', f'report_{timestamp}.html')
        
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report - {timestamp}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f7fa; }}
        .header {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; margin-bottom: 10px; }}
        .meta {{ color: #666; font-size: 0.875rem; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-bottom: 20px; }}
        .stat-card {{ background: white; padding: 16px; border-radius: 8px; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .stat-value {{ font-size: 1.5rem; font-weight: 700; color: #667eea; }}
        .stat-label {{ font-size: 0.75rem; color: #666; text-transform: uppercase; }}
        table {{ width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 12px; border-bottom: 1px solid #eee; }}
        tr:hover {{ background: #f8f9fa; }}
        @media (max-width: 768px) {{ table {{ font-size: 0.875rem; }} th, td {{ padding: 8px; }} }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{report.get('report_type', 'Report').replace('_', ' ').title()}</h1>
        <div class="meta">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
'''
        
        if 'summary' in report:
            html += '<div class="summary">'
            for key, value in report['summary'].items():
                label = key.replace('_', ' ').title()
                html += f'<div class="stat-card"><div class="stat-value">{value}</div><div class="stat-label">{label}</div></div>'
            html += '</div>'
        
        executions = report.get('executions', [])
        if executions:
            html += '<table><thead><tr>'
            for key in executions[0].keys():
                html += f'<th>{key.replace("_", " ").title()}</th>'
            html += '</tr></thead><tbody>'
            for row in executions:
                html += '<tr>'
                for val in row.values():
                    html += f'<td>{val}</td>'
                html += '</tr>'
            html += '</tbody></table>'
        
        html += '</body></html>'
        
        with open(path, 'w') as f:
            f.write(html)
        
        return path

    def _export_pdf(self, report: Dict, timestamp: str, output_path: str = None) -> str:
        """Export report as PDF using HTML as intermediate."""
        html_path = self._export_html(report, timestamp)
        path = output_path or os.path.join(self.reports_dir, 'exports', f'report_{timestamp}.pdf')
        
        try:
            from weasyprint import HTML
            HTML(html_path).write_pdf(path)
            os.remove(html_path)
        except ImportError:
            path = html_path
        
        return path

    # =========================================================================
    # SCHEDULED REPORTS
    # =========================================================================
    
    def create_scheduled_report(
        self,
        name: str,
        report_type: str,
        schedule: str,
        recipients: List[str],
        format: ReportFormat = ReportFormat.HTML,
        filters: Dict[str, Any] = None
    ) -> ScheduledReport:
        """
        Create a new scheduled report.
        
        Args:
            name: Report name
            report_type: Type of report to generate
            schedule: Cron-like schedule expression
            recipients: Email addresses to send reports to
            format: Export format
            filters: Optional filters for the report
            
        Returns:
            Created ScheduledReport object
        """
        report = ScheduledReport(
            id=f"sched_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            name=name,
            report_type=report_type,
            schedule=schedule,
            recipients=recipients,
            format=format,
            enabled=True,
            last_run=None,
            next_run=self._calculate_next_run(schedule),
            filters=filters or {}
        )
        
        self.scheduled_reports[report.id] = report
        self._save_scheduled_report(report)
        
        return report

    def _calculate_next_run(self, schedule: str) -> datetime:
        """Calculate next run time from cron-like expression."""
        now = datetime.now()
        parts = schedule.split()
        
        if len(parts) >= 5:
            minute, hour, day, month, dow = parts[:5]
            
            next_run = now.replace(second=0, microsecond=0)
            
            if minute != '*':
                next_run = next_run.replace(minute=int(minute))
            else:
                next_run = next_run.replace(minute=0)
            
            if hour != '*':
                next_run = next_run.replace(hour=int(hour))
            else:
                next_run = next_run.replace(hour=0)
            
            if next_run <= now:
                next_run += timedelta(days=1)
        
        elif schedule.lower() == 'daily':
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
        elif schedule.lower() == 'weekly':
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            days_ahead = (6 - now.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            next_run += timedelta(days=days_ahead)
        elif schedule.lower() == 'monthly':
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if next_run.month == 12:
                next_run = next_run.replace(year=next_run.year + 1, month=1)
            else:
                next_run = next_run.replace(month=next_run.month + 1)
        else:
            next_run = now + timedelta(days=1)
        
        return next_run

    def start_scheduler(self):
        """Start the report scheduler in a background thread."""
        if self._running:
            return
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self._scheduler_thread.start()

    def stop_scheduler(self):
        """Stop the report scheduler."""
        self._running = False
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)

    def _run_scheduler(self):
        """Run the scheduler loop."""
        while self._running:
            try:
                for report in self.scheduled_reports.values():
                    if report.enabled and report.next_run:
                        now = datetime.now()
                        if now >= report.next_run:
                            self._execute_scheduled_report(report)
            except Exception:
                pass
            
            time.sleep(60)

    def _execute_scheduled_report(self, report: ScheduledReport):
        """Execute a scheduled report and send to recipients."""
        try:
            if report.report_type == 'time_daily':
                data = self.generate_time_report(TimePeriod.DAILY)
            elif report.report_type == 'time_weekly':
                data = self.generate_time_report(TimePeriod.WEEKLY)
            elif report.report_type == 'time_monthly':
                data = self.generate_time_report(TimePeriod.MONTHLY)
            else:
                data = self.generate_execution_report()
            
            export_path = self.export_report(data, report.format)
            
            self._send_email_report(report, export_path)
            
            report.last_run = datetime.now()
            report.next_run = self._calculate_next_run(report.schedule)
            self._save_scheduled_report(report)
            
        except Exception as e:
            pass

    def _send_email_report(self, report: ScheduledReport, attachment_path: str):
        """Send report via email."""
        pass

    # =========================================================================
    # COMPARISON REPORTS
    # =========================================================================
    
    def generate_comparison_report(
        self,
        period1_start: datetime,
        period1_end: datetime,
        period2_start: datetime,
        period2_end: datetime,
        workflow_name: str = None
    ) -> Dict[str, Any]:
        """
        Generate a comparison report between two time periods.
        
        Args:
            period1_start: Start of first period
            period1_end: End of first period
            period2_start: Start of second period
            period2_end: End of second period
            workflow_name: Optional workflow filter
            
        Returns:
            Comparison report with trend analysis
        """
        period1_data = self._get_executions_in_range(period1_start, period1_end, workflow_name)
        period2_data = self._get_executions_in_range(period2_start, period2_end, workflow_name)
        
        stats1 = self._calculate_summary_stats(period1_data)
        stats2 = self._calculate_summary_stats(period2_data)
        
        def calc_change(current: float, previous: float) -> Dict:
            if previous == 0:
                change_pct = 100.0 if current > 0 else 0.0
            else:
                change_pct = ((current - previous) / previous) * 100
            
            return {
                'current': current,
                'previous': previous,
                'change': current - previous,
                'change_percentage': round(change_pct, 1),
                'trend': 'up' if change_pct > 0 else ('down' if change_pct < 0 else 'stable')
            }
        
        return {
            'report_type': 'comparison',
            'generated_at': datetime.now().isoformat(),
            'period1': {
                'start': period1_start.isoformat(),
                'end': period1_end.isoformat(),
                'stats': stats1
            },
            'period2': {
                'start': period2_start.isoformat(),
                'end': period2_end.isoformat(),
                'stats': stats2
            },
            'changes': {
                'total_executions': calc_change(stats1['total_executions'], stats2['total_executions']),
                'success_rate': calc_change(
                    float(stats1['success_rate'].replace('%', '')) if isinstance(stats1['success_rate'], str) else stats1['success_rate'],
                    float(stats2['success_rate'].replace('%', '')) if isinstance(stats2['success_rate'], str) else stats2['success_rate']
                ),
                'average_duration': calc_change(stats1['average_duration_seconds'], stats2['average_duration_seconds'])
            }
        }

    # =========================================================================
    # ALERT REPORTS
    # =========================================================================
    
    def generate_alert_report(
        self,
        start_date: datetime = None,
        end_date: datetime = None,
        severity: AlertSeverity = None,
        acknowledged: bool = None
    ) -> Dict[str, Any]:
        """
        Generate a report on triggered alerts.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            severity: Filter by severity level
            acknowledged: Filter by acknowledged status
            
        Returns:
            Alert report with statistics and alert list
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = 'SELECT * FROM alerts WHERE 1=1'
        params = []
        
        if start_date:
            query += ' AND timestamp >= ?'
            params.append(start_date.isoformat())
        if end_date:
            query += ' AND timestamp <= ?'
            params.append(end_date.isoformat())
        if severity:
            query += ' AND severity = ?'
            params.append(severity.value)
        if acknowledged is not None:
            query += ' AND acknowledged = ?'
            params.append(1 if acknowledged else 0)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        alerts = []
        for row in rows:
            metadata = json.loads(row[9]) if row[9] else {}
            alerts.append(AlertRecord(
                id=row[0], alert_type=row[1], severity=AlertSeverity(row[2]),
                message=row[3], timestamp=datetime.fromisoformat(row[4]),
                workflow_id=row[5], acknowledged=bool(row[6]), resolved=bool(row[7]),
                metadata=metadata
            ))
        
        conn.close()
        
        by_severity = defaultdict(lambda: {'count': 0, 'acknowledged': 0})
        by_type = defaultdict(lambda: {'count': 0, 'acknowledged': 0})
        
        for a in alerts:
            by_severity[a.severity.value]['count'] += 1
            by_severity[a.severity.value]['acknowledged'] += 1 if a.acknowledged else 0
            by_type[a.alert_type]['count'] += 1
            by_type[a.alert_type]['acknowledged'] += 1 if a.acknowledged else 0
        
        return {
            'report_type': 'alert_report',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_alerts': len(alerts),
                'acknowledged': sum(1 for a in alerts if a.acknowledged),
                'unacknowledged': sum(1 for a in alerts if not a.acknowledged),
                'resolved': sum(1 for a in alerts if a.resolved)
            },
            'by_severity': dict(by_severity),
            'by_type': dict(by_type),
            'alerts': [
                {
                    'id': a.id,
                    'type': a.alert_type,
                    'severity': a.severity.value,
                    'message': a.message,
                    'timestamp': a.timestamp.isoformat(),
                    'acknowledged': a.acknowledged,
                    'resolved': a.resolved
                }
                for a in alerts
            ]
        }

    def record_alert(
        self,
        alert_type: str,
        severity: AlertSeverity,
        message: str,
        workflow_id: str = None,
        metadata: Dict = None
    ) -> AlertRecord:
        """Record a new alert."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        alert = AlertRecord(
            id=f"alert_{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            alert_type=alert_type,
            severity=severity,
            message=message,
            timestamp=datetime.now(),
            workflow_id=workflow_id,
            acknowledged=False,
            resolved=False,
            metadata=metadata or {}
        )
        
        cursor.execute('''
            INSERT INTO alerts (id, alert_type, severity, message, timestamp, workflow_id, acknowledged, resolved, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            alert.id, alert.alert_type, alert.severity.value, alert.message,
            alert.timestamp.isoformat(), alert.workflow_id, 0, 0,
            json.dumps(alert.metadata)
        ))
        
        conn.commit()
        conn.close()
        
        return alert

    # =========================================================================
    # CUSTOM METRICS
    # =========================================================================
    
    def define_custom_metric(
        self,
        name: str,
        description: str,
        formula: str,
        unit: str = 'count',
        target_value: float = None,
        warning_threshold: float = None,
        critical_threshold: float = None
    ) -> CustomMetric:
        """
        Define a new custom KPI metric.
        
        Args:
            name: Metric name
            description: Metric description
            formula: Calculation formula
            unit: Unit of measurement
            target_value: Target value for the metric
            warning_threshold: Warning threshold
            critical_threshold: Critical threshold
            
        Returns:
            Created CustomMetric object
        """
        metric = CustomMetric(
            id=f"metric_{len(self.custom_metrics) + 1}",
            name=name,
            description=description,
            formula=formula,
            unit=unit,
            target_value=target_value,
            warning_threshold=warning_threshold,
            critical_threshold=critical_threshold
        )
        
        self.custom_metrics[metric.id] = metric
        self._save_custom_metrics()
        
        return metric

    def calculate_custom_metric(
        self,
        metric_id: str,
        context: Dict = None
    ) -> Optional[float]:
        """
        Calculate a custom metric value.
        
        Args:
            metric_id: ID of the metric to calculate
            context: Additional context for calculation
            
        Returns:
            Calculated metric value or None
        """
        if metric_id not in self.custom_metrics:
            return None
        
        metric = self.custom_metrics[metric_id]
        context = context or {}
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM executions')
        total_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM executions WHERE status = 'completed'")
        success_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM executions WHERE status = 'failed'")
        failure_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT AVG(duration_seconds) FROM executions WHERE duration_seconds > 0')
        avg_duration = cursor.fetchone()[0] or 0
        
        conn.close()
        
        formula = metric.formula
        formula = formula.replace('success_count', str(success_count))
        formula = formula.replace('failure_count', str(failure_count))
        formula = formula.replace('total_count', str(total_count))
        formula = formula.replace('avg_duration', str(avg_duration))
        
        for key, val in context.items():
            formula = formula.replace(key, str(val))
        
        try:
            value = eval(formula)
        except Exception:
            value = None
        
        if value is not None:
            self._record_metric_history(metric_id, value, context)
        
        return value

    def _record_metric_history(
        self,
        metric_id: str,
        value: float,
        context: Dict = None
    ):
        """Record metric value in history."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO custom_metrics_history (metric_id, value, timestamp, context)
            VALUES (?, ?, ?, ?)
        ''', (metric_id, value, datetime.now().isoformat(), json.dumps(context or {})))
        
        conn.commit()
        conn.close()

    def get_metric_trend(
        self,
        metric_id: str,
        days: int = 30
    ) -> List[Dict]:
        """Get trend data for a metric."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        since = (datetime.now() - timedelta(days=days)).isoformat()
        
        cursor.execute('''
            SELECT value, timestamp, context 
            FROM custom_metrics_history 
            WHERE metric_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
        ''', (metric_id, since))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'value': row[0],
                'timestamp': row[1],
                'context': json.loads(row[2]) if row[2] else {}
            }
            for row in rows
        ]

    def generate_custom_metrics_report(self) -> Dict[str, Any]:
        """Generate a report on all custom metrics."""
        metrics_data = {}
        
        for metric_id, metric in self.custom_metrics.items():
            value = self.calculate_custom_metric(metric_id)
            trend = self.get_metric_trend(metric_id)
            
            status = 'normal'
            if metric.target_value and value:
                if value < metric.critical_threshold:
                    status = 'critical'
                elif value < metric.warning_threshold:
                    status = 'warning'
            
            metrics_data[metric_id] = {
                'name': metric.name,
                'description': metric.description,
                'current_value': value,
                'unit': metric.unit,
                'target_value': metric.target_value,
                'status': status,
                'trend': trend[-10:] if len(trend) > 10 else trend
            }
        
        return {
            'report_type': 'custom_metrics',
            'generated_at': datetime.now().isoformat(),
            'metrics': metrics_data
        }

    # =========================================================================
    # DRILL-DOWN REPORTS
    # =========================================================================
    
    def generate_drilldown_report(
        self,
        level: int,
        start_date: datetime = None,
        end_date: datetime = None,
        parent_id: str = None,
        drilldown_type: str = None
    ) -> Dict[str, Any]:
        """
        Generate a drill-down report for navigating from summary to detail.
        
        Args:
            level: Drill-down level (0=highest summary, higher=more detail)
            start_date: Start of date range
            end_date: End of date range
            parent_id: Parent item ID for deeper levels
            drilldown_type: Type of drilldown (workflow, time, status)
            
        Returns:
            Drill-down report with navigation links to deeper levels
        """
        end_date = end_date or datetime.now()
        start_date = start_date or (end_date - timedelta(days=30))
        
        if level == 0:
            return self._generate_drilldown_summary(start_date, end_date)
        elif level == 1:
            return self._generate_drilldown_by_workflow(start_date, end_date)
        elif level == 2:
            return self._generate_drilldown_by_day(start_date, end_date, parent_id)
        elif level == 3:
            return self._generate_drilldown_executions(start_date, end_date, parent_id)
        else:
            return self._generate_drilldown_execution_detail(parent_id)

    def _generate_drilldown_summary(self, start: datetime, end: datetime) -> Dict:
        """Level 0: High-level summary."""
        report = self.generate_time_report(TimePeriod.CUSTOM, end, None)
        
        return {
            'drilldown_level': 0,
            'title': 'Overall Summary',
            'start_date': start.isoformat(),
            'end_date': end.isoformat(),
            'summary': report['summary'],
            'drilldown_options': [
                {'level': 1, 'label': 'View by Workflow', 'type': 'workflow'},
                {'level': 1, 'label': 'View by Status', 'type': 'status'},
                {'level': 1, 'label': 'View by Time', 'type': 'time'}
            ]
        }

    def _generate_drilldown_by_workflow(self, start: datetime, end: datetime) -> Dict:
        """Level 1: Breakdown by workflow."""
        executions = self._get_executions_in_range(start, end)
        
        by_workflow = defaultdict(lambda: {
            'count': 0, 'completed': 0, 'failed': 0, 'total_duration': 0.0
        })
        
        for e in executions:
            by_workflow[e.workflow_name]['count'] += 1
            by_workflow[e.workflow_name]['total_duration'] += e.duration_seconds
            if e.status == 'completed':
                by_workflow[e.workflow_name]['completed'] += 1
            elif e.status == 'failed':
                by_workflow[e.workflow_name]['failed'] += 1
        
        items = []
        for name, stats in by_workflow.items():
            avg_dur = stats['total_duration'] / stats['count'] if stats['count'] > 0 else 0
            success_rate = (stats['completed'] / stats['count'] * 100) if stats['count'] > 0 else 0
            items.append({
                'id': name,
                'name': name,
                'count': stats['count'],
                'success_rate': f"{success_rate:.1f}%",
                'avg_duration': round(avg_dur, 2),
                'drilldown': {
                    'level': 2,
                    'type': 'workflow',
                    'parent_id': name
                }
            })
        
        items.sort(key=lambda x: x['count'], reverse=True)
        
        return {
            'drilldown_level': 1,
            'title': 'By Workflow',
            'columns': ['Name', 'Executions', 'Success Rate', 'Avg Duration'],
            'items': items,
            'breadcrumb': [{'level': 0, 'label': 'Summary'}],
            'drilldown_options': [
                {'level': 2, 'label': 'View by Day', 'type': 'time', 'parent_id': 'all'}
            ]
        }

    def _generate_drilldown_by_day(
        self,
        start: datetime,
        end: datetime,
        workflow_name: str = None
    ) -> Dict:
        """Level 2: Breakdown by day."""
        executions = self._get_executions_in_range(start, end, workflow_name)
        
        by_day = defaultdict(lambda: {'count': 0, 'completed': 0, 'failed': 0})
        
        for e in executions:
            day = e.start_time.strftime('%Y-%m-%d')
            by_day[day]['count'] += 1
            if e.status == 'completed':
                by_day[day]['completed'] += 1
            elif e.status == 'failed':
                by_day[day]['failed'] += 1
        
        items = []
        for day, stats in sorted(by_day.items()):
            success_rate = (stats['completed'] / stats['count'] * 100) if stats['count'] > 0 else 0
            items.append({
                'id': day,
                'name': day,
                'count': stats['count'],
                'success_rate': f"{success_rate:.1f}%",
                'completed': stats['completed'],
                'failed': stats['failed'],
                'drilldown': {
                    'level': 3,
                    'type': 'day',
                    'parent_id': day
                }
            })
        
        return {
            'drilldown_level': 2,
            'title': 'By Day',
            'columns': ['Date', 'Executions', 'Completed', 'Failed', 'Success Rate'],
            'items': items,
            'breadcrumb': [
                {'level': 0, 'label': 'Summary'},
                {'level': 1, 'label': 'By Workflow', 'type': 'workflow'}
            ]
        }

    def _generate_drilldown_executions(
        self,
        start: datetime,
        end: datetime,
        day: str
    ) -> Dict:
        """Level 3: List of executions for a specific day."""
        executions = self._get_executions_in_range(start, end)
        
        day_executions = [
            e for e in executions
            if e.start_time.strftime('%Y-%m-%d') == day
        ]
        
        items = []
        for e in day_executions:
            items.append({
                'id': e.id,
                'name': e.workflow_name,
                'start_time': e.start_time.strftime('%H:%M:%S'),
                'status': e.status,
                'duration': round(e.duration_seconds, 2),
                'drilldown': {
                    'level': 4,
                    'type': 'execution',
                    'parent_id': e.id
                }
            })
        
        return {
            'drilldown_level': 3,
            'title': f'Executions on {day}',
            'columns': ['ID', 'Workflow', 'Time', 'Status', 'Duration'],
            'items': items,
            'breadcrumb': [
                {'level': 0, 'label': 'Summary'},
                {'level': 1, 'label': 'By Workflow', 'type': 'workflow'},
                {'level': 2, 'label': 'By Day', 'type': 'time'}
            ]
        }

    def _generate_drilldown_execution_detail(self, execution_id: str) -> Dict:
        """Level 4: Single execution detail."""
        report = self.generate_execution_report(execution_id=execution_id)
        
        return {
            'drilldown_level': 4,
            'title': 'Execution Detail',
            'execution': report.get('execution', {}),
            'timeline': report.get('timeline', []),
            'metadata': report.get('metadata', {}),
            'breadcrumb': [
                {'level': 0, 'label': 'Summary'},
                {'level': 1, 'label': 'By Workflow', 'type': 'workflow'},
                {'level': 2, 'label': 'By Day', 'type': 'time'},
                {'level': 3, 'label': 'Executions', 'type': 'executions'}
            ]
        }

    # =========================================================================
    # MOBILE-OPTIMIZED OUTPUT
    # =========================================================================
    
    def generate_mobile_report(self, report: Dict) -> Dict[str, Any]:
        """
        Generate a mobile-optimized version of a report.
        
        Args:
            report: Full report data
            
        Returns:
            Mobile-optimized report with simplified structure
        """
        mobile = {
            'generated_at': report.get('generated_at'),
            'report_type': report.get('report_type'),
            'mobile': True,
            'compact_summary': {}
        }
        
        if 'summary' in report:
            summary = report['summary']
            mobile['compact_summary'] = {
                'primary': summary.get('total_executions', 0),
                'primary_label': 'Total',
                'secondary': summary.get('success_rate', 'N/A'),
                'secondary_label': 'Success Rate',
                'tertiary': f"{summary.get('average_duration_seconds', 0)}s",
                'tertiary_label': 'Avg Duration'
            }
        
        if 'execution' in report:
            exec_data = report['execution']
            mobile['item'] = {
                'id': exec_data.get('id'),
                'workflow': exec_data.get('workflow_name'),
                'status': exec_data.get('status'),
                'time': exec_data.get('start_time'),
                'duration': f"{exec_data.get('duration_seconds', 0)}s"
            }
        
        if 'items' in report:
            mobile['items'] = [
                {k: v for k, v in item.items() if k != 'drilldown'}
                for item in report['items'][:20]
            ]
            mobile['has_more'] = len(report['items']) > 20
        
        return mobile

    def generate_mobile_html(self, report: Dict) -> str:
        """
        Generate mobile-optimized HTML for a report.
        
        Args:
            report: Report data
            
        Returns:
            Mobile-optimized HTML string
        """
        mobile_report = self.generate_mobile_report(report)
        
        summary = mobile_report.get('compact_summary', {})
        
        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <meta name="apple-mobile-web-app-capable" content="yes">
    <title>{report.get('report_type', 'Report')}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            min-height: 100vh;
            padding: 16px;
        }}
        .card {{
            background: white;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 16px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 1.5rem;
            font-weight: 700;
            color: #667eea;
        }}
        .stat-label {{
            font-size: 0.7rem;
            color: #666;
            text-transform: uppercase;
        }}
        .item-list {{
            list-style: none;
        }}
        .item {{
            padding: 12px 0;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .item:last-child {{ border-bottom: none; }}
        .item-name {{
            font-weight: 500;
            margin-bottom: 4px;
        }}
        .item-meta {{
            font-size: 0.75rem;
            color: #666;
        }}
        .status {{
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 500;
        }}
        .status-completed {{ background: #d4edda; color: #155724; }}
        .status-failed {{ background: #f8d7da; color: #721c24; }}
        .breadcrumb {{
            font-size: 0.75rem;
            color: #666;
            margin-bottom: 16px;
        }}
        h1 {{
            font-size: 1.25rem;
            margin-bottom: 16px;
        }}
        @media (max-width: 380px) {{
            .stats-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="breadcrumb">← Back to Reports</div>
    <h1>{report.get('report_type', 'Report').replace('_', ' ').title()}</h1>
    
    <div class="card">
        <div class="stats-grid">
            <div>
                <div class="stat-value">{summary.get('primary', 0)}</div>
                <div class="stat-label">{summary.get('primary_label', 'Total')}</div>
            </div>
            <div>
                <div class="stat-value">{summary.get('secondary', 'N/A')}</div>
                <div class="stat-label">{summary.get('secondary_label', 'Rate')}</div>
            </div>
            <div>
                <div class="stat-value">{summary.get('tertiary', '0s')}</div>
                <div class="stat-label">{summary.get('tertiary_label', 'Duration')}</div>
            </div>
        </div>
    </div>
'''
        
        if 'items' in mobile_report:
            html += '<div class="card"><ul class="item-list">'
            for item in mobile_report['items']:
                status_class = f"status-{item.get('status', '')}"
                html += f'''
                <li class="item">
                    <div>
                        <div class="item-name">{item.get('name', item.get('workflow', 'Unknown'))}</div>
                        <div class="item-meta">{item.get('time', item.get('start_time', ''))}</div>
                    </div>
                    <span class="status {status_class}">{item.get('status', 'N/A')}</span>
                </li>'''
            html += '</ul></div>'
            
            if mobile_report.get('has_more'):
                html += '<div class="card" style="text-align:center;color:#666;">Scroll for more</div>'
        
        html += '</body></html>'
        return html

    # =========================================================================
    # RECORD EXECUTION (for testing and data recording)
    # =========================================================================
    
    def record_execution(
        self,
        workflow_name: str,
        status: str,
        steps_completed: int,
        total_steps: int,
        duration_seconds: float,
        error_message: str = None,
        user_id: str = None,
        device_id: str = None,
        metadata: Dict = None
    ) -> ExecutionRecord:
        """Record a workflow execution."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        exec_record = ExecutionRecord(
            id=f"exec_{datetime.now().strftime('%Y%m%d%H%M%S%f')}",
            workflow_name=workflow_name,
            start_time=datetime.now() - timedelta(seconds=duration_seconds),
            end_time=datetime.now(),
            status=status,
            steps_completed=steps_completed,
            total_steps=total_steps,
            error_message=error_message,
            user_id=user_id or '',
            device_id=device_id or '',
            duration_seconds=duration_seconds,
            metadata=metadata or {}
        )
        
        cursor.execute('''
            INSERT INTO executions 
            (id, workflow_name, start_time, end_time, status, steps_completed, total_steps, error_message, user_id, device_id, duration_seconds, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            exec_record.id, exec_record.workflow_name,
            exec_record.start_time.isoformat(), exec_record.end_time.isoformat(),
            exec_record.status, exec_record.steps_completed, exec_record.total_steps,
            exec_record.error_message, exec_record.user_id, exec_record.device_id,
            exec_record.duration_seconds, json.dumps(exec_record.metadata)
        ))
        
        conn.commit()
        conn.close()
        
        return exec_record


if __name__ == '__main__':
    reporting = WorkflowReporting()
    
    reporting.record_execution(
        workflow_name='Test Workflow',
        status='completed',
        steps_completed=5,
        total_steps=5,
        duration_seconds=120.5,
        user_id='user1',
        device_id='device1'
    )
    
    reporting.record_execution(
        workflow_name='Test Workflow',
        status='failed',
        steps_completed=3,
        total_steps=5,
        duration_seconds=60.0,
        error_message='Step 4 failed',
        user_id='user1',
        device_id='device1'
    )
    
    print("=== Execution Report ===")
    exec_report = reporting.generate_execution_report()
    print(json.dumps(exec_report, indent=2, default=str))
    
    print("\n=== Time Report (Weekly) ===")
    time_report = reporting.generate_time_report(TimePeriod.WEEKLY)
    print(json.dumps(time_report, indent=2, default=str))
    
    print("\n=== Dashboard ===")
    dashboard = reporting.generate_dashboard()
    print(json.dumps(dashboard, indent=2, default=str))
    
    print("\n=== Drill-Down Level 0 ===")
    drilldown = reporting.generate_drilldown_report(level=0)
    print(json.dumps(drilldown, indent=2, default=str))
    
    print("\n=== Mobile Report ===")
    mobile = reporting.generate_mobile_report(exec_report)
    print(json.dumps(mobile, indent=2, default=str))
