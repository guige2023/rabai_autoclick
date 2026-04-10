"""
Tests for workflow_reporting module - Comprehensive reporting and visualization system
for workflow executions, including time-based reports, dashboards, multi-format export,
scheduled reports, comparison reports, alerts, custom metrics, and drill-down reporting.
"""

import sys
import os
import json
import time
import unittest
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime, timedelta, date
from pathlib import Path

sys.path.insert(0, '/Users/guige/my_project')

# Import the workflow_reporting module
import src.workflow_reporting as wr_module
from src.workflow_reporting import (
    ReportFormat,
    TimePeriod,
    AlertSeverity,
    ExecutionRecord,
    AlertRecord,
    CustomMetric,
    ScheduledReport,
    WorkflowReporting
)


class TestReportFormatEnum(unittest.TestCase):
    """Test ReportFormat enum values."""

    def test_report_format_values(self):
        """Test all ReportFormat enum values."""
        self.assertEqual(ReportFormat.PDF.value, "pdf")
        self.assertEqual(ReportFormat.HTML.value, "html")
        self.assertEqual(ReportFormat.JSON.value, "json")
        self.assertEqual(ReportFormat.CSV.value, "csv")


class TestTimePeriodEnum(unittest.TestCase):
    """Test TimePeriod enum values."""

    def test_time_period_values(self):
        """Test all TimePeriod enum values."""
        self.assertEqual(TimePeriod.DAILY.value, "daily")
        self.assertEqual(TimePeriod.WEEKLY.value, "weekly")
        self.assertEqual(TimePeriod.MONTHLY.value, "monthly")
        self.assertEqual(TimePeriod.CUSTOM.value, "custom")


class TestAlertSeverityEnum(unittest.TestCase):
    """Test AlertSeverity enum values."""

    def test_alert_severity_values(self):
        """Test all AlertSeverity enum values."""
        self.assertEqual(AlertSeverity.INFO.value, "info")
        self.assertEqual(AlertSeverity.WARNING.value, "warning")
        self.assertEqual(AlertSeverity.ERROR.value, "error")
        self.assertEqual(AlertSeverity.CRITICAL.value, "critical")


class TestExecutionRecord(unittest.TestCase):
    """Test ExecutionRecord dataclass."""

    def test_execution_record_creation(self):
        """Test creating an ExecutionRecord."""
        record = ExecutionRecord(
            id="exec_001",
            workflow_name="test_workflow",
            start_time=datetime.now(),
            end_time=datetime.now(),
            status="completed",
            steps_completed=5,
            total_steps=5,
            error_message=None,
            user_id="user1",
            device_id="device1",
            duration_seconds=120.5
        )
        self.assertEqual(record.id, "exec_001")
        self.assertEqual(record.workflow_name, "test_workflow")
        self.assertEqual(record.status, "completed")
        self.assertEqual(record.steps_completed, 5)

    def test_execution_record_with_metadata(self):
        """Test ExecutionRecord with metadata."""
        record = ExecutionRecord(
            id="exec_002",
            workflow_name="test_workflow",
            start_time=datetime.now(),
            end_time=None,
            status="running",
            steps_completed=2,
            total_steps=10,
            error_message=None,
            user_id="user1",
            device_id="device1",
            duration_seconds=0,
            metadata={"branch": "main", "commit": "abc123"}
        )
        self.assertEqual(record.metadata["branch"], "main")


class TestAlertRecord(unittest.TestCase):
    """Test AlertRecord dataclass."""

    def test_alert_record_creation(self):
        """Test creating an AlertRecord."""
        alert = AlertRecord(
            id="alert_001",
            alert_type="workflow_failure",
            severity=AlertSeverity.ERROR,
            message="Workflow failed after 3 retries",
            timestamp=datetime.now(),
            workflow_id="wf_001",
            acknowledged=False,
            resolved=False
        )
        self.assertEqual(alert.id, "alert_001")
        self.assertEqual(alert.severity, AlertSeverity.ERROR)
        self.assertFalse(alert.acknowledged)


class TestCustomMetric(unittest.TestCase):
    """Test CustomMetric dataclass."""

    def test_custom_metric_creation(self):
        """Test creating a CustomMetric."""
        metric = CustomMetric(
            id="metric_001",
            name="Success Rate",
            description="Percentage of successful workflow executions",
            formula="success_count / total_count * 100",
            unit="%",
            target_value=95.0,
            warning_threshold=90.0,
            critical_threshold=80.0
        )
        self.assertEqual(metric.name, "Success Rate")
        self.assertEqual(metric.formula, "success_count / total_count * 100")
        self.assertEqual(metric.unit, "%")


class TestScheduledReport(unittest.TestCase):
    """Test ScheduledReport dataclass."""

    def test_scheduled_report_creation(self):
        """Test creating a ScheduledReport."""
        report = ScheduledReport(
            id="sched_001",
            name="Daily Summary",
            report_type="time_daily",
            schedule="0 9 * * *",
            recipients=["admin@example.com"],
            format=ReportFormat.HTML,
            enabled=True,
            last_run=None,
            next_run=datetime.now() + timedelta(days=1)
        )
        self.assertEqual(report.name, "Daily Summary")
        self.assertEqual(report.schedule, "0 9 * * *")
        self.assertTrue(report.enabled)


class TestWorkflowReporting(unittest.TestCase):
    """Test WorkflowReporting main class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test_reports.db')
        self.reports_dir = os.path.join(self.temp_dir, 'reports')
        
        # Patch os.makedirs and os.path.exists to avoid actual file operations
        self.patcher_makedirs = patch('os.makedirs')
        self.patcher_exists = patch('os.path.exists', return_value=False)
        self.patcher_makedirs.start()
        self.patcher_exists.start()
        
        self.reporting = WorkflowReporting(
            db_path=self.db_path,
            reports_dir=self.reports_dir
        )

    def tearDown(self):
        """Clean up after tests."""
        self.patcher_makedirs.stop()
        self.patcher_exists.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization(self):
        """Test WorkflowReporting initializes correctly."""
        self.assertTrue(os.path.dirname(self.reporting.db_path), self.temp_dir)
        self.assertEqual(self.reporting.reports_dir, self.reports_dir)
        self.assertEqual(len(self.reporting.custom_metrics), 0)
        self.assertEqual(len(self.reporting.scheduled_reports), 0)

    def test_init_database_creates_tables(self):
        """Test database initialization creates required tables."""
        conn = self.reporting._init_database()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        self.assertIn('executions', tables)
        self.assertIn('alerts', tables)
        self.assertIn('custom_metrics_history', tables)
        self.assertIn('scheduled_reports', tables)

    def test_generate_execution_report_empty(self):
        """Test generating report with no executions."""
        report = self.reporting.generate_execution_report()
        self.assertEqual(report['report_type'], 'execution_summary')
        self.assertEqual(report['summary']['total_executions'], 0)

    def test_generate_execution_report_single_execution(self):
        """Test generating report for single execution."""
        # Insert a test execution directly
        conn = self.reporting._init_database()
        cursor = conn.cursor()
        start = datetime.now().isoformat()
        end = (datetime.now() + timedelta(seconds=60)).isoformat()
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, error_message, 
                                    user_id, device_id, duration_seconds, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_001', 'TestWorkflow', start, end, 'completed', 5, 5, None, 
              'user1', 'device1', 60.0, '{}'))
        conn.commit()
        conn.close()
        
        report = self.reporting.generate_execution_report(execution_id='exec_001')
        self.assertEqual(report['report_type'], 'single_execution')
        self.assertEqual(report['execution']['id'], 'exec_001')
        self.assertEqual(report['success'], True)

    def test_generate_time_report_daily(self):
        """Test generating daily time report."""
        report = self.reporting.generate_time_report(TimePeriod.DAILY)
        self.assertIn('report_type', report)
        self.assertIn('summary', report)
        self.assertIn('hourly_distribution', report)
        self.assertIn('daily_distribution', report)

    def test_generate_time_report_weekly(self):
        """Test generating weekly time report."""
        report = self.reporting.generate_time_report(TimePeriod.WEEKLY)
        self.assertIn('period', report)
        self.assertEqual(report['report_type'], 'time_report_weekly')

    def test_generate_time_report_monthly(self):
        """Test generating monthly time report."""
        report = self.reporting.generate_time_report(TimePeriod.MONTHLY)
        self.assertIn('period', report)
        self.assertEqual(report['report_type'], 'time_report_monthly')

    def test_generate_time_report_custom(self):
        """Test generating custom time report."""
        report = self.reporting.generate_time_report(TimePeriod.CUSTOM)
        self.assertEqual(report['report_type'], 'time_report_custom')

    def test_calculate_summary_stats(self):
        """Test calculating summary statistics."""
        # Create mock executions
        from datetime import datetime
        mock_executions = [
            ExecutionRecord(
                id=f"exec_{i}", workflow_name="WF1",
                start_time=datetime.now(), end_time=datetime.now(),
                status="completed", steps_completed=5, total_steps=5,
                error_message=None, user_id="u1", device_id="d1",
                duration_seconds=60.0
            )
            for i in range(3)
        ]
        mock_executions.append(ExecutionRecord(
            id="exec_4", workflow_name="WF1",
            start_time=datetime.now(), end_time=datetime.now(),
            status="failed", steps_completed=2, total_steps=5,
            error_message="Error", user_id="u1", device_id="d1",
            duration_seconds=30.0
        ))
        
        stats = self.reporting._calculate_summary_stats(mock_executions)
        self.assertEqual(stats['total_executions'], 4)
        self.assertEqual(stats['completed'], 3)
        self.assertEqual(stats['failed'], 1)
        self.assertEqual(stats['success_rate'], "75.0%")

    def test_generate_dashboard(self):
        """Test generating dashboard configuration."""
        dashboard = self.reporting.generate_dashboard(
            title="Test Dashboard",
            time_period=TimePeriod.WEEKLY,
            include_charts=True
        )
        
        self.assertEqual(dashboard['title'], "Test Dashboard")
        self.assertIn('widgets', dashboard)
        self.assertTrue(len(dashboard['widgets']) > 0)
        
        # Check for KPI widgets
        kpi_widgets = [w for w in dashboard['widgets'] if w['type'] == 'kpi']
        self.assertTrue(len(kpi_widgets) >= 3)

    def test_generate_html_dashboard(self):
        """Test generating HTML dashboard."""
        dashboard = self.reporting.generate_dashboard(title="Test Dashboard")
        html = self.reporting.generate_html_dashboard(dashboard, title="My HTML Dashboard")
        
        self.assertIn('<!DOCTYPE html>', html)
        self.assertIn('Test Dashboard', html)
        self.assertIn('chart', html.lower())

    def test_export_report_json(self):
        """Test exporting report as JSON."""
        report = {'report_type': 'test', 'data': {'key': 'value'}}
        output_path = os.path.join(self.temp_dir, 'test_export.json')
        
        path = self.reporting.export_report(report, ReportFormat.JSON, output_path)
        
        self.assertEqual(path, output_path)
        self.assertTrue(os.path.exists(path))
        
        with open(path, 'r') as f:
            loaded = json.load(f)
            self.assertEqual(loaded['report_type'], 'test')

    def test_export_report_csv(self):
        """Test exporting report as CSV."""
        report = {
            'executions': [
                {'id': '1', 'workflow_name': 'WF1', 'status': 'completed'},
                {'id': '2', 'workflow_name': 'WF2', 'status': 'failed'}
            ]
        }
        output_path = os.path.join(self.temp_dir, 'test_export.csv')
        
        path = self.reporting.export_report(report, ReportFormat.CSV, output_path)
        
        self.assertEqual(path, output_path)
        self.assertTrue(os.path.exists(path))

    def test_export_report_html(self):
        """Test exporting report as HTML."""
        report = {
            'report_type': 'test_report',
            'summary': {'total': 10, 'passed': 8}
        }
        output_path = os.path.join(self.temp_dir, 'test_export.html')
        
        path = self.reporting.export_report(report, ReportFormat.HTML, output_path)
        
        self.assertEqual(path, output_path)
        self.assertTrue(os.path.exists(path))
        
        with open(path, 'r') as f:
            content = f.read()
            self.assertIn('Test Report', content)

    def test_create_scheduled_report(self):
        """Test creating a scheduled report."""
        report = self.reporting.create_scheduled_report(
            name="Weekly Summary",
            report_type="time_weekly",
            schedule="0 9 * * 1",
            recipients=["admin@example.com"],
            format=ReportFormat.HTML,
            filters={"status": "completed"}
        )
        
        self.assertEqual(report.name, "Weekly Summary")
        self.assertEqual(report.report_type, "time_weekly")
        self.assertTrue(report.enabled)
        self.assertIn(report.id, self.reporting.scheduled_reports)

    def test_calculate_next_run_daily(self):
        """Test calculating next run time for daily schedule."""
        next_run = self.reporting._calculate_next_run("daily")
        self.assertIsNotNone(next_run)
        self.assertGreaterEqual(next_run, datetime.now())

    def test_calculate_next_run_weekly(self):
        """Test calculating next run time for weekly schedule."""
        next_run = self.reporting._calculate_next_run("weekly")
        self.assertIsNotNone(next_run)

    def test_calculate_next_run_monthly(self):
        """Test calculating next run time for monthly schedule."""
        next_run = self.reporting._calculate_next_run("monthly")
        self.assertIsNotNone(next_run)

    def test_calculate_next_run_cron(self):
        """Test calculating next run time from cron expression."""
        next_run = self.reporting._calculate_next_run("30 14 * * *")
        self.assertIsNotNone(next_run)

    def test_generate_comparison_report(self):
        """Test generating comparison report between two periods."""
        now = datetime.now()
        period1_start = now - timedelta(days=14)
        period1_end = now - timedelta(days=7)
        period2_start = now - timedelta(days=7)
        period2_end = now
        
        report = self.reporting.generate_comparison_report(
            period1_start=period1_start,
            period1_end=period1_end,
            period2_start=period2_start,
            period2_end=period2_end
        )
        
        self.assertEqual(report['report_type'], 'comparison')
        self.assertIn('period1', report)
        self.assertIn('period2', report)
        self.assertIn('changes', report)

    def test_record_alert(self):
        """Test recording an alert."""
        alert = self.reporting.record_alert(
            alert_type="workflow_timeout",
            severity=AlertSeverity.WARNING,
            message="Workflow wf_001 timed out after 300 seconds",
            workflow_id="wf_001",
            metadata={"timeout": 300}
        )
        
        self.assertEqual(alert.alert_type, "workflow_timeout")
        self.assertEqual(alert.severity, AlertSeverity.WARNING)
        self.assertFalse(alert.acknowledged)

    def test_generate_alert_report(self):
        """Test generating alert report."""
        # First record some alerts
        self.reporting.record_alert(
            alert_type="test_alert",
            severity=AlertSeverity.INFO,
            message="Test info message"
        )
        
        report = self.reporting.generate_alert_report()
        
        self.assertEqual(report['report_type'], 'alert_report')
        self.assertIn('summary', report)
        self.assertIn('by_severity', report)
        self.assertIn('by_type', report)

    def test_define_custom_metric(self):
        """Test defining a custom metric."""
        metric = self.reporting.define_custom_metric(
            name="Average Execution Time",
            description="Average duration of workflow executions",
            formula="avg_duration",
            unit="seconds",
            target_value=60.0,
            warning_threshold=90.0,
            critical_threshold=120.0
        )
        
        self.assertEqual(metric.name, "Average Execution Time")
        self.assertIn(metric.id, self.reporting.custom_metrics)

    def test_calculate_custom_metric(self):
        """Test calculating a custom metric."""
        # Define a metric
        metric = self.reporting.define_custom_metric(
            name="Success Count",
            description="Number of successful executions",
            formula="success_count",
            unit="count"
        )
        
        # Insert some test data
        conn = self.reporting._init_database()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_1', 'WF1', datetime.now().isoformat(), datetime.now().isoformat(),
              'completed', 5, 5, 60.0))
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_2', 'WF1', datetime.now().isoformat(), datetime.now().isoformat(),
              'completed', 5, 5, 60.0))
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_3', 'WF1', datetime.now().isoformat(), datetime.now().isoformat(),
              'failed', 2, 5, 30.0))
        conn.commit()
        conn.close()
        
        value = self.reporting.calculate_custom_metric(metric.id)
        self.assertEqual(value, 2)  # 2 successful executions

    def test_get_metric_trend(self):
        """Test getting metric trend data."""
        # Define and calculate a metric
        metric = self.reporting.define_custom_metric(
            name="Test Metric",
            description="Test",
            formula="total_count",
            unit="count"
        )
        
        # Record some history
        self.reporting._record_metric_history(metric.id, 10.0)
        self.reporting._record_metric_history(metric.id, 15.0)
        
        trend = self.reporting.get_metric_trend(metric.id)
        self.assertIsInstance(trend, list)

    def test_generate_custom_metrics_report(self):
        """Test generating custom metrics report."""
        self.reporting.define_custom_metric(
            name="Test Metric",
            description="Test",
            formula="total_count",
            unit="count",
            target_value=100.0
        )
        
        report = self.reporting.generate_custom_metrics_report()
        self.assertEqual(report['report_type'], 'custom_metrics')
        self.assertIn('metrics', report)

    def test_generate_drilldown_report_level_0(self):
        """Test generating drilldown report at level 0 (summary)."""
        report = self.reporting.generate_drilldown_report(level=0)
        
        self.assertEqual(report['drilldown_level'], 0)
        self.assertIn('drilldown_options', report)

    def test_generate_drilldown_report_level_1(self):
        """Test generating drilldown report at level 1 (by workflow)."""
        report = self.reporting.generate_drilldown_report(
            level=1,
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now()
        )
        
        self.assertEqual(report['drilldown_level'], 1)

    def test_generate_drilldown_report_level_2(self):
        """Test generating drilldown report at level 2 (by day)."""
        report = self.reporting.generate_drilldown_report(
            level=2,
            start_date=datetime.now() - timedelta(days=7),
            end_date=datetime.now(),
            parent_id="WF1"
        )
        
        self.assertEqual(report['drilldown_level'], 2)

    def test_scheduler_start_stop(self):
        """Test starting and stopping the scheduler."""
        self.reporting.start_scheduler()
        self.assertTrue(self.reporting._running)
        
        self.reporting.stop_scheduler()
        self.assertFalse(self.reporting._running)


class TestWorkflowReportingEdgeCases(unittest.TestCase):
    """Test edge cases and error handling in WorkflowReporting."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test_reports.db')
        self.reports_dir = os.path.join(self.temp_dir, 'reports')
        
        self.patcher_makedirs = patch('os.makedirs')
        self.patcher_exists = patch('os.path.exists', return_value=False)
        self.patcher_makedirs.start()
        self.patcher_exists.start()
        
        self.reporting = WorkflowReporting(
            db_path=self.db_path,
            reports_dir=self.reports_dir
        )

    def tearDown(self):
        """Clean up after tests."""
        self.patcher_makedirs.stop()
        self.patcher_exists.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_executions_in_range_no_data(self):
        """Test getting executions in range with no data."""
        executions = self.reporting._get_executions_in_range(
            start=datetime.now() - timedelta(days=30),
            end=datetime.now()
        )
        self.assertEqual(len(executions), 0)

    def test_get_executions_in_range_with_filter(self):
        """Test getting executions in range with workflow filter."""
        # Insert test data
        conn = self.reporting._init_database()
        cursor = conn.cursor()
        now = datetime.now()
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_1', 'WF1', now.isoformat(), now.isoformat(), 'completed', 5, 5, 60.0))
        cursor.execute('''
            INSERT INTO executions (id, workflow_name, start_time, end_time, status, 
                                    steps_completed, total_steps, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('exec_2', 'WF2', now.isoformat(), now.isoformat(), 'completed', 5, 5, 60.0))
        conn.commit()
        conn.close()
        
        executions = self.reporting._get_executions_in_range(
            start=now - timedelta(days=1),
            end=now + timedelta(days=1),
            workflow_name='WF1'
        )
        
        self.assertEqual(len(executions), 1)
        self.assertEqual(executions[0].workflow_name, 'WF1')

    def test_export_unsupported_format(self):
        """Test exporting with unsupported format raises error."""
        # Create a mock format
        class UnsupportedFormat:
            value = "unsupported"
        
        report = {'report_type': 'test'}
        
        with self.assertRaises(ValueError):
            self.reporting.export_report(report, UnsupportedFormat())

    def test_calculate_trend(self):
        """Test trend calculation."""
        trend = self.reporting._calculate_trend('success_rate')
        self.assertIsNotNone(trend)
        self.assertIn('direction', trend)

    def test_prepare_bar_chart_data(self):
        """Test preparing bar chart data."""
        data = {
            '0': {'count': 5, 'completed': 4, 'failed': 1},
            '1': {'count': 10, 'completed': 8, 'failed': 2},
            '2': {'count': 3, 'completed': 3, 'failed': 0}
        }
        
        chart_data = self.reporting._prepare_bar_chart_data(data, 'Hour', 'Executions')
        
        self.assertIn('labels', chart_data)
        self.assertIn('datasets', chart_data)
        self.assertEqual(chart_data['x_label'], 'Hour')
        self.assertEqual(chart_data['y_label'], 'Executions')

    def test_prepare_line_chart_data(self):
        """Test preparing line chart data."""
        data = {
            '2024-01-01': {'count': 5, 'completed': 4, 'failed': 1},
            '2024-01-02': {'count': 10, 'completed': 8, 'failed': 2}
        }
        
        chart_data = self.reporting._prepare_line_chart_data(data, 'Date', 'Executions')
        
        self.assertIn('labels', chart_data)
        self.assertIn('datasets', chart_data)


if __name__ == '__main__':
    unittest.main()
