"""
Tests for Workflow Analytics Module
"""
import unittest
import tempfile
import shutil
import json
import os
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, mock_open

import sys
sys.path.insert(0, '/Users/guige/my_project')

from src.workflow_analytics import (
    WorkflowAnalytics,
    ExecutionRecord,
    ActionMetrics,
    WorkflowMetrics,
    TimeSeriesPoint,
    Alert,
    ScheduledReport,
    MetricType,
    TimeGranularity,
    AlertSeverity,
    TrendDirection,
    create_workflow_analytics
)


class TestExecutionRecord(unittest.TestCase):
    """Test ExecutionRecord dataclass"""

    def test_create_execution_record(self):
        """Test creating an execution record"""
        now = datetime.now()
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test Workflow",
            start_time=now,
            end_time=now + timedelta(seconds=10),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click", "type"],
            actions_failed=[],
            user_id="user_001"
        )
        
        self.assertEqual(record.execution_id, "exec_001")
        self.assertEqual(record.workflow_id, "wf_001")
        self.assertTrue(record.success)
        self.assertEqual(record.duration_seconds, 10.0)
        self.assertEqual(len(record.actions_executed), 2)


class TestActionMetrics(unittest.TestCase):
    """Test ActionMetrics dataclass"""

    def test_update_success(self):
        """Test updating action metrics with success"""
        metrics = ActionMetrics(action_name="click")
        metrics.update(success=True, duration=1.5)
        
        self.assertEqual(metrics.total_executions, 1)
        self.assertEqual(metrics.success_count, 1)
        self.assertEqual(metrics.failure_count, 0)
        self.assertEqual(metrics.avg_duration, 1.5)
        self.assertEqual(metrics.success_rate, 1.0)

    def test_update_failure(self):
        """Test updating action metrics with failure"""
        metrics = ActionMetrics(action_name="click")
        metrics.update(success=False, duration=2.0)
        
        self.assertEqual(metrics.total_executions, 1)
        self.assertEqual(metrics.success_count, 0)
        self.assertEqual(metrics.failure_count, 1)
        self.assertEqual(metrics.success_rate, 0.0)

    def test_multiple_updates(self):
        """Test multiple metric updates"""
        metrics = ActionMetrics(action_name="type")
        metrics.update(success=True, duration=1.0)
        metrics.update(success=True, duration=2.0)
        metrics.update(success=False, duration=3.0)
        
        self.assertEqual(metrics.total_executions, 3)
        self.assertEqual(metrics.success_count, 2)
        self.assertEqual(metrics.failure_count, 1)
        self.assertAlmostEqual(metrics.avg_duration, 2.0)
        self.assertAlmostEqual(metrics.success_rate, 2/3)


class TestWorkflowMetrics(unittest.TestCase):
    """Test WorkflowMetrics dataclass"""

    def test_update_success(self):
        """Test updating workflow metrics with successful execution"""
        metrics = WorkflowMetrics(workflow_id="wf_001", workflow_name="Test")
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(seconds=5),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click", "type"],
            actions_failed=[]
        )
        
        metrics.update(record)
        
        self.assertEqual(metrics.total_executions, 1)
        self.assertEqual(metrics.success_count, 1)
        self.assertEqual(metrics.failure_count, 0)
        self.assertEqual(metrics.success_rate, 1.0)
        self.assertEqual(metrics.min_duration, 5.0)
        self.assertEqual(metrics.max_duration, 5.0)

    def test_update_multiple_executions(self):
        """Test updating metrics across multiple executions"""
        metrics = WorkflowMetrics(workflow_id="wf_001", workflow_name="Test")
        
        for i in range(5):
            record = ExecutionRecord(
                execution_id=f"exec_{i}",
                workflow_id="wf_001",
                workflow_name="Test",
                start_time=datetime.now(),
                end_time=datetime.now() + timedelta(seconds=10),
                success=i % 2 == 0,
                duration_seconds=10.0 + i,
                actions_executed=["click"],
                actions_failed=[]
            )
            metrics.update(record)
        
        self.assertEqual(metrics.total_executions, 5)
        self.assertEqual(metrics.success_count, 3)
        self.assertEqual(metrics.failure_count, 2)


class TestWorkflowAnalytics(unittest.TestCase):
    """Test WorkflowAnalytics main class"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.analytics = WorkflowAnalytics(data_dir=self.temp_dir)

    def tearDown(self):
        """Clean up temporary files"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_init_creates_directories(self, mock_exists, mock_makedirs, mock_file):
        """Test initialization creates data directory"""
        mock_exists.return_value = False
        analytics = WorkflowAnalytics(data_dir=self.temp_dir)
        self.assertIsNotNone(analytics)

    def test_record_execution(self):
        """Test recording a workflow execution"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test Workflow",
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(seconds=10),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click", "type"],
            actions_failed=[],
            user_id="user_001"
        )
        
        self.analytics.record_execution(record)
        
        self.assertIn("exec_001", self.analytics.executions)
        self.assertIn("wf_001", self.analytics.workflow_metrics)
        self.assertEqual(len(self.analytics.alerts), 0)  # No alert for first execution

    def test_record_execution_updates_action_metrics(self):
        """Test that recording execution updates action metrics"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click", "type"],
            actions_failed=["hover"]
        )
        
        self.analytics.record_execution(record)
        
        self.assertIn("click", self.analytics.action_metrics)
        self.assertIn("type", self.analytics.action_metrics)
        self.assertIn("hover", self.analytics.action_metrics)
        self.assertEqual(self.analytics.action_metrics["click"].success_count, 1)
        self.assertEqual(self.analytics.action_metrics["hover"].failure_count, 1)

    def test_record_execution_updates_user_activity(self):
        """Test that recording execution updates user activity"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click"],
            actions_failed=[],
            user_id="user_001"
        )
        
        self.analytics.record_execution(record)
        
        self.assertIn("user_001", self.analytics.user_activity)
        self.assertEqual(len(self.analytics.user_activity["user_001"]), 1)

    def test_get_execution_metrics_single_workflow(self):
        """Test getting execution metrics for a specific workflow"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test Workflow",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        metrics = self.analytics.get_execution_metrics("wf_001")
        
        self.assertEqual(metrics['workflow_id'], "wf_001")
        self.assertEqual(metrics['workflow_name'], "Test Workflow")
        self.assertEqual(metrics['total_executions'], 1)
        self.assertEqual(metrics['success_rate'], 1.0)

    def test_get_execution_metrics_all_workflows(self):
        """Test getting execution metrics for all workflows"""
        for i in range(3):
            record = ExecutionRecord(
                execution_id=f"exec_{i}",
                workflow_id=f"wf_{i}",
                workflow_name=f"Workflow {i}",
                start_time=datetime.now(),
                success=True,
                duration_seconds=10.0,
                actions_executed=["click"],
                actions_failed=[]
            )
            self.analytics.record_execution(record)
        
        metrics = self.analytics.get_execution_metrics()
        
        self.assertEqual(len(metrics), 3)
        for i in range(3):
            self.assertIn(f"wf_{i}", metrics)

    def test_get_action_analytics(self):
        """Test getting action analytics"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click", "click", "type"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        analytics = self.analytics.get_action_analytics()
        
        self.assertIn("click", analytics)
        self.assertIn("type", analytics)
        self.assertEqual(analytics["click"].total_executions, 2)

    def test_get_most_used_actions(self):
        """Test getting most used actions"""
        for i in range(5):
            record = ExecutionRecord(
                execution_id=f"exec_{i}",
                workflow_id="wf_001",
                workflow_name="Test",
                start_time=datetime.now(),
                success=True,
                duration_seconds=5.0,
                actions_executed=["click"],
                actions_failed=[]
            )
            self.analytics.record_execution(record)
        
        top_actions = self.analytics.get_most_used_actions(limit=3)
        
        self.assertLessEqual(len(top_actions), 3)
        if top_actions:
            self.assertEqual(top_actions[0][0], "click")

    def test_get_user_behavior_analytics(self):
        """Test getting user behavior analytics"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click"],
            actions_failed=[],
            user_id="user_001"
        )
        self.analytics.record_execution(record)
        
        behavior = self.analytics.get_user_behavior_analytics()
        
        self.assertIn('most_active_users', behavior)
        self.assertIn('peak_hours', behavior)
        self.assertIn('most_active_workflows', behavior)

    def test_get_peak_usage_times(self):
        """Test getting peak usage times"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        peak_times = self.analytics.get_peak_usage_times()
        
        self.assertIsInstance(peak_times, list)

    def test_acknowledge_alert(self):
        """Test acknowledging an alert"""
        # First create an alert
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=5.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        # Add an alert manually
        alert = Alert(
            alert_id="alert_001",
            workflow_id="wf_001",
            workflow_name="Test",
            severity=AlertSeverity.WARNING,
            message="Test alert",
            timestamp=datetime.now(),
            metric_name="success_rate",
            metric_value=0.5,
            threshold=0.8
        )
        self.analytics.alerts.append(alert)
        
        result = self.analytics.acknowledge_alert("alert_001")
        self.assertTrue(result)
        self.assertTrue(self.analytics.alerts[0].acknowledged)

    def test_acknowledge_alert_not_found(self):
        """Test acknowledging non-existent alert"""
        result = self.analytics.acknowledge_alert("nonexistent")
        self.assertFalse(result)

    def test_resolve_alert(self):
        """Test resolving an alert"""
        alert = Alert(
            alert_id="alert_001",
            workflow_id="wf_001",
            workflow_name="Test",
            severity=AlertSeverity.WARNING,
            message="Test alert",
            timestamp=datetime.now(),
            metric_name="success_rate",
            metric_value=0.5,
            threshold=0.8
        )
        self.analytics.alerts.append(alert)
        
        result = self.analytics.resolve_alert("alert_001")
        self.assertTrue(result)
        self.assertTrue(self.analytics.alerts[0].resolved)
        self.assertTrue(self.analytics.alerts[0].acknowledged)

    def test_register_alert_callback(self):
        """Test registering an alert callback"""
        callback_called = []
        
        def test_callback(alert):
            callback_called.append(alert)
        
        self.analytics.register_alert_callback(test_callback)
        self.assertEqual(len(self.analytics.alert_callbacks), 1)

    def test_detect_trends_stable(self):
        """Test detecting stable trend"""
        # Need at least 3 executions for trend detection
        for i in range(5):
            record = ExecutionRecord(
                execution_id=f"exec_{i}",
                workflow_id="wf_001",
                workflow_name="Test",
                start_time=datetime.now() + timedelta(hours=i),
                success=True,
                duration_seconds=10.0,
                actions_executed=["click"],
                actions_failed=[]
            )
            self.analytics.record_execution(record)
        
        direction, slope = self.analytics.detect_trends("wf_001", MetricType.SUCCESS_RATE)
        
        self.assertIsInstance(direction, TrendDirection)
        self.assertIsInstance(slope, float)

    def test_detect_trends_insufficient_data(self):
        """Test trend detection with insufficient data"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        direction, slope = self.analytics.detect_trends("wf_001", MetricType.SUCCESS_RATE)
        
        self.assertEqual(direction, TrendDirection.STABLE)
        self.assertEqual(slope, 0.0)

    def test_get_time_series(self):
        """Test getting time series data"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        series = self.analytics.get_time_series("wf_001", TimeGranularity.DAILY)
        
        self.assertIsInstance(series, list)

    def test_get_time_series_with_date_filter(self):
        """Test getting time series with date filter"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        start_date = datetime.now() - timedelta(days=1)
        end_date = datetime.now() + timedelta(days=1)
        
        series = self.analytics.get_time_series(
            "wf_001", 
            TimeGranularity.DAILY,
            start_date=start_date,
            end_date=end_date
        )
        
        self.assertIsInstance(series, list)

    def test_create_scheduled_report(self):
        """Test creating a scheduled report"""
        report = self.analytics.create_scheduled_report(
            name="Weekly Report",
            report_type="weekly",
            recipients=["user@example.com"],
            include_sections=["execution_metrics", "alerts"]
        )
        
        self.assertIsNotNone(report)
        self.assertEqual(report.name, "Weekly Report")
        self.assertEqual(report.report_type, "weekly")
        self.assertIn("user@example.com", report.recipients)

    def test_generate_report(self):
        """Test generating a report"""
        report = ScheduledReport(
            report_id="report_001",
            name="Test Report",
            report_type="weekly",
            recipients=["user@example.com"],
            include_sections=["execution_metrics"]
        )
        
        report_data = self.analytics.generate_report(report)
        
        self.assertEqual(report_data['report_id'], "report_001")
        self.assertEqual(report_data['name'], "Test Report")
        self.assertIn('sections', report_data)

    def test_export_analytics_json(self):
        """Test exporting analytics as JSON"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        json_export = self.analytics.export_analytics_json(workflow_id="wf_001")
        
        self.assertIsInstance(json_export, str)
        data = json.loads(json_export)
        self.assertIn('workflow_metrics', data)

    def test_get_summary(self):
        """Test getting analytics summary"""
        record = ExecutionRecord(
            execution_id="exec_001",
            workflow_id="wf_001",
            workflow_name="Test",
            start_time=datetime.now(),
            success=True,
            duration_seconds=10.0,
            actions_executed=["click"],
            actions_failed=[]
        )
        self.analytics.record_execution(record)
        
        summary = self.analytics.get_summary()
        
        self.assertIn('total_workflows', summary)
        self.assertIn('total_executions', summary)
        self.assertIn('overall_success_rate', summary)

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_data(self, mock_makedirs, mock_file):
        """Test saving analytics data"""
        self.analytics._save_data()
        # Verify no exception was raised

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_load_data(self, mock_makedirs, mock_file):
        """Test loading analytics data"""
        self.analytics._load_data()
        # Verify no exception was raised


class TestCreateWorkflowAnalytics(unittest.TestCase):
    """Test create_workflow_analytics factory function"""

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_create_analytics(self, mock_exists, mock_makedirs, mock_file):
        """Test factory function creates analytics instance"""
        mock_exists.return_value = False
        analytics = create_workflow_analytics(data_dir="/tmp/test")
        self.assertIsInstance(analytics, WorkflowAnalytics)


if __name__ == '__main__':
    unittest.main()
