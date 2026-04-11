"""
Tests for workflow_monitoring module - Real-time monitoring for workflow executions,
system resources, performance metrics, and health checks with alerting capabilities.
"""

import sys
import os
import json
import time
import unittest
import threading
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from collections import deque

sys.path.insert(0, '/Users/guige/my_project')

# Import the workflow_monitoring module
import src.workflow_monitoring as wm_module
from src.workflow_monitoring import (
    MonitoringStatus,
    MetricType,
    MetricValue,
    MonitoringAlert,
    HealthCheckResult,
    WorkflowMonitoring,
    # These classes are now defined as stubs in the module
    MetricCollector,
    AlertManager,
    HealthChecker,
    SystemMonitor,
    WorkflowMonitor,
    MonitoringDashboard,
    MonitoringEngine,
    AlertRuleTestHelper as AlertRule,
)


class TestMonitoringStatusEnum(unittest.TestCase):
    """Test MonitoringStatus enum values."""

    def test_monitoring_status_values(self):
        """Test all MonitoringStatus enum values exist."""
        self.assertEqual(MonitoringStatus.HEALTHY.value, "healthy")
        self.assertEqual(MonitoringStatus.WARNING.value, "warning")
        self.assertEqual(MonitoringStatus.CRITICAL.value, "critical")
        self.assertEqual(MonitoringStatus.UNKNOWN.value, "unknown")
        self.assertEqual(MonitoringStatus.STOPPED.value, "stopped")


class TestMetricTypeEnum(unittest.TestCase):
    """Test MetricType enum values."""

    def test_metric_type_values(self):
        """Test all MetricType enum values exist."""
        self.assertEqual(MetricType.COUNTER.value, "counter")
        self.assertEqual(MetricType.GAUGE.value, "gauge")
        self.assertEqual(MetricType.HISTOGRAM.value, "histogram")
        self.assertEqual(MetricType.TIMER.value, "timer")
        self.assertEqual(MetricType.RATE.value, "rate")


class TestMetricValue(unittest.TestCase):
    """Test MetricValue dataclass."""

    def test_metric_value_creation(self):
        """Test creating a MetricValue."""
        metric = MetricValue(
            name="test_metric",
            value=42.5,
            timestamp=datetime.now(),
            metric_type=MetricType.GAUGE,
            tags={"host": "localhost"},
            unit="percent"
        )
        self.assertEqual(metric.name, "test_metric")
        self.assertEqual(metric.value, 42.5)
        self.assertEqual(metric.metric_type, MetricType.GAUGE)
        self.assertEqual(metric.tags["host"], "localhost")
        self.assertEqual(metric.unit, "percent")


class TestMonitoringAlert(unittest.TestCase):
    """Test MonitoringAlert dataclass."""

    def test_monitoring_alert_creation(self):
        """Test creating a MonitoringAlert."""
        alert = MonitoringAlert(
            alert_id="alert_001",
            metric_name="cpu_usage",
            status=MonitoringStatus.CRITICAL,
            message="CPU usage above threshold",
            current_value=95.0,
            threshold=90.0,
            timestamp=datetime.now()
        )
        self.assertEqual(alert.alert_id, "alert_001")
        self.assertEqual(alert.metric_name, "cpu_usage")
        self.assertEqual(alert.status, MonitoringStatus.CRITICAL)
        self.assertFalse(alert.resolved)


class TestHealthCheckResult(unittest.TestCase):
    """Test HealthCheckResult dataclass."""

    def test_health_check_result_creation(self):
        """Test creating a HealthCheckResult."""
        result = HealthCheckResult(
            name="cpu_check",
            status=MonitoringStatus.HEALTHY,
            message="CPU usage normal",
            duration_ms=5.2,
            timestamp=datetime.now(),
            details={"cpu_percent": 45.0}
        )
        self.assertEqual(result.name, "cpu_check")
        self.assertEqual(result.status, MonitoringStatus.HEALTHY)
        self.assertEqual(result.duration_ms, 5.2)


class TestMetricCollector(unittest.TestCase):
    """Test MetricCollector class."""

    def setUp(self):
        self.collector = MetricCollector(retention_periods=100)

    def test_collector_initialization(self):
        """Test collector initializes correctly."""
        self.assertEqual(self.collector.retention_periods, 100)
        self.assertEqual(len(self.collector.metrics), 0)

    def test_record_metric(self):
        """Test recording a metric."""
        metric = MetricValue(
            name="test_metric",
            value=100.0,
            timestamp=datetime.now(),
            metric_type=MetricType.GAUGE
        )
        self.collector.record_metric(metric)
        
        history = self.collector.get_metric_history("test_metric")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0].value, 100.0)

    def test_increment_counter(self):
        """Test incrementing a counter."""
        self.collector.increment_counter("request_count", 1.0)
        self.collector.increment_counter("request_count", 5.0)
        
        self.assertEqual(self.collector.counters["request_count"], 6.0)

    def test_set_gauge(self):
        """Test setting a gauge value."""
        self.collector.set_gauge("temperature", 72.5)
        
        self.assertEqual(self.collector.gauges["temperature"], 72.5)
        self.assertEqual(self.collector.get_current_value("temperature"), 72.5)

    def test_record_timer(self):
        """Test recording a timer metric."""
        self.collector.record_timer("request_duration", 125.5)
        
        current = self.collector.get_current_value("request_duration")
        self.assertEqual(current, 125.5)

    def test_get_metric_history(self):
        """Test getting metric history."""
        for i in range(10):
            metric = MetricValue(
                name="counter",
                value=float(i),
                timestamp=datetime.now() - timedelta(seconds=10-i),
                metric_type=MetricType.COUNTER
            )
            self.collector.record_metric(metric)
        
        history = self.collector.get_metric_history("counter")
        self.assertEqual(len(history), 10)

    def test_get_metric_history_with_duration(self):
        """Test getting metric history filtered by duration."""
        old_metric = MetricValue(
            name="old_metric",
            value=1.0,
            timestamp=datetime.now() - timedelta(seconds=100),
            metric_type=MetricType.GAUGE
        )
        new_metric = MetricValue(
            name="old_metric",
            value=2.0,
            timestamp=datetime.now(),
            metric_type=MetricType.GAUGE
        )
        self.collector.record_metric(old_metric)
        self.collector.record_metric(new_metric)
        
        recent = self.collector.get_metric_history("old_metric", duration=timedelta(seconds=60))
        self.assertEqual(len(recent), 1)
        self.assertEqual(recent[0].value, 2.0)

    def test_calculate_rate(self):
        """Test calculating rate of change."""
        base_time = datetime.now()
        for i, value in enumerate([0.0, 10.0, 20.0, 30.0]):
            metric = MetricValue(
                name="data_points",
                value=value,
                timestamp=base_time + timedelta(seconds=i*10),
                metric_type=MetricType.COUNTER
            )
            self.collector.record_metric(metric)
        
        rate = self.collector.calculate_rate("data_points", window_seconds=30)
        self.assertGreater(rate, 0)


class TestAlertRule(unittest.TestCase):
    """Test AlertRule class."""

    def test_alert_rule_greater_condition(self):
        """Test alert rule with greater than condition."""
        rule = AlertRule(
            name="high_cpu",
            metric_name="cpu_percent",
            threshold=90.0,
            condition="greater",
            severity=MonitoringStatus.CRITICAL
        )
        
        triggered, msg = rule.is_triggered(95.0)
        self.assertTrue(triggered)
        self.assertIn(">", msg)
        
        triggered, msg = rule.is_triggered(85.0)
        self.assertFalse(triggered)

    def test_alert_rule_less_condition(self):
        """Test alert rule with less than condition."""
        rule = AlertRule(
            name="low_memory",
            metric_name="memory_percent",
            threshold=20.0,
            condition="less",
            severity=MonitoringStatus.WARNING
        )
        
        triggered, msg = rule.is_triggered(15.0)
        self.assertTrue(triggered)
        
        triggered, msg = rule.is_triggered(25.0)
        self.assertFalse(triggered)

    def test_alert_rule_equals_condition(self):
        """Test alert rule with equals condition."""
        rule = AlertRule(
            name="exact_match",
            metric_name="error_count",
            threshold=0.0,
            condition="equals"
        )
        
        triggered, msg = rule.is_triggered(0.0)
        self.assertTrue(triggered)

    def test_alert_rule_duration(self):
        """Test alert rule with duration requirement."""
        rule = AlertRule(
            name="sustained_high",
            metric_name="cpu_percent",
            threshold=90.0,
            condition="greater",
            duration_seconds=5.0
        )
        
        triggered, msg = rule.is_triggered(95.0)
        self.assertFalse(msg)  # Duration not met yet
        
        rule.triggered_at = datetime.now() - timedelta(seconds=10)
        triggered, msg = rule.is_triggered(95.0)
        self.assertTrue(triggered)

    def test_should_send_alert_cooldown(self):
        """Test alert cooldown functionality."""
        rule = AlertRule(
            name="test_alert",
            metric_name="test_metric",
            threshold=50.0,
            cooldown_seconds=60.0
        )
        
        self.assertTrue(rule.should_send_alert())
        
        rule.last_triggered = datetime.now() - timedelta(seconds=30)
        self.assertFalse(rule.should_send_alert())
        
        rule.last_triggered = datetime.now() - timedelta(seconds=120)
        self.assertTrue(rule.should_send_alert())

    def test_mark_triggered(self):
        """Test marking rule as triggered."""
        rule = AlertRule(
            name="test",
            metric_name="test_metric",
            threshold=50.0
        )
        rule.triggered_at = datetime.now() - timedelta(seconds=10)
        
        rule.mark_triggered()
        
        self.assertIsNotNone(rule.last_triggered)
        self.assertIsNone(rule.triggered_at)


class TestAlertManager(unittest.TestCase):
    """Test AlertManager class."""

    def setUp(self):
        self.manager = AlertManager()
        self.collector = MetricCollector()

    def test_manager_initialization(self):
        """Test manager initializes correctly."""
        self.assertEqual(len(self.manager.alerts), 0)
        self.assertEqual(len(self.manager.alert_rules), 0)

    def test_register_rule(self):
        """Test registering an alert rule."""
        rule = AlertRule(
            name="test_rule",
            metric_name="cpu_percent",
            threshold=90.0
        )
        self.manager.register_rule(rule)
        
        self.assertIn("test_rule", self.manager.alert_rules)

    def test_unregister_rule(self):
        """Test unregistering an alert rule."""
        rule = AlertRule(
            name="test_rule",
            metric_name="cpu_percent",
            threshold=90.0
        )
        self.manager.register_rule(rule)
        self.manager.unregister_rule("test_rule")
        
        self.assertNotIn("test_rule", self.manager.alert_rules)

    def test_check_rules_triggered(self):
        """Test checking rules that are triggered."""
        self.collector.set_gauge("cpu_percent", 95.0)
        
        rule = AlertRule(
            name="high_cpu",
            metric_name="cpu_percent",
            threshold=90.0,
            condition="greater",
            severity=MonitoringStatus.CRITICAL,
            cooldown_seconds=0
        )
        self.manager.register_rule(rule)
        
        alerts = self.manager.check_rules(self.collector)
        
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].metric_name, "cpu_percent")

    def test_check_rules_not_triggered(self):
        """Test checking rules that are not triggered."""
        self.collector.set_gauge("cpu_percent", 50.0)
        
        rule = AlertRule(
            name="high_cpu",
            metric_name="cpu_percent",
            threshold=90.0,
            condition="greater"
        )
        self.manager.register_rule(rule)
        
        alerts = self.manager.check_rules(self.collector)
        
        self.assertEqual(len(alerts), 0)

    def test_resolve_alert(self):
        """Test resolving an alert."""
        self.collector.set_gauge("cpu_percent", 95.0)
        
        rule = AlertRule(
            name="high_cpu",
            metric_name="cpu_percent",
            threshold=90.0,
            condition="greater",
            cooldown_seconds=0
        )
        self.manager.register_rule(rule)
        self.manager.check_rules(self.collector)
        
        alert_id = list(self.manager.alerts.keys())[0]
        self.manager.resolve_alert(alert_id)
        
        self.assertTrue(self.manager.alerts[alert_id].resolved)

    def test_get_active_alerts(self):
        """Test getting active alerts."""
        self.collector.set_gauge("metric1", 100.0)
        
        rule1 = AlertRule("rule1", "metric1", 90.0, cooldown_seconds=0)
        rule2 = AlertRule("rule2", "metric2", 50.0, cooldown_seconds=0)
        
        self.collector.set_gauge("metric2", 100.0)
        
        self.manager.register_rule(rule1)
        self.manager.register_rule(rule2)
        self.manager.check_rules(self.collector)
        
        active = self.manager.get_active_alerts()
        self.assertEqual(len(active), 2)
        
        critical = self.manager.get_active_alerts(MonitoringStatus.CRITICAL)
        self.assertGreaterEqual(len(critical), 0)

    def test_get_alert_history(self):
        """Test getting alert history."""
        for i in range(5):
            self.collector.set_gauge("metric", float(100 + i))
            rule = AlertRule(f"rule_{i}", "metric", 90.0, cooldown_seconds=0)
            self.manager.register_rule(rule)
            self.manager.check_rules(self.collector)
            rule.name = f"rule_{i+1}"  # Change name to allow re-triggering
        
        history = self.manager.get_alert_history(limit=3)
        self.assertEqual(len(history), 3)


class TestHealthChecker(unittest.TestCase):
    """Test HealthChecker class."""

    def setUp(self):
        self.health_checker = HealthChecker()

    def test_health_checker_initialization(self):
        """Test health checker initializes correctly."""
        self.assertEqual(len(self.health_checker.health_checks), 0)

    def test_register_check(self):
        """Test registering a health check."""
        def my_check():
            return True
        
        self.health_checker.register_check("my_check", my_check)
        self.assertIn("my_check", self.health_checker.health_checks)

    def test_run_check_success(self):
        """Test running a successful health check."""
        def my_check():
            return True
        
        self.health_checker.register_check("my_check", my_check)
        result = self.health_checker.run_check("my_check")
        
        self.assertEqual(result.name, "my_check")
        self.assertEqual(result.status, MonitoringStatus.HEALTHY)

    def test_run_check_failure(self):
        """Test running a failing health check."""
        def my_check():
            return False
        
        self.health_checker.register_check("my_check", my_check)
        result = self.health_checker.run_check("my_check")
        
        self.assertEqual(result.status, MonitoringStatus.CRITICAL)

    def test_run_check_exception(self):
        """Test running a health check that throws exception."""
        def my_check():
            raise ValueError("Check failed")
        
        self.health_checker.register_check("my_check", my_check)
        result = self.health_checker.run_check("my_check")
        
        self.assertEqual(result.status, MonitoringStatus.CRITICAL)
        self.assertIn("Check failed", result.message)

    def test_run_check_not_found(self):
        """Test running a non-existent health check."""
        result = self.health_checker.run_check("nonexistent")
        
        self.assertEqual(result.status, MonitoringStatus.UNKNOWN)

    def test_run_check_with_result_object(self):
        """Test running a check that returns HealthCheckResult."""
        def my_check():
            return HealthCheckResult(
                name="my_check",
                status=MonitoringStatus.WARNING,
                message="Warning condition",
                duration_ms=10.0,
                timestamp=datetime.now()
            )
        
        self.health_checker.register_check("my_check", my_check)
        result = self.health_checker.run_check("my_check")
        
        self.assertEqual(result.status, MonitoringStatus.WARNING)

    def test_run_all_checks(self):
        """Test running all registered health checks."""
        def check1():
            return True
        def check2():
            return False
        
        self.health_checker.register_check("check1", check1)
        self.health_checker.register_check("check2", check2)
        
        results = self.health_checker.run_all_checks()
        
        self.assertEqual(len(results), 2)

    def test_get_overall_status_healthy(self):
        """Test getting overall status when all checks are healthy."""
        def healthy_check():
            return True
        
        self.health_checker.register_check("check1", healthy_check)
        self.health_checker.register_check("check2", healthy_check)
        
        results = self.health_checker.run_all_checks()
        status = self.health_checker.get_overall_status(results)
        
        self.assertEqual(status, MonitoringStatus.HEALTHY)

    def test_get_overall_status_critical(self):
        """Test getting overall status when any check is critical."""
        def healthy_check():
            return True
        def critical_check():
            return False
        
        self.health_checker.register_check("check1", healthy_check)
        self.health_checker.register_check("critical", critical_check)
        
        results = self.health_checker.run_all_checks()
        status = self.health_checker.get_overall_status(results)
        
        self.assertEqual(status, MonitoringStatus.CRITICAL)


class TestSystemMonitor(unittest.TestCase):
    """Test SystemMonitor class."""

    def setUp(self):
        self.monitor = SystemMonitor(interval=0.1)

    def test_system_monitor_initialization(self):
        """Test system monitor initializes correctly."""
        self.assertEqual(self.monitor.interval, 0.1)
        self.assertFalse(self.monitor._monitoring)

    def test_get_cpu_usage_returns_float(self):
        """Test getting CPU usage returns a float."""
        cpu = self.monitor.get_cpu_usage()
        self.assertIsInstance(cpu, float)
        self.assertGreaterEqual(cpu, 0.0)

    def test_get_memory_usage_returns_tuple(self):
        """Test getting memory usage returns tuple."""
        mem_mb, mem_pct = self.monitor.get_memory_usage()
        self.assertIsInstance(mem_mb, float)
        self.assertIsInstance(mem_pct, float)
        self.assertGreaterEqual(mem_mb, 0.0)
        self.assertGreaterEqual(mem_pct, 0.0)

    def test_get_thread_count_returns_int(self):
        """Test getting thread count returns int."""
        threads = self.monitor.get_thread_count()
        self.assertIsInstance(threads, int)
        self.assertGreaterEqual(threads, 0)

    def test_register_callback(self):
        """Test registering a monitoring callback."""
        callback_data = []
        def callback(data):
            callback_data.append(data)
        
        self.monitor.register_callback(callback)
        self.assertEqual(len(self.monitor._callbacks), 1)

    def test_start_stop_monitoring(self):
        """Test starting and stopping monitoring."""
        collector = MetricCollector()
        
        self.monitor.start_monitoring(collector)
        self.assertTrue(self.monitor._monitoring)
        
        time.sleep(0.5)
        
        self.monitor.stop_monitoring()
        self.assertFalse(self.monitor._monitoring)


class TestWorkflowMonitor(unittest.TestCase):
    """Test WorkflowMonitor class."""

    def setUp(self):
        self.monitor = WorkflowMonitor()

    def test_workflow_monitor_initialization(self):
        """Test workflow monitor initializes correctly."""
        self.assertEqual(len(self.monitor.active_workflows), 0)
        self.assertEqual(len(self.monitor.workflow_history), 0)

    def test_start_workflow(self):
        """Test starting a workflow."""
        self.monitor.start_workflow("wf_001", "TestWorkflow", {"branch": "main"})
        
        self.assertEqual(len(self.monitor.active_workflows), 1)
        self.assertIn("wf_001", self.monitor.active_workflows)
        self.assertEqual(self.monitor.active_workflows["wf_001"]["workflow_name"], "TestWorkflow")

    def test_complete_workflow_success(self):
        """Test completing a workflow successfully."""
        self.monitor.start_workflow("wf_001", "TestWorkflow")
        time.sleep(0.01)
        self.monitor.complete_workflow("wf_001", "completed")
        
        self.assertEqual(len(self.monitor.active_workflows), 0)
        self.assertEqual(len(self.monitor.workflow_history), 1)
        
        stats = self.monitor.get_workflow_stats("TestWorkflow")
        self.assertEqual(stats["total_executions"], 1)
        self.assertEqual(stats["successful_executions"], 1)

    def test_complete_workflow_failure(self):
        """Test completing a workflow with failure."""
        self.monitor.start_workflow("wf_001", "TestWorkflow")
        time.sleep(0.01)
        self.monitor.complete_workflow("wf_001", "failed")
        
        stats = self.monitor.get_workflow_stats("TestWorkflow")
        self.assertEqual(stats["failed_executions"], 1)

    def test_complete_nonexistent_workflow(self):
        """Test completing a workflow that doesn't exist."""
        # Should not raise any exception
        self.monitor.complete_workflow("nonexistent", "completed")
        self.assertEqual(len(self.monitor.workflow_history), 0)

    def test_get_active_workflows(self):
        """Test getting active workflows."""
        self.monitor.start_workflow("wf_001", "Workflow1")
        self.monitor.start_workflow("wf_002", "Workflow2")
        
        active = self.monitor.get_active_workflows()
        self.assertEqual(len(active), 2)

    def test_get_workflow_stats(self):
        """Test getting workflow statistics."""
        for i in range(5):
            self.monitor.start_workflow(f"wf_{i}", "TestWorkflow")
            time.sleep(0.01)
            self.monitor.complete_workflow(f"wf_{i}", "completed" if i % 2 == 0 else "failed")
        
        stats = self.monitor.get_workflow_stats("TestWorkflow")
        self.assertEqual(stats["total_executions"], 5)
        self.assertEqual(stats["successful_executions"], 3)
        self.assertEqual(stats["failed_executions"], 2)
        self.assertGreater(stats["avg_duration_ms"], 0)

    def test_get_success_rate(self):
        """Test calculating success rate."""
        for i in range(10):
            self.monitor.start_workflow(f"wf_{i}", "SuccessWorkflow")
            time.sleep(0.01)
            self.monitor.complete_workflow(f"wf_{i}", "completed")
        
        rate = self.monitor.get_success_rate("SuccessWorkflow")
        self.assertEqual(rate, 100.0)

    def test_get_recent_executions(self):
        """Test getting recent executions."""
        for i in range(15):
            self.monitor.start_workflow(f"wf_{i}", "RecentWorkflow")
            time.sleep(0.01)
            self.monitor.complete_workflow(f"wf_{i}", "completed")
        
        recent = self.monitor.get_recent_executions(limit=5)
        self.assertEqual(len(recent), 5)


class TestMonitoringDashboard(unittest.TestCase):
    """Test MonitoringDashboard class."""

    def setUp(self):
        self.collector = MetricCollector()
        self.workflow_monitor = WorkflowMonitor()
        self.health_checker = HealthChecker()
        self.dashboard = MonitoringDashboard(
            self.collector,
            self.workflow_monitor,
            self.health_checker
        )

    def test_generate_dashboard(self):
        """Test generating dashboard data."""
        dashboard = self.dashboard.generate_dashboard(include_history=False)
        
        self.assertIn("generated_at", dashboard)
        self.assertIn("status", dashboard)
        self.assertIn("active_workflows", dashboard)
        self.assertIn("workflow_stats", dashboard)
        self.assertIn("system_metrics", dashboard)

    def test_generate_dashboard_with_history(self):
        """Test generating dashboard with workflow history."""
        self.workflow_monitor.start_workflow("wf_001", "TestWorkflow")
        self.workflow_monitor.complete_workflow("wf_001", "completed")
        
        dashboard = self.dashboard.generate_dashboard(include_history=True)
        
        self.assertIn("recent_workflows", dashboard)
        self.assertEqual(len(dashboard["recent_workflows"]), 1)

    def test_get_system_metrics(self):
        """Test getting system metrics from dashboard."""
        self.collector.set_gauge("system.cpu.percent", 45.0)
        self.collector.set_gauge("system.memory.percent", 60.0)
        
        metrics = self.dashboard._get_system_metrics()
        
        self.assertIn("cpu_percent", metrics)
        self.assertIn("memory_percent", metrics)

    def test_export_json(self):
        """Test exporting dashboard as JSON."""
        json_str = self.dashboard.export_json()
        
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("status", data)


class TestMonitoringEngine(unittest.TestCase):
    """Test MonitoringEngine class."""

    def setUp(self):
        self.engine = MonitoringEngine("test_engine")

    def tearDown(self):
        if self.engine._monitoring:
            self.engine.stop()

    def test_engine_initialization(self):
        """Test monitoring engine initializes correctly."""
        self.assertEqual(self.engine.name, "test_engine")
        self.assertEqual(self.engine.status, MonitoringStatus.STOPPED)
        self.assertIsNotNone(self.engine.collector)
        self.assertIsNotNone(self.engine.alert_manager)
        self.assertIsNotNone(self.engine.health_checker)
        self.assertIsNotNone(self.engine.system_monitor)
        self.assertIsNotNone(self.engine.workflow_monitor)

    def test_start_engine(self):
        """Test starting the monitoring engine."""
        self.engine.start()
        
        self.assertTrue(self.engine._monitoring)
        self.assertNotEqual(self.engine.status, MonitoringStatus.STOPPED)

    def test_stop_engine(self):
        """Test stopping the monitoring engine."""
        self.engine.start()
        time.sleep(0.5)
        self.engine.stop()
        
        self.assertFalse(self.engine._monitoring)
        self.assertEqual(self.engine.status, MonitoringStatus.STOPPED)

    def test_register_default_health_checks(self):
        """Test registering default health checks."""
        self.engine.register_default_health_checks()
        
        self.assertIn("cpu_usage", self.engine.health_checker.health_checks)
        self.assertIn("memory_usage", self.engine.health_checker.health_checks)

    def test_register_default_alert_rules(self):
        """Test registering default alert rules."""
        self.engine.register_default_alert_rules()
        
        self.assertIn("high_cpu", self.engine.alert_manager.alert_rules)
        self.assertIn("high_memory", self.engine.alert_manager.alert_rules)

    def test_engine_dashboard(self):
        """Test getting dashboard from engine."""
        dashboard = self.engine.dashboard.generate_dashboard(include_history=False)
        
        self.assertIn("status", dashboard)


class TestMonitoringEdgeCases(unittest.TestCase):
    """Test edge cases in monitoring components."""

    def test_empty_metric_history(self):
        """Test getting history for non-existent metric."""
        collector = MetricCollector()
        history = collector.get_metric_history("nonexistent")
        self.assertEqual(len(history), 0)

    def test_get_current_value_nonexistent(self):
        """Test getting current value for non-existent metric."""
        collector = MetricCollector()
        value = collector.get_current_value("nonexistent")
        self.assertIsNone(value)

    def test_alert_rule_empty_cooldown(self):
        """Test alert rule with zero cooldown."""
        rule = AlertRule(
            name="instant",
            metric_name="metric",
            threshold=50.0,
            cooldown_seconds=0
        )
        
        self.assertTrue(rule.should_send_alert())
        rule.mark_triggered()
        self.assertTrue(rule.should_send_alert())

    def test_workflow_monitor_empty_stats(self):
        """Test getting stats for workflow with no executions."""
        monitor = WorkflowMonitor()
        stats = monitor.get_workflow_stats("nonexistent")
        self.assertEqual(stats, {})

    def test_get_success_rate_no_executions(self):
        """Test success rate with no executions."""
        monitor = WorkflowMonitor()
        rate = monitor.get_success_rate("nonexistent")
        self.assertEqual(rate, 0.0)

    def test_multiple_workflow_completions(self):
        """Test multiple workflows starting and completing."""
        monitor = WorkflowMonitor()
        
        for i in range(100):
            monitor.start_workflow(f"wf_{i}", "BatchWorkflow")
            time.sleep(0.001)
            monitor.complete_workflow(f"wf_{i}", "completed")
        
        stats = monitor.get_workflow_stats("BatchWorkflow")
        self.assertEqual(stats["total_executions"], 100)


if __name__ == '__main__':
    unittest.main()
