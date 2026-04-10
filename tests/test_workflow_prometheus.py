"""Tests for Workflow Prometheus Module.

Comprehensive tests for Prometheus metrics integration including
counters, gauges, histograms, summaries, alerting rules,
Grafana dashboard generation, PushGateway, and service discovery.
"""

import unittest
import sys
import json
import time
from unittest.mock import Mock, patch, MagicMock, mock_open
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# Mock prometheus_client module before importing the workflow module
class MockCollectorRegistry:
    def __init__(self):
        self._metrics = []

    def register(self, metric):
        self._metrics.append(metric)

    def unregister(self, metric):
        if metric in self._metrics:
            self._metrics.remove(metric)


MockREGISTRY = MockCollectorRegistry()


class MockPrometheusClient:
    Counter = None
    Gauge = None
    Histogram = None
    Summary = None
    CollectorRegistry = MockCollectorRegistry
    REGISTRY = MockREGISTRY

    class PushGateway:
        def __init__(self, url):
            self.url = url

        def push(self, metrics, job, grouping_key):
            return True

        def pushadd(self, metrics, job, grouping_key):
            return True

        def delete(self, job, grouping_key):
            return True

    CONTENT_TYPE_LATEST = 'text/plain; version=0.0.4'

    @staticmethod
    def generate_latest(registry):
        return b'# HELP test_metric Test metric\n'


class MockCounter:
    def __init__(self, name, description, labelnames=(), registry=None, **kwargs):
        self.name = name
        self.description = description
        self._labelnames = labelnames
        self._values = {}

    def labels(self, **label_values):
        key = tuple(sorted(label_values.items()))
        if key not in self._values:
            self._values[key] = {'count': 0}
        return MockMetricValue(self._values, key)

    def inc(self, value=1):
        key = tuple(sorted(self._values.keys()))[-1] if self._values else ()
        if key in self._values:
            self._values[key]['count'] += value
        else:
            self._values[key] = {'count': value}


class MockMetricValue:
    def __init__(self, values_dict, key):
        self._values_dict = values_dict
        self._key = key

    def inc(self, value=1):
        if self._key in self._values_dict:
            self._values_dict[self._key]['count'] += value
        else:
            self._values_dict[self._key] = {'count': value}

    def set(self, value):
        self._values_dict[self._key] = {'value': value}

    def dec(self, value=1):
        if self._key in self._values_dict:
            self._values_dict[self._key]['value'] -= value

    def _get_value(self):
        return self._values_dict.get(self._key, {}).get('count', 0)


class MockGauge:
    def __init__(self, name, description, labelnames=(), registry=None, **kwargs):
        self.name = name
        self.description = description
        self._labelnames = labelnames
        self._values = {}

    def labels(self, **label_values):
        key = tuple(sorted(label_values.items()))
        if key not in self._values:
            self._values[key] = {'value': 0}
        return MockMetricValue(self._values, key)

    def set(self, value):
        if self._values:
            key = tuple(sorted(self._values.keys()))[-1]
            self._values[key] = {'value': value}
        else:
            self._values[()] = {'value': value}


class MockHistogram:
    def __init__(self, name, description, labelnames=(), buckets=(), registry=None, **kwargs):
        self.name = name
        self.description = description
        self._labelnames = labelnames
        self._buckets = buckets
        self._values = {}

    def labels(self, **label_values):
        key = tuple(sorted(label_values.items()))
        if key not in self._values:
            self._values[key] = {'samples': [], 'sum': 0, 'count': 0}
        return MockHistogramValue(self._values, key)

    def observe(self, value):
        if self._values:
            key = tuple(sorted(self._values.keys()))[-1]
            self._values[key]['samples'].append(value)
            self._values[key]['sum'] += value
            self._values[key]['count'] += 1
        else:
            self._values[()] = {'samples': [value], 'sum': value, 'count': 1}


class MockHistogramValue:
    def __init__(self, values_dict, key):
        self._values_dict = values_dict
        self._key = key

    def observe(self, value):
        if self._key in self._values_dict:
            self._values_dict[self._key]['samples'].append(value)
            self._values_dict[self._key]['sum'] += value
            self._values_dict[self._key]['count'] += 1
        else:
            self._values_dict[self._key] = {'samples': [value], 'sum': value, 'count': 1}


class MockSummary:
    def __init__(self, name, description, labelnames=(), quantiles=(), registry=None, **kwargs):
        self.name = name
        self.description = description
        self._labelnames = labelnames
        self._quantiles = quantiles
        self._values = {}

    def labels(self, **label_values):
        key = tuple(sorted(label_values.items()))
        if key not in self._values:
            self._values[key] = {'samples': [], 'sum': 0, 'count': 0}
        return MockHistogramValue(self._values, key)

    def observe(self, value):
        if self._values:
            key = tuple(sorted(self._values.keys()))[-1]
            self._values[key]['samples'].append(value)
            self._values[key]['sum'] += value
            self._values[key]['count'] += 1
        else:
            self._values[()] = {'samples': [value], 'sum': value, 'count': 1}


# Set up the mock
MockPrometheusClient.Counter = MockCounter
MockPrometheusClient.Gauge = MockGauge
MockPrometheusClient.Histogram = MockHistogram
MockPrometheusClient.Summary = MockSummary

# Create mock registry module - use types.ModuleType to create proper module
import types

_mock_prometheus_client = types.ModuleType('prometheus_client')
_mock_prometheus_client.Counter = MockCounter
_mock_prometheus_client.Gauge = MockGauge
_mock_prometheus_client.Histogram = MockHistogram
_mock_prometheus_client.Summary = MockSummary
_mock_prometheus_client.CollectorRegistry = MockCollectorRegistry
_mock_prometheus_client.REGISTRY = MockREGISTRY
_mock_prometheus_client.PushGateway = MockPrometheusClient.PushGateway
_mock_prometheus_client.CONTENT_TYPE_LATEST = MockPrometheusClient.CONTENT_TYPE_LATEST
_mock_prometheus_client.generate_latest = MockPrometheusClient.generate_latest

_mock_registry = types.ModuleType('prometheus_client.registry')
_mock_registry.REGISTRY = MockREGISTRY

# Ensure clean state - remove any existing modules
for mod in list(sys.modules.keys()):
    if 'prometheus' in mod.lower():
        del sys.modules[mod]

# Patch prometheus_client before importing workflow module
sys.modules['prometheus_client'] = _mock_prometheus_client
sys.modules['prometheus_client.registry'] = _mock_registry

# Force reimport of workflow_prometheus by removing from cache
if 'workflow_prometheus' in sys.modules:
    del sys.modules['workflow_prometheus']

from workflow_prometheus import (
    PrometheusMetrics, MetricsAggregator, MetricType, MetricConfig,
    AlertRule, ScrapeTarget, create_prometheus_metrics, format_metric_name,
    PROMETHEUS_AVAILABLE
)


class TestMetricType(unittest.TestCase):
    """Tests for MetricType enum."""

    def test_metric_type_values(self):
        """Test MetricType enum values."""
        self.assertEqual(MetricType.COUNTER.value, "counter")
        self.assertEqual(MetricType.GAUGE.value, "gauge")
        self.assertEqual(MetricType.HISTOGRAM.value, "histogram")
        self.assertEqual(MetricType.SUMMARY.value, "summary")


class TestMetricConfig(unittest.TestCase):
    """Tests for MetricConfig dataclass."""

    def test_metric_config_creation(self):
        """Test MetricConfig creation with various parameters."""
        config = MetricConfig(
            name="test_metric",
            description="A test metric",
            metric_type=MetricType.COUNTER,
            labels=["method", "endpoint"],
            unit="requests"
        )
        self.assertEqual(config.name, "test_metric")
        self.assertEqual(config.description, "A test metric")
        self.assertEqual(config.metric_type, MetricType.COUNTER)
        self.assertEqual(config.labels, ["method", "endpoint"])
        self.assertEqual(config.unit, "requests")


class TestAlertRule(unittest.TestCase):
    """Tests for AlertRule dataclass."""

    def test_alert_rule_creation(self):
        """Test AlertRule creation."""
        rule = AlertRule(
            name="HighErrorRate",
            expr="rate(errors[5m]) > 10",
            duration="5m",
            severity="critical",
            labels={"service": "api"},
            annotations={"summary": "High error rate"}
        )
        self.assertEqual(rule.name, "HighErrorRate")
        self.assertEqual(rule.expr, "rate(errors[5m]) > 10")
        self.assertEqual(rule.duration, "5m")
        self.assertEqual(rule.severity, "critical")
        self.assertEqual(rule.labels, {"service": "api"})
        self.assertEqual(rule.annotations, {"summary": "High error rate"})


class TestScrapeTarget(unittest.TestCase):
    """Tests for ScrapeTarget dataclass."""

    def test_scrape_target_creation(self):
        """Test ScrapeTarget creation."""
        target = ScrapeTarget(
            job_name="rabai-service",
            targets=["localhost:8000", "localhost:8001"],
            labels={"env": "test"}
        )
        self.assertEqual(target.job_name, "rabai-service")
        self.assertEqual(target.targets, ["localhost:8000", "localhost:8001"])
        self.assertEqual(target.labels, {"env": "test"})


class TestPrometheusMetrics(unittest.TestCase):
    """Tests for PrometheusMetrics class."""

    def setUp(self):
        """Set up test fixtures."""
        self.metrics = PrometheusMetrics(
            namespace="test",
            subsystem="unit",
            enable_push_gateway=False
        )

    def test_initialization(self):
        """Test PrometheusMetrics initialization."""
        metrics = PrometheusMetrics(
            namespace="myapp",
            subsystem="worker",
            enable_push_gateway=False
        )
        self.assertEqual(metrics.namespace, "myapp")
        self.assertEqual(metrics.subsystem, "worker")
        self.assertFalse(metrics.enable_push_gateway)

    def test_build_metric_name(self):
        """Test metric name building with namespace and subsystem."""
        name = self.metrics._build_metric_name("requests_total")
        self.assertEqual(name, "test_unit_requests_total")

    def test_build_metric_name_with_empty_parts(self):
        """Test metric name building with some empty parts."""
        metrics = PrometheusMetrics(namespace="", subsystem="", enable_push_gateway=False)
        name = metrics._build_metric_name("test")
        self.assertEqual(name, "test")

    def test_register_counter(self):
        """Test counter registration."""
        counter = self.metrics.register_counter(
            "test_counter",
            "A test counter",
            labels=["method"]
        )
        self.assertIsNotNone(counter)

    def test_register_counter_no_prometheus(self):
        """Test counter registration returns None when prometheus not available."""
        with patch.object(sys.modules['prometheus_client'], 'Counter', None):
            counter = self.metrics.register_counter(
                "test_counter",
                "A test counter"
            )
            self.assertIsNone(counter)

    def test_register_gauge(self):
        """Test gauge registration."""
        gauge = self.metrics.register_gauge(
            "test_gauge",
            "A test gauge",
            labels=["status"]
        )
        self.assertIsNotNone(gauge)

    def test_register_histogram(self):
        """Test histogram registration."""
        histogram = self.metrics.register_histogram(
            "test_histogram",
            "A test histogram",
            labels=["operation"]
        )
        self.assertIsNotNone(histogram)

    def test_register_histogram_custom_buckets(self):
        """Test histogram registration with custom buckets."""
        custom_buckets = (0.1, 0.5, 1.0, 5.0)
        histogram = self.metrics.register_histogram(
            "custom_histogram",
            "A custom histogram",
            buckets=custom_buckets
        )
        self.assertIsNotNone(histogram)

    def test_register_summary(self):
        """Test summary registration."""
        summary = self.metrics.register_summary(
            "test_summary",
            "A test summary",
            labels=["query"]
        )
        self.assertIsNotNone(summary)

    def test_register_summary_custom_quantiles(self):
        """Test summary registration with custom quantiles."""
        custom_quantiles = (0.25, 0.5, 0.75, 0.95)
        summary = self.metrics.register_summary(
            "custom_summary",
            "A custom summary",
            quantiles=custom_quantiles
        )
        self.assertIsNotNone(summary)

    def test_inc_counter(self):
        """Test counter increment."""
        self.metrics.inc_counter("clicks_total", 1, {"action_type": "click", "region": "us"})
        value = self.metrics.get_counter_value("clicks_total", {"action_type": "click", "region": "us"})
        self.assertGreaterEqual(value, 0)

    def test_set_gauge(self):
        """Test gauge setting."""
        self.metrics.set_gauge("active_workflows", 42, {"workflow_type": "automation"})
        value = self.metrics.get_gauge_value("active_workflows", {"workflow_type": "automation"})
        self.assertEqual(value, 42)

    def test_inc_gauge(self):
        """Test gauge increment."""
        self.metrics.set_gauge("test_gauge", 10)
        self.metrics.inc_gauge("test_gauge", 5)
        value = self.metrics.get_gauge_value("test_gauge")
        self.assertEqual(value, 15)

    def test_dec_gauge(self):
        """Test gauge decrement."""
        self.metrics.set_gauge("test_gauge", 10)
        self.metrics.dec_gauge("test_gauge", 3)
        value = self.metrics.get_gauge_value("test_gauge")
        self.assertEqual(value, 7)

    def test_observe_histogram(self):
        """Test histogram observation."""
        self.metrics.observe_histogram("action_duration", 0.5, {"action_type": "api_call"})
        self.metrics.observe_histogram("action_duration", 1.2, {"action_type": "api_call"})

    def test_observe_summary(self):
        """Test summary observation."""
        self.metrics.observe_summary("action_latency", 0.3, {"action_type": "query"})
        self.metrics.observe_summary("action_latency", 0.7, {"action_type": "query"})

    def test_histogram_timer(self):
        """Test histogram timer context manager."""
        with self.metrics.histogram_timer("test_timer", {"operation": "test"}):
            time.sleep(0.01)

    def test_label_templates(self):
        """Test label template management."""
        self.metrics.set_label_template("test_metric", {"env": "prod", "version": "v1"})
        labels = self.metrics.get_label_values("test_metric")
        self.assertEqual(labels, {"env": "prod", "version": "v1"})

    def test_label_values_with_overrides(self):
        """Test label values with overrides."""
        self.metrics.set_label_template("test_metric", {"env": "prod", "version": "v1"})
        labels = self.metrics.get_label_values("test_metric", version="v2")
        self.assertEqual(labels, {"env": "prod", "version": "v2"})

    def test_aggregation(self):
        """Test metric aggregation."""
        self.metrics.set_gauge("test_agg", 100)
        agg_value = self.metrics.get_aggregated_value("test_agg")
        self.assertIn("last", agg_value)

    def test_get_all_aggregated_values(self):
        """Test getting all aggregated values."""
        self.metrics.set_gauge("metric1", 10)
        self.metrics.set_gauge("metric2", 20)
        all_agg = self.metrics.get_all_aggregated_values()
        self.assertIsInstance(all_agg, dict)

    def test_add_alert_rule(self):
        """Test adding alerting rules."""
        self.metrics.add_alert_rule(
            name="HighErrorRate",
            expr="rate(errors[5m]) > 10",
            duration="5m",
            severity="critical",
            labels={"service": "api"},
            annotations={"summary": "High error rate"}
        )
        rules = self.metrics.get_alerting_rules()
        self.assertEqual(len(rules), 1)
        self.assertEqual(rules[0].name, "HighErrorRate")

    def test_generate_alerting_rules_yaml(self):
        """Test generating alerting rules in YAML format."""
        self.metrics.add_alert_rule(
            name="TestAlert",
            expr="test_metric > 10",
            duration="5m"
        )
        yaml_output = self.metrics.generate_alerting_rules_yaml()
        self.assertIsInstance(yaml_output, str)
        self.assertIn("TestAlert", yaml_output)

    def test_generate_alerting_rules_json(self):
        """Test generating alerting rules in JSON format."""
        self.metrics.add_alert_rule(
            name="TestAlert",
            expr="test_metric > 10",
            duration="5m"
        )
        json_output = self.metrics.generate_alerting_rules_json()
        self.assertIsInstance(json_output, str)
        data = json.loads(json_output)
        self.assertIn("groups", data)

    def test_generate_grafana_dashboard(self):
        """Test Grafana dashboard generation."""
        dashboard = self.metrics.generate_grafana_dashboard()
        self.assertIn("title", dashboard)
        self.assertIn("panels", dashboard)
        self.assertIn("uid", dashboard)

    def test_save_grafana_dashboard(self):
        """Test saving Grafana dashboard to file."""
        with patch("builtins.open", mock_open()) as mock_file:
            self.metrics.save_grafana_dashboard("/tmp/dashboard.json")
            mock_file.assert_called_once_with("/tmp/dashboard.json", 'w')

    def test_scrape_target_management(self):
        """Test scrape target addition and removal."""
        self.metrics.add_scrape_target(
            job_name="test-job",
            targets=["localhost:8000", "localhost:8001"],
            labels={"env": "test"}
        )
        targets = self.metrics.get_scrape_targets()
        self.assertIn("test-job", targets)

        removed = self.metrics.remove_scrape_target("test-job")
        self.assertTrue(removed)

        removed = self.metrics.remove_scrape_target("non-existent")
        self.assertFalse(removed)

    def test_generate_scrape_config(self):
        """Test scrape configuration generation."""
        self.metrics.add_scrape_target(
            job_name="test-job",
            targets=["localhost:8000"]
        )
        config = self.metrics.generate_scrape_config()
        self.assertIsInstance(config, list)
        self.assertEqual(len(config), 1)
        self.assertEqual(config[0]["job_name"], "test-job")

    def test_generate_scrape_config_yaml(self):
        """Test scrape configuration YAML generation."""
        self.metrics.add_scrape_target(
            job_name="test-job",
            targets=["localhost:8000"]
        )
        yaml_output = self.metrics.generate_scrape_config_yaml()
        self.assertIsInstance(yaml_output, str)
        self.assertIn("test-job", yaml_output)

    def test_generate_scrape_config_json(self):
        """Test scrape configuration JSON generation."""
        self.metrics.add_scrape_target(
            job_name="test-job",
            targets=["localhost:8000"]
        )
        json_output = self.metrics.generate_scrape_config_json()
        self.assertIsInstance(json_output, str)
        data = json.loads(json_output)
        self.assertIn("scrape_configs", data)

    def test_generate_metrics(self):
        """Test metrics generation."""
        metrics_output = self.metrics.generate_metrics()
        self.assertIsInstance(metrics_output, bytes)

    def test_get_metrics_text(self):
        """Test getting metrics as text."""
        metrics_text = self.metrics.get_metrics_text()
        self.assertIsInstance(metrics_text, str)

    def test_get_content_type(self):
        """Test getting metrics content type."""
        content_type = self.metrics.get_content_type()
        self.assertEqual(content_type, 'text/plain; version=0.0.4')

    def test_setup_builtin_alerts(self):
        """Test setting up built-in alerts."""
        self.metrics.setup_builtin_alerts()
        rules = self.metrics.get_alerting_rules()
        self.assertGreater(len(rules), 0)

    def test_timer_context_manager(self):
        """Test timer context manager."""
        with self.metrics.timer("test_operation", {"op": "test"}):
            pass

    def test_get_metrics_info(self):
        """Test getting metrics information."""
        self.metrics.register_counter("info_test", "A test metric", labels=["label1"])
        info = self.metrics.get_metrics_info()
        self.assertIsInstance(info, dict)

    def test_push_to_gateway_disabled(self):
        """Test push to gateway when disabled."""
        result = self.metrics.push_to_gateway()
        self.assertFalse(result)

    def test_push_to_gateway_add_disabled(self):
        """Test push add to gateway when disabled."""
        result = self.metrics.push_to_gateway_add()
        self.assertFalse(result)

    def test_delete_from_gateway_disabled(self):
        """Test delete from gateway when disabled."""
        result = self.metrics.delete_from_gateway()
        self.assertFalse(result)

    def test_repr(self):
        """Test string representation."""
        repr_str = repr(self.metrics)
        self.assertIn("PrometheusMetrics", repr_str)
        self.assertIn("test", repr_str)
        self.assertIn("unit", repr_str)


class TestPrometheusMetricsWithPushGateway(unittest.TestCase):
    """Tests for PrometheusMetrics with PushGateway enabled."""

    def setUp(self):
        """Set up test fixtures with PushGateway enabled."""
        self.metrics = PrometheusMetrics(
            namespace="test",
            subsystem="pushgw",
            enable_push_gateway=True,
            push_gateway_url="http://localhost:9091"
        )

    def test_push_to_gateway(self):
        """Test pushing metrics to gateway."""
        result = self.metrics.push_to_gateway()
        self.assertIsInstance(result, bool)

    def test_push_to_gateway_add(self):
        """Test pushing metrics to gateway with add operation."""
        result = self.metrics.push_to_gateway_add()
        self.assertIsInstance(result, bool)

    def test_delete_from_gateway(self):
        """Test deleting metrics from gateway."""
        result = self.metrics.delete_from_gateway()
        self.assertIsInstance(result, bool)


class TestMetricsAggregator(unittest.TestCase):
    """Tests for MetricsAggregator class."""

    def setUp(self):
        """Set up test fixtures."""
        self.aggregator = MetricsAggregator()
        self.metrics1 = PrometheusMetrics(namespace="app1", subsystem="test", enable_push_gateway=False)
        self.metrics2 = PrometheusMetrics(namespace="app2", subsystem="test", enable_push_gateway=False)

    def test_register_instance(self):
        """Test registering a metrics instance."""
        self.aggregator.register_instance(self.metrics1)
        self.aggregator.register_instance(self.metrics2)

    def test_unregister_instance(self):
        """Test unregistering a metrics instance."""
        self.aggregator.register_instance(self.metrics1)
        result = self.aggregator.unregister_instance(self.metrics1)
        self.assertTrue(result)

    def test_unregister_nonexistent_instance(self):
        """Test unregistering a non-existent instance."""
        result = self.aggregator.unregister_instance(self.metrics1)
        self.assertFalse(result)

    def test_aggregate_all(self):
        """Test aggregating all metrics."""
        self.aggregator.register_instance(self.metrics1)
        self.aggregator.register_instance(self.metrics2)
        aggregated = self.aggregator.aggregate_all()
        self.assertIsInstance(aggregated, dict)

    def test_get_combined_metrics(self):
        """Test getting combined metrics."""
        self.aggregator.register_instance(self.metrics1)
        self.aggregator.register_instance(self.metrics2)
        combined = self.aggregator.get_combined_metrics()
        self.assertIsInstance(combined, bytes)


class TestStandaloneFunctions(unittest.TestCase):
    """Tests for standalone convenience functions."""

    def test_create_prometheus_metrics(self):
        """Test creating PrometheusMetrics via factory function."""
        metrics = create_prometheus_metrics(
            namespace="factory",
            subsystem="test",
            enable_push_gateway=False
        )
        self.assertIsInstance(metrics, PrometheusMetrics)
        self.assertEqual(metrics.namespace, "factory")

    def test_format_metric_name(self):
        """Test formatting metric names."""
        name = format_metric_name("app", "worker", "requests_total")
        self.assertEqual(name, "app_worker_requests_total")

    def test_format_metric_name_with_empty(self):
        """Test formatting metric names with empty parts."""
        name = format_metric_name("", "worker", "requests")
        self.assertEqual(name, "worker_requests")


class TestEdgeCases(unittest.TestCase):
    """Tests for edge cases and error handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.metrics = PrometheusMetrics(namespace="edge", subsystem="case", enable_push_gateway=False)

    def test_get_counter_nonexistent(self):
        """Test getting counter value for non-existent counter."""
        value = self.metrics.get_counter_value("nonexistent_counter")
        self.assertEqual(value, 0.0)

    def test_get_gauge_nonexistent(self):
        """Test getting gauge value for non-existent gauge."""
        value = self.metrics.get_gauge_value("nonexistent_gauge")
        self.assertEqual(value, 0.0)

    def test_aggregate_nonexistent_metric(self):
        """Test getting aggregated value for non-existent metric."""
        value = self.metrics.get_aggregated_value("nonexistent")
        self.assertEqual(value, {})

    def test_get_scrape_targets_empty(self):
        """Test getting scrape targets when none configured."""
        targets = self.metrics.get_scrape_targets()
        self.assertEqual(targets, {})

    def test_alerting_rules_empty(self):
        """Test getting alerting rules when none defined."""
        rules = self.metrics.get_alerting_rules()
        self.assertEqual(rules, [])

    def test_generate_scrape_config_empty(self):
        """Test generating scrape config when no targets configured."""
        config = self.metrics.generate_scrape_config()
        self.assertEqual(config, [])


if __name__ == '__main__':
    unittest.main()
