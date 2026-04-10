"""
Prometheus Metrics Integration for Rabai AutoClick

This module provides comprehensive Prometheus metrics integration including:
- Counter, Gauge, Histogram, and Summary metrics
- Labels and metric aggregation
- Alerting rules generation
- Grafana dashboard configuration
- PushGateway support
- Service discovery for auto-discovering scrape targets
"""

import time
import json
import threading
import socket
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from contextlib import contextmanager
import hashlib
import yaml

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary, 
        CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST,
        PushGateway, GaugeMetricFamily, CounterMetricFamily,
        HistogramMetricFamily, SummaryMetricFamily
    )
    from prometheus_client.registry import REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = Summary = None


logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Supported metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricConfig:
    """Configuration for a metric."""
    name: str
    description: str
    metric_type: MetricType
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None  # For histogram
    quantiles: Optional[List[float]] = None  # For summary
    unit: str = ""


@dataclass
class AlertRule:
    """Prometheus alerting rule definition."""
    name: str
    expr: str
    duration: str
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    severity: str = "warning"


@dataclass
class ScrapeTarget:
    """Service discovery target."""
    job_name: str
    targets: List[str]
    labels: Dict[str, str] = field(default_factory=dict)


class PrometheusMetrics:
    """
    Comprehensive Prometheus metrics integration class.
    
    Features:
    - Counter metrics: Count occurrences of events
    - Gauge metrics: Track current values
    - Histogram metrics: Track distributions
    - Summary metrics: Track percentiles
    - Labels: Add labels to metrics
    - Metric aggregation: Aggregate metrics across instances
    - Alerting rules: Define Prometheus alerting rules
    - Grafana dashboards: Generate Grafana dashboard configs
    - Push gateway: Push metrics to Prometheus PushGateway
    - Service discovery: Auto-discover targets for scraping
    """
    
    # Default histogram buckets
    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    
    # Default summary quantiles
    DEFAULT_QUANTILES = (0.5, 0.9, 0.95, 0.99)
    
    def __init__(
        self,
        namespace: str = "rabai",
        subsystem: str = "autoclick",
        registry: Optional[CollectorRegistry] = None,
        enable_push_gateway: bool = False,
        push_gateway_url: str = "http://localhost:9091",
        job_name: str = "rabai_autoclick"
    ):
        """
        Initialize Prometheus metrics.
        
        Args:
            namespace: Prometheus namespace
            subsystem: Prometheus subsystem
            registry: Custom CollectorRegistry (uses default if None)
            enable_push_gateway: Enable PushGateway support
            push_gateway_url: PushGateway URL
            job_name: Job name for PushGateway
        """
        self.namespace = namespace
        self.subsystem = subsystem
        self.job_name = job_name
        self.enable_push_gateway = enable_push_gateway
        self.push_gateway_url = push_gateway_url
        
        # Use provided registry or get the default
        self.registry = registry if registry is not None else REGISTRY
        
        # Metrics storage
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._summaries: Dict[str, Summary] = {}
        
        # Metric configurations
        self._metric_configs: Dict[str, MetricConfig] = {}
        
        # Labels templates
        self._label_templates: Dict[str, Dict[str, str]] = {}
        
        # Aggregation data
        self._aggregation_lock = threading.Lock()
        self._aggregated_values: Dict[str, Dict[str, float]] = defaultdict(dict)
        
        # Service discovery
        self._scrape_targets: Dict[str, ScrapeTarget] = {}
        self._discovery_lock = threading.Lock()
        
        # Alerting rules
        self._alert_rules: List[AlertRule] = []
        
        # PushGateway handler
        self._push_gateway: Optional[PushGateway] = None
        if enable_push_gateway and PROMETHEUS_AVAILABLE:
            self._push_gateway = PushGateway(push_gateway_url)
        
        # Thread safety for metric operations
        self._metrics_lock = threading.RLock()
        
        # Initialize built-in metrics
        self._init_builtin_metrics()
    
    def _init_builtin_metrics(self) -> None:
        """Initialize built-in metrics."""
        # Core autoclick metrics
        self.register_counter(
            "clicks_total",
            "Total number of clicks performed",
            ["action_type", "region"]
        )
        
        self.register_gauge(
            "active_workflows",
            "Number of currently active workflows",
            ["workflow_type"]
        )
        
        self.register_histogram(
            "action_duration_seconds",
            "Duration of actions in seconds",
            ["action_type"],
            buckets=self.DEFAULT_BUCKETS
        )
        
        self.register_summary(
            "action_latency_seconds",
            "Action latency in seconds",
            ["action_type"],
            quantiles=self.DEFAULT_QUANTILES
        )
        
        self.register_gauge(
            "error_count",
            "Current error count by type",
            ["error_type", "severity"]
        )
        
        self.register_counter(
            "workflow_events_total",
            "Total workflow events",
            ["event_type", "source"]
        )
    
    def _build_metric_name(self, name: str) -> str:
        """Build full metric name with namespace and subsystem."""
        parts = [self.namespace, self.subsystem, name]
        return "_".join(p for p in parts if p)
    
    def register_counter(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        **kwargs
    ) -> Optional[Counter]:
        """
        Register a counter metric.
        
        Args:
            name: Metric name
            description: Metric description
            labels: List of label names
            **kwargs: Additional Counter arguments
            
        Returns:
            Registered Counter or None if prometheus_client not available
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not available")
            return None
        
        full_name = self._build_metric_name(name)
        labels = labels or []
        
        with self._metrics_lock:
            if full_name in self._counters:
                return self._counters[full_name]
            
            config = MetricConfig(
                name=full_name,
                description=description,
                metric_type=MetricType.COUNTER,
                labels=labels
            )
            self._metric_configs[full_name] = config
            
            counter = Counter(
                full_name,
                description,
                labels,
                registry=self.registry,
                **kwargs
            )
            self._counters[full_name] = counter
            return counter
    
    def register_gauge(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        **kwargs
    ) -> Optional[Gauge]:
        """
        Register a gauge metric.
        
        Args:
            name: Metric name
            description: Metric description
            labels: List of label names
            **kwargs: Additional Gauge arguments
            
        Returns:
            Registered Gauge or None if prometheus_client not available
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not available")
            return None
        
        full_name = self._build_metric_name(name)
        labels = labels or []
        
        with self._metrics_lock:
            if full_name in self._gauges:
                return self._gauges[full_name]
            
            config = MetricConfig(
                name=full_name,
                description=description,
                metric_type=MetricType.GAUGE,
                labels=labels
            )
            self._metric_configs[full_name] = config
            
            gauge = Gauge(
                full_name,
                description,
                labels,
                registry=self.registry,
                **kwargs
            )
            self._gauges[full_name] = gauge
            return gauge
    
    def register_histogram(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[Tuple[float, ...]] = None,
        **kwargs
    ) -> Optional[Histogram]:
        """
        Register a histogram metric.
        
        Args:
            name: Metric name
            description: Metric description
            labels: List of label names
            buckets: Histogram buckets
            **kwargs: Additional Histogram arguments
            
        Returns:
            Registered Histogram or None if prometheus_client not available
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not available")
            return None
        
        full_name = self._build_metric_name(name)
        labels = labels or []
        buckets = buckets or self.DEFAULT_BUCKETS
        
        with self._metrics_lock:
            if full_name in self._histograms:
                return self._histograms[full_name]
            
            config = MetricConfig(
                name=full_name,
                description=description,
                metric_type=MetricType.HISTOGRAM,
                labels=labels,
                buckets=list(buckets)
            )
            self._metric_configs[full_name] = config
            
            histogram = Histogram(
                full_name,
                description,
                labels,
                buckets,
                registry=self.registry,
                **kwargs
            )
            self._histograms[full_name] = histogram
            return histogram
    
    def register_summary(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        quantiles: Optional[Tuple[float, ...]] = None,
        **kwargs
    ) -> Optional[Summary]:
        """
        Register a summary metric.
        
        Args:
            name: Metric name
            description: Metric description
            labels: List of label names
            quantiles: Quantiles to track
            **kwargs: Additional Summary arguments
            
        Returns:
            Registered Summary or None if prometheus_client not available
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not available")
            return None
        
        full_name = self._build_metric_name(name)
        labels = labels or []
        quantiles = quantiles or self.DEFAULT_QUANTILES
        
        with self._metrics_lock:
            if full_name in self._summaries:
                return self._summaries[full_name]
            
            config = MetricConfig(
                name=full_name,
                description=description,
                metric_type=MetricType.SUMMARY,
                labels=labels,
                quantiles=list(quantiles)
            )
            self._metric_configs[full_name] = config
            
            summary = Summary(
                full_name,
                description,
                labels,
                registry=self.registry,
                **kwargs
            )
            self._summaries[full_name] = summary
            return summary
    
    # Counter operations
    def inc_counter(self, name: str, value: float = 1, label_values: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._counters:
                self.register_counter(name, f"Counter: {name}")
            self._counters[full_name].labels(**(label_values or {})).inc(value)
            self._aggregate_metric(full_name, value, "inc")
    
    def get_counter_value(self, name: str, label_values: Optional[Dict[str, str]] = None) -> float:
        """Get current counter value."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._counters:
                return 0.0
            return self._counters[full_name].labels(**(label_values or {}))._value.get()
    
    # Gauge operations
    def set_gauge(self, name: str, value: float, label_values: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric value."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._gauges:
                self.register_gauge(name, f"Gauge: {name}")
            self._gauges[full_name].labels(**(label_values or {})).set(value)
            self._aggregate_metric(full_name, value, "set")
    
    def inc_gauge(self, name: str, value: float = 1, label_values: Optional[Dict[str, str]] = None) -> None:
        """Increment a gauge metric."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._gauges:
                self.register_gauge(name, f"Gauge: {name}")
            self._gauges[full_name].labels(**(label_values or {})).inc(value)
            self._aggregate_metric(full_name, value, "inc")
    
    def dec_gauge(self, name: str, value: float = 1, label_values: Optional[Dict[str, str]] = None) -> None:
        """Decrement a gauge metric."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._gauges:
                self.register_gauge(name, f"Gauge: {name}")
            self._gauges[full_name].labels(**(label_values or {})).dec(value)
            self._aggregate_metric(full_name, -value, "inc")
    
    def get_gauge_value(self, name: str, label_values: Optional[Dict[str, str]] = None) -> float:
        """Get current gauge value."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._gauges:
                return 0.0
            return self._gauges[full_name].labels(**(label_values or {}))._value.get()
    
    # Histogram operations
    def observe_histogram(self, name: str, value: float, label_values: Optional[Dict[str, str]] = None) -> None:
        """Observe a value for histogram."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._histograms:
                self.register_histogram(name, f"Histogram: {name}")
            self._histograms[full_name].labels(**(label_values or {})).observe(value)
            self._aggregate_metric(full_name, value, "observe")
    
    @contextmanager
    def histogram_timer(self, name: str, label_values: Optional[Dict[str, str]] = None):
        """Context manager to time histogram observations."""
        start_time = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - start_time
            self.observe_histogram(name, duration, label_values)
    
    # Summary operations
    def observe_summary(self, name: str, value: float, label_values: Optional[Dict[str, str]] = None) -> None:
        """Observe a value for summary."""
        full_name = self._build_metric_name(name)
        with self._metrics_lock:
            if full_name not in self._summaries:
                self.register_summary(name, f"Summary: {name}")
            self._summaries[full_name].labels(**(label_values or {})).observe(value)
            self._aggregate_metric(full_name, value, "observe")
    
    # Labels management
    def set_label_template(self, metric_name: str, labels: Dict[str, str]) -> None:
        """
        Set label template for a metric.
        
        Args:
            metric_name: Name of the metric
            labels: Label name to default value mapping
        """
        full_name = self._build_metric_name(metric_name)
        with self._metrics_lock:
            self._label_templates[full_name] = labels
    
    def get_label_values(self, metric_name: str, **overrides) -> Dict[str, str]:
        """
        Get label values with template and overrides.
        
        Args:
            metric_name: Name of the metric
            **overrides: Label values to override
            
        Returns:
            Combined label values dictionary
        """
        full_name = self._build_metric_name(metric_name)
        with self._metrics_lock:
            template = self._label_templates.get(full_name, {})
            result = template.copy()
            result.update(overrides)
            return result
    
    # Metric aggregation
    def _aggregate_metric(self, metric_name: str, value: float, operation: str) -> None:
        """
        Aggregate metric value across instances.
        
        Args:
            metric_name: Full metric name
            value: Metric value
            operation: Operation type (inc, set, observe)
        """
        with self._aggregation_lock:
            if operation == "inc":
                self._aggregated_values[metric_name]["sum"] = \
                    self._aggregated_values[metric_name].get("sum", 0) + value
                self._aggregated_values[metric_name]["count"] = \
                    self._aggregated_values[metric_name].get("count", 0) + 1
            elif operation == "set":
                self._aggregated_values[metric_name]["last"] = value
            elif operation == "observe":
                self._aggregated_values[metric_name]["sum"] = \
                    self._aggregated_values[metric_name].get("sum", 0) + value
                self._aggregated_values[metric_name]["count"] = \
                    self._aggregated_values[metric_name].get("count", 0) + 1
                self._aggregated_values[metric_name]["min"] = min(
                    self._aggregated_values[metric_name].get("min", float('inf')),
                    value
                )
                self._aggregated_values[metric_name]["max"] = max(
                    self._aggregated_values[metric_name].get("max", float('-inf')),
                    value
                )
    
    def get_aggregated_value(self, metric_name: str) -> Dict[str, float]:
        """
        Get aggregated value for a metric.
        
        Args:
            metric_name: Full metric name
            
        Returns:
            Dictionary with aggregated values
        """
        full_name = self._build_metric_name(metric_name)
        with self._aggregation_lock:
            return self._aggregated_values.get(full_name, {}).copy()
    
    def get_all_aggregated_values(self) -> Dict[str, Dict[str, float]]:
        """
        Get all aggregated values.
        
        Returns:
            Dictionary of all aggregated values
        """
        with self._aggregation_lock:
            return {k: v.copy() for k, v in self._aggregated_values.items()}
    
    # Alerting rules
    def add_alert_rule(
        self,
        name: str,
        expr: str,
        duration: str = "5m",
        labels: Optional[Dict[str, str]] = None,
        annotations: Optional[Dict[str, str]] = None,
        severity: str = "warning"
    ) -> None:
        """
        Add a Prometheus alerting rule.
        
        Args:
            name: Alert name
            expr: PromQL expression
            duration: Alert duration before firing
            labels: Alert labels
            annotations: Alert annotations
            severity: Alert severity (warning, critical)
        """
        rule = AlertRule(
            name=name,
            expr=expr,
            duration=duration,
            labels=labels or {},
            annotations=annotations or {},
            severity=severity
        )
        self._alert_rules.append(rule)
    
    def get_alerting_rules(self) -> List[AlertRule]:
        """Get all defined alerting rules."""
        return self._alert_rules.copy()
    
    def generate_alerting_rules_yaml(self) -> str:
        """
        Generate Prometheus alerting rules in YAML format.
        
        Returns:
            YAML string with alerting rules
        """
        groups = []
        for rule in self._alert_rules:
            alert_entry = {
                "alert": rule.name,
                "expr": rule.expr,
                "duration": rule.duration,
                "labels": rule.labels,
                "annotations": rule.annotations
            }
            groups.append(alert_entry)
        
        rules_config = {
            "groups": [{
                "name": f"{self.namespace}_{self.subsystem}_alerts",
                "rules": groups
            }]
        }
        
        return yaml.dump(rules_config, default_flow_style=False, sort_keys=False)
    
    def generate_alerting_rules_json(self) -> str:
        """
        Generate Prometheus alerting rules in JSON format.
        
        Returns:
            JSON string with alerting rules
        """
        groups = []
        for rule in self._alert_rules:
            groups.append({
                "name": rule.name,
                "query": rule.expr,
                "duration": rule.duration,
                "labels": rule.labels,
                "annotations": rule.annotations
            })
        
        rules_config = {
            "groups": [{
                "name": f"{self.namespace}_{self.subsystem}_alerts",
                "interval": "30s",
                "rules": groups
            }]
        }
        
        return json.dumps(rules_config, indent=2)
    
    # Grafana dashboards
    def generate_grafana_dashboard(self) -> Dict[str, Any]:
        """
        Generate Grafana dashboard configuration.
        
        Returns:
            Grafana dashboard JSON structure
        """
        dashboard = {
            "title": f"{self.namespace.title()} {self.subsystem.title()} Dashboard",
            "uid": f"{self.namespace}_{self.subsystem}",
            "tags": [self.namespace, self.subsystem, "autoclick", "monitoring"],
            "timezone": "browser",
            "refresh": "10s",
            "panels": [],
            "templating": {
                "list": []
            },
            "time": {
                "from": "now-1h",
                "to": "now"
            },
            "dashboardVersion": 1
        }
        
        # Add metric panels
        panel_id = 1
        
        # Counter panels
        for name, config in self._metric_configs.items():
            if config.metric_type == MetricType.COUNTER:
                panel = self._create_stat_panel(
                    name=f"{config.name} (Total)",
                    query=f'increase({config.name}_total[5m])',
                    panel_id=panel_id,
                    description=config.description
                )
                dashboard["panels"].append(panel)
                panel_id += 1
        
        # Gauge panels
        for name, config in self._metric_configs.items():
            if config.metric_type == MetricType.GAUGE:
                panel = self._create_gauge_panel(
                    name=f"{config.name} (Current)",
                    query=config.name,
                    panel_id=panel_id,
                    description=config.description
                )
                dashboard["panels"].append(panel)
                panel_id += 1
        
        # Histogram panels (as heatmap or histogram)
        for name, config in self._metric_configs.items():
            if config.metric_type == MetricType.HISTOGRAM:
                panel = self._create_histogram_panel(
                    name=f"{config.name} (Distribution)",
                    query=config.name,
                    panel_id=panel_id,
                    description=config.description,
                    buckets=config.buckets
                )
                dashboard["panels"].append(panel)
                panel_id += 1
        
        # Summary panels
        for name, config in self._metric_configs.items():
            if config.metric_type == MetricType.SUMMARY:
                for quantile in (config.quantiles or [0.5, 0.9, 0.99]):
                    panel = self._create_stat_panel(
                        name=f"{config.name} (p{int(quantile*100)})",
                        query=f'{config.name}{{quantile="{quantile}"}}',
                        panel_id=panel_id,
                        description=f"{config.description} (p{int(quantile*100)})"
                    )
                    dashboard["panels"].append(panel)
                    panel_id += 1
        
        return dashboard
    
    def _create_stat_panel(
        self,
        name: str,
        query: str,
        panel_id: int,
        description: str = ""
    ) -> Dict[str, Any]:
        """Create a stat panel configuration."""
        return {
            "id": panel_id,
            "title": name,
            "type": "stat",
            "description": description,
            "gridPos": {
                "x": ((panel_id - 1) % 2) * 12,
                "y": ((panel_id - 1) // 2) * 8,
                "w": 12,
                "h": 8
            },
            "targets": [{
                "expr": query,
                "refId": "A"
            }],
            "options": {
                "colorMode": "value",
                "graphMode": "area",
                "justifyMode": "auto",
                "orientation": "auto"
            },
            "fieldConfig": {
                "defaults": {
                    "unit": "short"
                }
            }
        }
    
    def _create_gauge_panel(
        self,
        name: str,
        query: str,
        panel_id: int,
        description: str = ""
    ) -> Dict[str, Any]:
        """Create a gauge panel configuration."""
        return {
            "id": panel_id,
            "title": name,
            "type": "gauge",
            "description": description,
            "gridPos": {
                "x": ((panel_id - 1) % 2) * 12,
                "y": ((panel_id - 1) // 2) * 8,
                "w": 12,
                "h": 8
            },
            "targets": [{
                "expr": query,
                "refId": "A"
            }],
            "fieldConfig": {
                "defaults": {
                    "unit": "short",
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 70},
                            {"color": "red", "value": 90}
                        ]
                    }
                }
            }
        }
    
    def _create_histogram_panel(
        self,
        name: str,
        query: str,
        panel_id: int,
        description: str = "",
        buckets: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """Create a histogram panel configuration."""
        return {
            "id": panel_id,
            "title": name,
            "type": "histogram",
            "description": description,
            "gridPos": {
                "x": ((panel_id - 1) % 2) * 12,
                "y": ((panel_id - 1) // 2) * 8,
                "w": 12,
                "h": 8
            },
            "targets": [{
                "expr": query,
                "refId": "A"
            }],
            "options": {
                "bucketOffset": 0,
                "combine": False
            }
        }
    
    def save_grafana_dashboard(self, filepath: str) -> None:
        """
        Save Grafana dashboard to file.
        
        Args:
            filepath: Path to save the dashboard JSON
        """
        dashboard = self.generate_grafana_dashboard()
        with open(filepath, 'w') as f:
            json.dump(dashboard, f, indent=2)
    
    # PushGateway
    def push_to_gateway(self, grouping_key: Optional[Dict[str, str]] = None) -> bool:
        """
        Push metrics to Prometheus PushGateway.
        
        Args:
            grouping_key: Grouping key for PushGateway
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enable_push_gateway or not PROMETHEUS_AVAILABLE:
            logger.warning("PushGateway not enabled or prometheus_client not available")
            return False
        
        try:
            grouping_key = grouping_key or {}
            job = self.job_name
            
            # Generate metrics
            metrics = generate_latest(self.registry)
            
            # Push to gateway
            self._push_gateway.push(metrics, job, grouping_key)
            logger.info(f"Pushed metrics to PushGateway: {self.push_gateway_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to push to PushGateway: {e}")
            return False
    
    def push_to_gateway_add(self, grouping_key: Optional[Dict[str, str]] = None) -> bool:
        """
        Push metrics to PushGateway using add operation.
        
        Args:
            grouping_key: Grouping key for PushGateway
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enable_push_gateway or not PROMETHEUS_AVAILABLE:
            logger.warning("PushGateway not enabled or prometheus_client not available")
            return False
        
        try:
            grouping_key = grouping_key or {}
            job = self.job_name
            
            # Generate metrics
            metrics = generate_latest(self.registry)
            
            # Push to gateway with add
            self._push_gateway.pushadd(metrics, job, grouping_key)
            logger.info(f"Pushed metrics to PushGateway (add): {self.push_gateway_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to push add to PushGateway: {e}")
            return False
    
    def delete_from_gateway(self, grouping_key: Optional[Dict[str, str]] = None) -> bool:
        """
        Delete metrics from PushGateway.
        
        Args:
            grouping_key: Grouping key for PushGateway
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enable_push_gateway or not PROMETHEUS_AVAILABLE:
            return False
        
        try:
            grouping_key = grouping_key or {}
            job = self.job_name
            
            self._push_gateway.delete(job, grouping_key)
            logger.info(f"Deleted metrics from PushGateway: {self.push_gateway_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete from PushGateway: {e}")
            return False
    
    # Service discovery
    def add_scrape_target(
        self,
        job_name: str,
        targets: List[str],
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Add a scrape target for service discovery.
        
        Args:
            job_name: Name of the scrape job
            targets: List of target endpoints
            labels: Additional labels for the targets
        """
        with self._discovery_lock:
            self._scrape_targets[job_name] = ScrapeTarget(
                job_name=job_name,
                targets=targets,
                labels=labels or {}
            )
    
    def remove_scrape_target(self, job_name: str) -> bool:
        """
        Remove a scrape target.
        
        Args:
            job_name: Name of the scrape job
            
        Returns:
            True if removed, False if not found
        """
        with self._discovery_lock:
            if job_name in self._scrape_targets:
                del self._scrape_targets[job_name]
                return True
            return False
    
    def get_scrape_targets(self) -> Dict[str, ScrapeTarget]:
        """Get all configured scrape targets."""
        with self._discovery_lock:
            return {k: v for k, v in self._scrape_targets.items()}
    
    def discover_local_targets(self, port_range: Tuple[int, int] = (8000, 9000)) -> List[str]:
        """
        Auto-discover local targets by scanning specified port range.
        
        Args:
            port_range: Range of ports to scan
            
        Returns:
            List of discovered target endpoints
        """
        discovered = []
        hostname = socket.gethostname()
        
        for port in range(port_range[0], port_range[1] + 1):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.1)
                result = sock.connect_ex((hostname, port))
                sock.close()
                
                if result == 0:
                    discovered.append(f"{hostname}:{port}")
            except Exception:
                continue
        
        return discovered
    
    def discover_targets_from_dns(self, dns_name: str, port: int = 8000) -> List[str]:
        """
        Auto-discover targets from DNS SRV records.
        
        Args:
            dns_name: DNS name to lookup
            port: Default port to append if not in SRV record
            
        Returns:
            List of discovered target endpoints
        """
        discovered = []
        try:
            import dns.resolver
            answers = dns.resolver.resolve(dns_name, 'SRV')
            for answer in answers:
                target = answer.target.to_text().rstrip('.')
                discovered.append(f"{target}:{answer.port}")
        except Exception as e:
            logger.warning(f"DNS discovery failed: {e}")
            
            # Fallback: try direct A record
            try:
                import dns.resolver
                answers = dns.resolver.resolve(dns_name, 'A')
                for answer in answers:
                    discovered.append(f"{answer.to_text()}:{port}")
            except Exception:
                pass
        
        return discovered
    
    def generate_scrape_config(self) -> List[Dict[str, Any]]:
        """
        Generate Prometheus scrape configuration.
        
        Returns:
            List of scrape configs
        """
        configs = []
        
        with self._discovery_lock:
            for job_name, target in self._scrape_targets.items():
                config = {
                    "job_name": job_name,
                    "static_configs": [{
                        "targets": target.targets,
                        "labels": target.labels
                    }],
                    "scrape_interval": "15s",
                    "scrape_timeout": "10s"
                }
                configs.append(config)
        
        return configs
    
    def generate_scrape_config_yaml(self) -> str:
        """
        Generate Prometheus scrape configuration as YAML.
        
        Returns:
            YAML string with scrape configuration
        """
        configs = self.generate_scrape_config()
        return yaml.dump({"scrape_configs": configs}, default_flow_style=False, sort_keys=False)
    
    def generate_scrape_config_json(self) -> str:
        """
        Generate Prometheus scrape configuration as JSON.
        
        Returns:
            JSON string with scrape configuration
        """
        configs = self.generate_scrape_config()
        return json.dumps({"scrape_configs": configs}, indent=2)
    
    # Metrics export
    def generate_metrics(self) -> bytes:
        """
        Generate metrics in Prometheus text format.
        
        Returns:
            Metrics as bytes
        """
        return generate_latest(self.registry)
    
    def get_metrics_text(self) -> str:
        """
        Get metrics as text format.
        
        Returns:
            Metrics as string
        """
        return self.generate_metrics().decode('utf-8')
    
    def get_content_type(self) -> str:
        """
        Get Prometheus content type.
        
        Returns:
            Content type string
        """
        return CONTENT_TYPE_LATEST
    
    # Built-in alerts
    def setup_builtin_alerts(self) -> None:
        """Set up built-in alerting rules."""
        # High error rate alert
        self.add_alert_rule(
            name="HighErrorRate",
            expr=f"rate({self._build_metric_name('error_count')}[5m]) > 10",
            duration="5m",
            severity="critical",
            labels={"service": self.subsystem},
            annotations={
                "summary": "High error rate detected",
                "description": "Error rate exceeds 10/min for 5 minutes"
            }
        )
        
        # Workflow saturation alert
        self.add_alert_rule(
            name="WorkflowSaturation",
            expr=f"{self._build_metric_name('active_workflows')} > 100",
            duration="10m",
            severity="warning",
            labels={"service": self.subsystem},
            annotations={
                "summary": "High number of active workflows",
                "description": "Active workflows exceed 100"
            }
        )
        
        # High latency alert
        self.add_alert_rule(
            name="HighLatency",
            expr=f"histogram_quantile(0.95, rate({self._build_metric_name('action_duration_seconds')}_bucket[5m])) > 1",
            duration="5m",
            severity="warning",
            labels={"service": self.subsystem},
            annotations={
                "summary": "High action latency detected",
                "description": "95th percentile latency exceeds 1 second"
            }
        )
    
    # Context manager for timing
    @contextmanager
    def timer(self, metric_name: str, label_values: Optional[Dict[str, str]] = None):
        """
        Context manager to time operations and record to histogram.
        
        Args:
            metric_name: Name of the histogram metric
            label_values: Label values for the metric
        """
        start_time = time.perf_counter()
        try:
            yield
        finally:
            duration = time.perf_counter() - start_time
            self.observe_histogram(metric_name, duration, label_values)
    
    # Get all metrics info
    def get_metrics_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all registered metrics.
        
        Returns:
            Dictionary with metric information
        """
        info = {}
        
        for name, config in self._metric_configs.items():
            info[name] = {
                "description": config.description,
                "type": config.metric_type.value,
                "labels": config.labels,
            }
            if config.buckets:
                info[name]["buckets"] = config.buckets
            if config.quantiles:
                info[name]["quantiles"] = config.quantiles
        
        return info
    
    def __repr__(self) -> str:
        return (
            f"PrometheusMetrics(namespace={self.namespace}, "
            f"subsystem={self.subsystem}, "
            f"counters={len(self._counters)}, "
            f"gauges={len(self._gauges)}, "
            f"histograms={len(self._histograms)}, "
            f"summaries={len(self._summaries)})"
        )


class MetricsAggregator:
    """
    Aggregate metrics from multiple PrometheusMetrics instances.
    """
    
    def __init__(self):
        """Initialize the aggregator."""
        self._instances: List[PrometheusMetrics] = []
        self._lock = threading.Lock()
    
    def register_instance(self, instance: PrometheusMetrics) -> None:
        """Register a PrometheusMetrics instance."""
        with self._lock:
            self._instances.append(instance)
    
    def unregister_instance(self, instance: PrometheusMetrics) -> bool:
        """Unregister a PrometheusMetrics instance."""
        with self._lock:
            try:
                self._instances.remove(instance)
                return True
            except ValueError:
                return False
    
    def aggregate_all(self) -> Dict[str, Dict[str, float]]:
        """
        Aggregate metrics from all registered instances.
        
        Returns:
            Dictionary of aggregated metrics
        """
        aggregated = defaultdict(lambda: defaultdict(float))
        
        with self._lock:
            for instance in self._instances:
                for metric_name, values in instance.get_all_aggregated_values().items():
                    for key, value in values.items():
                        aggregated[metric_name][key] += value
        
        return {k: dict(v) for k, v in aggregated.items()}
    
    def get_combined_metrics(self) -> bytes:
        """
        Get combined metrics from all instances.
        
        Returns:
            Combined metrics in Prometheus format
        """
        all_metrics = []
        with self._lock:
            for instance in self._instances:
                all_metrics.append(instance.generate_metrics())
        
        return b"\n".join(all_metrics)


# Standalone functions for convenience
def create_prometheus_metrics(
    namespace: str = "rabai",
    subsystem: str = "autoclick",
    **kwargs
) -> PrometheusMetrics:
    """
    Create a new PrometheusMetrics instance.
    
    Args:
        namespace: Prometheus namespace
        subsystem: Prometheus subsystem
        **kwargs: Additional arguments for PrometheusMetrics
        
    Returns:
        New PrometheusMetrics instance
    """
    return PrometheusMetrics(namespace=namespace, subsystem=subsystem, **kwargs)


def format_metric_name(namespace: str, subsystem: str, name: str) -> str:
    """
    Format a metric name with namespace and subsystem.
    
    Args:
        namespace: Namespace
        subsystem: Subsystem
        name: Metric name
        
    Returns:
        Formatted metric name
    """
    parts = [namespace, subsystem, name]
    return "_".join(p for p in parts if p)
