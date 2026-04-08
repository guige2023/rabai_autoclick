"""API Dashboard Action Module.

Provides API monitoring dashboard capabilities including metrics
collection, visualization data generation, and dashboard widget support.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, asdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricPoint:
    """Single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = None


class MetricsCollectorAction(BaseAction):
    """Collect and aggregate API metrics.
    
    Supports counters, gauges, histograms, and time series storage.
    """
    action_type = "metrics_collector"
    display_name = "指标收集"
    description = "收集和聚合API指标，支持计数器和直方图"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, Dict] = defaultdict(lambda: {
            'counter': 0,
            'values': deque(maxlen=1000),
            'timestamps': deque(maxlen=1000)
        })

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Collect and record metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - metric_name: Name of the metric.
                - metric_type: 'counter', 'gauge', 'histogram'.
                - value: Metric value.
                - labels: Optional metric labels.
                - increment: Amount to increment counter.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with collection result or error.
        """
        metric_name = params.get('metric_name', '')
        metric_type = params.get('metric_type', 'counter')
        value = params.get('value', 1)
        labels = params.get('labels', {})
        increment = params.get('increment', 1)
        output_var = params.get('output_var', 'metric_result')

        if not metric_name:
            return ActionResult(
                success=False,
                message="Parameter 'metric_name' is required"
            )

        try:
            metric_key = self._make_key(metric_name, labels)
            metric = self._metrics[metric_key]

            if metric_type == 'counter':
                metric['counter'] += increment
                metric['values'].append(metric['counter'])
            elif metric_type == 'gauge':
                metric['counter'] = value
                metric['values'].append(value)
            elif metric_type == 'histogram':
                metric['values'].append(value)

            metric['timestamps'].append(time.time())
            metric['last_updated'] = time.time()
            metric['labels'] = labels

            result = {
                'metric_name': metric_name,
                'metric_type': metric_type,
                'value': metric['counter'] if metric_type in ('counter', 'gauge') else value,
                'labels': labels
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Metric '{metric_name}' recorded: {result['value']}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Metrics collection failed: {str(e)}"
            )

    def _make_key(self, name: str, labels: Dict) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name
        label_str = ','.join(f'{k}={v}' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class DashboardWidgetAction(BaseAction):
    """Generate dashboard widget data.
    
    Supports charts, graphs, tables, and KPI cards.
    """
    action_type = "dashboard_widget"
    display_name = "仪表板组件"
    description = "生成仪表板组件数据，支持图表和KPI卡片"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate dashboard widget data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - widget_type: 'line', 'bar', 'pie', 'table', 'kpi', 'gauge'.
                - title: Widget title.
                - data: Widget data.
                - options: Widget-specific options.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with widget data or error.
        """
        widget_type = params.get('widget_type', 'line')
        title = params.get('title', 'Widget')
        data = params.get('data', [])
        options = params.get('options', {})
        output_var = params.get('output_var', 'widget')

        try:
            if widget_type == 'line':
                result = self._generate_line_chart(title, data, options)
            elif widget_type == 'bar':
                result = self._generate_bar_chart(title, data, options)
            elif widget_type == 'pie':
                result = self._generate_pie_chart(title, data, options)
            elif widget_type == 'table':
                result = self._generate_table(title, data, options)
            elif widget_type == 'kpi':
                result = self._generate_kpi(title, data, options)
            elif widget_type == 'gauge':
                result = self._generate_gauge(title, data, options)
            else:
                result = {'type': widget_type, 'title': title, 'data': data}

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Generated {widget_type} widget: {title}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Dashboard widget generation failed: {str(e)}"
            )

    def _generate_line_chart(self, title: str, data: List, options: Dict) -> Dict:
        """Generate line chart widget data."""
        return {
            'type': 'line',
            'title': title,
            'data': {
                'labels': [d.get('label', d.get('x', i)) for i, d in enumerate(data)],
                'datasets': [{
                    'label': options.get('series_name', 'Series 1'),
                    'data': [d.get('value', d.get('y', 0)) for d in data],
                    'borderColor': options.get('color', '#3b82f6'),
                    'fill': options.get('fill', False)
                }]
            },
            'options': {
                'responsive': True,
                'scales': options.get('scales', {}),
                'plugins': options.get('plugins', {})
            }
        }

    def _generate_bar_chart(self, title: str, data: List, options: Dict) -> Dict:
        """Generate bar chart widget data."""
        return {
            'type': 'bar',
            'title': title,
            'data': {
                'labels': [d.get('label', d.get('x', i)) for i, d in enumerate(data)],
                'datasets': [{
                    'label': options.get('series_name', 'Series 1'),
                    'data': [d.get('value', d.get('y', 0)) for d in data],
                    'backgroundColor': options.get('color', '#3b82f6')
                }]
            },
            'options': {
                'responsive': True,
                'scales': options.get('scales', {}),
                'plugins': options.get('plugins', {})
            }
        }

    def _generate_pie_chart(self, title: str, data: List, options: Dict) -> Dict:
        """Generate pie chart widget data."""
        colors = options.get('colors', ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6'])
        return {
            'type': 'pie',
            'title': title,
            'data': {
                'labels': [d.get('label', d.get('name', i)) for i, d in enumerate(data)],
                'datasets': [{
                    'data': [d.get('value', d.get('count', 0)) for d in data],
                    'backgroundColor': colors[:len(data)]
                }]
            },
            'options': {
                'responsive': True,
                'plugins': options.get('plugins', {})
            }
        }

    def _generate_table(self, title: str, data: List, options: Dict) -> Dict:
        """Generate table widget data."""
        columns = options.get('columns', [])
        if not columns and data and isinstance(data[0], dict):
            columns = list(data[0].keys())

        return {
            'type': 'table',
            'title': title,
            'data': {
                'columns': columns,
                'rows': [[d.get(col) for col in columns] for d in data]
            },
            'options': {
                'sortable': options.get('sortable', True),
                'pageSize': options.get('page_size', 10)
            }
        }

    def _generate_kpi(self, title: str, data: List, options: Dict) -> Dict:
        """Generate KPI card widget data."""
        value = data[0].get('value', 0) if data else 0
        change = data[0].get('change', 0) if data else 0
        trend = 'up' if change > 0 else 'down' if change < 0 else 'flat'

        return {
            'type': 'kpi',
            'title': title,
            'data': {
                'value': value,
                'change': change,
                'trend': trend,
                'format': options.get('format', 'number'),
                'unit': options.get('unit', '')
            },
            'options': {
                'showTrend': options.get('show_trend', True),
                'showSparkline': options.get('show_sparkline', False)
            }
        }

    def _generate_gauge(self, title: str, data: List, options: Dict) -> Dict:
        """Generate gauge widget data."""
        value = data[0].get('value', 0) if data else 0
        min_val = options.get('min', 0)
        max_val = options.get('max', 100)

        return {
            'type': 'gauge',
            'title': title,
            'data': {
                'value': value,
                'min': min_val,
                'max': max_val,
                'color': options.get('color', '#10b981')
            },
            'options': {
                'thresholds': options.get('thresholds', [
                    {'value': 33, 'color': '#ef4444'},
                    {'value': 66, 'color': '#f59e0b'},
                    {'value': 100, 'color': '#10b981'}
                ])
            }
        }


class AlertRuleAction(BaseAction):
    """Define and evaluate alert rules for API monitoring.
    
    Supports threshold, trend, and anomaly-based alerts.
    """
    action_type = "alert_rule"
    display_name = "告警规则"
    description = "定义和评估API监控告警规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Evaluate an alert rule.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - rule_name: Name of the alert rule.
                - metric_name: Metric to evaluate.
                - condition: 'gt', 'lt', 'eq', 'gte', 'lte'.
                - threshold: Threshold value.
                - duration: Duration in seconds condition must persist.
                - severity: 'info', 'warning', 'critical'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with alert evaluation result or error.
        """
        rule_name = params.get('rule_name', '')
        metric_name = params.get('metric_name', '')
        condition = params.get('condition', 'gt')
        threshold = params.get('threshold', 0)
        duration = params.get('duration', 0)
        severity = params.get('severity', 'warning')
        output_var = params.get('output_var', 'alert_result')

        if not rule_name or not metric_name:
            return ActionResult(
                success=False,
                message="Parameters 'rule_name' and 'metric_name' are required"
            )

        try:
            # Get metric value from context
            metric_value = context.variables.get(f'metric_{metric_name}', 0)

            # Evaluate condition
            triggered = self._evaluate_condition(metric_value, condition, threshold)

            # Check duration (simplified - in real impl would track state)
            alert_state = 'triggered' if triggered else 'ok'

            result = {
                'rule_name': rule_name,
                'metric_name': metric_name,
                'metric_value': metric_value,
                'condition': f'{condition} {threshold}',
                'severity': severity,
                'state': alert_state,
                'triggered_at': datetime.now().isoformat() if triggered else None
            }

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Alert rule '{rule_name}': {alert_state}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Alert rule evaluation failed: {str(e)}"
            )

    def _evaluate_condition(self, value: float, condition: str, threshold: float) -> bool:
        """Evaluate a condition."""
        ops = {
            'gt': lambda v, t: v > t,
            'lt': lambda v, t: v < t,
            'eq': lambda v, t: v == t,
            'gte': lambda v, t: v >= t,
            'lte': lambda v, t: v <= t,
        }
        return ops.get(condition, lambda v, t: False)(value, threshold)


class TimeSeriesAnalysisAction(BaseAction):
    """Perform time series analysis on metrics data.
    
    Supports moving averages, trend detection, and forecasting.
    """
    action_type = "timeseries_analysis"
    display_name = "时序分析"
    description = "对指标数据进行时序分析，支持趋势检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Analyze time series data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Time series data list.
                - timestamp_field: Field containing timestamps.
                - value_field: Field containing values.
                - analysis_type: 'trend', 'forecast', 'anomaly'.
                - window_size: Window size for moving average.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with analysis result or error.
        """
        data = params.get('data', [])
        timestamp_field = params.get('timestamp_field', 'timestamp')
        value_field = params.get('value_field', 'value')
        analysis_type = params.get('analysis_type', 'trend')
        window_size = params.get('window_size', 5)
        output_var = params.get('output_var', 'analysis')

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message=f"Expected list for data, got {type(data).__name__}"
            )

        try:
            # Sort by timestamp
            sorted_data = sorted(
                data,
                key=lambda x: x.get(timestamp_field, 0)
            )

            if analysis_type == 'trend':
                result = self._analyze_trend(sorted_data, value_field, window_size)
            elif analysis_type == 'forecast':
                result = self._forecast(sorted_data, value_field, params.get('horizon', 5))
            elif analysis_type == 'anomaly':
                result = self._detect_anomalies(sorted_data, value_field)
            else:
                result = {'error': 'Unknown analysis type'}

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Time series analysis completed: {analysis_type}"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Time series analysis failed: {str(e)}"
            )

    def _analyze_trend(self, data: List, value_field: str, window: int) -> Dict:
        """Analyze trend using moving average."""
        values = [d.get(value_field, 0) for d in data]

        # Calculate moving average
        ma = []
        for i in range(len(values)):
            start = max(0, i - window + 1)
            ma.append(sum(values[start:i+1]) / (i - start + 1))

        # Determine trend direction
        if len(ma) >= 2:
            slope = ma[-1] - ma[0]
            trend = 'increasing' if slope > 0.1 else 'decreasing' if slope < -0.1 else 'stable'
        else:
            trend = 'unknown'
            slope = 0

        return {
            'trend': trend,
            'slope': slope,
            'moving_average': ma,
            'current_value': values[-1] if values else 0
        }

    def _forecast(self, data: List, value_field: str, horizon: int) -> Dict:
        """Simple forecast using linear regression."""
        values = [d.get(value_field, 0) for d in data]

        if len(values) < 2:
            return {'forecast': [], 'error': 'Insufficient data'}

        # Simple linear regression
        n = len(values)
        x = list(range(n))
        x_mean = sum(x) / n
        y_mean = sum(values) / n

        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            slope = 0
            intercept = y_mean
        else:
            slope = numerator / denominator
            intercept = y_mean - slope * x_mean

        # Forecast future values
        forecast = []
        for i in range(n, n + horizon):
            forecast.append(slope * i + intercept)

        return {
            'forecast': forecast,
            'slope': slope,
            'intercept': intercept,
            'r_squared': self._calculate_r_squared(values, slope, intercept)
        }

    def _detect_anomalies(self, data: List, value_field: str) -> Dict:
        """Detect anomalies using Z-score."""
        values = [d.get(value_field, 0) for d in data]

        if len(values) < 3:
            return {'anomalies': [], 'error': 'Insufficient data'}

        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance ** 0.5

        anomalies = []
        threshold = 2.5  # Z-score threshold

        for i, d in enumerate(data):
            z_score = abs((d.get(value_field) - mean) / std) if std > 0 else 0
            if z_score > threshold:
                anomalies.append({
                    'index': i,
                    'timestamp': d.get('timestamp', i),
                    'value': d.get(value_field),
                    'z_score': z_score
                })

        return {
            'anomalies': anomalies,
            'anomaly_count': len(anomalies),
            'mean': mean,
            'std': std
        }

    def _calculate_r_squared(self, values: List, slope: float, intercept: float) -> float:
        """Calculate R-squared for regression."""
        if len(values) < 2:
            return 0

        n = len(values)
        x = list(range(n))
        y_mean = sum(values) / n

        ss_res = sum((values[i] - (slope * x[i] + intercept)) ** 2 for i in range(n))
        ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))

        return 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
