"""Automation Monitor Action.

Monitors automation execution health with metrics collection, anomaly
detection, performance tracking, and alerting integration.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict, deque
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricSample:
    """A single metric sample."""
    name: str
    value: float
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)


class AutomationMonitorAction(BaseAction):
    """Monitor automation execution health and performance.
    
    Collects metrics, detects anomalies, tracks performance,
    and integrates with alerting systems.
    """
    action_type = "automation_monitor"
    display_name = "自动化监控"
    description = "监控自动化执行健康状态，支持指标收集和异常检测"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._lock = threading.RLock()
        self._alert_callbacks: List[Callable] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor automation execution.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'record', 'get', 'alert', 'status', 'reset'.
                - metric_name: Name of metric to record/get.
                - value: Value to record (for record action).
                - tags: Tags for metric (for record action).
                - time_window: Time window in seconds (for anomaly detection).
                - threshold: Threshold for alerting.
                - operator: 'gt', 'lt', 'eq', 'stddev' (for anomaly detection).
                - save_to_var: Variable name for results.
        
        Returns:
            ActionResult with monitoring data.
        """
        try:
            action = params.get('action', 'record')
            save_to_var = params.get('save_to_var', 'monitor_data')

            if action == 'record':
                return self._record_metric(params, save_to_var)
            elif action == 'get':
                return self._get_metrics(params, save_to_var)
            elif action == 'alert':
                return self._check_alert(params, save_to_var)
            elif action == 'status':
                return self._get_status(save_to_var)
            elif action == 'reset':
                return self._reset(params, save_to_var)
            else:
                return ActionResult(success=False, message=f"Unknown monitor action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Monitor error: {e}")

    def _record_metric(self, params: Dict, save_to_var: str) -> ActionResult:
        """Record a metric sample."""
        metric_name = params.get('metric_name')
        value = params.get('value')
        tags = params.get('tags', {})
        
        if not metric_name:
            return ActionResult(success=False, message="metric_name is required")

        sample = MetricSample(name=metric_name, value=value, timestamp=time.time(), tags=tags)
        
        with self._lock:
            self._metrics[metric_name].append(sample)
            self._gauges[metric_name] = value

        context.set_variable(save_to_var, {'recorded': True, 'metric': metric_name, 'value': value})
        return ActionResult(success=True, message=f"Recorded {metric_name}={value}")

    def _get_metrics(self, params: Dict, save_to_var: str) -> ActionResult:
        """Get metric data."""
        metric_name = params.get('metric_name')
        time_window = params.get('time_window', 60)
        stats = params.get('stats', ['avg', 'min', 'max', 'count'])

        with self._lock:
            if metric_name:
                samples = list(self._metrics.get(metric_name, []))
                cutoff = time.time() - time_window
                samples = [s for s in samples if s.timestamp >= cutoff]
                values = [s.value for s in samples]
                
                result = self._calculate_stats(values, stats)
                result['samples'] = len(values)
                result['metric_name'] = metric_name
                result['time_window'] = time_window
            else:
                # Get all metrics
                result = {
                    'metrics': {
                        name: self._calculate_stats([s.value for s in samples[-100:]], stats)
                        for name, samples in self._metrics.items()
                    },
                    'counters': dict(self._counters),
                    'gauges': dict(self._gauges)
                }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result)

    def _calculate_stats(self, values: List[float], stats: List[str]) -> Dict:
        """Calculate statistics for values."""
        if not values:
            return {stat: None for stat in stats}

        import statistics
        result = {}
        
        for stat in stats:
            if stat == 'avg' or stat == 'mean':
                result['avg'] = statistics.mean(values)
            elif stat == 'min':
                result['min'] = min(values)
            elif stat == 'max':
                result['max'] = max(values)
            elif stat == 'count':
                result['count'] = len(values)
            elif stat == 'sum':
                result['sum'] = sum(values)
            elif stat == 'stddev':
                result['stddev'] = statistics.stdev(values) if len(values) > 1 else 0
            elif stat == 'median':
                result['median'] = statistics.median(values)
            elif stat == 'p95':
                sorted_vals = sorted(values)
                idx = int(len(sorted_vals) * 0.95)
                result['p95'] = sorted_vals[min(idx, len(sorted_vals) - 1)]
            elif stat == 'p99':
                sorted_vals = sorted(values)
                idx = int(len(sorted_vals) * 0.99)
                result['p99'] = sorted_vals[min(idx, len(sorted_vals) - 1)]

        return result

    def _check_alert(self, params: Dict, save_to_var: str) -> ActionResult:
        """Check for alert conditions."""
        metric_name = params.get('metric_name')
        threshold = params.get('threshold')
        operator = params.get('operator', 'gt')
        time_window = params.get('time_window', 60)

        if not metric_name or threshold is None:
            return ActionResult(success=False, message="metric_name and threshold are required")

        with self._lock:
            samples = list(self._metrics.get(metric_name, []))
            cutoff = time.time() - time_window
            samples = [s for s in samples if s.timestamp >= cutoff]
            values = [s.value for s in samples]
            
            if not values:
                return ActionResult(success=True, data={'alert': False, 'reason': 'no_data'})
            
            current_value = values[-1] if values else None
            avg_value = statistics.mean(values) if values else 0

        triggered = False
        reason = ""

        if operator == 'gt':
            triggered = current_value > threshold
            reason = f"{current_value} > {threshold}"
        elif operator == 'lt':
            triggered = current_value < threshold
            reason = f"{current_value} < {threshold}"
        elif operator == 'eq':
            triggered = current_value == threshold
            reason = f"{current_value} == {threshold}"
        elif operator == 'stddev':
            if len(values) > 1:
                import statistics
                stddev = statistics.stdev(values)
                triggered = stddev > threshold
                reason = f"stddev={stddev} > {threshold}"
        elif operator == 'anomaly':
            # Simple anomaly: value is > 2 stddev from mean
            if len(values) > 2:
                import statistics
                mean = statistics.mean(values)
                stddev = statistics.stdev(values)
                triggered = abs(current_value - mean) > (threshold * stddev)
                reason = f"|{current_value} - {mean}| > {threshold} * {stddev}"

        result = {
            'alert': triggered,
            'metric_name': metric_name,
            'threshold': threshold,
            'operator': operator,
            'current_value': current_value,
            'reason': reason
        }

        if triggered:
            for callback in self._alert_callbacks:
                try:
                    callback(result)
                except Exception:
                    pass

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result, 
                          message=f"Alert {'triggered' if triggered else 'not triggered'}")

    def _get_status(self, save_to_var: str) -> ActionResult:
        """Get overall monitoring status."""
        with self._lock:
            total_samples = sum(len(samples) for samples in self._metrics.values())
            result = {
                'total_metrics': len(self._metrics),
                'total_samples': total_samples,
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'metric_names': list(self._metrics.keys())
            }

        context.set_variable(save_to_var, result)
        return ActionResult(success=True, data=result)

    def _reset(self, params: Dict, save_to_var: str) -> ActionResult:
        """Reset monitoring data."""
        metric_name = params.get('metric_name')
        
        with self._lock:
            if metric_name and metric_name in self._metrics:
                self._metrics[metric_name].clear()
                if metric_name in self._gauges:
                    del self._gauges[metric_name]
            else:
                self._metrics.clear()
                self._counters.clear()
                self._gauges.clear()

        return ActionResult(success=True, message="Monitoring data reset")
