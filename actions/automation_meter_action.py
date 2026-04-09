"""Automation Meter Action Module.

Provides metrics collection and monitoring for automation workflows.
"""

import time
import traceback
import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict, deque

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationMeterAction(BaseAction):
    """Collect and report automation metrics.
    
    Tracks execution times, success rates, and resource usage.
    """
    action_type = "automation_meter"
    display_name = "自动化计量"
    description = "收集和报告自动化指标"
    
    def __init__(self):
        super().__init__()
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._timers: Dict[str, float] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metering operation.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, metric_name, value.
        
        Returns:
            ActionResult with metering result.
        """
        action = params.get('action', 'count')
        metric_name = params.get('metric_name', '')
        
        if not metric_name:
            return ActionResult(
                success=False,
                data=None,
                error="Metric name required"
            )
        
        if action == 'count':
            return self._increment_counter(metric_name, params)
        elif action == 'gauge':
            return self._set_gauge(metric_name, params)
        elif action == 'histogram':
            return self._record_histogram(metric_name, params)
        elif action == 'timer_start':
            return self._start_timer(metric_name)
        elif action == 'timer_stop':
            return self._stop_timer(metric_name)
        elif action == 'get':
            return self._get_metric(metric_name)
        elif action == 'get_all':
            return self._get_all_metrics()
        elif action == 'reset':
            return self._reset_metric(metric_name)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _increment_counter(self, name: str, params: Dict) -> ActionResult:
        """Increment a counter."""
        increment = params.get('increment', 1)
        self._counters[name] += increment
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'type': 'counter',
                'value': self._counters[name]
            },
            error=None
        )
    
    def _set_gauge(self, name: str, params: Dict) -> ActionResult:
        """Set a gauge value."""
        value = params.get('value', 0)
        self._gauges[name] = value
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'type': 'gauge',
                'value': value
            },
            error=None
        )
    
    def _record_histogram(self, name: str, params: Dict) -> ActionResult:
        """Record a histogram value."""
        value = params.get('value', 0)
        self._histograms[name].append(value)
        
        values = list(self._histograms[name])
        sorted_values = sorted(values)
        count = len(sorted_values)
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'type': 'histogram',
                'count': count,
                'min': min(sorted_values) if sorted_values else 0,
                'max': max(sorted_values) if sorted_values else 0,
                'mean': sum(sorted_values) / count if count > 0 else 0,
                'p50': sorted_values[count // 2] if count > 0 else 0,
                'p95': sorted_values[int(count * 0.95)] if count > 0 else 0,
                'p99': sorted_values[int(count * 0.99)] if count > 0 else 0
            },
            error=None
        )
    
    def _start_timer(self, name: str) -> ActionResult:
        """Start a timer."""
        self._timers[name] = time.time()
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'type': 'timer',
                'status': 'started'
            },
            error=None
        )
    
    def _stop_timer(self, name: str) -> ActionResult:
        """Stop a timer and record duration."""
        if name not in self._timers:
            return ActionResult(
                success=False,
                data=None,
                error=f"Timer {name} not started"
            )
        
        duration = time.time() - self._timers[name]
        del self._timers[name]
        
        self._histograms[name].append(duration)
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'type': 'timer',
                'duration': duration
            },
            error=None
        )
    
    def _get_metric(self, name: str) -> ActionResult:
        """Get a specific metric."""
        if name in self._counters:
            return ActionResult(
                success=True,
                data={
                    'metric': name,
                    'type': 'counter',
                    'value': self._counters[name]
                },
                error=None
            )
        elif name in self._gauges:
            return ActionResult(
                success=True,
                data={
                    'metric': name,
                    'type': 'gauge',
                    'value': self._gauges[name]
                },
                error=None
            )
        elif name in self._histograms:
            values = list(self._histograms[name])
            return ActionResult(
                success=True,
                data={
                    'metric': name,
                    'type': 'histogram',
                    'count': len(values),
                    'values': values[-10:]
                },
                error=None
            )
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Metric {name} not found"
            )
    
    def _get_all_metrics(self) -> ActionResult:
        """Get all metrics."""
        return ActionResult(
            success=True,
            data={
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {
                    name: {
                        'count': len(values),
                        'latest': values[-1] if values else None
                    }
                    for name, values in self._histograms.items()
                }
            },
            error=None
        )
    
    def _reset_metric(self, name: str) -> ActionResult:
        """Reset a metric."""
        if name in self._counters:
            del self._counters[name]
        if name in self._gauges:
            del self._gauges[name]
        if name in self._histograms:
            del self._histograms[name]
        if name in self._timers:
            del self._timers[name]
        
        return ActionResult(
            success=True,
            data={'metric': name, 'reset': True},
            error=None
        )


class AutomationRateTrackerAction(BaseAction):
    """Track rates over time windows.
    
    Monitors events per second/minute/hour.
    """
    action_type = "automation_rate_tracker"
    display_name = "速率追踪"
    description = "追踪时间窗口内的事件速率"
    
    def __init__(self):
        super().__init__()
        self._events: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rate tracking.
        
        Args:
            context: Execution context.
            params: Dict with keys: action, metric_name.
        
        Returns:
            ActionResult with rate data.
        """
        action = params.get('action', 'record')
        metric_name = params.get('metric_name', '')
        window = params.get('window', 60)
        
        if not metric_name:
            return ActionResult(
                success=False,
                data=None,
                error="Metric name required"
            )
        
        if action == 'record':
            return self._record_event(metric_name)
        elif action == 'rate':
            return self._calculate_rate(metric_name, window)
        elif action == 'clear':
            return self._clear_events(metric_name)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown action: {action}"
            )
    
    def _record_event(self, name: str) -> ActionResult:
        """Record an event."""
        self._events[name].append(time.time())
        
        return ActionResult(
            success=True,
            data={'metric': name, 'recorded': True},
            error=None
        )
    
    def _calculate_rate(self, name: str, window: int) -> ActionResult:
        """Calculate rate per window."""
        cutoff = time.time() - window
        events = [e for e in self._events[name] if e >= cutoff]
        
        rate = len(events) / window if window > 0 else 0
        
        return ActionResult(
            success=True,
            data={
                'metric': name,
                'window_seconds': window,
                'event_count': len(events),
                'rate_per_second': rate,
                'rate_per_minute': rate * 60
            },
            error=None
        )
    
    def _clear_events(self, name: str) -> ActionResult:
        """Clear events for a metric."""
        count = len(self._events[name])
        self._events[name].clear()
        
        return ActionResult(
            success=True,
            data={'metric': name, 'cleared': count},
            error=None
        )


def register_actions():
    """Register all Automation Meter actions."""
    return [
        AutomationMeterAction,
        AutomationRateTrackerAction,
    ]
