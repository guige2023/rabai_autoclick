"""Metrics collector action module for RabAI AutoClick.

Provides metrics collection and aggregation for monitoring
workflow and action performance with various output formats.
"""

import time
import statistics
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MetricsCollectorAction(BaseAction):
    """Collect and aggregate performance metrics.
    
    Collects timing, count, and error metrics from
    action executions and aggregates them.
    """
    action_type = "metrics_collector"
    display_name = "指标收集"
    description = "收集和聚合性能指标"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Collect or report metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (record|report|reset),
                   metric_name, value, tags, aggregation_window.
        
        Returns:
            ActionResult with metrics data.
        """
        operation = params.get('operation', 'record')
        metric_name = params.get('metric_name', '')
        value = params.get('value')
        tags = params.get('tags', {})
        window = params.get('aggregation_window', 'minute')
        start_time = time.time()

        if not hasattr(context, '_metrics'):
            context._metrics = {}

        if operation == 'record':
            if metric_name not in context._metrics:
                context._metrics[metric_name] = {
                    'values': [],
                    'tags': [],
                    'timestamps': []
                }

            context._metrics[metric_name]['values'].append(value)
            context._metrics[metric_name]['tags'].append(tags)
            context._metrics[metric_name]['timestamps'].append(time.time())

            return ActionResult(
                success=True,
                message=f"Recorded metric: {metric_name}",
                data={
                    'metric_name': metric_name,
                    'value': value,
                    'total_samples': len(context._metrics[metric_name]['values'])
                }
            )

        elif operation == 'report':
            if metric_name and metric_name in context._metrics:
                return ActionResult(
                    success=True,
                    message=f"Metrics report for: {metric_name}",
                    data=self._compute_metrics(metric_name, context._metrics[metric_name])
                )
            else:
                all_metrics = {}
                for name, data in context._metrics.items():
                    all_metrics[name] = self._compute_metrics(name, data)
                return ActionResult(
                    success=True,
                    message=f"Report for {len(all_metrics)} metrics",
                    data={'metrics': all_metrics}
                )

        elif operation == 'reset':
            if metric_name and metric_name in context._metrics:
                del context._metrics[metric_name]
            else:
                context._metrics.clear()
            return ActionResult(
                success=True,
                message=f"Reset metrics: {metric_name or 'all'}"
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _compute_metrics(self, name: str, data: Dict) -> Dict:
        """Compute aggregated metrics."""
        values = [v for v in data['values'] if isinstance(v, (int, float))]
        if not values:
            return {'metric_name': name, 'count': len(data['values']), 'numeric_count': 0}

        sorted_vals = sorted(values)
        return {
            'metric_name': name,
            'count': len(values),
            'total_samples': len(data['values']),
            'sum': round(sum(values), 4),
            'mean': round(statistics.mean(values), 4),
            'median': round(statistics.median(sorted_vals), 4),
            'min': round(min(values), 4),
            'max': round(max(values), 4),
            'stdev': round(statistics.stdev(values), 4) if len(values) > 1 else 0,
            'p95': round(sorted_vals[int(len(sorted_vals) * 0.95)], 4),
            'p99': round(sorted_vals[int(len(sorted_vals) * 0.99)], 4),
        }


class TimerAction(BaseAction):
    """Time the execution of actions or code blocks.
    
    Provides precise timing measurements with
    start/stop/pause capabilities.
    """
    action_type = "timer"
    display_name = "计时器"
    description = "计时动作或代码块执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Control timer.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (start|stop|pause|resume|get),
                   timer_name, lap.
        
        Returns:
            ActionResult with timer result.
        """
        operation = params.get('operation', 'get')
        timer_name = params.get('timer_name', 'default')
        start_time = time.time()

        if not hasattr(context, '_timers'):
            context._timers = {}

        if timer_name not in context._timers:
            context._timers[timer_name] = {
                'start': None,
                'elapsed': 0,
                'running': False,
                'laps': []
            }

        timer = context._timers[timer_name]

        if operation == 'start':
            timer['start'] = time.time()
            timer['elapsed'] = 0
            timer['running'] = True
            timer['laps'] = []
            return ActionResult(
                success=True,
                message=f"Timer '{timer_name}' started",
                data={'timer_name': timer_name, 'running': True}
            )

        elif operation == 'stop':
            if timer['running'] and timer['start']:
                timer['elapsed'] += time.time() - timer['start']
            timer['running'] = False
            return ActionResult(
                success=True,
                message=f"Timer '{timer_name}' stopped",
                data={
                    'timer_name': timer_name,
                    'elapsed_seconds': round(timer['elapsed'], 4),
                    'laps': timer['laps']
                }
            )

        elif operation == 'pause':
            if timer['running'] and timer['start']:
                timer['elapsed'] += time.time() - timer['start']
                timer['running'] = False
            return ActionResult(
                success=True,
                message=f"Timer '{timer_name}' paused",
                data={'elapsed_seconds': round(timer['elapsed'], 4)}
            )

        elif operation == 'resume':
            if not timer['running']:
                timer['start'] = time.time()
                timer['running'] = True
            return ActionResult(
                success=True,
                message=f"Timer '{timer_name}' resumed",
                data={'running': True}
            )

        elif operation == 'lap':
            if timer['running'] and timer['start']:
                lap_time = time.time() - timer['start']
                timer['elapsed'] = lap_time
                timer['laps'].append(lap_time)
                return ActionResult(
                    success=True,
                    message=f"Lap {len(timer['laps'])}: {round(lap_time, 4)}s",
                    data={
                        'lap': len(timer['laps']),
                        'lap_time': round(lap_time, 4),
                        'total_elapsed': round(timer['elapsed'], 4)
                    }
                )

        current_elapsed = timer['elapsed']
        if timer['running'] and timer['start']:
            current_elapsed = timer['elapsed'] + (time.time() - timer['start'])

        return ActionResult(
            success=True,
            message=f"Timer '{timer_name}': {round(current_elapsed, 4)}s",
            data={
                'timer_name': timer_name,
                'elapsed_seconds': round(current_elapsed, 4),
                'running': timer['running'],
                'lap_count': len(timer['laps'])
            }
        )


class CounterAction(BaseAction):
    """Increment and track counters.
    
    Provides atomic counter operations with
    increment, decrement, and reset capabilities.
    """
    action_type = "counter"
    display_name = "计数器"
    description = "递增和跟踪计数器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Operate on counter.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (inc|dec|get|reset),
                   counter_name, amount, max_value.
        
        Returns:
            ActionResult with counter value.
        """
        operation = params.get('operation', 'get')
        counter_name = params.get('counter_name', 'default')
        amount = params.get('amount', 1)
        max_value = params.get('max_value')
        start_time = time.time()

        if not hasattr(context, '_counters'):
            context._counters = {}

        if counter_name not in context._counters:
            context._counters[counter_name] = {'count': 0, 'history': []}

        counter = context._counters[counter_name]

        if operation == 'inc':
            new_val = counter['count'] + amount
            if max_value and new_val > max_value:
                new_val = max_value
            counter['count'] = new_val
            counter['history'].append(new_val)
            return ActionResult(
                success=True,
                message=f"Counter '{counter_name}': {new_val}",
                data={'counter_name': counter_name, 'value': new_val}
            )

        elif operation == 'dec':
            new_val = counter['count'] - amount
            counter['count'] = max(0, new_val)
            counter['history'].append(counter['count'])
            return ActionResult(
                success=True,
                message=f"Counter '{counter_name}': {counter['count']}",
                data={'counter_name': counter_name, 'value': counter['count']}
            )

        elif operation == 'reset':
            counter['count'] = 0
            counter['history'] = []
            return ActionResult(
                success=True,
                message=f"Counter '{counter_name}' reset",
                data={'counter_name': counter_name, 'value': 0}
            )

        return ActionResult(
            success=True,
            message=f"Counter '{counter_name}': {counter['count']}",
            data={
                'counter_name': counter_name,
                'value': counter['count'],
                'history_length': len(counter['history'])
            }
        )
