"""Metric14 action module for RabAI AutoClick.

Provides additional metric operations:
- MetricCounterAction: Increment counter
- MetricGaugeAction: Set gauge value
- MetricHistogramAction: Record histogram
- MetricTimerAction: Record timing
- MetricSummaryAction: Record summary
- MetricLabelAction: Add metric label
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MetricCounterAction(BaseAction):
    """Increment counter."""
    action_type = "metric14_counter"
    display_name = "计数器"
    description = "增加计数器指标"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute counter increment.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with counter value.
        """
        name = params.get('name', 'counter')
        value = params.get('value', 1)
        output_var = params.get('output_var', 'counter_value')

        try:
            resolved_name = context.resolve_value(name) if name else 'counter'
            resolved_value = int(context.resolve_value(value)) if value else 1

            current = getattr(context, '_metrics', {}).get(resolved_name, 0)
            result = current + resolved_value

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            context._metrics[resolved_name] = result

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"计数器 {resolved_name}: {result}",
                data={
                    'name': resolved_name,
                    'value': result,
                    'increment': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'value': 1, 'output_var': 'counter_value'}


class MetricGaugeAction(BaseAction):
    """Set gauge value."""
    action_type = "metric14_gauge"
    display_name = "仪表值"
    description = "设置仪表指标值"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute gauge set.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with gauge value.
        """
        name = params.get('name', 'gauge')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'gauge_value')

        try:
            resolved_name = context.resolve_value(name) if name else 'gauge'
            resolved_value = float(context.resolve_value(value)) if value else 0

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            context._metrics[resolved_name] = resolved_value

            context.set(output_var, resolved_value)

            return ActionResult(
                success=True,
                message=f"仪表 {resolved_name}: {resolved_value}",
                data={
                    'name': resolved_name,
                    'value': resolved_value,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"仪表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'gauge_value'}


class MetricHistogramAction(BaseAction):
    """Record histogram."""
    action_type = "metric14_histogram"
    display_name = "直方图"
    description = "记录直方图指标"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute histogram record.

        Args:
            context: Execution context.
            params: Dict with name, value, buckets, output_var.

        Returns:
            ActionResult with histogram data.
        """
        name = params.get('name', 'histogram')
        value = params.get('value', 0)
        buckets = params.get('buckets', [0.1, 0.5, 1.0, 5.0, 10.0])
        output_var = params.get('output_var', 'histogram_value')

        try:
            resolved_name = context.resolve_value(name) if name else 'histogram'
            resolved_value = float(context.resolve_value(value)) if value else 0
            resolved_buckets = context.resolve_value(buckets) if buckets else [0.1, 0.5, 1.0, 5.0, 10.0]

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            if resolved_name not in context._metrics:
                context._metrics[resolved_name] = []
            context._metrics[resolved_name].append(resolved_value)

            result = {
                'name': resolved_name,
                'value': resolved_value,
                'count': len(context._metrics[resolved_name]),
                'min': min(context._metrics[resolved_name]),
                'max': max(context._metrics[resolved_name]),
                'sum': sum(context._metrics[resolved_name]),
                'buckets': resolved_buckets
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"直方图 {resolved_name}: {resolved_value}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"直方图失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'buckets': [0.1, 0.5, 1.0, 5.0, 10.0], 'output_var': 'histogram_value'}


class MetricTimerAction(BaseAction):
    """Record timing."""
    action_type = "metric14_timer"
    display_name = "计时器"
    description = "记录时间指标"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timer record.

        Args:
            context: Execution context.
            params: Dict with name, duration, output_var.

        Returns:
            ActionResult with timer data.
        """
        name = params.get('name', 'timer')
        duration = params.get('duration', 0)
        output_var = params.get('output_var', 'timer_value')

        try:
            import time

            resolved_name = context.resolve_value(name) if name else 'timer'
            resolved_duration = float(context.resolve_value(duration)) if duration else 0

            if resolved_duration == 0:
                resolved_duration = time.time()

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            if resolved_name not in context._metrics:
                context._metrics[resolved_name] = []

            if isinstance(context._metrics[resolved_name], list) and len(context._metrics[resolved_name]) > 0:
                if isinstance(context._metrics[resolved_name][-1], float):
                    resolved_duration = time.time() - context._metrics[resolved_name][-1]

            context._metrics[resolved_name].append(resolved_duration)

            result = {
                'name': resolved_name,
                'duration': resolved_duration,
                'count': len(context._metrics[resolved_name]),
                'output_var': output_var
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"计时器 {resolved_name}: {resolved_duration:.4f}s",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计时器失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'duration': 0, 'output_var': 'timer_value'}


class MetricSummaryAction(BaseAction):
    """Record summary."""
    action_type = "metric14_summary"
    display_name = "摘要"
    description = "记录摘要指标"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute summary record.

        Args:
            context: Execution context.
            params: Dict with name, value, output_var.

        Returns:
            ActionResult with summary data.
        """
        name = params.get('name', 'summary')
        value = params.get('value', 0)
        output_var = params.get('output_var', 'summary_value')

        try:
            resolved_name = context.resolve_value(name) if name else 'summary'
            resolved_value = float(context.resolve_value(value)) if value else 0

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            if resolved_name not in context._metrics:
                context._metrics[resolved_name] = []
            context._metrics[resolved_name].append(resolved_value)

            values = context._metrics[resolved_name]
            sorted_values = sorted(values)
            count = len(sorted_values)

            result = {
                'name': resolved_name,
                'count': count,
                'sum': sum(sorted_values),
                'mean': sum(sorted_values) / count if count > 0 else 0,
                'min': sorted_values[0] if count > 0 else 0,
                'max': sorted_values[-1] if count > 0 else 0,
                'p50': sorted_values[count // 2] if count > 0 else 0,
                'p95': sorted_values[int(count * 0.95)] if count > 0 else 0,
                'p99': sorted_values[int(count * 0.99)] if count > 0 else 0,
                'output_var': output_var
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"摘要 {resolved_name}: mean={result['mean']:.2f}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"摘要失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['name', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'summary_value'}


class MetricLabelAction(BaseAction):
    """Add metric label."""
    action_type = "metric14_label"
    display_name = "指标标签"
    description = "为指标添加标签"
    version = "14.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute label add.

        Args:
            context: Execution context.
            params: Dict with metric, label, value, output_var.

        Returns:
            ActionResult with labeled metric.
        """
        metric = params.get('metric', '')
        label = params.get('label', '')
        value = params.get('value', '')
        output_var = params.get('output_var', 'labeled_metric')

        try:
            resolved_metric = context.resolve_value(metric) if metric else ''
            resolved_label = context.resolve_value(label) if label else ''
            resolved_value = context.resolve_value(value) if value else ''

            if not hasattr(context, '_metrics'):
                context._metrics = {}
            if resolved_metric not in context._metrics:
                context._metrics[resolved_metric] = {}
            context._metrics[resolved_metric][resolved_label] = resolved_value

            result = {
                'metric': resolved_metric,
                'label': resolved_label,
                'value': resolved_value
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"指标标签: {resolved_metric}.{resolved_label}={resolved_value}",
                data=result
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"指标标签失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['metric', 'label', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'labeled_metric'}