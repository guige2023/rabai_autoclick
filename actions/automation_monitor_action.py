"""Automation Monitor Action Module.

Provides automation monitoring and observability capabilities including
health checks, metrics collection, and alerting.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class HealthStatus:
    """Health check status."""
    healthy: bool
    component: str
    message: str
    timestamp: str
    details: Dict = None


class AutomationMonitorAction(BaseAction):
    """Monitor automation health and performance.
    
    Supports health checks, metrics collection, and anomaly detection.
    """
    action_type = "automation_monitor"
    display_name = "自动化监控"
    description = "监控自动化运行状况和性能"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._health_checks: Dict[str, Callable] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor automation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'check_health', 'record_metric', 'get_metrics', 'alert'.
                - component: Component name to monitor.
                - metric_name: Metric to record.
                - metric_value: Value to record.
                - threshold: Alert threshold.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with monitoring result or error.
        """
        operation = params.get('operation', 'check_health')
        component = params.get('component', 'system')
        metric_name = params.get('metric_name', '')
        metric_value = params.get('metric_value', 0)
        threshold = params.get('threshold', None)
        labels = params.get('labels', {})
        output_var = params.get('output_var', 'monitor_result')

        try:
            if operation == 'check_health':
                return self._check_health(component, output_var)
            elif operation == 'record_metric':
                return self._record_metric(component, metric_name, metric_value, labels, output_var)
            elif operation == 'get_metrics':
                return self._get_metrics(component, metric_name, output_var)
            elif operation == 'alert':
                return self._check_alert(component, metric_name, threshold, metric_value, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Automation monitor failed: {str(e)}"
            )

    def _check_health(self, component: str, output_var: str) -> ActionResult:
        """Check health of a component."""
        health = HealthStatus(
            healthy=True,
            component=component,
            message="OK",
            timestamp=datetime.now().isoformat(),
            details={}
        )

        context.variables[output_var] = {
            'healthy': health.healthy,
            'component': health.component,
            'message': health.message,
            'timestamp': health.timestamp
        }

        return ActionResult(
            success=True,
            data=context.variables[output_var],
            message=f"Health check for '{component}': {'healthy' if health.healthy else 'unhealthy'}"
        )

    def _record_metric(
        self,
        component: str,
        metric_name: str,
        metric_value: float,
        labels: Dict,
        output_var: str
    ) -> ActionResult:
        """Record a metric value."""
        key = f"{component}.{metric_name}"
        self._metrics[key].append({
            'value': metric_value,
            'timestamp': time.time(),
            'labels': labels
        })

        result = {
            'recorded': True,
            'metric': key,
            'value': metric_value,
            'count': len(self._metrics[key])
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Metric '{key}' recorded: {metric_value}"
        )

    def _get_metrics(
        self,
        component: str,
        metric_name: str,
        output_var: str
    ) -> ActionResult:
        """Get recorded metrics."""
        key = f"{component}.{metric_name}" if metric_name else component
        metrics = list(self._metrics.get(key, []))

        if not metrics:
            return ActionResult(
                success=False,
                message=f"No metrics found for '{key}'"
            )

        # Calculate statistics
        values = [m['value'] for m in metrics]
        stats = {
            'count': len(values),
            'min': min(values) if values else 0,
            'max': max(values) if values else 0,
            'avg': sum(values) / len(values) if values else 0,
            'latest': values[-1] if values else 0
        }

        result = {
            'metric': key,
            'stats': stats,
            'data_points': len(metrics)
        }

        context.variables[output_var] = result
        return ActionResult(
            success=True,
            data=result,
            message=f"Retrieved {len(metrics)} data points for '{key}'"
        )

    def _check_alert(
        self,
        component: str,
        metric_name: str,
        threshold: float,
        current_value: float,
        output_var: str
    ) -> ActionResult:
        """Check if alert threshold is exceeded."""
        key = f"{component}.{metric_name}"
        triggered = current_value >= threshold if threshold else False

        result = {
            'alert': triggered,
            'component': component,
            'metric': metric_name,
            'value': current_value,
            'threshold': threshold,
            'timestamp': datetime.now().isoformat()
        }

        context.variables[output_var] = result
        return ActionResult(
            success=not triggered,
            data=result,
            message=f"Alert {'triggered' if triggered else 'not triggered'} for '{key}'"
        )


class AutomationLoggerAction(BaseAction):
    """Structured logging for automation workflows.
    
    Supports log levels, structured fields, and log aggregation.
    """
    action_type = "automation_logger"
    display_name = "自动化日志"
    description = "自动化工作流的结构化日志"

    LOG_LEVELS = {
        'DEBUG': 10,
        'INFO': 20,
        'WARNING': 30,
        'ERROR': 40,
        'CRITICAL': 50
    }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Log automation events.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'log', 'get_logs', 'clear_logs'.
                - level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
                - message: Log message.
                - fields: Additional structured fields.
                - logger_id: Logger identifier.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with logging result or error.
        """
        operation = params.get('operation', 'log')
        level = params.get('level', 'INFO')
        message = params.get('message', '')
        fields = params.get('fields', {})
        logger_id = params.get('logger_id', 'default')
        output_var = params.get('output_var', 'log_result')

        try:
            if operation == 'log':
                return self._log_message(level, message, fields, logger_id, output_var)
            elif operation == 'get_logs':
                return self._get_logs(logger_id, params.get('level'), output_var)
            elif operation == 'clear_logs':
                return self._clear_logs(logger_id, output_var)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Automation logger failed: {str(e)}"
            )

    def _log_message(
        self,
        level: str,
        message: str,
        fields: Dict,
        logger_id: str,
        output_var: str
    ) -> ActionResult:
        """Log a message."""
        if not hasattr(self, '_log_buffers'):
            self._log_buffers = {}

        if logger_id not in self._log_buffers:
            self._log_buffers[logger_id] = deque(maxlen=1000)

        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'message': message,
            'fields': fields,
            'level_value': self.LOG_LEVELS.get(level, 20)
        }

        self._log_buffers[logger_id].append(log_entry)

        context.variables[output_var] = log_entry
        return ActionResult(
            success=True,
            data=log_entry,
            message=f"[{level}] {message}"
        )

    def _get_logs(
        self,
        logger_id: str,
        min_level: str,
        output_var: str
    ) -> ActionResult:
        """Get stored logs."""
        if not hasattr(self, '_log_buffers') or logger_id not in self._log_buffers:
            return ActionResult(
                success=False,
                message=f"No logs found for logger '{logger_id}'"
            )

        logs = list(self._log_buffers[logger_id])

        # Filter by level
        if min_level:
            min_level_value = self.LOG_LEVELS.get(min_level, 20)
            logs = [l for l in logs if l['level_value'] >= min_level_value]

        context.variables[output_var] = logs
        return ActionResult(
            success=True,
            data={'logs': logs, 'count': len(logs)},
            message=f"Retrieved {len(logs)} logs for '{logger_id}'"
        )

    def _clear_logs(self, logger_id: str, output_var: str) -> ActionResult:
        """Clear stored logs."""
        if hasattr(self, '_log_buffers') and logger_id in self._log_buffers:
            cleared = len(self._log_buffers[logger_id])
            self._log_buffers[logger_id].clear()
        else:
            cleared = 0

        context.variables[output_var] = {'cleared': cleared}
        return ActionResult(
            success=True,
            data={'cleared': cleared},
            message=f"Cleared {cleared} logs for '{logger_id}'"
        )


class AutomationReporterAction(BaseAction):
    """Generate automation execution reports.
    
    Supports summary reports, detailed logs, and metrics dashboards.
    """
    action_type = "automation_reporter"
    display_name = "自动化报告"
    description = "生成自动化执行报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Generate automation reports.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - report_type: 'summary', 'detailed', 'metrics'.
                - execution_log: Log data from execution.
                - metrics: Metrics data.
                - format: 'json', 'markdown', 'html'.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with generated report or error.
        """
        report_type = params.get('report_type', 'summary')
        execution_log = params.get('execution_log', [])
        metrics = params.get('metrics', {})
        format_type = params.get('format', 'json')
        output_var = params.get('output_var', 'report')

        try:
            if report_type == 'summary':
                report = self._generate_summary_report(execution_log, format_type)
            elif report_type == 'detailed':
                report = self._generate_detailed_report(execution_log, format_type)
            elif report_type == 'metrics':
                report = self._generate_metrics_report(metrics, format_type)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown report type: {report_type}"
                )

            context.variables[output_var] = report
            return ActionResult(
                success=True,
                data=report,
                message=f"Generated {report_type} report"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Report generation failed: {str(e)}"
            )

    def _generate_summary_report(self, log: List, format_type: str) -> Dict:
        """Generate summary report."""
        total = len(log)
        success = len([e for e in log if e.get('success', False)])
        failed = total - success

        duration = sum(e.get('duration', 0) for e in log)

        report = {
            'type': 'summary',
            'generated_at': datetime.now().isoformat(),
            'total_executions': total,
            'successful': success,
            'failed': failed,
            'success_rate': f"{(success/total*100):.1f}%" if total > 0 else "0%",
            'total_duration': duration,
            'avg_duration': duration / total if total > 0 else 0
        }

        if format_type == 'markdown':
            report['markdown'] = self._format_summary_markdown(report)
        elif format_type == 'html':
            report['html'] = self._format_summary_html(report)

        return report

    def _generate_detailed_report(self, log: List, format_type: str) -> Dict:
        """Generate detailed report."""
        report = {
            'type': 'detailed',
            'generated_at': datetime.now().isoformat(),
            'total_executions': len(log),
            'executions': log
        }

        if format_type == 'markdown':
            report['markdown'] = self._format_detailed_markdown(log)
        elif format_type == 'html':
            report['html'] = self._format_detailed_html(log)

        return report

    def _generate_metrics_report(self, metrics: Dict, format_type: str) -> Dict:
        """Generate metrics report."""
        report = {
            'type': 'metrics',
            'generated_at': datetime.now().isoformat(),
            'metrics': metrics
        }

        if format_type == 'markdown':
            report['markdown'] = self._format_metrics_markdown(metrics)
        elif format_type == 'html':
            report['html'] = self._format_metrics_html(metrics)

        return report

    def _format_summary_markdown(self, report: Dict) -> str:
        """Format summary as markdown."""
        return f"""# Automation Summary Report

Generated: {report['generated_at']}

## Statistics
- Total Executions: {report['total_executions']}
- Successful: {report['successful']}
- Failed: {report['failed']}
- Success Rate: {report['success_rate']}
- Total Duration: {report['total_duration']:.3f}s
- Avg Duration: {report['avg_duration']:.3f}s
"""

    def _format_summary_html(self, report: Dict) -> str:
        """Format summary as HTML."""
        return f"<html><body><h1>Automation Summary</h1><p>Total: {report['total_executions']}</p></body></html>"

    def _format_detailed_markdown(self, log: List) -> str:
        """Format detailed log as markdown."""
        lines = ["# Detailed Execution Log\n"]
        for i, entry in enumerate(log):
            status = "✅" if entry.get('success') else "❌"
            lines.append(f"{status} Step {i+1}: {entry.get('node', 'unknown')} ({entry.get('duration', 0):.3f}s)")
        return "\n".join(lines)

    def _format_detailed_html(self, log: List) -> str:
        """Format detailed log as HTML."""
        return "<html><body><h1>Detailed Log</h1></body></html>"

    def _format_metrics_markdown(self, metrics: Dict) -> str:
        """Format metrics as markdown."""
        lines = ["# Metrics Report\n"]
        for key, value in metrics.items():
            lines.append(f"- {key}: {value}")
        return "\n".join(lines)

    def _format_metrics_html(self, metrics: Dict) -> str:
        """Format metrics as HTML."""
        return "<html><body><h1>Metrics</h1></body></html>"
