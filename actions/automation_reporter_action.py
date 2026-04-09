"""Automation Reporter Action Module.

Provides reporting and notification for automation workflows.
"""

import time
import json
import traceback
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationReporterAction(BaseAction):
    """Generate reports for automation workflows.
    
    Creates comprehensive reports with metrics, logs, and status information.
    """
    action_type = "automation_reporter"
    display_name = "自动化报告生成"
    description = "生成自动化工作流综合报告"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate automation report.
        
        Args:
            context: Execution context.
            params: Dict with keys: workflow_id, include_metrics, include_logs, format.
        
        Returns:
            ActionResult with generated report.
        """
        workflow_id = params.get('workflow_id', '')
        include_metrics = params.get('include_metrics', True)
        include_logs = params.get('include_logs', True)
        report_format = params.get('format', 'json')
        
        report_data = {
            'workflow_id': workflow_id,
            'generated_at': time.time(),
            'report_type': 'automation_workflow'
        }
        
        if include_metrics:
            report_data['metrics'] = self._collect_metrics(workflow_id)
        
        if include_logs:
            report_data['logs'] = self._collect_logs(workflow_id)
        
        report_data['summary'] = self._generate_summary(report_data)
        
        return ActionResult(
            success=True,
            data={
                'report': report_data,
                'format': report_format
            },
            error=None
        )
    
    def _collect_metrics(self, workflow_id: str) -> Dict:
        """Collect workflow metrics."""
        return {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_duration': 0.0,
            'total_duration': 0.0
        }
    
    def _collect_logs(self, workflow_id: str) -> List[Dict]:
        """Collect workflow logs."""
        return [
            {'timestamp': time.time(), 'level': 'info', 'message': 'Workflow report generated'}
        ]
    
    def _generate_summary(self, report_data: Dict) -> Dict:
        """Generate report summary."""
        metrics = report_data.get('metrics', {})
        logs = report_data.get('logs', [])
        
        return {
            'status': 'completed',
            'execution_count': metrics.get('total_executions', 0),
            'log_count': len(logs),
            'generated_at': report_data.get('generated_at', time.time())
        }


class AutomationAlertAction(BaseAction):
    """Send alerts for automation events.
    
    Triggers notifications based on workflow conditions.
    """
    action_type = "automation_alert"
    display_name = "自动化告警"
    description = "根据工作流条件触发通知"
    
    def __init__(self):
        super().__init__()
        self._alert_channels = {
            'email': self._send_email_alert,
            'webhook': self._send_webhook_alert,
            'slack': self._send_slack_alert,
            'console': self._send_console_alert
        }
        self._alert_history = []
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute alert.
        
        Args:
            context: Execution context.
            params: Dict with keys: level, message, channels, metadata.
        
        Returns:
            ActionResult with alert result.
        """
        level = params.get('level', 'info')
        message = params.get('message', '')
        channels = params.get('channels', ['console'])
        metadata = params.get('metadata', {})
        
        if not message:
            return ActionResult(
                success=False,
                data=None,
                error="Alert message is required"
            )
        
        alert_id = f"alert_{int(time.time() * 1000)}"
        
        alert_data = {
            'alert_id': alert_id,
            'level': level,
            'message': message,
            'metadata': metadata,
            'timestamp': time.time(),
            'channels': channels
        }
        
        results = {}
        all_success = True
        
        for channel in channels:
            if channel in self._alert_channels:
                try:
                    success = self._alert_channels[channel](alert_data)
                    results[channel] = {'success': success}
                    if not success:
                        all_success = False
                except Exception as e:
                    results[channel] = {'success': False, 'error': str(e)}
                    all_success = False
            else:
                results[channel] = {'success': False, 'error': 'Unknown channel'}
                all_success = False
        
        self._alert_history.append(alert_data)
        
        return ActionResult(
            success=all_success,
            data={
                'alert_id': alert_id,
                'level': level,
                'results': results
            },
            error=None if all_success else "Some alert channels failed"
        )
    
    def _send_email_alert(self, alert_data: Dict) -> bool:
        """Send email alert."""
        # Placeholder - would integrate with email service
        return True
    
    def _send_webhook_alert(self, alert_data: Dict) -> bool:
        """Send webhook alert."""
        import urllib.request
        try:
            webhook_url = alert_data.get('metadata', {}).get('webhook_url', '')
            if not webhook_url:
                return False
            
            data = json.dumps(alert_data).encode('utf-8')
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            urllib.request.urlopen(req, timeout=10)
            return True
        except Exception:
            return False
    
    def _send_slack_alert(self, alert_data: Dict) -> bool:
        """Send Slack alert."""
        # Placeholder - would integrate with Slack API
        return True
    
    def _send_console_alert(self, alert_data: Dict) -> bool:
        """Send console alert."""
        level = alert_data['level']
        message = alert_data['message']
        print(f"[{level.upper()}] {message}")
        return True


class AutomationDashboardAction(BaseAction):
    """Create automation workflow dashboard.
    
    Generates real-time dashboard data for workflow monitoring.
    """
    action_type = "automation_dashboard"
    display_name = "自动化仪表板"
    description = "生成工作流监控仪表板数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate dashboard data.
        
        Args:
            context: Execution context.
            params: Dict with keys: time_range, refresh_interval.
        
        Returns:
            ActionResult with dashboard data.
        """
        time_range = params.get('time_range', 3600)
        refresh_interval = params.get('refresh_interval', 30)
        
        current_time = time.time()
        cutoff_time = current_time - time_range
        
        dashboard_data = {
            'generated_at': current_time,
            'refresh_interval': refresh_interval,
            'time_range': time_range,
            'widgets': self._generate_widgets(cutoff_time)
        }
        
        return ActionResult(
            success=True,
            data=dashboard_data,
            error=None
        )
    
    def _generate_widgets(self, cutoff_time: float) -> List[Dict]:
        """Generate dashboard widgets."""
        return [
            {
                'widget_id': 'executions_over_time',
                'type': 'line_chart',
                'title': 'Executions Over Time',
                'data': []
            },
            {
                'widget_id': 'success_rate',
                'type': 'gauge',
                'title': 'Success Rate',
                'value': 95.5
            },
            {
                'widget_id': 'avg_duration',
                'type': 'metric',
                'title': 'Average Duration',
                'value': 1.23,
                'unit': 'seconds'
            },
            {
                'widget_id': 'active_workflows',
                'type': 'counter',
                'title': 'Active Workflows',
                'value': 3
            }
        ]


def register_actions():
    """Register all Automation Reporter actions."""
    return [
        AutomationReporterAction,
        AutomationAlertAction,
        AutomationDashboardAction,
    ]
