"""Prometheus action module for RabAI AutoClick.

Provides Prometheus monitoring operations:
- PrometheusQueryAction: Execute PromQL query
- PrometheusQueryRangeAction: Execute range query
- PrometheusRulesAction: List alerting rules
- PrometheusTargetsAction: List scrape targets
- PrometheusAlertsAction: List active alerts
- PrometheusPushAction: Push metrics to Pushgateway
- PrometheusMetricsAction: Get metrics from target
"""

import json
import urllib.request
import urllib.parse
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


def prometheus_request(url: str, params: Dict = None) -> Dict:
    """Make Prometheus API request."""
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


class PrometheusQueryAction(BaseAction):
    """Execute PromQL query."""
    action_type = "prometheus_query"
    display_name = "Prometheus查询"
    description = "执行PromQL即时查询"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute query.

        Args:
            context: Execution context.
            params: Dict with query, time, host, port, output_var.

        Returns:
            ActionResult with query results.
        """
        query = params.get('query', '')
        time_param = params.get('time', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        output_var = params.get('output_var', 'prometheus_result')

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_query = context.resolve_value(query)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_time = context.resolve_value(time_param) if time_param else None

            base_url = f"http://{resolved_host}:{resolved_port}/api/v1/query"

            req_params = {'query': resolved_query}
            if resolved_time:
                req_params['time'] = resolved_time

            result = prometheus_request(base_url, req_params)

            if result.get('status') != 'success':
                return ActionResult(
                    success=False,
                    message=f"查询失败: {result.get('error', 'unknown')}"
                )

            data = result.get('data', {})
            result_type = data.get('resultType', '')
            results = data.get('result', [])

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"PromQL查询完成: {len(results)} 结果 ({result_type})",
                data={'count': len(results), 'results': results, 'type': result_type, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'time': '', 'host': 'localhost', 'port': 9090, 'output_var': 'prometheus_result'}


class PrometheusQueryRangeAction(BaseAction):
    """Execute PromQL range query."""
    action_type = "prometheus_query_range"
    display_name = "Prometheus范围查询"
    description = "执行PromQL范围查询"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range query.

        Args:
            context: Execution context.
            params: Dict with query, start, end, step, host, port, output_var.

        Returns:
            ActionResult with range results.
        """
        query = params.get('query', '')
        start = params.get('start', '')
        end = params.get('end', '')
        step = params.get('step', '15s')
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        output_var = params.get('output_var', 'prometheus_range')

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_query = context.resolve_value(query)
            resolved_start = context.resolve_value(start)
            resolved_end = context.resolve_value(end)
            resolved_step = context.resolve_value(step)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            base_url = f"http://{resolved_host}:{resolved_port}/api/v1/query_range"

            req_params = {
                'query': resolved_query,
                'start': resolved_start,
                'end': resolved_end,
                'step': resolved_step
            }

            result = prometheus_request(base_url, req_params)

            if result.get('status') != 'success':
                return ActionResult(
                    success=False,
                    message=f"范围查询失败: {result.get('error', 'unknown')}"
                )

            data = result.get('data', {})
            results = data.get('result', [])

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"范围查询完成: {len(results)} 序列",
                data={'count': len(results), 'results': results, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus范围查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['query', 'start', 'end']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'step': '15s', 'host': 'localhost', 'port': 9090, 'output_var': 'prometheus_range'}


class PrometheusTargetsAction(BaseAction):
    """List scrape targets."""
    action_type = "prometheus_targets"
    display_name = "Prometheus目标列表"
    description = "列出Prometheus抓取目标"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute targets.

        Args:
            context: Execution context.
            params: Dict with state, host, port, output_var.

        Returns:
            ActionResult with targets.
        """
        state = params.get('state', 'any')
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        output_var = params.get('output_var', 'prometheus_targets')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_state = context.resolve_value(state)

            base_url = f"http://{resolved_host}:{resolved_port}/api/v1/targets"

            result = prometheus_request(base_url, {'state': resolved_state} if resolved_state != 'any' else {})

            if result.get('status') != 'success':
                return ActionResult(
                    success=False,
                    message=f"获取目标失败: {result.get('error', 'unknown')}"
                )

            data = result.get('data', {})
            active = data.get('activeTargets', [])
            dropped = data.get('droppedTargets', [])

            context.set(output_var, active)

            return ActionResult(
                success=True,
                message=f"目标: {len(active)} 活跃, {len(dropped)} 丢弃",
                data={'active': len(active), 'dropped': len(dropped), 'targets': active, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus目标查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'state': 'any', 'host': 'localhost', 'port': 9090, 'output_var': 'prometheus_targets'}


class PrometheusAlertsAction(BaseAction):
    """List active alerts."""
    action_type = "prometheus_alerts"
    display_name = "Prometheus告警列表"
    description = "列出Prometheus活跃告警"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute alerts.

        Args:
            context: Execution context.
            params: Dict with host, port, output_var.

        Returns:
            ActionResult with alerts.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        output_var = params.get('output_var', 'prometheus_alerts')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            base_url = f"http://{resolved_host}:{resolved_port}/api/v1/alerts"

            result = prometheus_request(base_url)

            if result.get('status') != 'success':
                return ActionResult(
                    success=False,
                    message=f"获取告警失败: {result.get('error', 'unknown')}"
                )

            data = result.get('data', {})
            alerts = data.get('alerts', [])

            # Count by state
            firing = sum(1 for a in alerts if a.get('state') == 'firing')
            pending = sum(1 for a in alerts if a.get('state') == 'pending')

            context.set(output_var, alerts)

            return ActionResult(
                success=True,
                message=f"告警: {firing} firing, {pending} pending",
                data={'alerts': alerts, 'firing': firing, 'pending': pending, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus告警查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9090, 'output_var': 'prometheus_alerts'}


class PrometheusRulesAction(BaseAction):
    """List alerting rules."""
    action_type = "prometheus_rules"
    display_name = "Prometheus规则列表"
    description = "列出Prometheus告警规则"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rules.

        Args:
            context: Execution context.
            params: Dict with type, host, port, output_var.

        Returns:
            ActionResult with rules.
        """
        rule_type = params.get('type', 'alert')
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        output_var = params.get('output_var', 'prometheus_rules')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_type = context.resolve_value(rule_type)

            base_url = f"http://{resolved_host}:{resolved_port}/api/v1/rules"

            result = prometheus_request(base_url)

            if result.get('status') != 'success':
                return ActionResult(
                    success=False,
                    message=f"获取规则失败: {result.get('error', 'unknown')}"
                )

            data = result.get('data', {})
            groups = data.get('groups', [])

            all_rules = []
            for group in groups:
                for rule in group.get('rules', []):
                    if resolved_type == 'all' or rule.get('type', '').lower() == resolved_type:
                        all_rules.append(rule)

            context.set(output_var, all_rules)

            return ActionResult(
                success=True,
                message=f"规则: {len(all_rules)} 个",
                data={'count': len(all_rules), 'rules': all_rules, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus规则查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'type': 'alert', 'host': 'localhost', 'port': 9090, 'output_var': 'prometheus_rules'}


class PrometheusPushAction(BaseAction):
    """Push metrics to Pushgateway."""
    action_type = "prometheus_push"
    display_name = "Prometheus推送指标"
    description = "推送指标到Pushgateway"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute push.

        Args:
            context: Execution context.
            params: Dict with metrics, job, instance, host, port.

        Returns:
            ActionResult indicating success.
        """
        metrics = params.get('metrics', {})
        job = params.get('job', 'batch_job')
        instance = params.get('instance', '')
        host = params.get('host', 'localhost')
        port = params.get('port', 9091)

        valid, msg = self.validate_type(metrics, dict, 'metrics')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_metrics = context.resolve_value(metrics)
            resolved_job = context.resolve_value(job)
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)

            # Build metrics text format
            lines = []
            for name, value in resolved_metrics.items():
                lines.append(f"{name} {value}")

            metrics_text = '\n'.join(lines)

            url = f"http://{resolved_host}:{resolved_port}/metrics/job/{resolved_job}"
            if instance:
                resolved_instance = context.resolve_value(instance)
                url += f"/instance/{resolved_instance}"

            req = urllib.request.Request(url, data=metrics_text.encode('utf-8'), method='POST')
            req.add_header('Content-Type', 'text/plain')

            with urllib.request.urlopen(req, timeout=30) as resp:
                status = resp.status

            return ActionResult(
                success=status in (200, 202),
                message=f"已推送 {len(resolved_metrics)} 个指标到 Pushgateway",
                data={'metrics': len(resolved_metrics), 'job': resolved_job}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus推送失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['metrics', 'job']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'instance': '', 'host': 'localhost', 'port': 9091}


class PrometheusMetricsAction(BaseAction):
    """Get metrics from target."""
    action_type = "prometheus_metrics"
    display_name = "Prometheus获取指标"
    description = "从目标获取指标"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metrics.

        Args:
            context: Execution context.
            params: Dict with host, port, target, output_var.

        Returns:
            ActionResult with metrics.
        """
        host = params.get('host', 'localhost')
        port = params.get('port', 9090)
        target = params.get('target', '')
        output_var = params.get('output_var', 'prometheus_metrics')

        valid, msg = self.validate_type(output_var, str, 'output_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_host = context.resolve_value(host)
            resolved_port = context.resolve_value(port)
            resolved_target = context.resolve_value(target) if target else None

            if resolved_target:
                query = f'{{__address__="{resolved_target}"}}'
                result = prometheus_request(
                    f"http://{resolved_host}:{resolved_port}/api/v1/query",
                    {'query': query}
                )
            else:
                result = prometheus_request(
                    f"http://{resolved_host}:{resolved_port}/api/v1/label/__name__/values"
                )

            if result.get('status') == 'success':
                data = result.get('data', {})
                if isinstance(data, list):
                    context.set(output_var, data)
                    return ActionResult(
                        success=True,
                        message=f"指标名: {len(data)} 个",
                        data={'count': len(data), 'metrics': data, 'output_var': output_var}
                    )
                else:
                    context.set(output_var, data)
                    return ActionResult(
                        success=True,
                        message="指标获取成功",
                        data={'metrics': data, 'output_var': output_var}
                    )

            return ActionResult(
                success=False,
                message=f"获取指标失败: {result.get('error', 'unknown')}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Prometheus指标查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 9090, 'target': '', 'output_var': 'prometheus_metrics'}
