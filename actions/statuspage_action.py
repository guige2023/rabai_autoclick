"""Statuspage.io integration for RabAI AutoClick.

Provides actions to manage status pages, incidents, and component statuses.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StatusPageIncidentAction(BaseAction):
    """Manage status page incidents - create, update, resolve incidents.

    Handles incident lifecycle and subscriber notifications.
    """
    action_type = "statuspage_incident"
    display_name = "StatusPage事件"
    description = "管理StatusPage事件：创建、更新、解决"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Statuspage incidents.

        Args:
            context: Execution context.
            params: Dict with keys:
                - page_id: Statuspage page ID
                - api_key: Statuspage API key
                - operation: create | update | resolve | list | get
                - incident_id: Incident ID (for update/resolve)
                - name: Incident name
                - status: investigating | identified | monitoring | resolved
                - severity: critical | major | minor | cosmetic
                - body: Incident body (Markdown)
                - component_id: Component ID to affect
                - component_status: operational | degraded_performance | partial_outage | major_outage
                - message: Status message

        Returns:
            ActionResult with incident data.
        """
        page_id = params.get('page_id') or os.environ.get('STATUSPAGE_PAGE_ID')
        api_key = params.get('api_key') or os.environ.get('STATUSPAGE_API_KEY')
        operation = params.get('operation', 'list')

        if not all([page_id, api_key]):
            return ActionResult(success=False, message="page_id and api_key are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'create':
                name = params.get('name')
                status = params.get('status', 'investigating')
                severity = params.get('severity', 'major')

                if not name:
                    return ActionResult(success=False, message="name is required")

                payload = {
                    'incident': {
                        'name': name,
                        'status': status,
                        'severity': severity,
                        'body': params.get('body', ''),
                    }
                }

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/incidents',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                # Update component status if specified
                if params.get('component_id') and params.get('component_status'):
                    comp_payload = {'component': {'status': params['component_status']}}
                    req2 = urllib.request.Request(
                        f'https://api.statuspage.io/v1/pages/{page_id}/components/{params["component_id"]}',
                        data=json.dumps(comp_payload).encode('utf-8'),
                        method='PATCH',
                        headers=headers
                    )
                    try:
                        urllib.request.urlopen(req2, timeout=15)
                    except Exception:
                        pass

                return ActionResult(success=True, message="Incident created", data=result)

            elif operation == 'update':
                incident_id = params.get('incident_id')
                if not incident_id:
                    return ActionResult(success=False, message="incident_id is required")

                payload = {'incident': {}}
                for key in ['status', 'body', 'name']:
                    if params.get(key):
                        payload['incident'][key] = params[key]

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/incidents/{incident_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Incident updated", data=result)

            elif operation == 'resolve':
                incident_id = params.get('incident_id')
                if not incident_id:
                    return ActionResult(success=False, message="incident_id is required")

                payload = {
                    'incident': {
                        'status': 'resolved',
                        'body': params.get('body', 'This incident has been resolved.'),
                    }
                }

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/incidents/{incident_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                # Restore component status
                if params.get('component_id'):
                    comp_payload = {'component': {'status': 'operational'}}
                    req2 = urllib.request.Request(
                        f'https://api.statuspage.io/v1/pages/{page_id}/components/{params["component_id"]}',
                        data=json.dumps(comp_payload).encode('utf-8'),
                        method='PATCH',
                        headers=headers
                    )
                    try:
                        urllib.request.urlopen(req2, timeout=15)
                    except Exception:
                        pass

                return ActionResult(success=True, message="Incident resolved", data=result)

            elif operation == 'get':
                incident_id = params.get('incident_id')
                if not incident_id:
                    return ActionResult(success=False, message="incident_id is required")

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/incidents/{incident_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Incident retrieved", data=result)

            elif operation == 'list':
                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/incidents',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    incidents = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(incidents)} incidents", data={'incidents': incidents})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Statuspage API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Statuspage error: {str(e)}")


class StatusPageComponentAction(BaseAction):
    """Manage status page components and groups.

    Handles component CRUD and status updates.
    """
    action_type = "statuspage_component"
    display_name = "StatusPage组件"
    description = "管理StatusPage组件和组件组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Statuspage components.

        Args:
            context: Execution context.
            params: Dict with keys:
                - page_id: Statuspage page ID
                - api_key: Statuspage API key
                - operation: create | update | delete | list
                - component_id: Component ID (for update/delete)
                - name: Component name
                - description: Component description
                - status: operational | degraded_performance | partial_outage | major_outage
                - group_id: Component group ID
                - group_name: Component group name (for creating group)

        Returns:
            ActionResult with component data.
        """
        page_id = params.get('page_id') or os.environ.get('STATUSPAGE_PAGE_ID')
        api_key = params.get('api_key') or os.environ.get('STATUSPAGE_API_KEY')
        operation = params.get('operation', 'list')

        if not all([page_id, api_key]):
            return ActionResult(success=False, message="page_id and api_key are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'create':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                payload = {
                    'component': {
                        'name': name,
                        'description': params.get('description', ''),
                        'status': params.get('status', 'operational'),
                    }
                }
                if params.get('group_id'):
                    payload['component']['group_id'] = params['group_id']

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/components',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Component created", data=result)

            elif operation == 'update':
                component_id = params.get('component_id')
                if not component_id:
                    return ActionResult(success=False, message="component_id is required")

                payload = {'component': {}}
                for key in ['name', 'description', 'status']:
                    if key in params:
                        payload['component'][key] = params[key]

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/components/{component_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Component updated", data=result)

            elif operation == 'delete':
                component_id = params.get('component_id')
                if not component_id:
                    return ActionResult(success=False, message="component_id is required")

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/components/{component_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8')) if resp.status != 204 else {}
                return ActionResult(success=True, message="Component deleted", data=result)

            elif operation == 'list':
                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/components',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    components = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(components)} components", data={'components': components})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Statuspage API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Statuspage error: {str(e)}")


class StatusPageMetricAction(BaseAction):
    """Manage Statuspage metrics and metric providers.

    Handles metric data point ingestion and provider configuration.
    """
    action_type = "statuspage_metric"
    display_name = "StatusPage指标"
    description = "管理StatusPage指标和指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Statuspage metrics.

        Args:
            context: Execution context.
            params: Dict with keys:
                - page_id: Statuspage page ID
                - api_key: Statuspage API key
                - operation: post_metrics | list_metrics | create_metric
                - metric_id: Metric ID (for posting data)
                - data_points: List of {timestamp, value} dicts
                - metric_name: Metric name (for create)
                - metric_identifier: Metric identifier (for create)
                - metric_type: number | percentage

        Returns:
            ActionResult with metric data.
        """
        page_id = params.get('page_id') or os.environ.get('STATUSPAGE_PAGE_ID')
        api_key = params.get('api_key') or os.environ.get('STATUSPAGE_API_KEY')
        operation = params.get('operation', 'post_metrics')

        if not all([page_id, api_key]):
            return ActionResult(success=False, message="page_id and api_key are required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'post_metrics':
                metric_id = params.get('metric_id')
                data_points = params.get('data_points', [])

                if not metric_id or not data_points:
                    return ActionResult(success=False, message="metric_id and data_points are required")

                payload = {'data': [
                    {'timestamp': dp.get('timestamp', int(time.time())), 'value': dp.get('value', 0)}
                    for dp in data_points
                ]}

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/metrics/{metric_id}/data',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Posted {len(data_points)} data points", data=result)

            elif operation == 'list_metrics':
                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/metrics',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    metrics = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(metrics)} metrics", data={'metrics': metrics})

            elif operation == 'create_metric':
                metric_name = params.get('metric_name')
                metric_identifier = params.get('metric_identifier')

                if not metric_name or not metric_identifier:
                    return ActionResult(success=False, message="metric_name and metric_identifier are required")

                payload = {
                    'metric': {
                        'name': metric_name,
                        'identifier': metric_identifier,
                        'metric_type': params.get('metric_type', 'number'),
                    }
                }

                req = urllib.request.Request(
                    f'https://api.statuspage.io/v1/pages/{page_id}/metrics',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Metric created", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Statuspage API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Statuspage error: {str(e)}")
