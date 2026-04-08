"""Grafana integration for RabAI AutoClick.

Provides actions to manage dashboards, alerts, annotations, and data sources in Grafana.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GrafanaDashboardAction(BaseAction):
    """Manage Grafana dashboards - create, update, and fetch dashboards.

    Uses Grafana HTTP API with Basic or Token authentication.
    """
    action_type = "grafana_dashboard"
    display_name = "Grafana仪表板"
    description = "创建、更新、获取Grafana仪表板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Grafana dashboards.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Grafana URL (e.g. http://localhost:3000)
                - api_key: Grafana API key
                - operation: create | update | get | delete | list
                - uid: Dashboard UID (for update/get/delete)
                - dashboard: Dashboard JSON (for create/update)
                - folder_id: Folder ID to place dashboard
                - title: Dashboard title

        Returns:
            ActionResult with dashboard data.
        """
        url = params.get('url', os.environ.get('GRAFANA_URL', 'http://localhost:3000')).rstrip('/')
        api_key = params.get('api_key') or os.environ.get('GRAFANA_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="GRAFANA_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}

        try:
            if operation == 'list':
                req = urllib.request.Request(f'{url}/api/search?type=dash-db', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    dashboards = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(dashboards)} dashboards", data={'dashboards': dashboards})

            elif operation == 'get':
                uid = params.get('uid')
                if not uid:
                    return ActionResult(success=False, message="uid is required for get")
                req = urllib.request.Request(f'{url}/api/dashboards/uid/{uid}', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Dashboard retrieved", data=data)

            elif operation in ('create', 'update'):
                dashboard = params.get('dashboard', {})
                if not dashboard:
                    dashboard = {'title': params.get('title', 'New Dashboard'), 'tags': [], 'timezone': 'browser'}
                dashboard['title'] = dashboard.get('title') or params.get('title', 'New Dashboard')

                payload = {
                    'dashboard': dashboard,
                    'overwrite': operation == 'update',
                    'folderId': params.get('folder_id', 0),
                }

                req = urllib.request.Request(
                    f'{url}/api/dashboards/db',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Dashboard '{operation}' completed", data=result)

            elif operation == 'delete':
                uid = params.get('uid')
                if not uid:
                    return ActionResult(success=False, message="uid is required for delete")
                req = urllib.request.Request(f'{url}/api/dashboards/uid/{uid}', method='DELETE', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Dashboard deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Grafana API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Grafana error: {str(e)}")


class GrafanaAlertAction(BaseAction):
    """Manage Grafana alerts and notification policies.

    Supports alert rules, folder management, and contact points.
    """
    action_type = "grafana_alert"
    display_name = "Grafana告警"
    description = "管理Grafana告警规则和通知策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Grafana alerts.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Grafana URL
                - api_key: Grafana API key
                - operation: list_rules | create_rule | update_rule | delete_rule | pause_rule
                - rule_uid: Rule UID (for update/delete/pause)
                - rule: Alert rule dict (for create/update)
                - folder: Folder name for rules
                - rule_group: Rule group name

        Returns:
            ActionResult with alert data.
        """
        url = params.get('url', os.environ.get('GRAFANA_URL', 'http://localhost:3000')).rstrip('/')
        api_key = params.get('api_key') or os.environ.get('GRAFANA_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="GRAFANA_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}

        try:
            if params.get('operation') == 'list_rules':
                req = urllib.request.Request(f'{url}/api/ruler/grafana/api/v1/rules', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                groups = data.get('groups', [])
                return ActionResult(success=True, message=f"Found {len(groups)} rule groups", data=data)

            elif params.get('operation') == 'create_rule':
                folder = params.get('folder', 'default')
                rule_group = params.get('rule_group', 'default_group')
                rule = params.get('rule', {})
                if not rule:
                    return ActionResult(success=False, message="rule dict is required for create_rule")

                payload = {
                    'name': folder,
                    'rules': [rule]
                }
                req = urllib.request.Request(
                    f'{url}/api/ruler/grafana/api/v1/rules/{folder}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Alert rule created", data=result)

            elif params.get('operation') == 'pause_rule':
                rule_uid = params.get('rule_uid')
                if not rule_uid:
                    return ActionResult(success=False, message="rule_uid is required")
                req = urllib.request.Request(
                    f'{url}/api/v1/pauseAlert',
                    data=json.dumps({'uid': rule_uid, 'paused': True}).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Alert paused", data=result)

            elif params.get('operation') == 'delete_rule':
                rule_uid = params.get('rule_uid')
                folder = params.get('folder', 'default')
                if not rule_uid:
                    return ActionResult(success=False, message="rule_uid is required")
                req = urllib.request.Request(
                    f'{url}/api/ruler/grafana/api/v1/rules/{folder}/uid/{rule_uid}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Alert rule deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {params.get('operation')}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Grafana API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Grafana error: {str(e)}")


class GrafanaAnnotationAction(BaseAction):
    """Create and query Grafana annotations.

    Annotations mark events on Grafana graphs.
    """
    action_type = "grafana_annotation"
    display_name = "Grafana注释"
    description = "创建和查询Grafana图表注释"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Grafana annotations.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Grafana URL
                - api_key: Grafana API key
                - operation: create | list | update | delete
                - dashboard_id: Dashboard ID
                - panel_id: Panel ID
                - time: Start time (Unix epoch ms)
                - time_end: End time for range annotations
                - text: Annotation text
                - tags: List of tags
                - annotation_id: Annotation ID for update/delete

        Returns:
            ActionResult with annotation data.
        """
        url = params.get('url', os.environ.get('GRAFANA_URL', 'http://localhost:3000')).rstrip('/')
        api_key = params.get('api_key') or os.environ.get('GRAFANA_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="GRAFANA_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}

        try:
            if operation == 'create':
                data = {
                    'dashboardId': params.get('dashboard_id', 0),
                    'panelId': params.get('panel_id', 0),
                    'time': params.get('time', int(time.time() * 1000)),
                    'timeEnd': params.get('time_end'),
                    'text': params.get('text', ''),
                    'tags': params.get('tags', []),
                }
                data = {k: v for k, v in data.items() if v is not None}
                req = urllib.request.Request(
                    f'{url}/api/annotations',
                    data=json.dumps(data).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Annotation created", data=result)

            elif operation == 'list':
                query_params = []
                if params.get('dashboard_id'):
                    query_params.append(f"dashboardId={params['dashboard_id']}")
                if params.get('from'):
                    query_params.append(f"from={params['from']}")
                if params.get('to'):
                    query_params.append(f"to={params['to']}")
                query = '&'.join(query_params) if query_params else ''
                url2 = f'{url}/api/annotations?{query}' if query else f'{url}/api/annotations'
                req = urllib.request.Request(url2, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    annotations = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(annotations)} annotations", data={'annotations': annotations})

            elif operation == 'update':
                annotation_id = params.get('annotation_id')
                if not annotation_id:
                    return ActionResult(success=False, message="annotation_id is required")
                data = {'text': params.get('text'), 'tags': params.get('tags')}
                data = {k: v for k, v in data.items() if v is not None}
                req = urllib.request.Request(
                    f'{url}/api/annotations/{annotation_id}',
                    data=json.dumps(data).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Annotation updated", data=result)

            elif operation == 'delete':
                annotation_id = params.get('annotation_id')
                if not annotation_id:
                    return ActionResult(success=False, message="annotation_id is required")
                req = urllib.request.Request(f'{url}/api/annotations/{annotation_id}', method='DELETE', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Annotation deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"Grafana API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"Grafana error: {str(e)}")


class GrafanaDataSourceAction(BaseAction):
    """Manage Grafana data sources.

    Supports creating, listing, and testing data source connections.
    """
    action_type = "grafana_datasource"
    display_name = "Grafana数据源"
    description = "管理Grafana数据源连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Grafana data sources.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Grafana URL
                - api_key: Grafana API key
                - operation: list | create | update | delete | test
                - uid: Data source UID (for update/delete/test)
                - name: Data source name
                - type: Data source type (prometheus, influxdb, etc.)
                - url_ds: Data source URL
                - database: Database name
                - access_mode: proxy | direct

        Returns:
            ActionResult with data source data.
        """
        url = params.get('url', os.environ.get('GRAFANA_URL', 'http://localhost:3000')).rstrip('/')
        api_key = params.get('api_key') or os.environ.get('GRAFANA_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="GRAFANA_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}

        try:
            if operation == 'list':
                req = urllib.request.Request(f'{url}/api/datasources', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    sources = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(sources)} data sources", data={'datasources': sources})

            elif operation == 'create':
                payload = {
                    'name': params.get('name', 'New Datasource'),
                    'type': params.get('type', 'prometheus'),
                    'url': params.get('url_ds', ''),
                    'database': params.get('database', ''),
                    'access': params.get('access_mode', 'proxy'),
                }
                for key in ['user', 'password', 'basicAuth', 'jsonData']:
                    if key in params:
                        payload[key] = params[key]
                req = urllib.request.Request(
                    f'{url}/api/datasources',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Data source created", data=result)

            elif operation == 'test':
                uid = params.get('uid')
                if not uid:
                    return ActionResult(success=False, message="uid is required")
                req = urllib.request.Request(f'{url}/api/datasources/uid/{uid}/health', method='GET', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Health check result", data=result)

            elif operation == 'delete':
                uid = params.get('uid')
                if not uid:
                    return ActionResult(success=False, message="uid is required")
                req = urllib.request.Request(f'{url}/api/datasources/uid/{uid}', method='DELETE', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Data source deleted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            return ActionResult(success=False, message=f"Grafana API error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"Grafana error: {str(e)}")
