"""Datadog integration for RabAI AutoClick.

Provides actions to manage monitors, dashboards, metrics, and events in Datadog.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DatadogMonitorAction(BaseAction):
    """Manage Datadog monitors - create, update, mute, resolve.

    Handles alert monitoring and notification management.
    """
    action_type = "datadog_monitor"
    display_name = "Datadog监控"
    description = "管理Datadog监控告警：创建、更新、静默、解决"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Datadog monitors.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Datadog API key
                - app_key: Datadog app key
                - operation: create | update | get | delete | list | mute | unmute
                - monitor_id: Monitor ID (for update/delete/mute)
                - name: Monitor name
                - type: metric alert type (e.g. metric alert, service check, etc.)
                - query: Monitor query
                - message: Notification message
                - tags: List of tags
                - priority: Alert priority (1-5)
                - options: Dict of monitor options

        Returns:
            ActionResult with monitor data.
        """
        import hashlib
        import base64

        api_key = params.get('api_key') or os.environ.get('DATADOG_API_KEY')
        app_key = params.get('app_key') or os.environ.get('DATADOG_APP_KEY')

        if not api_key or not app_key:
            return ActionResult(success=False, message="DATADOG_API_KEY and DATADOG_APP_KEY are required")

        import urllib.request
        import urllib.error

        auth = base64.b64encode(f"{api_key}".encode()).decode()
        headers = {
            'Authorization': f'Basic {auth}',
            'Content-Type': 'application/json',
            'DD-API-KEY': api_key,
            'DD-APPLICATION-KEY': app_key,
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'create':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                payload = {
                    'name': name,
                    'type': params.get('type', 'metric alert'),
                    'query': params.get('query', ''),
                    'message': params.get('message', ''),
                    'tags': params.get('tags', []),
                    'priority': params.get('priority'),
                    'options': params.get('options', {}),
                }

                req = urllib.request.Request(
                    'https://api.datadoghq.com/api/v1/monitor',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Monitor {name} created", data={'id': result.get('id')})

            elif operation == 'update':
                monitor_id = params.get('monitor_id')
                if not monitor_id:
                    return ActionResult(success=False, message="monitor_id is required")

                payload = {}
                for key in ['name', 'type', 'query', 'message', 'tags', 'priority', 'options']:
                    if key in params:
                        payload[key] = params[key]

                req = urllib.request.Request(
                    f'https://api.datadoghq.com/api/v1/monitor/{monitor_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Monitor {monitor_id} updated", data={'id': result.get('id')})

            elif operation == 'get':
                monitor_id = params.get('monitor_id')
                if not monitor_id:
                    return ActionResult(success=False, message="monitor_id is required")

                req = urllib.request.Request(
                    f'https://api.datadoghq.com/api/v1/monitor/{monitor_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Monitor retrieved", data=result)

            elif operation == 'delete':
                monitor_id = params.get('monitor_id')
                if not monitor_id:
                    return ActionResult(success=False, message="monitor_id is required")

                req = urllib.request.Request(
                    f'https://api.datadoghq.com/api/v1/monitor/{monitor_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Monitor {monitor_id} deleted", data=result)

            elif operation == 'list':
                url = 'https://api.datadoghq.com/api/v1/monitor?'
                query_params = []
                if params.get('type'):
                    query_params.append(f'type={params["type"]}')
                if params.get('tags'):
                    tags = ','.join(params['tags']) if isinstance(params['tags'], list) else params['tags']
                    query_params.append(f'tags={tags}')
                if params.get('status'):
                    query_params.append(f'monitor_state={params["status"]}')
                url += '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    monitors = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Found {len(monitors)} monitors", data={'monitors': monitors})

            elif operation == 'mute':
                monitor_id = params.get('monitor_id')
                if not monitor_id:
                    return ActionResult(success=False, message="monitor_id is required")

                payload = {}
                if params.get('end'):
                    payload['end'] = params['end']

                req = urllib.request.Request(
                    f'https://api.datadoghq.com/api/v1/monitor/{monitor_id}/mute',
                    data=json.dumps(payload).encode('utf-8') if payload else b'{}',
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Monitor {monitor_id} muted", data=result)

            elif operation == 'unmute':
                monitor_id = params.get('monitor_id')
                if not monitor_id:
                    return ActionResult(success=False, message="monitor_id is required")

                req = urllib.request.Request(
                    f'https://api.datadoghq.com/api/v1/monitor/{monitor_id}/unmute',
                    data=b'{}',
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Monitor {monitor_id} unmuted", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Datadog API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Datadog error: {str(e)}")


class DatadogMetricsAction(BaseAction):
    """Submit and query Datadog metrics.

    Handles custom metric submission and data retrieval.
    """
    action_type = "datadog_metrics"
    display_name = "Datadog指标"
    description = "提交和查询Datadog指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Submit or query Datadog metrics.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Datadog API key
                - operation: submit | query | list_metrics
                - metric: Metric name
                - points: List of {timestamp, value} points
                - tags: List of metric tags
                - type: metric type (gauge, count, histogram, etc.)
                - from_ts: Start timestamp (for query)
                - to: End timestamp (for query)
                - query_filter: Metric name filter (for list_metrics)

        Returns:
            ActionResult with metric data.
        """
        import base64

        api_key = params.get('api_key') or os.environ.get('DATADOG_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="DATADOG_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Content-Type': 'application/json',
            'DD-API-KEY': api_key,
        }

        try:
            operation = params.get('operation', 'submit')

            if operation == 'submit':
                metric = params.get('metric')
                points = params.get('points', [])

                if not metric or not points:
                    return ActionResult(success=False, message="metric and points are required")

                series = [{
                    'metric': metric,
                    'points': [[p.get('timestamp', int(time.time())), p.get('value', 0)] for p in points],
                    'tags': params.get('tags', []),
                    'type': params.get('type', 'gauge'),
                    'host': params.get('host'),
                }]

                req = urllib.request.Request(
                    'https://api.datadoghq.com/api/v1/series',
                    data=json.dumps({'series': series}).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Metric {metric} submitted", data=result)

            elif operation == 'query':
                from_ts = params.get('from_ts', int(time.time()) - 3600)
                to = params.get('to', int(time.time()))
                query = params.get('query')

                if not query:
                    return ActionResult(success=False, message="query is required")

                url = f'https://api.datadoghq.com/api/v1/query?from={from_ts}&to={to}&query={query}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                series = result.get('series', [])
                return ActionResult(success=True, message=f"Query returned {len(series)} series", data={'series': series})

            elif operation == 'list_metrics':
                url = 'https://api.datadoghq.com/api/v1/metrics'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                metrics = result.get('metrics', [])
                return ActionResult(success=True, message=f"Found {len(metrics)} metrics", data={'metrics': metrics})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Datadog API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Datadog error: {str(e)}")


class DatadogEventAction(BaseAction):
    """Create and manage Datadog events.

    Handles event creation for timeline tracking.
    """
    action_type = "datadog_event"
    display_name = "Datadog事件"
    description = "创建和管理Datadog事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create or query Datadog events.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Datadog API key
                - operation: create | list
                - title: Event title
                - text: Event body
                - tags: List of tags
                - priority: normal or low
                - alert_type: info, warning, error, or success
                - source_type: Event source type

        Returns:
            ActionResult with event data.
        """
        api_key = params.get('api_key') or os.environ.get('DATADOG_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="DATADOG_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Content-Type': 'application/json',
            'DD-API-KEY': api_key,
        }

        try:
            operation = params.get('operation', 'create')

            if operation == 'create':
                title = params.get('title')
                text = params.get('text', '')

                if not title:
                    return ActionResult(success=False, message="title is required")

                payload = {
                    'title': title,
                    'text': text,
                    'tags': params.get('tags', []),
                    'priority': params.get('priority', 'normal'),
                    'alert_type': params.get('alert_type', 'info'),
                    'source_type_name': params.get('source_type', 'API'),
                }

                req = urllib.request.Request(
                    'https://api.datadoghq.com/api/v1/events',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Event created: {title}", data={'event_id': result.get('event', {}).get('id')})

            elif operation == 'list':
                url = 'https://api.datadoghq.com/api/v1/events?'
                query_params = []
                if params.get('tags'):
                    tags = ','.join(params['tags']) if isinstance(params['tags'], list) else params['tags']
                    query_params.append(f'tags={tags}')
                if params.get('start'):
                    query_params.append(f'start={params["start"]}')
                if params.get('end'):
                    query_params.append(f'end={params["end"]}')
                url += '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                events = result.get('events', [])
                return ActionResult(success=True, message=f"Found {len(events)} events", data={'events': events})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Datadog API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Datadog error: {str(e)}")
