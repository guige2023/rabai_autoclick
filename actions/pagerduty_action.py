"""PagerDuty integration for RabAI AutoClick.

Provides actions to manage PagerDuty incidents, schedules, and on-call alerts.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PagerDutyIncidentAction(BaseAction):
    """Manage PagerDuty incidents - trigger, acknowledge, resolve.

    Handles incident lifecycle and escalation.
    """
    action_type = "pagerduty_incident"
    display_name = "PagerDuty事件"
    description = "管理PagerDuty事件：触发、确认、解决"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage PagerDuty incidents.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: PagerDuty API key
                - operation: trigger | acknowledge | resolve | get | list
                - dedup_key: Deduplication key (for trigger)
                - incident_id: Incident ID (for acknowledge/resolve/get)
                - title: Incident title (for trigger)
                - severity: critical | error | warning | info
                - source: Source of the incident
                - user_id: User ID to assign
                - escalation_policy_id: Escalation policy ID
                - urgency: high or low

        Returns:
            ActionResult with incident data.
        """
        api_key = params.get('api_key') or os.environ.get('PAGERDUTY_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="PAGERDUTY_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            operation = params.get('operation', 'trigger')

            if operation == 'trigger':
                title = params.get('title')
                if not title:
                    return ActionResult(success=False, message="title is required")

                payload = {
                    'routing_key': params.get('integration_key') or os.environ.get('PAGERDUTY_ROUTING_KEY'),
                    'dedup_key': params.get('dedup_key'),
                    'event_action': 'trigger',
                    'payload': {
                        'summary': title,
                        'severity': params.get('severity', 'warning'),
                        'source': params.get('source', 'rabai-autoclick'),
                        'custom_details': params.get('custom_details', {}),
                    },
                }
                if params.get('urgency'):
                    payload['urgency'] = params['urgency']

                req = urllib.request.Request(
                    'https://events.pagerduty.com/v2/enqueue',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Incident triggered: {title}", data={'dedup_key': result.get('dedup_key')})

            elif operation == 'acknowledge':
                incident_id = params.get('incident_id')
                if not incident_id:
                    return ActionResult(success=False, message="incident_id is required")

                payload = {
                    'routing_key': params.get('integration_key') or os.environ.get('PAGERDUTY_ROUTING_KEY'),
                    'event_action': 'acknowledge',
                }

                req = urllib.request.Request(
                    'https://events.pagerduty.com/v2/enqueue',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Incident acknowledged", data=result)

            elif operation == 'resolve':
                dedup_key = params.get('dedup_key')
                if not dedup_key:
                    return ActionResult(success=False, message="dedup_key is required")

                payload = {
                    'routing_key': params.get('integration_key') or os.environ.get('PAGERDUTY_ROUTING_KEY'),
                    'event_action': 'resolve',
                    'dedup_key': dedup_key,
                }

                req = urllib.request.Request(
                    'https://events.pagerduty.com/v2/enqueue',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers={'Content-Type': 'application/json'}
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Incident resolved", data=result)

            elif operation == 'get':
                incident_id = params.get('incident_id')
                if not incident_id:
                    return ActionResult(success=False, message="incident_id is required")

                req = urllib.request.Request(
                    f'https://api.pagerduty.com/incidents/{incident_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Incident retrieved", data={'incident': result.get('incident')})

            elif operation == 'list':
                url = 'https://api.pagerduty.com/incidents?'
                query_params = []
                if params.get('status'):
                    query_params.append(f'statuses[]={params["status"]}')
                if params.get('urgency'):
                    query_params.append(f'urgencies[]={params["urgency"]}')
                url += '&'.join(query_params)

                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                incidents = result.get('incidents', [])
                return ActionResult(success=True, message=f"Found {len(incidents)} incidents", data={'incidents': incidents})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"PagerDuty API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"PagerDuty error: {str(e)}")


class PagerDutyScheduleAction(BaseAction):
    """Manage PagerDuty schedules and on-call rotations.

    Handles schedule management and on-call lookup.
    """
    action_type = "pagerduty_schedule"
    display_name = "PagerDuty排班"
    description = "管理PagerDuty排班和值班查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage PagerDuty schedules.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: PagerDuty API key
                - operation: list | get_oncall | list_users
                - schedule_id: Schedule ID (for get_oncall)
                - time_zone: Time zone for lookup

        Returns:
            ActionResult with schedule data.
        """
        api_key = params.get('api_key') or os.environ.get('PAGERDUTY_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="PAGERDUTY_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Token token={api_key}',
            'Accept': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                req = urllib.request.Request(
                    f'https://api.pagerduty.com/{params.get("query_type", "schedules")}?limit={params.get("limit", 25)}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                schedules = result.get('schedules', [])
                return ActionResult(success=True, message=f"Found {len(schedules)} schedules", data={'schedules': schedules})

            elif operation == 'get_oncall':
                schedule_id = params.get('schedule_id')
                if not schedule_id:
                    return ActionResult(success=False, message="schedule_id is required")

                time_zone = params.get('time_zone', 'UTC')
                req = urllib.request.Request(
                    f'https://api.pagerduty.com/oncalls?schedule_ids[]={schedule_id}&time_zone={time_zone}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                oncalls = result.get('oncalls', [])
                return ActionResult(success=True, message=f"Found {len(oncalls)} oncall users", data={'oncalls': oncalls})

            elif operation == 'list_users':
                req = urllib.request.Request(
                    f'https://api.pagerduty.com/users?limit={params.get("limit", 25)}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                users = result.get('users', [])
                return ActionResult(success=True, message=f"Found {len(users)} users", data={'users': users})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"PagerDuty API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"PagerDuty error: {str(e)}")


class PagerDutyServiceAction(BaseBaseAction):
    """Manage PagerDuty services and integrations.

    Handles service management and integration key creation.
    """
    action_type = "pagerduty_service"
    display_name = "PagerDuty服务"
    description = "管理PagerDuty服务和集成"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage PagerDuty services.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: PagerDuty API key
                - operation: list | create | get
                - name: Service name (for create)
                - description: Service description
                - escalation_policy_id: Escalation policy ID

        Returns:
            ActionResult with service data.
        """
        api_key = params.get('api_key') or os.environ.get('PAGERDUTY_API_KEY')

        if not api_key:
            return ActionResult(success=False, message="PAGERDUTY_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            operation = params.get('operation', 'list')

            if operation == 'list':
                req = urllib.request.Request(
                    f'https://api.pagerduty.com/services?limit={params.get("limit", 25)}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                services = result.get('services', [])
                return ActionResult(success=True, message=f"Found {len(services)} services", data={'services': services})

            elif operation == 'create':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                payload = {
                    'service': {
                        'name': name,
                        'description': params.get('description', ''),
                        'escalation_policy_id': params.get('escalation_policy_id'),
                        'status': params.get('status', 'active'),
                    }
                }

                req = urllib.request.Request(
                    'https://api.pagerduty.com/services',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message=f"Service {name} created", data={'service': result.get('service')})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"PagerDuty API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"PagerDuty error: {str(e)}")
