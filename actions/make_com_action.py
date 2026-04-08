"""Make (Formerly Integromat) scenario action module for RabAI AutoClick.

Provides operations for triggering and managing Make.com scenarios.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MakeComTriggerAction(BaseAction):
    """Trigger a Make.com scenario via webhook.

    Supports POST and GET request triggering with custom payloads.
    """
    action_type = "makecom_trigger"
    display_name = "Make.com触发"
    description = "触发Make.com场景执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Trigger Make.com scenario.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: Make.com webhook URL
                - method: HTTP method (POST or GET)
                - data: Payload data
                - headers: Custom headers

        Returns:
            ActionResult with trigger response.
        """
        webhook_url = params.get('webhook_url', '')
        method = params.get('method', 'POST').upper()
        data = params.get('data', {})
        headers = params.get('headers', {})

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")

        import urllib.request
        import urllib.parse

        start = time.time()
        try:
            if method == 'GET':
                if data:
                    query = urllib.parse.urlencode(data)
                    webhook_url = f"{webhook_url}?{query}" if '?' not in webhook_url else f"{webhook_url}&{query}"
                req = urllib.request.Request(webhook_url, headers=headers, method='GET')
            else:
                body = json.dumps(data).encode('utf-8')
                headers['Content-Type'] = 'application/json'
                req = urllib.request.Request(webhook_url, data=body, headers=headers, method='POST')

            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode('utf-8')
                try:
                    response_data = json.loads(body)
                except json.JSONDecodeError:
                    response_data = body
                duration = time.time() - start
                return ActionResult(
                    success=True, message=f"Scenario triggered (status {resp.status})",
                    data={'status_code': resp.status, 'response': response_data},
                    duration=duration
                )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Make.com HTTP error {e.code}: {body}")
        except Exception as e:
            return ActionResult(success=False, message=f"Make.com trigger error: {str(e)}")


class MakeComScenarioAction(BaseAction):
    """Get Make.com scenario information and status."""
    action_type = "makecom_scenario"
    display_name = "Make.com场景管理"
    description = "Make.com场景信息查询"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get scenario info or list.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: Make.com API key
                - organization_id: Organization ID
                - team_id: Team ID (optional)
                - action: 'list' or 'get'
                - scenario_id: Scenario ID (for get action)

        Returns:
            ActionResult with scenario data.
        """
        api_key = params.get('api_key', '')
        organization_id = params.get('organization_id', '')
        team_id = params.get('team_id', '')
        action = params.get('action', 'list')
        scenario_id = params.get('scenario_id', '')

        if not api_key or not organization_id:
            return ActionResult(success=False, message="api_key and organization_id are required")

        import urllib.request

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
        }
        start = time.time()

        try:
            if action == 'list':
                url = f"https://www.make.com/en/api/v2/organizations/{organization_id}/scenarios"
                if team_id:
                    url = f"https://www.make.com/en/api/v2/teams/{team_id}/scenarios"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    duration = time.time() - start
                    return ActionResult(
                        success=True, message="Scenarios listed",
                        data=data, duration=duration
                    )
            elif action == 'get':
                if not scenario_id:
                    return ActionResult(success=False, message="scenario_id required for get action")
                url = f"https://www.make.com/en/api/v2/scenarios/{scenario_id}"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    duration = time.time() - start
                    return ActionResult(success=True, message="Scenario retrieved", data=data, duration=duration)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Make.com API error: {str(e)}")
