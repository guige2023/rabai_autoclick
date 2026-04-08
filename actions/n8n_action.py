"""n8n workflow automation action module for RabAI AutoClick.

Provides operations for triggering and managing n8n workflow executions.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class N8NTriggerAction(BaseAction):
    """Trigger an n8n workflow execution via webhook or API.

    Supports triggering workflows via webhook URL or REST API.
    """
    action_type = "n8n_trigger"
    display_name = "n8n触发"
    description = "触发n8n工作流执行"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Trigger n8n workflow.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: n8n webhook URL
                - method: HTTP method (POST or GET)
                - data: Payload data for POST requests
                - headers: Optional custom headers

        Returns:
            ActionResult with execution response.
        """
        webhook_url = params.get('webhook_url', '')
        method = params.get('method', 'POST').upper()
        data = params.get('data', {})
        headers = params.get('headers', {})
        auth_token = params.get('auth_token', None)

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url is required")

        import urllib.request
        import urllib.parse

        if auth_token:
            headers['Authorization'] = f'Bearer {auth_token}'

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
                    success=True, message=f"Workflow triggered (status {resp.status})",
                    data={'status_code': resp.status, 'response': response_data},
                    duration=duration
                )
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"n8n HTTP error {e.code}: {body}")
        except Exception as e:
            return ActionResult(success=False, message=f"n8n trigger error: {str(e)}")


class N8NWorkflowInfoAction(BaseAction):
    """Get information about an n8n workflow.

    Retrieves workflow metadata including nodes, connections, and settings.
    """
    action_type = "n8n_workflow_info"
    display_name = "n8n工作流信息"
    description = "获取n8n工作流详情"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get n8n workflow information.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: n8n server base URL
                - api_key: n8n API key
                - workflow_id: Workflow ID to query

        Returns:
            ActionResult with workflow metadata.
        """
        base_url = params.get('base_url', '').rstrip('/')
        api_key = params.get('api_key', '')
        workflow_id = params.get('workflow_id', '')

        if not base_url or not api_key:
            return ActionResult(success=False, message="base_url and api_key are required")

        import urllib.request

        url = f"{base_url}/rest/workflows/{workflow_id}"
        headers = {'X-N8N-API-KEY': api_key}
        start = time.time()
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                duration = time.time() - start
                return ActionResult(
                    success=True, message="Workflow info retrieved",
                    data=data, duration=duration
                )
        except Exception as e:
            return ActionResult(success=False, message=f"n8n API error: {str(e)}")


class N8NExecutionAction(BaseAction):
    """Manage n8n workflow executions.

    List recent executions, get execution details, or stop running executions.
    """
    action_type = "n8n_execution"
    display_name = "n8n执行管理"
    description = "n8n执行历史与管理"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage n8n executions.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: n8n server base URL
                - api_key: n8n API key
                - action: 'list', 'get', or 'stop'
                - execution_id: Execution ID (for get/stop)

        Returns:
            ActionResult with execution data.
        """
        base_url = params.get('base_url', '').rstrip('/')
        api_key = params.get('api_key', '')
        action = params.get('action', 'list')
        execution_id = params.get('execution_id', '')

        if not base_url or not api_key:
            return ActionResult(success=False, message="base_url and api_key are required")

        import urllib.request

        headers = {'X-N8N-API-KEY': api_key}
        start = time.time()

        try:
            if action == 'list':
                url = f"{base_url}/rest/executions?limit=20"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    duration = time.time() - start
                    return ActionResult(
                        success=True, message="Executions listed",
                        data=data, duration=duration
                    )
            elif action == 'get':
                if not execution_id:
                    return ActionResult(success=False, message="execution_id required for get action")
                url = f"{base_url}/rest/executions/{execution_id}"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    duration = time.time() - start
                    return ActionResult(success=True, message="Execution retrieved", data=data, duration=duration)
            elif action == 'stop':
                if not execution_id:
                    return ActionResult(success=False, message="execution_id required for stop action")
                url = f"{base_url}/rest/executions/{execution_id}/stop"
                req = urllib.request.Request(url, data=b'{}', headers={**headers, 'Content-Type': 'application/json'}, method='POST')
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                    duration = time.time() - start
                    return ActionResult(success=True, message="Execution stopped", data=data, duration=duration)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"n8n execution error: {str(e)}")
