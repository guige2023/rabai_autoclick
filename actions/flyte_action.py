"""Flyte workflows integration for RabAI AutoClick.

Provides actions to launch, monitor, and manage Flyte workflow executions.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlyteLaunchAction(BaseAction):
    """Launch and manage Flyte workflow executions.

    Handles workflow launches and execution monitoring.
    """
    action_type = "flyte_launch"
    display_name = "Flyte工作流"
    description = "启动和管理Flyte工作流执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Launch or manage Flyte workflows.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Flyte admin URL
                - api_key: Auth API key
                - operation: launch | get | list | terminate
                - project: Project name
                - domain: Domain (e.g. development, production)
                - name: Workflow name
                - version: Workflow version
                - inputs: Dict of workflow inputs
                - execution_name: Execution name (for get/terminate)
                - limit: Max results for list

        Returns:
            ActionResult with execution data.
        """
        url = params.get('url') or os.environ.get('FLYTE_URL', 'http://localhost:8088')
        api_key = params.get('api_key') or os.environ.get('FLYTE_API_KEY', '')
        operation = params.get('operation', 'list')
        project = params.get('project', 'flytesnacks')
        domain = params.get('domain', 'development')

        import urllib.request
        import urllib.error

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}' if api_key else '',
        }

        try:
            if operation == 'launch':
                name = params.get('name')
                if not name:
                    return ActionResult(success=False, message="name is required")

                inputs = params.get('inputs', {})
                version = params.get('version', 'v1')

                # Construct the literal map inputs
                literal_inputs = {}
                for k, v in inputs.items():
                    literal_inputs[k] = self._to_literal(v)

                payload = {
                    'project': project,
                    'domain': domain,
                    'name': name,
                    'version': version,
                    'inputs': literal_inputs,
                }

                req = urllib.request.Request(
                    f'{url}/api/v1/executions',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                return ActionResult(
                    success=True,
                    message=f"Workflow {name} launched",
                    data={'execution_id': result.get('id', {}).get('name')}
                )

            elif operation == 'get':
                execution_name = params.get('execution_name')
                if not execution_name:
                    return ActionResult(success=False, message="execution_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/executions/{project}/{domain}/{execution_name}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                return ActionResult(
                    success=True,
                    message="Execution retrieved",
                    data={
                        'status': result.get('closure', {}).get('phase', 'UNKNOWN'),
                        'id': result.get('id', {}).get('name'),
                    }
                )

            elif operation == 'list':
                limit = params.get('limit', 20)
                req = urllib.request.Request(
                    f'{url}/api/v1/executions/{project}/{domain}?limit={limit}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))

                executions = result.get('executions', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(executions)} executions",
                    data={'executions': executions}
                )

            elif operation == 'terminate':
                execution_name = params.get('execution_name')
                if not execution_name:
                    return ActionResult(success=False, message="execution_name is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/executions/{project}/{domain}/{execution_name}/terminate',
                    data=json.dumps({'cause': params.get('cause', 'User requested')}).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Execution terminated", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Flyte API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Flyte error: {str(e)}")

    def _to_literal(self, value: Any) -> Dict[str, Any]:
        """Convert Python value to Flyte literal."""
        if isinstance(value, bool):
            return {'scalar': {'boolean': value}, 'type': {'simple': 'BOOLEAN'}}
        elif isinstance(value, int):
            return {'scalar': {'integer': value}, 'type': {'simple': 'INTEGER'}}
        elif isinstance(value, float):
            return {'scalar': {'float': value}, 'type': {'simple': 'FLOAT'}}
        elif isinstance(value, str):
            return {'scalar': {'string': value}, 'type': {'simple': 'STRING'}}
        elif isinstance(value, list):
            return {
                'collection': {'literals': [self._to_literal(v) for v in value]},
                'type': {'collectionType': {}}
            }
        elif isinstance(value, dict):
            return {
                'map': {'literals': {k: self._to_literal(v) for k, v in value.items()}},
                'type': {'mapValueType': {}}
            }
        else:
            return {'scalar': {'string': str(value)}, 'type': {'simple': 'STRING'}}


class FlyteNodeAction(BaseAction):
    """Monitor individual Flyte node executions.

    Provides node-level execution details and task logs.
    """
    action_type = "flyte_node"
    display_name = "Flyte节点"
    description = "监控Flyte节点执行和任务日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Monitor Flyte node executions.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Flyte admin URL
                - api_key: Auth API key
                - operation: get_node | get_task_logs
                - project: Project name
                - domain: Domain
                - execution_name: Execution name
                - node_id: Node ID (for get_node)

        Returns:
            ActionResult with node data.
        """
        url = params.get('url') or os.environ.get('FLYTE_URL', 'http://localhost:8088')
        api_key = params.get('api_key') or os.environ.get('FLYTE_API_KEY', '')
        project = params.get('project', 'flytesnacks')
        domain = params.get('domain', 'development')

        import urllib.request
        import urllib.error

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}' if api_key else '',
        }

        try:
            operation = params.get('operation', 'get_node')
            execution_name = params.get('execution_name')
            if not execution_name:
                return ActionResult(success=False, message="execution_name is required")

            if operation == 'get_node':
                node_id = params.get('node_id')
                if not node_id:
                    return ActionResult(success=False, message="node_id is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/executions/{project}/{domain}/{execution_name}/node/{node_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Node retrieved", data=result)

            elif operation == 'get_task_logs':
                node_id = params.get('node_id')
                if not node_id:
                    return ActionResult(success=False, message="node_id is required")

                req = urllib.request.Request(
                    f'{url}/api/v1/executions/{project}/{domain}/{execution_name}/node/{node_id}/taskLogs',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                logs = result.get('task_logs', [])
                return ActionResult(success=True, message=f"Found {len(logs)} task logs", data={'task_logs': logs})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Flyte API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Flyte error: {str(e)}")
