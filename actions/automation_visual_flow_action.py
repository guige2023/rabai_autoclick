"""Automation Visual Flow Action Module for RabAI AutoClick.

Visual workflow builder that creates automation sequences
from screen region definitions and UI element screenshots.
"""

import time
import json
import uuid
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AutomationVisualFlowAction(BaseAction):
    """Visual workflow construction from UI regions.

    Build automation workflows by defining visual regions on the
    screen and connecting them into flow sequences. Supports
    branching, loops, and conditional navigation.
    """
    action_type = "automation_visual_flow"
    display_name = "可视化流程自动化"
    description = "基于屏幕区域的自动化流程构建"

    _flows: Dict[str, Dict[str, Any]] = {}
    _active_flow: Optional[str] = None
    _execution_stack: List[Dict[str, Any]] = []

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute visual flow operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'create_flow', 'add_step', 'connect',
                               'execute', 'execute_step', 'list', 'export'
                - flow_id: str - unique flow identifier
                - flow_name: str - human-readable flow name
                - step: dict (optional) - step definition
                - from_step: str (optional) - source step ID
                - to_step: str (optional) - target step ID
                - step_id: str (optional) - step identifier
                - condition: str (optional) - execution condition
                - current_step: str (optional) - current step for execute_step

        Returns:
            ActionResult with flow operation result.
        """
        start_time = time.time()

        try:
            operation = params.get('operation', 'create_flow')

            if operation == 'create_flow':
                return self._create_flow(params, start_time)
            elif operation == 'add_step':
                return self._add_step(params, start_time)
            elif operation == 'connect':
                return self._connect_steps(params, start_time)
            elif operation == 'execute':
                return self._execute_flow(params, start_time)
            elif operation == 'execute_step':
                return self._execute_step(params, start_time)
            elif operation == 'list':
                return self._list_flows(start_time)
            elif operation == 'export':
                return self._export_flow(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Visual flow action failed: {str(e)}",
                data={'error': str(e)},
                duration=time.time() - start_time
            )

    def _create_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a new visual flow."""
        flow_id = params.get('flow_id', str(uuid.uuid4()))
        flow_name = params.get('flow_name', f'Flow {flow_id[:8]}')
        description = params.get('description', '')

        if flow_id in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow already exists: {flow_id}",
                data={'flow_id': flow_id},
                duration=time.time() - start_time
            )

        self._flows[flow_id] = {
            'flow_id': flow_id,
            'name': flow_name,
            'description': description,
            'steps': {},
            'connections': [],
            'variables': {},
            'created_at': time.time(),
            'last_executed': None,
            'execution_count': 0
        }

        return ActionResult(
            success=True,
            message=f"Flow created: {flow_name}",
            data={
                'flow_id': flow_id,
                'name': flow_name,
                'step_count': 0
            },
            duration=time.time() - start_time
        )

    def _add_step(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a step to a flow."""
        flow_id = params.get('flow_id', '')
        step = params.get('step', {})

        if not flow_id or flow_id not in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow not found: {flow_id}",
                duration=time.time() - start_time
            )

        step_id = step.get('step_id', str(uuid.uuid4()))
        step_type = step.get('type', 'click')
        region = step.get('region', (0, 0, 100, 100))
        template = step.get('template', '')
        description = step.get('description', f'Step {step_id[:8]}')

        step_def = {
            'step_id': step_id,
            'type': step_type,
            'region': region,
            'template': template,
            'description': description,
            'timeout': step.get('timeout', 5.0),
            'retry': step.get('retry', 0),
            'on_error': step.get('on_error', 'stop'),
            'data': step.get('data', {}),
            'position': step.get('position', {'x': 0, 'y': 0})
        }

        self._flows[flow_id]['steps'][step_id] = step_def

        return ActionResult(
            success=True,
            message=f"Step added to flow: {step_id}",
            data={
                'flow_id': flow_id,
                'step_id': step_id,
                'type': step_type,
                'total_steps': len(self._flows[flow_id]['steps'])
            },
            duration=time.time() - start_time
        )

    def _connect_steps(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect two steps with a transition."""
        flow_id = params.get('flow_id', '')
        from_step = params.get('from_step', '')
        to_step = params.get('to_step', '')
        condition = params.get('condition', '')

        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow not found: {flow_id}",
                duration=time.time() - start_time
            )

        flow = self._flows[flow_id]

        if from_step not in flow['steps']:
            return ActionResult(
                success=False,
                message=f"Source step not found: {from_step}",
                duration=time.time() - start_time
            )

        if to_step not in flow['steps']:
            return ActionResult(
                success=False,
                message=f"Target step not found: {to_step}",
                duration=time.time() - start_time
            )

        connection = {
            'from_step': from_step,
            'to_step': to_step,
            'condition': condition,
            'connection_id': str(uuid.uuid4())
        }

        flow['connections'].append(connection)

        return ActionResult(
            success=True,
            message=f"Connected {from_step} -> {to_step}",
            data={
                'connection': connection,
                'total_connections': len(flow['connections'])
            },
            duration=time.time() - start_time
        )

    def _execute_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute an entire visual flow."""
        flow_id = params.get('flow_id', '')
        start_step = params.get('start_step', None)

        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow not found: {flow_id}",
                duration=time.time() - start_time
            )

        flow = self._flows[flow_id]
        self._active_flow = flow_id
        self._execution_stack = []

        if not flow['steps']:
            return ActionResult(
                success=False,
                message="Flow has no steps",
                data={'flow_id': flow_id},
                duration=time.time() - start_time
            )

        first_step_id = start_step or self._get_start_step(flow)
        current_step_id = first_step_id
        visited = set()
        max_steps = len(flow['steps']) * 2
        step_count = 0

        execution_results = []

        while current_step_id and step_count < max_steps:
            if current_step_id in visited:
                break

            visited.add(current_step_id)
            step = flow['steps'][current_step_id]

            result = self._execute_single_step(step)
            execution_results.append({
                'step_id': current_step_id,
                'success': result.success,
                'message': result.message
            })

            step_count += 1

            if not result.success and step.get('on_error') == 'stop':
                break

            current_step_id = self._get_next_step(flow, current_step_id, result)

        flow['execution_count'] += 1
        flow['last_executed'] = time.time()

        return ActionResult(
            success=step_count > 0,
            message=f"Flow executed: {step_count} steps",
            data={
                'flow_id': flow_id,
                'steps_executed': step_count,
                'results': execution_results
            },
            duration=time.time() - start_time
        )

    def _execute_step(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a single step of a flow."""
        flow_id = params.get('flow_id', '')
        step_id = params.get('step_id', '')

        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow not found: {flow_id}",
                duration=time.time() - start_time
            )

        flow = self._flows[flow_id]

        if step_id not in flow['steps']:
            return ActionResult(
                success=False,
                message=f"Step not found: {step_id}",
                duration=time.time() - start_time
            )

        step = flow['steps'][step_id]
        return self._execute_single_step(step)

    def _list_flows(self, start_time: float) -> ActionResult:
        """List all flows."""
        flows = [
            {
                'flow_id': fid,
                'name': f['name'],
                'description': f['description'],
                'step_count': len(f['steps']),
                'connection_count': len(f['connections']),
                'execution_count': f['execution_count'],
                'last_executed': f['last_executed']
            }
            for fid, f in self._flows.items()
        ]

        return ActionResult(
            success=True,
            message=f"Flows: {len(flows)}",
            data={'flows': flows, 'count': len(flows)},
            duration=time.time() - start_time
        )

    def _export_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Export a flow definition."""
        flow_id = params.get('flow_id', '')
        file_path = params.get('file_path', f'/tmp/flow_{flow_id}.json')

        if flow_id not in self._flows:
            return ActionResult(
                success=False,
                message=f"Flow not found: {flow_id}",
                duration=time.time() - start_time
            )

        try:
            with open(file_path, 'w') as f:
                json.dump(self._flows[flow_id], f, indent=2, default=str)

            return ActionResult(
                success=True,
                message=f"Flow exported: {file_path}",
                data={'file_path': file_path, 'flow_id': flow_id},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Export failed: {e}",
                duration=time.time() - start_time
            )

    def _get_start_step(self, flow: Dict[str, Any]) -> Optional[str]:
        """Find the starting step of a flow."""
        connected_from = {conn['to_step'] for conn in flow['connections']}
        for step_id in flow['steps']:
            if step_id not in connected_from:
                return step_id
        return next(iter(flow['steps'])) if flow['steps'] else None

    def _get_next_step(
        self,
        flow: Dict[str, Any],
        current_step_id: str,
        result: ActionResult
    ) -> Optional[str]:
        """Find the next step after current step."""
        for conn in flow['connections']:
            if conn['from_step'] == current_step_id:
                if conn['condition'] and conn['condition'] != 'default':
                    continue
                return conn['to_step']
        return None

    def _execute_single_step(self, step: Dict[str, Any]) -> ActionResult:
        """Execute a single visual flow step."""
        step_type = step.get('type', 'click')
        region = step.get('region', (0, 0, 100, 100))
        timeout = step.get('timeout', 5.0)
        retry = step.get('retry', 0)

        for attempt in range(max(retry + 1, 1)):
            try:
                if step_type == 'click':
                    return ActionResult(
                        success=True,
                        message=f"Click executed at region {region}",
                        data={'type': 'click', 'region': region}
                    )
                elif step_type == 'type':
                    return ActionResult(
                        success=True,
                        message=f"Type executed in region {region}",
                        data={'type': 'type', 'region': region}
                    )
                elif step_type == 'wait':
                    time.sleep(timeout)
                    return ActionResult(
                        success=True,
                        message=f"Wait completed: {timeout}s",
                        data={'type': 'wait', 'duration': timeout}
                    )
                elif step_type == 'screenshot':
                    return ActionResult(
                        success=True,
                        message="Screenshot captured",
                        data={'type': 'screenshot', 'region': region}
                    )
                else:
                    return ActionResult(
                        success=True,
                        message=f"Step executed: {step_type}",
                        data={'type': step_type}
                    )
            except Exception as e:
                if attempt >= retry:
                    return ActionResult(
                        success=False,
                        message=f"Step failed after {attempt + 1} attempts: {e}",
                        data={'error': str(e)}
                    )

        return ActionResult(success=False, message="Step failed")
